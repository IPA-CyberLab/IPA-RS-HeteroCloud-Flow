use std::{
    env,
    net::{IpAddr, SocketAddr},
    str::FromStr,
    sync::Arc,
    time::Duration,
};

use anyhow::{Context, Result, bail};
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use http::{HeaderMap, HeaderName};
use ipnet::IpNet;
use redis::{
    Client, IntoConnectionInfo, Script,
    aio::MultiplexedConnection,
    sentinel::{SentinelClientBuilder, SentinelServerType},
};
use sha2::{Digest, Sha256};
use tokio::sync::Mutex;
use tokio::time::sleep;
use uuid::Uuid;

const X_FORWARDED_FOR: HeaderName = HeaderName::from_static("x-forwarded-for");
const REDIS_RETRY_ATTEMPTS: usize = 3;
const REDIS_RETRY_BACKOFF: Duration = Duration::from_millis(100);

fn redis_retry_delay(attempt: usize) -> Duration {
    REDIS_RETRY_BACKOFF * u32::try_from(attempt + 1).expect("Redis retry attempt count fits in u32")
}

const TOKEN_BUCKET_SCRIPT: &str = r"
local rate = tonumber(ARGV[1])
local capacity = tonumber(ARGV[2])
local current_time = redis.call('TIME')
local now_ms = (tonumber(current_time[1]) * 1000) + math.floor(tonumber(current_time[2]) / 1000)
local state = redis.call('HMGET', KEYS[1], 'tokens', 'updated_at_ms')
local tokens = math.min(capacity, tonumber(state[1]) or capacity)
local updated_at_ms = tonumber(state[2]) or now_ms

if now_ms > updated_at_ms then
  tokens = math.min(capacity, tokens + ((now_ms - updated_at_ms) * rate / 1000))
end

local allowed = 0
local retry_after_ms = 0
if tokens >= 1 then
  tokens = tokens - 1
  allowed = 1
else
  retry_after_ms = math.ceil((1 - tokens) * 1000 / rate)
end

local reset_after_ms = math.ceil((capacity - tokens) * 1000 / rate)
redis.call('HSET', KEYS[1], 'tokens', tostring(tokens), 'updated_at_ms', now_ms)
redis.call('PEXPIRE', KEYS[1], math.max(math.ceil(capacity * 2000 / rate), 1000))

return {allowed, math.floor(tokens), retry_after_ms, reset_after_ms}
";

#[derive(Clone)]
pub struct RedisBackend {
    direct_url: Option<String>,
    sentinel_urls: Vec<String>,
    sentinel_master: String,
    redis_password: Option<String>,
    sentinel_password: Option<String>,
}

impl RedisBackend {
    /// Creates a direct Redis backend, primarily for local deployments and tests.
    ///
    /// # Errors
    ///
    /// Returns an error when the URL is empty or malformed.
    pub fn direct(url: impl Into<String>) -> Result<Self> {
        let url = url.into();
        if url.is_empty() {
            bail!("Redis URL is required");
        }
        url.as_str()
            .into_connection_info()
            .context("parse Redis URL")?;
        Ok(Self {
            direct_url: Some(url),
            sentinel_urls: Vec::new(),
            sentinel_master: "mymaster".into(),
            redis_password: None,
            sentinel_password: None,
        })
    }

    /// Loads a direct Redis or Redis Sentinel connection from the shared Flow environment.
    ///
    /// # Errors
    ///
    /// Returns an error when neither connection mode is configured.
    pub fn from_env() -> Result<Self> {
        let direct_url = nonempty_env("REDIS_URL");
        let sentinel_urls = env::var("REDIS_SENTINEL_URLS")
            .unwrap_or_default()
            .split(',')
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(ToOwned::to_owned)
            .collect::<Vec<_>>();
        if direct_url.is_none() && sentinel_urls.is_empty() {
            bail!("REDIS_URL or REDIS_SENTINEL_URLS is required");
        }
        Ok(Self {
            direct_url,
            sentinel_urls,
            sentinel_master: env::var("REDIS_SENTINEL_MASTER")
                .unwrap_or_else(|_| "mymaster".into()),
            redis_password: nonempty_env("REDIS_PASSWORD"),
            sentinel_password: nonempty_env("REDIS_SENTINEL_PASSWORD"),
        })
    }

    /// Resolves the current writable Redis server.
    ///
    /// # Errors
    ///
    /// Returns an error for malformed URLs or an unavailable Sentinel quorum.
    pub async fn client(&self) -> Result<Client> {
        if let Some(url) = &self.direct_url {
            let mut information = url
                .as_str()
                .into_connection_info()
                .context("parse REDIS_URL")?;
            if let Some(password) = &self.redis_password {
                let settings = information.redis_settings().clone().set_password(password);
                information = information.set_redis_settings(settings);
            }
            return Client::open(information).context("open REDIS_URL");
        }
        let addresses = self
            .sentinel_urls
            .iter()
            .map(|url| {
                url.as_str()
                    .into_connection_info()
                    .map(|information| information.addr().clone())
            })
            .collect::<redis::RedisResult<Vec<_>>>()
            .context("parse Redis Sentinel URLs")?;
        for attempt in 0..REDIS_RETRY_ATTEMPTS {
            let mut builder = SentinelClientBuilder::new(
                addresses.clone(),
                &self.sentinel_master,
                SentinelServerType::Master,
            )
            .context("configure Redis Sentinel")?;
            if let Some(password) = &self.redis_password {
                builder = builder.set_client_to_redis_password(password);
            }
            if let Some(password) = &self.sentinel_password {
                builder = builder.set_client_to_sentinel_password(password);
            }
            let mut sentinel = builder.build().context("build Redis Sentinel client")?;
            match sentinel.async_get_client().await {
                Ok(client) => return Ok(client),
                Err(error) if attempt + 1 < REDIS_RETRY_ATTEMPTS => {
                    tracing::warn!(
                        attempt = attempt + 1,
                        %error,
                        "Redis Sentinel master resolution failed; retrying"
                    );
                    sleep(redis_retry_delay(attempt)).await;
                }
                Err(error) => return Err(error).context("resolve Redis master"),
            }
        }
        unreachable!("Redis Sentinel resolution always returns from the retry loop")
    }

    /// Checks that the current Redis primary is reachable.
    ///
    /// # Errors
    ///
    /// Returns an error when Redis cannot be resolved, connected, or pinged.
    pub async fn ping(&self) -> Result<()> {
        let client = self.client().await?;
        let mut connection = client
            .get_multiplexed_async_connection()
            .await
            .context("connect to Redis")?;
        let response: String = redis::cmd("PING")
            .query_async(&mut connection)
            .await
            .context("ping Redis")?;
        if response != "PONG" {
            bail!("unexpected Redis PING response");
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RateLimitPolicy {
    requests_per_second: u32,
    burst: u32,
}

impl RateLimitPolicy {
    /// Creates and validates a public request policy.
    ///
    /// # Errors
    ///
    /// Returns an error when either value is outside the supported range.
    pub fn new(requests_per_second: u32, burst: u32) -> Result<Self> {
        let policy = Self {
            requests_per_second,
            burst,
        };
        policy.validate()?;
        Ok(policy)
    }

    /// Loads the deployment source-IP ceiling. Defaults to 1000 requests per
    /// second with a burst of 5000; lower service policies are supplied per call.
    ///
    /// # Errors
    ///
    /// Returns an error when either value is outside the supported range.
    pub fn from_env() -> Result<Self> {
        Self::new(
            parse_env_or("FLOW_PUBLIC_RATE_LIMIT_RPS", 1_000)?,
            parse_env_or("FLOW_PUBLIC_RATE_LIMIT_BURST", 5_000)?,
        )
    }

    fn validate(self) -> Result<()> {
        if !(1..=10_000).contains(&self.requests_per_second) {
            bail!("FLOW_PUBLIC_RATE_LIMIT_RPS must be between 1 and 10000");
        }
        if !(1..=1_000_000).contains(&self.burst) {
            bail!("FLOW_PUBLIC_RATE_LIMIT_BURST must be between 1 and 1000000");
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RateLimitDecision {
    pub allowed: bool,
    pub limit: u32,
    pub remaining: u32,
    pub retry_after_seconds: u64,
    pub reset_after_seconds: u64,
}

#[derive(Clone)]
pub struct IpRateLimiter {
    backend: RedisBackend,
    policy: RateLimitPolicy,
    connection: Arc<Mutex<Option<MultiplexedConnection>>>,
}

impl IpRateLimiter {
    /// Creates a distributed source-IP token bucket backed by the current Redis primary.
    #[must_use]
    pub fn new(backend: RedisBackend, policy: RateLimitPolicy) -> Self {
        Self {
            backend,
            policy,
            connection: Arc::new(Mutex::new(None)),
        }
    }

    /// Consumes one token for a source IP.
    ///
    /// # Errors
    ///
    /// Returns an error when the Redis primary is unavailable. The cached
    /// connection is discarded and Sentinel is consulted once before failing.
    pub async fn check(&self, address: IpAddr) -> Result<RateLimitDecision> {
        self.check_key(system_rate_limit_key(address), self.policy)
            .await
    }

    /// Consumes one token from a service-specific source-IP bucket.
    ///
    /// # Errors
    ///
    /// Returns an error when the Redis primary is unavailable. The cached
    /// connection is discarded and Sentinel is consulted once before failing.
    pub async fn check_service(
        &self,
        service_instance_id: Uuid,
        address: IpAddr,
        policy: RateLimitPolicy,
    ) -> Result<RateLimitDecision> {
        self.check_key(service_rate_limit_key(service_instance_id, address), policy)
            .await
    }

    async fn check_key(&self, key: String, policy: RateLimitPolicy) -> Result<RateLimitDecision> {
        for attempt in 0..REDIS_RETRY_ATTEMPTS {
            let mut connection = match self.connection().await {
                Ok(connection) => connection,
                Err(error) if attempt + 1 < REDIS_RETRY_ATTEMPTS => {
                    tracing::warn!(
                        attempt = attempt + 1,
                        %error,
                        "rate-limit Redis connection failed; retrying"
                    );
                    sleep(redis_retry_delay(attempt)).await;
                    continue;
                }
                Err(error) => return Err(error).context("connect to rate-limit Redis primary"),
            };
            let result = Script::new(TOKEN_BUCKET_SCRIPT)
                .key(&key)
                .arg(policy.requests_per_second)
                .arg(policy.burst)
                .invoke_async::<(i64, i64, i64, i64)>(&mut connection)
                .await;
            match result {
                Ok((allowed, remaining, retry_after_ms, reset_after_ms)) => {
                    return Ok(RateLimitDecision {
                        allowed: allowed == 1,
                        limit: policy.burst,
                        remaining: nonnegative_u32(remaining),
                        retry_after_seconds: milliseconds_to_seconds(retry_after_ms),
                        reset_after_seconds: milliseconds_to_seconds(reset_after_ms),
                    });
                }
                Err(_) if attempt + 1 < REDIS_RETRY_ATTEMPTS => {
                    self.invalidate_connection().await;
                    sleep(redis_retry_delay(attempt)).await;
                }
                Err(error) => return Err(error).context("apply Redis IP rate limit"),
            }
        }
        unreachable!("rate limiter retries always return from the retry loop")
    }

    /// Checks the cached limiter connection, resolving the current primary when needed.
    ///
    /// # Errors
    ///
    /// Returns an error when Redis is unavailable.
    pub async fn ping(&self) -> Result<()> {
        for attempt in 0..REDIS_RETRY_ATTEMPTS {
            let mut connection = match self.connection().await {
                Ok(connection) => connection,
                Err(_error) if attempt + 1 < REDIS_RETRY_ATTEMPTS => {
                    sleep(redis_retry_delay(attempt)).await;
                    continue;
                }
                Err(error) => return Err(error).context("connect to rate-limit Redis primary"),
            };
            let result = redis::cmd("PING")
                .query_async::<String>(&mut connection)
                .await;
            match result {
                Ok(response) if response == "PONG" => return Ok(()),
                Ok(_) => bail!("unexpected Redis PING response"),
                Err(_) if attempt + 1 < REDIS_RETRY_ATTEMPTS => {
                    self.invalidate_connection().await;
                    sleep(redis_retry_delay(attempt)).await;
                }
                Err(error) => return Err(error).context("ping rate-limit Redis connection"),
            }
        }
        unreachable!("rate-limit Redis ping retries always return from the retry loop")
    }

    async fn connection(&self) -> Result<MultiplexedConnection> {
        let mut cached = self.connection.lock().await;
        if let Some(connection) = cached.as_ref() {
            return Ok(connection.clone());
        }
        let client = self.backend.client().await?;
        let connection = client
            .get_multiplexed_async_connection()
            .await
            .context("connect to rate-limit Redis primary")?;
        *cached = Some(connection.clone());
        Ok(connection)
    }

    async fn invalidate_connection(&self) {
        *self.connection.lock().await = None;
    }
}

#[derive(Clone)]
pub struct TrustedProxies(Arc<Vec<IpNet>>);

impl TrustedProxies {
    /// Parses a comma-separated list of trusted reverse-proxy CIDRs.
    ///
    /// # Errors
    ///
    /// Returns an error for an empty list or malformed network.
    pub fn from_csv(value: &str) -> Result<Self> {
        let networks = value
            .split(',')
            .map(str::trim)
            .filter(|entry| !entry.is_empty())
            .map(|entry| {
                IpNet::from_str(entry).with_context(|| format!("invalid proxy CIDR {entry}"))
            })
            .collect::<Result<Vec<_>>>()?;
        if networks.is_empty() {
            bail!("at least one trusted proxy CIDR is required");
        }
        Ok(Self(Arc::new(networks)))
    }

    /// Loads trusted proxy CIDRs. Only loopback is trusted by default.
    ///
    /// # Errors
    ///
    /// Returns an error when the configured list is invalid.
    pub fn from_env() -> Result<Self> {
        Self::from_csv(
            &env::var("FLOW_TRUSTED_PROXY_CIDRS").unwrap_or_else(|_| "127.0.0.0/8,::1/128".into()),
        )
    }

    /// Resolves the public client address without trusting caller-supplied
    /// forwarding headers from an untrusted peer.
    #[must_use]
    pub fn client_ip(&self, peer: SocketAddr, headers: &HeaderMap) -> IpAddr {
        let peer_ip = normalize_ip(peer.ip());
        if !self.0.iter().any(|network| network.contains(&peer_ip)) {
            return peer_ip;
        }
        let Some(raw) = headers.get_all(&X_FORWARDED_FOR).iter().next_back() else {
            return peer_ip;
        };
        let Ok(raw) = raw.to_str() else {
            return peer_ip;
        };
        raw.rsplit(',')
            .next()
            .and_then(parse_forwarded_ip)
            .map_or(peer_ip, normalize_ip)
    }
}

fn parse_forwarded_ip(value: &str) -> Option<IpAddr> {
    let value = value.trim();
    value
        .parse()
        .ok()
        .or_else(|| value.strip_prefix('[')?.strip_suffix(']')?.parse().ok())
}

fn normalize_ip(address: IpAddr) -> IpAddr {
    match address {
        IpAddr::V6(address) => address
            .to_ipv4_mapped()
            .map_or(IpAddr::V6(address), IpAddr::V4),
        address @ IpAddr::V4(_) => address,
    }
}

fn system_rate_limit_key(address: IpAddr) -> String {
    format!("flow:rate-limit:v2:system:{}", ip_key(address))
}

fn service_rate_limit_key(service_instance_id: Uuid, address: IpAddr) -> String {
    format!(
        "flow:rate-limit:v2:service:{service_instance_id}:{}",
        ip_key(address)
    )
}

fn ip_key(address: IpAddr) -> String {
    let digest = Sha256::digest(normalize_ip(address).to_string().as_bytes());
    URL_SAFE_NO_PAD.encode(digest)
}

fn nonnegative_u32(value: i64) -> u32 {
    if value <= 0 {
        0
    } else if value > i64::from(u32::MAX) {
        u32::MAX
    } else {
        u32::try_from(value).unwrap_or(u32::MAX)
    }
}

fn milliseconds_to_seconds(value: i64) -> u64 {
    u64::try_from(value.max(0))
        .unwrap_or(u64::MAX)
        .saturating_add(999)
        / 1000
}

fn nonempty_env(name: &str) -> Option<String> {
    env::var(name).ok().filter(|value| !value.is_empty())
}

fn parse_env_or<T>(name: &'static str, default: T) -> Result<T>
where
    T: FromStr,
    T::Err: std::error::Error + Send + Sync + 'static,
{
    match env::var(name) {
        Ok(value) => value.parse().with_context(|| format!("{name} is invalid")),
        Err(env::VarError::NotPresent) => Ok(default),
        Err(error) => Err(error).with_context(|| format!("{name} is invalid")),
    }
}

#[cfg(test)]
mod tests {
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};

    use http::{HeaderMap, HeaderValue};

    use super::{
        TrustedProxies, milliseconds_to_seconds, service_rate_limit_key, system_rate_limit_key,
    };

    #[test]
    fn untrusted_peer_cannot_spoof_forwarded_address() {
        let proxies = TrustedProxies::from_csv("10.250.0.4/32").unwrap();
        let mut headers = HeaderMap::new();
        headers.insert("x-forwarded-for", HeaderValue::from_static("203.0.113.10"));
        let peer = SocketAddr::from(([198, 51, 100, 7], 443));
        assert_eq!(proxies.client_ip(peer, &headers), peer.ip());
    }

    #[test]
    fn trusted_proxy_uses_rightmost_forwarded_address() {
        let proxies = TrustedProxies::from_csv("10.250.0.0/24").unwrap();
        let mut headers = HeaderMap::new();
        headers.insert(
            "x-forwarded-for",
            HeaderValue::from_static("192.0.2.1, 198.51.100.9"),
        );
        let peer = SocketAddr::from(([10, 250, 0, 4], 51515));
        assert_eq!(
            proxies.client_ip(peer, &headers),
            IpAddr::V4(Ipv4Addr::new(198, 51, 100, 9))
        );
    }

    #[test]
    fn malformed_forwarded_address_falls_back_to_proxy_peer() {
        let proxies = TrustedProxies::from_csv("10.250.0.0/24").unwrap();
        let mut headers = HeaderMap::new();
        headers.insert(
            "x-forwarded-for",
            HeaderValue::from_static("198.51.100.9, invalid"),
        );
        let peer = SocketAddr::from(([10, 250, 0, 4], 51515));
        assert_eq!(proxies.client_ip(peer, &headers), peer.ip());
    }

    #[test]
    fn mapped_ipv4_addresses_share_one_private_key() {
        let ipv4 = IpAddr::V4(Ipv4Addr::new(203, 0, 113, 8));
        let mapped = IpAddr::V6(Ipv4Addr::new(203, 0, 113, 8).to_ipv6_mapped());
        assert_eq!(system_rate_limit_key(ipv4), system_rate_limit_key(mapped));
        assert!(!system_rate_limit_key(ipv4).contains("203.0.113.8"));
    }

    #[test]
    fn service_buckets_are_isolated_from_each_other_and_the_system_bucket() {
        let address = IpAddr::V4(Ipv4Addr::new(203, 0, 113, 8));
        let first = uuid::Uuid::new_v4();
        let second = uuid::Uuid::new_v4();
        assert_ne!(
            service_rate_limit_key(first, address),
            service_rate_limit_key(second, address)
        );
        assert_ne!(
            service_rate_limit_key(first, address),
            system_rate_limit_key(address)
        );
    }

    #[test]
    fn rounds_retry_duration_up_to_seconds() {
        assert_eq!(milliseconds_to_seconds(0), 0);
        assert_eq!(milliseconds_to_seconds(1), 1);
        assert_eq!(milliseconds_to_seconds(1000), 1);
        assert_eq!(milliseconds_to_seconds(1001), 2);
    }
}
