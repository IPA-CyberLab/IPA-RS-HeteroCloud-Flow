use std::{collections::BTreeSet, env, net::SocketAddr, str::FromStr, time::Duration};

use anyhow::{Context, Result, bail};
use flow_auth::{PrincipalAuthenticator, ProviderAuthenticator};
use flow_livekit::LiveKitClient;
use flow_rate_limit::{RateLimitPolicy, RedisBackend, TrustedProxies};
use flow_turn::TurnCredentialIssuer;
use url::Url;

use crate::coturn_metrics::{CoturnMetricsClient, LiveKitMetricsClient};

pub struct Config {
    pub bind_addr: SocketAddr,
    pub database_url: String,
    pub database_max_connections: u32,
    pub migrate_on_start: bool,
    pub principal_authenticator: PrincipalAuthenticator,
    pub provider_authenticator: ProviderAuthenticator,
    pub livekit: LiveKitClient,
    pub coturn_metrics: CoturnMetricsClient,
    pub livekit_metrics: LiveKitMetricsClient,
    pub api_urls: Vec<String>,
    pub livekit_ws_urls: Vec<String>,
    pub signaling_urls: Vec<String>,
    pub turn: TurnCredentialIssuer,
    pub participant_token_ttl: Duration,
    pub redis_backend: RedisBackend,
    pub rate_limit_policy: RateLimitPolicy,
    pub trusted_proxies: TrustedProxies,
}

impl Config {
    pub fn from_env() -> Result<Self> {
        let bind_addr = parse_or("FLOW_API_BIND_ADDR", "0.0.0.0:8080")?;
        let database_url = required("DATABASE_URL")?;
        let database_max_connections = parse_or("DATABASE_MAX_CONNECTIONS", "20")?;
        let migrate_on_start = parse_or("MIGRATE_ON_START", "true")?;
        let max_auth_ttl_seconds = parse_or("FLOW_PRINCIPAL_MAX_TTL_SECONDS", "300")?;
        let principal_authenticator = PrincipalAuthenticator::new(
            required("FLOW_PRINCIPAL_ISSUER")?,
            required("FLOW_PRINCIPAL_AUDIENCE")?,
            required("FLOW_PRINCIPAL_CONTEXT_HMAC_SECRET")?,
            Duration::from_secs(max_auth_ttl_seconds),
        )
        .context("invalid data-plane principal authentication configuration")?;
        let provider_authenticator = ProviderAuthenticator::from_public_keys_json(
            required("HETEROCLOUD_PROVIDER_ISSUER")?,
            required("HETEROCLOUD_PROVIDER_AUDIENCE")?,
            &required("HETEROCLOUD_PROVIDER_PUBLIC_KEYS_JSON")?,
        )
        .context("invalid HeteroCloud provider authentication configuration")?;
        let livekit = LiveKitClient::new(
            &required("LIVEKIT_URL")?,
            required("LIVEKIT_API_KEY")?,
            required("LIVEKIT_API_SECRET")?,
        )
        .context("invalid LiveKit configuration")?;
        let livekit_ws_urls = required_wss_url_list("LIVEKIT_WS_URLS")?;
        let signaling_urls = required_wss_url_list("FLOW_SIGNALING_URLS")?;
        let api_urls = match env::var("FLOW_API_URLS") {
            Ok(value) => parse_https_url_list(&value).context("FLOW_API_URLS is invalid")?,
            Err(env::VarError::NotPresent) => signaling_urls
                .iter()
                .map(|url| url.replacen("wss://", "https://", 1))
                .collect(),
            Err(error) => return Err(error).context("FLOW_API_URLS is invalid"),
        };
        let turn_urls = required("TURN_URLS")?
            .split(',')
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(ToOwned::to_owned)
            .collect();
        let turn_ttl_seconds: u64 = parse_or("TURN_CREDENTIAL_TTL_SECONDS", "300")?;
        let turn = TurnCredentialIssuer::new(
            turn_urls,
            required("TURN_SHARED_SECRET")?,
            Duration::from_secs(turn_ttl_seconds),
        )
        .context("invalid TURN configuration")?;
        let participant_token_ttl =
            Duration::from_secs(parse_or("LIVEKIT_TOKEN_TTL_SECONDS", "300")?);
        if participant_token_ttl.is_zero()
            || participant_token_ttl > Duration::from_secs(max_auth_ttl_seconds)
        {
            bail!("LIVEKIT_TOKEN_TTL_SECONDS must be positive and no longer than the auth TTL");
        }
        let coturn_metrics_urls = optional_url_list("COTURN_METRICS_URLS")?;
        let coturn_metrics = CoturnMetricsClient::new(coturn_metrics_urls)
            .context("COTURN_METRICS_URLS is invalid")?;
        let livekit_metrics_urls = optional_url_list("LIVEKIT_METRICS_URLS")?;
        let livekit_metrics = LiveKitMetricsClient::new(livekit_metrics_urls)
            .context("LIVEKIT_METRICS_URLS is invalid")?;
        let redis_backend = RedisBackend::from_env().context("invalid Redis configuration")?;
        let rate_limit_policy =
            RateLimitPolicy::from_env().context("invalid public rate-limit policy")?;
        let trusted_proxies =
            TrustedProxies::from_env().context("invalid trusted proxy configuration")?;

        Ok(Self {
            bind_addr,
            database_url,
            database_max_connections,
            migrate_on_start,
            principal_authenticator,
            provider_authenticator,
            livekit,
            coturn_metrics,
            livekit_metrics,
            api_urls,
            livekit_ws_urls,
            signaling_urls,
            turn,
            participant_token_ttl,
            redis_backend,
            rate_limit_policy,
            trusted_proxies,
        })
    }
}

fn optional_url_list(name: &'static str) -> Result<Vec<String>> {
    let value = match env::var(name) {
        Ok(value) => value,
        Err(env::VarError::NotPresent) => return Ok(Vec::new()),
        Err(error) => return Err(error).with_context(|| format!("{name} is invalid")),
    };
    if value.trim().is_empty() {
        return Ok(Vec::new());
    }
    let entries = value.split(',').map(str::trim).collect::<Vec<_>>();
    if entries.iter().any(|entry| entry.is_empty()) {
        bail!("{name} contains an empty URL");
    }
    Ok(entries.into_iter().map(ToOwned::to_owned).collect())
}

fn required(name: &'static str) -> Result<String> {
    env::var(name).with_context(|| format!("{name} is required"))
}

fn required_wss_url_list(name: &'static str) -> Result<Vec<String>> {
    parse_wss_url_list(&required(name)?).with_context(|| format!("{name} is invalid"))
}

fn parse_wss_url_list(value: &str) -> Result<Vec<String>> {
    parse_origin_url_list(value, "wss")
}

fn parse_https_url_list(value: &str) -> Result<Vec<String>> {
    parse_origin_url_list(value, "https")
}

fn parse_origin_url_list(value: &str, expected_scheme: &str) -> Result<Vec<String>> {
    let entries = value.split(',').map(str::trim).collect::<Vec<_>>();
    if entries.is_empty() || entries.len() > 16 || entries.iter().any(|entry| entry.is_empty()) {
        bail!("URL list must contain between 1 and 16 entries");
    }

    let mut seen = BTreeSet::new();
    let mut urls = Vec::with_capacity(entries.len());
    for entry in entries {
        let parsed = Url::parse(entry).context("URL cannot be parsed")?;
        if parsed.scheme() != expected_scheme
            || parsed.host().is_none()
            || !parsed.username().is_empty()
            || parsed.password().is_some()
            || parsed.query().is_some()
            || parsed.fragment().is_some()
            || !matches!(parsed.path(), "" | "/")
        {
            bail!("URL must use the required scheme and contain only an origin");
        }
        let normalized = parsed.as_str().trim_end_matches('/').to_owned();
        if !seen.insert(normalized.clone()) {
            bail!("URL list contains a duplicate");
        }
        urls.push(normalized);
    }
    Ok(urls)
}

fn parse_or<T>(name: &'static str, default: &'static str) -> Result<T>
where
    T: FromStr,
    T::Err: std::error::Error + Send + Sync + 'static,
{
    env::var(name)
        .unwrap_or_else(|_| default.to_owned())
        .parse()
        .with_context(|| format!("{name} is invalid"))
}

#[cfg(test)]
mod tests {
    use super::{parse_https_url_list, parse_wss_url_list};

    #[test]
    fn parses_ordered_secure_endpoint_list() {
        assert_eq!(
            parse_wss_url_list(
                "wss://flow-a.example.test/, wss://flow-b.example.test,wss://flow-c.example.test"
            )
            .unwrap(),
            [
                "wss://flow-a.example.test",
                "wss://flow-b.example.test",
                "wss://flow-c.example.test"
            ]
        );
    }

    #[test]
    fn rejects_insecure_or_duplicate_endpoints() {
        assert!(parse_wss_url_list("ws://flow-a.example.test").is_err());
        assert!(parse_wss_url_list("wss://flow-a.example.test,wss://flow-a.example.test").is_err());
    }

    #[test]
    fn parses_https_api_endpoint_list() {
        assert_eq!(
            parse_https_url_list("https://flow-a.example.test,https://flow-b.example.test/")
                .unwrap(),
            ["https://flow-a.example.test", "https://flow-b.example.test"]
        );
        assert!(parse_https_url_list("http://flow-a.example.test").is_err());
    }
}
