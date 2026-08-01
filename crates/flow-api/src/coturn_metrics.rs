use std::{
    collections::{BTreeMap, BTreeSet},
    io::{Read, Write},
    net::{TcpStream, ToSocketAddrs},
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::{Context, Result, anyhow, bail};
use url::{Host, Url};
use uuid::Uuid;

const MAX_METRICS_BODY_BYTES: u64 = 16 * 1024 * 1024;
const SCRAPE_TIMEOUT: Duration = Duration::from_secs(2);
const COTURN_RELEVANT_METRICS: [&str; 3] = [
    "turn_traffic_rcvb",
    "turn_traffic_sentb",
    "turn_total_allocations",
];
const LIVEKIT_RELEVANT_METRICS: [&str; 1] = ["livekit_service_packet_bytes"];

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CoturnMetrics {
    pub ingress_bytes: u64,
    pub egress_bytes: u64,
    pub allocations: Option<u64>,
}

impl CoturnMetrics {
    fn checked_add(self, other: Self) -> Result<Self> {
        Ok(Self {
            ingress_bytes: self
                .ingress_bytes
                .checked_add(other.ingress_bytes)
                .context("coturn ingress byte count overflowed")?,
            egress_bytes: self
                .egress_bytes
                .checked_add(other.egress_bytes)
                .context("coturn egress byte count overflowed")?,
            allocations: match (self.allocations, other.allocations) {
                (None, None) => None,
                (left, right) => Some(
                    left.unwrap_or(0)
                        .checked_add(right.unwrap_or(0))
                        .context("coturn allocation count overflowed")?,
                ),
            },
        })
    }
}

#[derive(Clone, Default)]
pub struct CoturnMetricsClient {
    endpoints: Arc<Vec<MetricsEndpoint>>,
}

impl CoturnMetricsClient {
    pub fn new(urls: Vec<String>) -> Result<Self> {
        Ok(Self {
            endpoints: parse_endpoints(urls, "COTURN_METRICS_URLS")?,
        })
    }

    pub async fn scrape(&self, service_instance_id: Uuid) -> Result<Option<CoturnMetrics>> {
        if self.endpoints.is_empty() {
            return Ok(None);
        }
        let mut tasks = tokio::task::JoinSet::new();
        for endpoint in self.endpoints.iter() {
            let endpoint = endpoint.clone();
            tasks.spawn_blocking(move || {
                let body = endpoint.fetch()?;
                parse_coturn_prometheus_metrics(&body, service_instance_id)
            });
        }

        let mut total = CoturnMetrics::default();
        while let Some(result) = tasks.join_next().await {
            let metrics =
                result.map_err(|error| anyhow!("coturn metrics task failed: {error}"))??;
            total = total.checked_add(metrics)?;
        }
        Ok(Some(total))
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct LiveKitMetrics {
    pub ingress_bytes: u64,
    pub egress_bytes: u64,
}

impl LiveKitMetrics {
    fn checked_add(self, other: Self) -> Result<Self> {
        Ok(Self {
            ingress_bytes: self
                .ingress_bytes
                .checked_add(other.ingress_bytes)
                .context("LiveKit ingress byte count overflowed")?,
            egress_bytes: self
                .egress_bytes
                .checked_add(other.egress_bytes)
                .context("LiveKit egress byte count overflowed")?,
        })
    }
}

#[derive(Clone, Default)]
pub struct LiveKitMetricsClient {
    endpoints: Arc<Vec<MetricsEndpoint>>,
}

impl LiveKitMetricsClient {
    pub fn new(urls: Vec<String>) -> Result<Self> {
        Ok(Self {
            endpoints: parse_endpoints(urls, "LIVEKIT_METRICS_URLS")?,
        })
    }

    pub async fn scrape(&self, service_instance_id: Uuid) -> Result<Option<LiveKitMetrics>> {
        if self.endpoints.is_empty() {
            return Ok(None);
        }
        let mut tasks = tokio::task::JoinSet::new();
        for endpoint in self.endpoints.iter() {
            let endpoint = endpoint.clone();
            tasks.spawn_blocking(move || {
                let body = endpoint.fetch()?;
                parse_livekit_prometheus_metrics(&body, service_instance_id)
            });
        }

        let mut total = LiveKitMetrics::default();
        while let Some(result) = tasks.join_next().await {
            let metrics =
                result.map_err(|error| anyhow!("LiveKit metrics task failed: {error}"))??;
            total = total.checked_add(metrics)?;
        }
        Ok(Some(total))
    }
}

fn parse_endpoints(urls: Vec<String>, variable_name: &str) -> Result<Arc<Vec<MetricsEndpoint>>> {
    let mut seen = BTreeSet::new();
    let mut endpoints = Vec::with_capacity(urls.len());
    for url in urls {
        let endpoint = MetricsEndpoint::parse(&url)?;
        if !seen.insert((endpoint.host.clone(), endpoint.port, endpoint.path.clone())) {
            bail!("{variable_name} contains a duplicate endpoint");
        }
        endpoints.push(endpoint);
    }
    if endpoints.len() > 16 {
        bail!("{variable_name} must contain at most 16 URLs");
    }
    Ok(Arc::new(endpoints))
}

#[derive(Clone)]
struct MetricsEndpoint {
    host: String,
    port: u16,
    host_header: String,
    path: String,
}

impl MetricsEndpoint {
    fn parse(value: &str) -> Result<Self> {
        let url = Url::parse(value).context("metrics URL cannot be parsed")?;
        if url.scheme() != "http"
            || !url.username().is_empty()
            || url.password().is_some()
            || url.query().is_some()
            || url.fragment().is_some()
        {
            bail!("metrics URL must be an http URL without credentials or query data");
        }
        let host = url
            .host_str()
            .filter(|host| !host.is_empty())
            .context("metrics URL host is required")?
            .to_owned();
        let port = url
            .port_or_known_default()
            .context("metrics URL port is required")?;
        let rendered_host = match url.host().context("metrics URL host is required")? {
            Host::Ipv6(address) => format!("[{address}]"),
            other => other.to_string(),
        };
        let host_header = if url.port().is_some() {
            format!("{rendered_host}:{port}")
        } else {
            rendered_host
        };
        let path = match url.path() {
            "" | "/" => "/metrics".to_owned(),
            path => path.to_owned(),
        };
        Ok(Self {
            host,
            port,
            host_header,
            path,
        })
    }

    fn fetch(&self) -> Result<String> {
        let addresses = (self.host.as_str(), self.port)
            .to_socket_addrs()
            .with_context(|| format!("resolve metrics at {}", self.host_header))?;
        let deadline = Instant::now() + SCRAPE_TIMEOUT;
        let mut last_error = None;
        let mut stream = None;
        for address in addresses.take(8) {
            let Some(remaining) = deadline.checked_duration_since(Instant::now()) else {
                break;
            };
            match TcpStream::connect_timeout(&address, remaining) {
                Ok(connected) => {
                    stream = Some(connected);
                    break;
                }
                Err(error) => last_error = Some(error),
            }
        }
        let mut stream = stream.ok_or_else(|| {
            last_error.map_or_else(
                || anyhow!("metrics endpoint resolved no addresses"),
                |error| anyhow!("connect to metrics at {}: {error}", self.host_header),
            )
        })?;
        stream.set_read_timeout(Some(SCRAPE_TIMEOUT))?;
        stream.set_write_timeout(Some(SCRAPE_TIMEOUT))?;
        write!(
            stream,
            "GET {} HTTP/1.1\r\nHost: {}\r\nAccept: text/plain\r\nAccept-Encoding: identity\r\nConnection: close\r\n\r\n",
            self.path, self.host_header
        )?;
        stream.flush()?;

        let mut response = Vec::new();
        stream
            .take(MAX_METRICS_BODY_BYTES + 1)
            .read_to_end(&mut response)?;
        if u64::try_from(response.len()).unwrap_or(u64::MAX) > MAX_METRICS_BODY_BYTES {
            bail!("metrics response exceeded the size limit");
        }
        Ok(parse_http_response(&response)?.to_owned())
    }
}

fn parse_http_response(response: &[u8]) -> Result<&str> {
    let header_end = response
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .context("metrics HTTP headers are incomplete")?;
    let headers = std::str::from_utf8(&response[..header_end])
        .context("metrics HTTP headers are not UTF-8")?;
    let mut lines = headers.split("\r\n");
    let status = lines.next().context("metrics HTTP status is missing")?;
    let mut status_parts = status.split_ascii_whitespace();
    let version = status_parts.next().unwrap_or_default();
    let code = status_parts.next().unwrap_or_default();
    if !matches!(version, "HTTP/1.0" | "HTTP/1.1") || code != "200" {
        bail!("metrics endpoint returned non-200 HTTP status");
    }

    let mut content_length = None;
    for line in lines {
        let (name, value) = line
            .split_once(':')
            .context("metrics HTTP header is malformed")?;
        let name = name.trim();
        let value = value.trim();
        if name.eq_ignore_ascii_case("transfer-encoding") && !value.eq_ignore_ascii_case("identity")
        {
            bail!("metrics chunked transfer encoding is unsupported");
        }
        if name.eq_ignore_ascii_case("content-encoding") && !value.eq_ignore_ascii_case("identity")
        {
            bail!("metrics content encoding is unsupported");
        }
        if name.eq_ignore_ascii_case("content-length") {
            let parsed = value
                .parse::<usize>()
                .context("metrics Content-Length is invalid")?;
            if content_length.replace(parsed).is_some() {
                bail!("metrics response has duplicate Content-Length headers");
            }
        }
    }
    let body = &response[header_end + 4..];
    if content_length.is_some_and(|expected| expected != body.len()) {
        bail!("metrics response body length does not match Content-Length");
    }
    std::str::from_utf8(body).context("metrics body is not UTF-8")
}

fn parse_coturn_prometheus_metrics(body: &str, service_instance_id: Uuid) -> Result<CoturnMetrics> {
    let service_id = service_instance_id.to_string();
    let mut metrics = CoturnMetrics::default();
    let mut scoped_allocations_seen = false;

    for (index, raw_line) in body.lines().enumerate() {
        let line = raw_line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let Some(sample) = parse_relevant_sample(line, &COTURN_RELEVANT_METRICS)
            .with_context(|| format!("invalid Prometheus sample on line {}", index + 1))?
        else {
            continue;
        };
        let Some(user) = sample.labels.get("user") else {
            continue;
        };
        let identity = user.split(':').collect::<Vec<_>>();
        if identity.len() != 5
            || identity[0].parse::<u64>().is_err()
            || identity[1].parse::<Uuid>().is_err()
            || identity[2].parse::<Uuid>().is_err()
            || identity[3].parse::<Uuid>().is_err()
            || identity[4].parse::<Uuid>().is_err()
            || identity[3] != service_id
        {
            continue;
        }
        match sample.name {
            "turn_traffic_rcvb" => {
                metrics.ingress_bytes = metrics
                    .ingress_bytes
                    .checked_add(sample.value)
                    .context("coturn ingress byte count overflowed")?;
            }
            "turn_traffic_sentb" => {
                metrics.egress_bytes = metrics
                    .egress_bytes
                    .checked_add(sample.value)
                    .context("coturn egress byte count overflowed")?;
            }
            "turn_total_allocations" => {
                scoped_allocations_seen = true;
                metrics.allocations = Some(
                    metrics
                        .allocations
                        .unwrap_or(0)
                        .checked_add(sample.value)
                        .context("coturn allocation count overflowed")?,
                );
            }
            _ => unreachable!("relevant metric names are exhaustive"),
        }
    }
    if !scoped_allocations_seen {
        metrics.allocations = None;
    }
    Ok(metrics)
}

fn parse_livekit_prometheus_metrics(
    body: &str,
    service_instance_id: Uuid,
) -> Result<LiveKitMetrics> {
    let service_id = service_instance_id.to_string();
    let mut metrics = LiveKitMetrics::default();

    for (index, raw_line) in body.lines().enumerate() {
        let line = raw_line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let Some(sample) = parse_relevant_sample(line, &LIVEKIT_RELEVANT_METRICS)
            .with_context(|| format!("invalid Prometheus sample on line {}", index + 1))?
        else {
            continue;
        };
        if sample.labels.get("service_id") != Some(&service_id) {
            continue;
        }
        if !matches!(
            sample.labels.get("transmission").map(String::as_str),
            Some("initial" | "retransmit")
        ) {
            bail!("LiveKit service byte sample has an invalid transmission label");
        }
        match sample.labels.get("direction").map(String::as_str) {
            Some("incoming") => {
                metrics.ingress_bytes = metrics
                    .ingress_bytes
                    .checked_add(sample.value)
                    .context("LiveKit ingress byte count overflowed")?;
            }
            Some("outgoing") => {
                metrics.egress_bytes = metrics
                    .egress_bytes
                    .checked_add(sample.value)
                    .context("LiveKit egress byte count overflowed")?;
            }
            _ => bail!("LiveKit service byte sample has an invalid direction label"),
        }
    }
    Ok(metrics)
}

struct Sample<'a> {
    name: &'a str,
    labels: BTreeMap<String, String>,
    value: u64,
}

fn parse_relevant_sample<'a>(
    line: &'a str,
    relevant_metrics: &[&str],
) -> Result<Option<Sample<'a>>> {
    let name_end = line
        .find(|character: char| character == '{' || character.is_ascii_whitespace())
        .unwrap_or(line.len());
    let name = &line[..name_end];
    if !relevant_metrics.contains(&name) {
        return Ok(None);
    }
    let mut remainder = &line[name_end..];
    let labels = if remainder.starts_with('{') {
        let close = find_label_set_end(remainder)?;
        let labels = parse_labels(&remainder[1..close])?;
        remainder = &remainder[close + 1..];
        labels
    } else {
        BTreeMap::new()
    };
    if remainder.is_empty() || !remainder.chars().next().is_some_and(char::is_whitespace) {
        bail!("metric value must be separated by whitespace");
    }
    let tokens = remainder.split_ascii_whitespace().collect::<Vec<_>>();
    if !(1..=2).contains(&tokens.len()) {
        bail!("metric sample must contain a value and optional timestamp");
    }
    let value = parse_nonnegative_integer(tokens[0])?;
    if tokens.len() == 2 && tokens[1].parse::<i64>().is_err() {
        bail!("metric timestamp is invalid");
    }
    Ok(Some(Sample {
        name,
        labels,
        value,
    }))
}

fn find_label_set_end(value: &str) -> Result<usize> {
    let mut quoted = false;
    let mut escaped = false;
    for (index, character) in value.char_indices().skip(1) {
        if escaped {
            escaped = false;
        } else if quoted && character == '\\' {
            escaped = true;
        } else if character == '"' {
            quoted = !quoted;
        } else if !quoted && character == '}' {
            return Ok(index);
        }
    }
    bail!("metric label set is unterminated")
}

fn parse_labels(mut value: &str) -> Result<BTreeMap<String, String>> {
    let mut labels = BTreeMap::new();
    while !value.is_empty() {
        let equals = value.find('=').context("metric label is missing equals")?;
        let name = &value[..equals];
        if !valid_label_name(name) {
            bail!("metric label name is invalid");
        }
        value = &value[equals + 1..];
        if !value.starts_with('"') {
            bail!("metric label value must be quoted");
        }
        let (decoded, consumed) = parse_quoted_label(value)?;
        if labels.insert(name.to_owned(), decoded).is_some() {
            bail!("metric label is duplicated");
        }
        value = &value[consumed..];
        if value.is_empty() {
            break;
        }
        value = value
            .strip_prefix(',')
            .context("metric labels must be comma separated")?;
        if value.is_empty() {
            bail!("metric label set has a trailing comma");
        }
    }
    Ok(labels)
}

fn parse_quoted_label(value: &str) -> Result<(String, usize)> {
    let mut decoded = String::new();
    let mut escaped = false;
    for (index, character) in value.char_indices().skip(1) {
        if escaped {
            decoded.push(match character {
                '\\' => '\\',
                '"' => '"',
                'n' => '\n',
                _ => bail!("metric label has an unsupported escape"),
            });
            escaped = false;
        } else if character == '\\' {
            escaped = true;
        } else if character == '"' {
            return Ok((decoded, index + character.len_utf8()));
        } else {
            decoded.push(character);
        }
    }
    bail!("metric label value is unterminated")
}

fn valid_label_name(value: &str) -> bool {
    let mut characters = value.chars();
    characters
        .next()
        .is_some_and(|character| character == '_' || character.is_ascii_alphabetic())
        && characters.all(|character| character == '_' || character.is_ascii_alphanumeric())
}

fn parse_nonnegative_integer(value: &str) -> Result<u64> {
    let value = value.strip_prefix('+').unwrap_or(value);
    if value.is_empty() || value.starts_with('-') {
        bail!("metric value must be a non-negative integer");
    }
    let (mantissa, exponent) = if let Some(index) = value.find(['e', 'E']) {
        (
            &value[..index],
            value[index + 1..]
                .parse::<i32>()
                .context("metric exponent is invalid")?,
        )
    } else {
        (value, 0_i32)
    };
    let (whole, fraction) = mantissa.split_once('.').unwrap_or((mantissa, ""));
    if whole.is_empty()
        || !whole.bytes().all(|byte| byte.is_ascii_digit())
        || !fraction.bytes().all(|byte| byte.is_ascii_digit())
    {
        bail!("metric value is invalid");
    }
    let digits = format!("{whole}{fraction}");
    let unscaled = digits
        .parse::<u128>()
        .context("metric value exceeds supported precision")?;
    let scale = exponent
        .checked_sub(i32::try_from(fraction.len()).context("metric precision is too large")?)
        .context("metric exponent overflowed")?;
    let integer = if scale >= 0 {
        unscaled
            .checked_mul(
                10_u128
                    .checked_pow(scale.unsigned_abs())
                    .context("metric exponent is too large")?,
            )
            .context("metric value overflowed")?
    } else {
        let divisor = 10_u128
            .checked_pow(scale.unsigned_abs())
            .context("metric exponent is too small")?;
        if unscaled % divisor != 0 {
            bail!("metric value is not an integer");
        }
        unscaled / divisor
    };
    u64::try_from(integer).context("metric value exceeds u64")
}

#[cfg(test)]
mod tests {
    use std::{
        io::{Read, Write},
        net::TcpListener,
        thread,
        time::Duration,
    };

    use uuid::Uuid;

    use super::{
        CoturnMetricsClient, LiveKitMetricsClient, parse_coturn_prometheus_metrics,
        parse_http_response, parse_livekit_prometheus_metrics, parse_nonnegative_integer,
    };

    #[test]
    fn parses_service_scoped_coturn_metrics_without_global_misattribution() {
        let service_id = Uuid::new_v4();
        let other = Uuid::new_v4();
        let organization_id = Uuid::new_v4();
        let project_id = Uuid::new_v4();
        let principal_id = Uuid::new_v4();
        let body = format!(
            r#"
# HELP turn_traffic_rcvb Represents finished sessions received bytes
turn_traffic_rcvb{{realm="flow",user="123:{organization_id}:{project_id}:{service_id}:{principal_id}"}} 120.000000
turn_traffic_sentb{{user="123:{organization_id}:{project_id}:{service_id}:{principal_id}",realm="flow"}} 8e1
turn_traffic_rcvb{{realm="flow",user="123:{organization_id}:{project_id}:{other}:{principal_id}"}} 999
turn_total_allocations{{type="UDP"}} 7
turn_total_allocations{{type="UDP",user="123:{organization_id}:{project_id}:{service_id}:{principal_id}"}} 2
"#
        );
        let metrics = parse_coturn_prometheus_metrics(&body, service_id).unwrap();
        assert_eq!(metrics.ingress_bytes, 120);
        assert_eq!(metrics.egress_bytes, 80);
        assert_eq!(metrics.allocations, Some(2));
    }

    #[test]
    fn rejects_fractional_or_malformed_relevant_samples() {
        assert!(parse_nonnegative_integer("1.5").is_err());
        assert!(parse_nonnegative_integer("NaN").is_err());
        let service_id = Uuid::new_v4();
        assert!(
            parse_coturn_prometheus_metrics(
                &format!("turn_traffic_rcvb{{user=\"123:{service_id}\",user=\"duplicate\"}} 1\n"),
                service_id,
            )
            .is_err()
        );
    }

    #[test]
    fn parses_service_scoped_livekit_metrics_without_global_misattribution() {
        let service_id = Uuid::new_v4();
        let other = Uuid::new_v4();
        let body = format!(
            r#"
# HELP livekit_service_packet_bytes Media bytes attributed to a HeteroCloud Flow service.
livekit_service_packet_bytes{{direction="incoming",node_id="node-a",node_type="SERVER",service_id="{service_id}",transmission="initial"}} 120
livekit_service_packet_bytes{{direction="incoming",node_id="node-a",node_type="SERVER",service_id="{service_id}",transmission="retransmit"}} 5
livekit_service_packet_bytes{{direction="outgoing",node_id="node-a",node_type="SERVER",service_id="{service_id}",transmission="initial"}} 8e1
livekit_service_packet_bytes{{direction="outgoing",node_id="node-a",node_type="SERVER",service_id="{other}",transmission="initial"}} 999
livekit_packet_bytes{{direction="outgoing",transmission="initial",country=""}} 4567
"#
        );
        let metrics = parse_livekit_prometheus_metrics(&body, service_id).unwrap();
        assert_eq!(metrics.ingress_bytes, 125);
        assert_eq!(metrics.egress_bytes, 80);
    }

    #[test]
    fn rejects_invalid_matching_livekit_labels() {
        let service_id = Uuid::new_v4();
        assert!(
            parse_livekit_prometheus_metrics(
                &format!(
                    "livekit_service_packet_bytes{{service_id=\"{service_id}\",direction=\"sideways\",transmission=\"initial\"}} 1\n"
                ),
                service_id,
            )
            .is_err()
        );
        assert!(
            parse_livekit_prometheus_metrics(
                &format!(
                    "livekit_service_packet_bytes{{service_id=\"{service_id}\",direction=\"incoming\",transmission=\"duplicate\"}} 1\n"
                ),
                service_id,
            )
            .is_err()
        );
    }

    #[test]
    fn parses_strict_http_response() {
        let response = b"HTTP/1.1 200 OK\r\nContent-Length: 4\r\nConnection: close\r\n\r\ntest";
        assert_eq!(parse_http_response(response).unwrap(), "test");
        assert!(
            parse_http_response(b"HTTP/1.1 503 Unavailable\r\nContent-Length: 0\r\n\r\n").is_err()
        );
    }

    #[tokio::test]
    async fn scrapes_http_endpoint_and_rejects_duplicates() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let organization_id = Uuid::new_v4();
        let project_id = Uuid::new_v4();
        let service_id = Uuid::new_v4();
        let principal_id = Uuid::new_v4();
        let body = format!(
            "turn_traffic_rcvb{{realm=\"flow\",user=\"123:{organization_id}:{project_id}:{service_id}:{principal_id}\"}} 5712\nturn_traffic_sentb{{realm=\"flow\",user=\"123:{organization_id}:{project_id}:{service_id}:{principal_id}\"}} 3112\n"
        );
        let server = thread::spawn(move || {
            let (mut connection, _) = listener.accept().unwrap();
            connection
                .set_read_timeout(Some(Duration::from_secs(2)))
                .unwrap();
            let mut request = Vec::new();
            while !request.windows(4).any(|window| window == b"\r\n\r\n") {
                let mut chunk = [0_u8; 256];
                let size = connection.read(&mut chunk).unwrap();
                assert!(size > 0, "HTTP request ended before its headers");
                request.extend_from_slice(&chunk[..size]);
                assert!(request.len() <= 4096, "HTTP request headers are too large");
            }
            let request = std::str::from_utf8(&request).unwrap();
            assert!(
                request.starts_with("GET /metrics HTTP/1.1\r\n"),
                "unexpected request: {request:?}"
            );
            write!(
                connection,
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            )
            .unwrap();
        });

        let url = format!("http://{address}/metrics");
        let client = CoturnMetricsClient::new(vec![url.clone()]).unwrap();
        let metrics = client.scrape(service_id).await.unwrap().unwrap();
        assert_eq!(metrics.ingress_bytes, 5712);
        assert_eq!(metrics.egress_bytes, 3112);
        assert!(CoturnMetricsClient::new(vec![url.clone(), url]).is_err());
        assert!(
            LiveKitMetricsClient::new(vec![
                format!("http://{address}/metrics"),
                format!("http://{address}/metrics"),
            ])
            .is_err()
        );
        server.join().unwrap();
    }
}
