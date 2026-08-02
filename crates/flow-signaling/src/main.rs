use std::{
    env,
    net::{IpAddr, SocketAddr},
    process::ExitCode,
    str::FromStr,
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::{Context, Result};
use axum::{
    Json, Router,
    body::Body,
    extract::{
        ConnectInfo, Extension, Path, State,
        ws::{Message, WebSocket, WebSocketUpgrade},
    },
    http::{HeaderMap, HeaderValue, Request, StatusCode},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::get,
};
use chrono::Utc;
use flow_auth::{
    PRINCIPAL_HEADER, PRINCIPAL_SIGNATURE_HEADER, PRINCIPAL_TIMESTAMP_HEADER,
    PrincipalAuthenticator,
};
use flow_domain::{
    NewAuditEvent, NewSignalingConnection, NewUsageEvent, PrincipalContext, RoomState,
    SIGNALING_HEARTBEAT_INTERVAL, SessionMode,
};
use flow_rate_limit::{
    IpRateLimiter, RateLimitDecision, RateLimitPolicy, RedisBackend, TrustedProxies,
};
use flow_store::PgStore;
use futures_util::{SinkExt, StreamExt};
use redis::AsyncCommands;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::net::TcpListener;
use tower_http::trace::TraceLayer;
use tracing::{info, warn};
use tracing_subscriber::{EnvFilter, fmt};
use uuid::Uuid;

#[derive(Clone)]
struct AppState {
    store: PgStore,
    principal_auth: PrincipalAuthenticator,
    redis: Arc<RedisBackend>,
    rate_limiter: Arc<IpRateLimiter>,
    trusted_proxies: TrustedProxies,
    auth_timeout: Duration,
}

#[derive(Clone, Copy)]
struct ClientIp(IpAddr);

struct Config {
    bind_addr: SocketAddr,
    database_url: String,
    database_max_connections: u32,
    migrate_on_start: bool,
    principal_auth: PrincipalAuthenticator,
    redis: RedisBackend,
    rate_limit_policy: RateLimitPolicy,
    trusted_proxies: TrustedProxies,
    auth_timeout: Duration,
}

#[tokio::main]
async fn main() -> ExitCode {
    init_tracing();
    match run().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            tracing::error!(error = ?error, "flow-signaling terminated");
            ExitCode::FAILURE
        }
    }
}

async fn run() -> Result<()> {
    let config = Config::from_env()?;
    let store = PgStore::connect(&config.database_url, config.database_max_connections)
        .await
        .context("connect to PostgreSQL")?;
    if config.migrate_on_start {
        store.migrate().await.context("run database migrations")?;
    }
    let bind_addr = config.bind_addr;
    let rate_limiter = Arc::new(IpRateLimiter::new(
        config.redis.clone(),
        config.rate_limit_policy,
    ));
    let state = AppState {
        store,
        principal_auth: config.principal_auth,
        redis: Arc::new(config.redis),
        rate_limiter,
        trusted_proxies: config.trusted_proxies,
        auth_timeout: config.auth_timeout,
    };
    let signal_route = Router::new()
        .route("/v1/signal/{room_id}", get(signal_upgrade))
        .route_layer(middleware::from_fn_with_state(
            state.clone(),
            enforce_ip_rate_limit,
        ));
    let app = Router::new()
        .route("/health/live", get(live))
        .route("/health/ready", get(ready))
        .merge(signal_route)
        .layer(TraceLayer::new_for_http())
        .with_state(state);
    let listener = TcpListener::bind(bind_addr)
        .await
        .with_context(|| format!("bind {bind_addr}"))?;
    info!(%bind_addr, "flow-signaling listening");
    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .with_graceful_shutdown(shutdown_signal())
    .await
    .context("serve signaling")
}

async fn live() -> Json<Value> {
    Json(json!({"status": "ok"}))
}

async fn ready(State(state): State<AppState>) -> Result<Json<Value>, StatusCode> {
    state
        .store
        .health()
        .await
        .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?;
    state
        .redis
        .ping()
        .await
        .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?;
    state
        .rate_limiter
        .ping()
        .await
        .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?;
    Ok(Json(json!({"status": "ready"})))
}

async fn enforce_ip_rate_limit(
    State(state): State<AppState>,
    mut request: Request<Body>,
    next: Next,
) -> Response {
    let peer = request
        .extensions()
        .get::<ConnectInfo<SocketAddr>>()
        .map_or_else(|| SocketAddr::from(([127, 0, 0, 1], 0)), |value| value.0);
    let client_ip = state.trusted_proxies.client_ip(peer, request.headers());
    let decision = match state.rate_limiter.check(client_ip).await {
        Ok(decision) => decision,
        Err(error) => {
            warn!(%error, %client_ip, "IP rate-limit backend is unavailable");
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({
                    "error": {
                        "code": "rate_limit_unavailable",
                        "message": "request admission service is unavailable"
                    }
                })),
            )
                .into_response();
        }
    };
    if !decision.allowed {
        let mut response = (
            StatusCode::TOO_MANY_REQUESTS,
            Json(json!({
                "error": {
                    "code": "rate_limit_exceeded",
                    "message": "source IP request limit exceeded"
                }
            })),
        )
            .into_response();
        insert_rate_limit_headers(&mut response, decision);
        insert_numeric_header(
            &mut response,
            "retry-after",
            decision.retry_after_seconds.max(1),
        );
        return response;
    }

    request.extensions_mut().insert(ClientIp(client_ip));
    let mut response = next.run(request).await;
    insert_rate_limit_headers(&mut response, decision);
    response
}

fn insert_rate_limit_headers(response: &mut Response, decision: RateLimitDecision) {
    insert_numeric_header(response, "x-ratelimit-limit", u64::from(decision.limit));
    insert_numeric_header(
        response,
        "x-ratelimit-remaining",
        u64::from(decision.remaining),
    );
    insert_numeric_header(response, "x-ratelimit-reset", decision.reset_after_seconds);
}

fn insert_numeric_header(response: &mut Response, name: &'static str, value: u64) {
    if let Ok(value) = HeaderValue::from_str(&value.to_string()) {
        response.headers_mut().insert(name, value);
    }
}

async fn signal_upgrade(
    State(state): State<AppState>,
    Path(room_id): Path<Uuid>,
    Extension(client_ip): Extension<ClientIp>,
    upgrade: WebSocketUpgrade,
) -> Response {
    upgrade
        .max_message_size(64 * 1024)
        .max_frame_size(64 * 1024)
        .on_upgrade(move |socket| handle_socket(socket, state, room_id, client_ip.0))
}

async fn handle_socket(mut socket: WebSocket, state: AppState, room_id: Uuid, client_ip: IpAddr) {
    let authentication = tokio::time::timeout(
        state.auth_timeout,
        authenticate(&mut socket, &state.principal_auth),
    )
    .await;
    let principal = match authentication {
        Ok(Ok(principal)) => principal,
        Ok(Err(error)) => {
            send_protocol_error(&mut socket, "authentication_failed", &error).await;
            return;
        }
        Err(_) => {
            send_protocol_error(
                &mut socket,
                "authentication_timeout",
                "authentication frame was not received in time",
            )
            .await;
            return;
        }
    };
    if !principal.allows("flow.signal.connect") {
        send_protocol_error(
            &mut socket,
            "permission_denied",
            "flow.signal.connect is required",
        )
        .await;
        return;
    }
    let service_rate_limit = match state
        .store
        .ready_service_rate_limit(
            principal.organization_id,
            principal.project_id,
            principal.service_instance_id,
        )
        .await
    {
        Ok(Some(rate_limit)) => rate_limit,
        Ok(None) => {
            send_protocol_error(
                &mut socket,
                "service_instance_unavailable",
                "service instance is not reconciled and ready",
            )
            .await;
            return;
        }
        Err(error) => {
            warn!(%error, "failed to validate service instance");
            send_protocol_error(
                &mut socket,
                "signaling_unavailable",
                "signaling backend is unavailable",
            )
            .await;
            return;
        }
    };
    let service_policy = match RateLimitPolicy::new(
        service_rate_limit.requests_per_second,
        service_rate_limit.burst,
    ) {
        Ok(policy) => policy,
        Err(error) => {
            warn!(%error, service_instance_id = %principal.service_instance_id, "invalid service IP rate limit");
            send_protocol_error(
                &mut socket,
                "signaling_unavailable",
                "service admission policy is unavailable",
            )
            .await;
            return;
        }
    };
    let rate_limit = match state
        .rate_limiter
        .check_service(principal.service_instance_id, client_ip, service_policy)
        .await
    {
        Ok(decision) => decision,
        Err(error) => {
            warn!(%error, %client_ip, service_instance_id = %principal.service_instance_id, "service IP rate-limit backend is unavailable");
            send_protocol_error(
                &mut socket,
                "signaling_unavailable",
                "request admission service is unavailable",
            )
            .await;
            return;
        }
    };
    if !rate_limit.allowed {
        let message = format!(
            "source IP request limit exceeded; retry after {} second(s)",
            rate_limit.retry_after_seconds.max(1)
        );
        send_protocol_error(&mut socket, "rate_limit_exceeded", &message).await;
        return;
    }
    let room = match state
        .store
        .get_room(
            principal.organization_id,
            principal.project_id,
            principal.service_instance_id,
            room_id,
        )
        .await
    {
        Ok(room) if room.mode == SessionMode::P2p && room.state == RoomState::Ready => room,
        Ok(_) => {
            send_protocol_error(
                &mut socket,
                "invalid_room",
                "signaling is available only for ready P2P rooms",
            )
            .await;
            return;
        }
        Err(_) => {
            send_protocol_error(&mut socket, "room_not_found", "room was not found").await;
            return;
        }
    };

    let channel = channel_name(&principal, room.id);
    let client = match state.redis.client().await {
        Ok(client) => client,
        Err(error) => {
            warn!(%error, "failed to resolve Redis master");
            send_protocol_error(
                &mut socket,
                "signaling_unavailable",
                "signaling backend is unavailable",
            )
            .await;
            return;
        }
    };
    let mut subscriber = match client.get_async_pubsub().await {
        Ok(subscriber) => subscriber,
        Err(error) => {
            warn!(%error, "failed to open Redis subscriber");
            return;
        }
    };
    if let Err(error) = subscriber.subscribe(&channel).await {
        warn!(%error, "failed to subscribe to signaling channel");
        return;
    }
    let publisher = match client.get_multiplexed_async_connection().await {
        Ok(connection) => connection,
        Err(error) => {
            warn!(%error, "failed to open Redis publisher");
            return;
        }
    };

    let connection_id = Uuid::now_v7();
    if let Err(error) = state
        .store
        .open_signaling_connection(NewSignalingConnection {
            connection_id,
            organization_id: principal.organization_id,
            project_id: principal.project_id,
            service_instance_id: principal.service_instance_id,
            room_id,
            principal_id: principal.principal_id,
        })
        .await
    {
        warn!(%error, "failed to persist signaling connection");
        send_protocol_error(
            &mut socket,
            "signaling_unavailable",
            "signaling backend is unavailable",
        )
        .await;
        return;
    }
    let authenticated = ServerFrame::Authenticated {
        connection_id,
        room_id,
        principal_id: principal.principal_id,
    };
    if send_server_frame(&mut socket, &authenticated)
        .await
        .is_err()
    {
        close_signaling_connection(&state.store, connection_id).await;
        return;
    }
    persist_open_audit(&state.store, &principal, room_id, connection_id).await;
    let started = Instant::now();
    let message_count = relay(
        socket,
        subscriber,
        publisher,
        channel,
        principal.clone(),
        connection_id,
        state.store.clone(),
    )
    .await;
    close_signaling_connection(&state.store, connection_id).await;
    persist_connection_usage(
        &state.store,
        &principal,
        room_id,
        connection_id,
        message_count,
        started.elapsed(),
    )
    .await;
}

async fn authenticate(
    socket: &mut WebSocket,
    authenticator: &PrincipalAuthenticator,
) -> Result<PrincipalContext, String> {
    let message = socket
        .recv()
        .await
        .ok_or_else(|| "connection closed before authentication".to_owned())?
        .map_err(|_| "invalid WebSocket frame".to_owned())?;
    let Message::Text(text) = message else {
        return Err("first frame must be a JSON text authentication frame".into());
    };
    let frame: AuthenticationFrame =
        serde_json::from_str(&text).map_err(|_| "authentication frame is invalid".to_owned())?;
    let mut headers = HeaderMap::new();
    match frame {
        AuthenticationFrame::SignedContext {
            principal_context,
            timestamp,
            signature,
        } => {
            headers.insert(
                PRINCIPAL_HEADER,
                HeaderValue::from_str(&principal_context)
                    .map_err(|_| "principal context is invalid".to_owned())?,
            );
            headers.insert(
                PRINCIPAL_TIMESTAMP_HEADER,
                HeaderValue::from_str(&timestamp).map_err(|_| "timestamp is invalid".to_owned())?,
            );
            headers.insert(
                PRINCIPAL_SIGNATURE_HEADER,
                HeaderValue::from_str(&signature).map_err(|_| "signature is invalid".to_owned())?,
            );
        }
    }
    authenticator
        .authenticate_headers(&headers)
        .map_err(|error| error.to_string())
}

async fn relay(
    socket: WebSocket,
    mut subscriber: redis::aio::PubSub,
    mut publisher: redis::aio::MultiplexedConnection,
    channel: String,
    principal: PrincipalContext,
    connection_id: Uuid,
    store: PgStore,
) -> u64 {
    let (mut sender, mut receiver) = socket.split();
    let mut redis_messages = subscriber.on_message();
    let mut count = 0_u64;
    let auth_remaining = (principal.expires_at - Utc::now())
        .to_std()
        .unwrap_or(Duration::ZERO);
    let authentication_expiry = tokio::time::sleep(auth_remaining);
    tokio::pin!(authentication_expiry);
    let mut heartbeat = tokio::time::interval(SIGNALING_HEARTBEAT_INTERVAL);
    heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    heartbeat.tick().await;
    loop {
        tokio::select! {
            () = &mut authentication_expiry => {
                let frame = ServerFrame::Error {
                    code: "session_expired",
                    message: "delegated principal context expired",
                };
                let _ = send_frame_to_sink(&mut sender, &frame).await;
                break;
            }
            _ = heartbeat.tick() => {
                match store.heartbeat_signaling_connection(connection_id).await {
                    Ok(true) => {}
                    Ok(false) => {
                        warn!(%connection_id, "signaling connection heartbeat was not persisted");
                        break;
                    }
                    Err(error) => {
                        warn!(%error, %connection_id, "failed to persist signaling heartbeat");
                        break;
                    }
                }
            }
            incoming = receiver.next() => {
                let Some(incoming) = incoming else {
                    break;
                };
                match incoming {
                    Ok(Message::Text(text)) => {
                        let signal = match parse_signal(&text) {
                            Ok(signal) => signal,
                            Err(message) => {
                                let frame = ServerFrame::Error {
                                    code: "invalid_signal",
                                    message: &message,
                                };
                                if send_frame_to_sink(&mut sender, &frame).await.is_err() {
                                    break;
                                }
                                continue;
                            }
                        };
                        let outbound_signal = PublishedSignal {
                            kind: signal.kind,
                            sender: principal.principal_id,
                            target: signal.target,
                            payload: signal.payload,
                            connection_id,
                            sent_at: Utc::now(),
                        };
                        let Ok(serialized) = serde_json::to_string(&outbound_signal) else {
                            continue;
                        };
                        let result: redis::RedisResult<usize> =
                            publisher.publish(&channel, serialized).await;
                        if let Err(error) = result {
                            warn!(%error, "failed to publish signaling frame");
                            break;
                        }
                        count = count.saturating_add(1);
                    }
                    Ok(Message::Ping(payload)) => {
                        if sender.send(Message::Pong(payload)).await.is_err() {
                            break;
                        }
                    }
                    Ok(Message::Close(_)) | Err(_) => break,
                    Ok(Message::Pong(_) | Message::Binary(_)) => {}
                }
            }
            redis_message = redis_messages.next() => {
                let Some(redis_message) = redis_message else {
                    break;
                };
                let payload: String = match redis_message.get_payload() {
                    Ok(payload) => payload,
                    Err(_) => continue,
                };
                let signal: PublishedSignal = match serde_json::from_str(&payload) {
                    Ok(signal) => signal,
                    Err(_) => continue,
                };
                if signal.connection_id == connection_id
                    || signal.target != principal.principal_id
                {
                    continue;
                }
                let frame = ServerFrame::Signal {
                    kind: signal.kind,
                    sender: signal.sender,
                    payload: signal.payload,
                    sent_at: signal.sent_at,
                };
                if send_frame_to_sink(&mut sender, &frame).await.is_err() {
                    break;
                }
            }
        }
    }
    count
}

async fn close_signaling_connection(store: &PgStore, connection_id: Uuid) {
    if let Err(error) = store.close_signaling_connection(connection_id).await {
        warn!(%error, %connection_id, "failed to close signaling connection record");
    }
}

fn parse_signal(text: &str) -> Result<ClientSignal, String> {
    let signal: ClientSignal =
        serde_json::from_str(text).map_err(|_| "signal frame is invalid".to_owned())?;
    if !signal.payload.is_object() {
        return Err("payload must be a JSON object".into());
    }
    Ok(signal)
}

async fn send_protocol_error(socket: &mut WebSocket, code: &'static str, message: &str) {
    let _ = send_server_frame(socket, &ServerFrame::Error { code, message }).await;
    let _ = socket.send(Message::Close(None)).await;
}

async fn send_server_frame(socket: &mut WebSocket, frame: &ServerFrame<'_>) -> Result<(), ()> {
    let serialized = serde_json::to_string(frame).map_err(|_| ())?;
    socket
        .send(Message::Text(serialized.into()))
        .await
        .map_err(|_| ())
}

async fn send_frame_to_sink<S>(sink: &mut S, frame: &ServerFrame<'_>) -> Result<(), ()>
where
    S: futures_util::Sink<Message> + Unpin,
{
    let serialized = serde_json::to_string(frame).map_err(|_| ())?;
    sink.send(Message::Text(serialized.into()))
        .await
        .map_err(|_| ())
}

fn channel_name(principal: &PrincipalContext, room_id: Uuid) -> String {
    format!(
        "flow:signal:{}:{}:{}:{room_id}",
        principal.organization_id, principal.project_id, principal.service_instance_id
    )
}

async fn persist_open_audit(
    store: &PgStore,
    principal: &PrincipalContext,
    room_id: Uuid,
    connection_id: Uuid,
) {
    if let Err(error) = store
        .append_audit(NewAuditEvent {
            id: Uuid::now_v7(),
            organization_id: principal.organization_id,
            project_id: principal.project_id,
            service_instance_id: principal.service_instance_id,
            principal_id: principal.principal_id,
            principal_context_id: Some(principal.token_id),
            request_id: connection_id.to_string(),
            action: "flow.signal.connect".into(),
            resource_type: "room".into(),
            resource_id: Some(room_id.to_string()),
            outcome: "allowed".into(),
            details: json!({"connection_id": connection_id}),
        })
        .await
    {
        warn!(%error, "failed to persist signaling audit event");
    }
}

async fn persist_connection_usage(
    store: &PgStore,
    principal: &PrincipalContext,
    room_id: Uuid,
    connection_id: Uuid,
    message_count: u64,
    duration: Duration,
) {
    let quantity = i64::try_from(message_count).unwrap_or(i64::MAX);
    if let Err(error) = store
        .record_usage(NewUsageEvent {
            id: Uuid::now_v7(),
            organization_id: principal.organization_id,
            project_id: principal.project_id,
            service_instance_id: principal.service_instance_id,
            principal_id: Some(principal.principal_id),
            event_type: "p2p_signaling_messages".into(),
            resource_id: Some(room_id.to_string()),
            quantity,
            idempotency_key: format!("signal-connection:{connection_id}"),
            dimensions: json!({
                "connection_id": connection_id,
                "duration_ms": duration.as_millis()
            }),
            occurred_at: Utc::now(),
        })
        .await
    {
        warn!(%error, "failed to persist signaling usage event");
    }
}

impl Config {
    fn from_env() -> Result<Self> {
        let max_auth_ttl_seconds = parse_or("FLOW_PRINCIPAL_MAX_TTL_SECONDS", "300")?;
        let principal_auth = PrincipalAuthenticator::new(
            required("FLOW_PRINCIPAL_ISSUER")?,
            required("FLOW_PRINCIPAL_AUDIENCE")?,
            required("FLOW_PRINCIPAL_CONTEXT_HMAC_SECRET")?,
            Duration::from_secs(max_auth_ttl_seconds),
        )?;
        Ok(Self {
            bind_addr: parse_or("FLOW_SIGNALING_BIND_ADDR", "0.0.0.0:8082")?,
            database_url: required("DATABASE_URL")?,
            database_max_connections: parse_or("DATABASE_MAX_CONNECTIONS", "20")?,
            migrate_on_start: parse_or("MIGRATE_ON_START", "false")?,
            principal_auth,
            redis: RedisBackend::from_env()?,
            rate_limit_policy: RateLimitPolicy::from_env()?,
            trusted_proxies: TrustedProxies::from_env()?,
            auth_timeout: Duration::from_secs(parse_or("SIGNAL_AUTH_TIMEOUT_SECONDS", "5")?),
        })
    }
}

#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum AuthenticationFrame {
    SignedContext {
        principal_context: String,
        timestamp: String,
        signature: String,
    },
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum SignalKind {
    Offer,
    Answer,
    IceCandidate,
    Renegotiate,
    Leave,
}

#[derive(Deserialize)]
struct ClientSignal {
    #[serde(rename = "type")]
    kind: SignalKind,
    target: Uuid,
    payload: Value,
}

#[derive(Serialize, Deserialize)]
struct PublishedSignal {
    #[serde(rename = "type")]
    kind: SignalKind,
    sender: Uuid,
    target: Uuid,
    payload: Value,
    connection_id: Uuid,
    sent_at: chrono::DateTime<Utc>,
}

#[derive(Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum ServerFrame<'a> {
    Authenticated {
        connection_id: Uuid,
        room_id: Uuid,
        principal_id: Uuid,
    },
    Signal {
        kind: SignalKind,
        sender: Uuid,
        payload: Value,
        sent_at: chrono::DateTime<Utc>,
    },
    Error {
        code: &'static str,
        message: &'a str,
    },
}

fn required(name: &'static str) -> Result<String> {
    env::var(name).with_context(|| format!("{name} is required"))
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

fn init_tracing() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    fmt().json().with_env_filter(filter).init();
}

async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("install Ctrl+C handler");
    };
    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("install SIGTERM handler")
            .recv()
            .await;
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();
    tokio::select! {
        () = ctrl_c => {},
        () = terminate => {},
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use uuid::Uuid;

    use super::{SignalKind, parse_signal};

    #[test]
    fn parses_targeted_offer() {
        let target = Uuid::new_v4();
        let signal = parse_signal(
            &json!({
                "type": "offer",
                "target": target,
                "payload": {"sdp": "v=0"}
            })
            .to_string(),
        )
        .unwrap();
        assert!(matches!(signal.kind, SignalKind::Offer));
        assert_eq!(signal.target, target);
    }

    #[test]
    fn rejects_non_object_payload() {
        let target = Uuid::new_v4();
        assert!(
            parse_signal(&json!({"type": "leave", "target": target, "payload": null}).to_string())
                .is_err()
        );
    }
}
