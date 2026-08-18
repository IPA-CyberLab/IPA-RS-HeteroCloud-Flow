use std::{
    collections::HashSet,
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
    SIGNALING_HEARTBEAT_INTERVAL, SessionMode, SignalingAuthenticationFrame as AuthenticationFrame,
    SignalingClientSignal as ClientSignal, SignalingPeer, SignalingServerFrame as ServerFrame,
    SignalingSignalKind as SignalKind,
};
use flow_rate_limit::{
    IpRateLimiter, RateLimitDecision, RateLimitPolicy, RedisBackend, TrustedProxies,
};
use flow_store::{PgStore, database_url_with_proxy};
use futures_util::{SinkExt, StreamExt};
use redis::AsyncCommands;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tower_http::trace::TraceLayer;
use tracing::{info, warn};
use tracing_subscriber::{EnvFilter, fmt};
use uuid::Uuid;

const REDIS_RECONNECT_ATTEMPTS: usize = 4;
const REDIS_RECONNECT_BACKOFF: Duration = Duration::from_millis(100);
const REDIS_RECONNECT_MAX_EXPONENT: usize = 3;
const REDIS_SUBSCRIBER_BUFFER: usize = 256;
const REDIS_PUBLISHER_BUFFER: usize = 256;

fn redis_reconnect_delay(attempt: usize) -> Duration {
    let exponent = attempt.min(REDIS_RECONNECT_MAX_EXPONENT);
    REDIS_RECONNECT_BACKOFF.saturating_mul(1_u32 << exponent)
}

#[derive(Clone)]
struct AppState {
    store: PgStore,
    principal_auth: PrincipalAuthenticator,
    redis: Arc<RedisBackend>,
    rate_limiter: Arc<IpRateLimiter>,
    trusted_proxies: TrustedProxies,
    auth_timeout: Duration,
    heartbeat_interval: Duration,
}

#[derive(Clone, Copy)]
struct ClientIp(IpAddr);

struct RelaySession {
    principal: PrincipalContext,
    connection_id: Uuid,
    known_peer_connections: HashSet<Uuid>,
    store: PgStore,
    redis: Arc<RedisBackend>,
    heartbeat_interval: Duration,
}

enum SubscriberEvent {
    Message(redis::Msg),
    Failed(String),
}

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
        heartbeat_interval: SIGNALING_HEARTBEAT_INTERVAL,
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
    match state
        .store
        .principal_context_is_revoked(
            principal.organization_id,
            principal.project_id,
            principal.service_instance_id,
            principal.token_id,
        )
        .await
    {
        Ok(false) => {}
        Ok(true) => {
            send_protocol_error(
                &mut socket,
                "principal_context_revoked",
                "delegated principal context is no longer valid",
            )
            .await;
            return;
        }
        Err(error) => {
            warn!(
                %error,
                organization_id = %principal.organization_id,
                project_id = %principal.project_id,
                service_instance_id = %principal.service_instance_id,
                context_id = %principal.token_id,
                "failed to check principal context revocation during authentication"
            );
            send_protocol_error(
                &mut socket,
                "authentication_unavailable",
                "credential status service is unavailable",
            )
            .await;
            return;
        }
    }
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
    let subscriber = match subscribe_to_channel(&state.redis, &channel).await {
        Ok(subscriber) => subscriber,
        Err(error) => {
            warn!(%error, "failed to subscribe to signaling channel");
            send_protocol_error(
                &mut socket,
                "signaling_unavailable",
                "signaling backend is unavailable",
            )
            .await;
            return;
        }
    };
    let mut publisher = match connect_publisher(&state.redis).await {
        Ok(connection) => connection,
        Err(error) => {
            warn!(%error, "failed to open Redis publisher");
            return;
        }
    };

    let connection_id = Uuid::now_v7();
    let peers = match state
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
        Ok(peers) => peers,
        Err(error) => {
            warn!(%error, "failed to persist signaling connection");
            send_protocol_error(
                &mut socket,
                "signaling_unavailable",
                "signaling backend is unavailable",
            )
            .await;
            return;
        }
    };
    let peer = SignalingPeer {
        connection_id,
        principal_id: principal.principal_id,
    };
    let known_peer_connections = peers.iter().map(|peer| peer.connection_id).collect();
    let authenticated = ServerFrame::Authenticated {
        connection_id,
        room_id,
        principal_id: principal.principal_id,
        peers,
    };
    if send_server_frame(&mut socket, &authenticated)
        .await
        .is_err()
    {
        close_and_publish_departure(&state.store, &state.redis, &mut publisher, &channel, peer)
            .await;
        return;
    }
    if let Err(error) = publish_frame(
        &state.redis,
        &mut publisher,
        &channel,
        &PublishedFrame::PeerJoined { peer },
    )
    .await
    {
        warn!(%error, "failed to publish signaling presence");
        send_protocol_error(
            &mut socket,
            "signaling_unavailable",
            "signaling backend is unavailable",
        )
        .await;
        close_and_publish_departure(&state.store, &state.redis, &mut publisher, &channel, peer)
            .await;
        return;
    }
    persist_open_audit(&state.store, &principal, room_id, connection_id).await;
    let started = Instant::now();
    let message_count = relay(
        socket,
        subscriber,
        publisher,
        channel,
        RelaySession {
            principal: principal.clone(),
            connection_id,
            known_peer_connections,
            store: state.store.clone(),
            redis: state.redis.clone(),
            heartbeat_interval: state.heartbeat_interval,
        },
    )
    .await;
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
    subscriber: redis::aio::PubSubStream,
    publisher: redis::aio::MultiplexedConnection,
    channel: String,
    session: RelaySession,
) -> u64 {
    let RelaySession {
        principal,
        connection_id,
        mut known_peer_connections,
        store,
        redis,
        heartbeat_interval,
    } = session;
    let (mut sender, mut receiver) = socket.split();
    let (subscriber_sender, mut subscriber_messages) = mpsc::channel(REDIS_SUBSCRIBER_BUFFER);
    let subscriber_task = tokio::spawn(forward_subscriber_messages(
        redis.clone(),
        channel.clone(),
        subscriber,
        subscriber_sender,
    ));
    let (publisher_sender, publisher_receiver) = mpsc::channel(REDIS_PUBLISHER_BUFFER);
    let (publisher_failure_sender, mut publisher_failures) = mpsc::channel(1);
    let publisher_task = tokio::spawn(forward_publisher_frames(
        redis.clone(),
        channel.clone(),
        publisher,
        publisher_receiver,
        publisher_failure_sender,
    ));
    let mut count = 0_u64;
    let auth_remaining = (principal.expires_at - Utc::now())
        .to_std()
        .unwrap_or(Duration::ZERO);
    let authentication_expiry = tokio::time::sleep(auth_remaining);
    tokio::pin!(authentication_expiry);
    let mut heartbeat = tokio::time::interval(heartbeat_interval);
    heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    heartbeat.tick().await;
    loop {
        tokio::select! {
            () = &mut authentication_expiry => {
                send_protocol_error_to_sink(
                    &mut sender,
                    "session_expired",
                    "delegated principal context expired",
                ).await;
                break;
            }
            _ = heartbeat.tick() => {
                match store
                    .principal_context_is_revoked(
                        principal.organization_id,
                        principal.project_id,
                        principal.service_instance_id,
                        principal.token_id,
                    )
                    .await
                {
                    Ok(false) => {}
                    Ok(true) => {
                        send_protocol_error_to_sink(
                            &mut sender,
                            "principal_context_revoked",
                            "delegated principal context is no longer valid",
                        ).await;
                        break;
                    }
                    Err(error) => {
                        warn!(
                            %error,
                            %connection_id,
                            service_instance_id = %principal.service_instance_id,
                            context_id = %principal.token_id,
                            "failed to check principal context revocation during heartbeat"
                        );
                        send_protocol_error_to_sink(
                            &mut sender,
                            "authentication_unavailable",
                            "credential status service is unavailable",
                        ).await;
                        break;
                    }
                }
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
                                    code: "invalid_signal".into(),
                                    message,
                                };
                                if send_frame_to_sink(&mut sender, &frame).await.is_err() {
                                    break;
                                }
                                continue;
                            }
                        };
                        let outbound_signal = PublishedFrame::Signal {
                            kind: signal.kind,
                            sender: principal.principal_id,
                            target: signal.target,
                            payload: signal.payload,
                            connection_id,
                            sent_at: Utc::now(),
                        };
                        match publisher_sender.try_send(outbound_signal) {
                            Ok(()) => count = count.saturating_add(1),
                            Err(mpsc::error::TrySendError::Full(_)) => {
                                warn!(%connection_id, "Redis signaling publish queue is full");
                                send_protocol_error_to_sink(
                                    &mut sender,
                                    "signaling_unavailable",
                                    "signaling backend is recovering",
                                ).await;
                                break;
                            }
                            Err(mpsc::error::TrySendError::Closed(_)) => break,
                        }
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
            subscriber_event = subscriber_messages.recv() => {
                let redis_message = match subscriber_event {
                    Some(SubscriberEvent::Message(redis_message)) => redis_message,
                    Some(SubscriberEvent::Failed(error)) => {
                        warn!(%error, "Redis subscriber recovery was exhausted");
                        break;
                    }
                    None => break,
                };
                let payload: String = match redis_message.get_payload() {
                    Ok(payload) => payload,
                    Err(_) => continue,
                };
                let relay_frame: PublishedFrame = match serde_json::from_str(&payload) {
                    Ok(relay_frame) => relay_frame,
                    Err(_) => continue,
                };
                let frame = match relay_frame {
                    PublishedFrame::Signal {
                        kind,
                        sender,
                        target,
                        payload,
                        connection_id: sender_connection_id,
                        sent_at,
                    } if sender_connection_id != connection_id
                        && target == principal.principal_id =>
                    {
                        Some(ServerFrame::Signal {
                            kind,
                            sender,
                            payload,
                            sent_at,
                        })
                    }
                    PublishedFrame::PeerJoined { peer }
                        if peer.connection_id != connection_id
                            && known_peer_connections.insert(peer.connection_id) =>
                    {
                        Some(ServerFrame::PeerJoined { peer })
                    }
                    PublishedFrame::PeerLeft { peer }
                        if peer.connection_id != connection_id
                            && known_peer_connections.remove(&peer.connection_id) =>
                    {
                        Some(ServerFrame::PeerLeft { peer })
                    }
                    _ => None,
                };
                if let Some(frame) = frame
                    && send_frame_to_sink(&mut sender, &frame).await.is_err()
                {
                    break;
                }
            }
            Some(error) = publisher_failures.recv() => {
                warn!(%error, %connection_id, "Redis signaling publisher recovery was exhausted");
                break;
            }
        }
    }
    subscriber_task.abort();
    let _ = subscriber_task.await;
    let peer = SignalingPeer {
        connection_id,
        principal_id: principal.principal_id,
    };
    close_signaling_connection(&store, connection_id).await;
    let departure_queued = publisher_sender
        .send(PublishedFrame::PeerLeft { peer })
        .await
        .is_ok();
    drop(publisher_sender);
    let publisher_result = publisher_task
        .await
        .map_err(|error| error.to_string())
        .and_then(|result| result);
    if !departure_queued || publisher_result.is_err() {
        match connect_publisher(&redis).await {
            Ok(mut publisher) => {
                if let Err(error) = publish_frame(
                    &redis,
                    &mut publisher,
                    &channel,
                    &PublishedFrame::PeerLeft { peer },
                )
                .await
                {
                    warn!(%error, "failed to publish signaling departure after recovery");
                }
            }
            Err(error) => warn!(%error, "failed to recover Redis publisher for departure"),
        }
    }
    count
}

async fn subscribe_to_channel(
    redis: &RedisBackend,
    channel: &str,
) -> Result<redis::aio::PubSubStream, String> {
    let mut last_error = "Redis subscriber connection failed".to_owned();
    for attempt in 0..REDIS_RECONNECT_ATTEMPTS {
        let result = async {
            let client = redis.client().await.map_err(|error| error.to_string())?;
            let mut subscriber = client
                .get_async_pubsub()
                .await
                .map_err(|error| error.to_string())?;
            subscriber
                .subscribe(channel)
                .await
                .map_err(|error| error.to_string())?;
            Ok::<_, String>(subscriber.into_on_message())
        }
        .await;
        match result {
            Ok(subscriber) => return Ok(subscriber),
            Err(error) => {
                last_error = error;
                if attempt + 1 < REDIS_RECONNECT_ATTEMPTS {
                    warn!(
                        attempt = attempt + 1,
                        error = %last_error,
                        "Redis signaling subscription failed; retrying"
                    );
                    tokio::time::sleep(redis_reconnect_delay(attempt)).await;
                }
            }
        }
    }
    Err(last_error)
}

async fn forward_subscriber_messages(
    redis: Arc<RedisBackend>,
    channel: String,
    mut subscriber: redis::aio::PubSubStream,
    sender: mpsc::Sender<SubscriberEvent>,
) {
    loop {
        while let Some(message) = subscriber.next().await {
            if sender
                .send(SubscriberEvent::Message(message))
                .await
                .is_err()
            {
                return;
            }
        }

        warn!(%channel, "Redis signaling subscriber ended; resolving current master");
        match subscribe_to_channel(&redis, &channel).await {
            Ok(next_subscriber) => {
                info!(%channel, "Redis signaling subscriber recovered");
                subscriber = next_subscriber;
            }
            Err(error) => {
                let _ = sender.send(SubscriberEvent::Failed(error)).await;
                return;
            }
        }
    }
}

async fn forward_publisher_frames(
    redis: Arc<RedisBackend>,
    channel: String,
    mut publisher: redis::aio::MultiplexedConnection,
    mut frames: mpsc::Receiver<PublishedFrame>,
    failures: mpsc::Sender<String>,
) -> Result<redis::aio::MultiplexedConnection, String> {
    while let Some(frame) = frames.recv().await {
        if let Err(error) = publish_frame(&redis, &mut publisher, &channel, &frame).await {
            let _ = failures.send(error.clone()).await;
            return Err(error);
        }
    }
    Ok(publisher)
}

async fn connect_publisher(
    redis: &RedisBackend,
) -> Result<redis::aio::MultiplexedConnection, String> {
    let mut last_error = "Redis publisher connection failed".to_owned();
    for attempt in 0..REDIS_RECONNECT_ATTEMPTS {
        let result = connect_publisher_once(redis).await;
        match result {
            Ok(publisher) => return Ok(publisher),
            Err(error) => {
                last_error = error;
                if attempt + 1 < REDIS_RECONNECT_ATTEMPTS {
                    warn!(
                        attempt = attempt + 1,
                        error = %last_error,
                        "Redis signaling publisher connection failed; retrying"
                    );
                    tokio::time::sleep(redis_reconnect_delay(attempt)).await;
                }
            }
        }
    }
    Err(last_error)
}

async fn connect_publisher_once(
    redis: &RedisBackend,
) -> Result<redis::aio::MultiplexedConnection, String> {
    let client = redis.client().await.map_err(|error| error.to_string())?;
    client
        .get_multiplexed_async_connection()
        .await
        .map_err(|error| error.to_string())
}

async fn close_signaling_connection(store: &PgStore, connection_id: Uuid) {
    if let Err(error) = store.close_signaling_connection(connection_id).await {
        warn!(%error, %connection_id, "failed to close signaling connection record");
    }
}

async fn close_and_publish_departure(
    store: &PgStore,
    redis: &RedisBackend,
    publisher: &mut redis::aio::MultiplexedConnection,
    channel: &str,
    peer: SignalingPeer,
) {
    close_signaling_connection(store, peer.connection_id).await;
    if let Err(error) = publish_frame(
        redis,
        publisher,
        channel,
        &PublishedFrame::PeerLeft { peer },
    )
    .await
    {
        warn!(%error, "failed to publish signaling departure");
    }
}

fn parse_signal(text: &str) -> Result<ClientSignal, String> {
    let signal: ClientSignal =
        serde_json::from_str(text).map_err(|_| "signal frame is invalid".to_owned())?;
    signal.validate().map_err(|error| error.to_string())?;
    Ok(signal)
}

async fn send_protocol_error(socket: &mut WebSocket, code: &'static str, message: &str) {
    let _ = send_server_frame(
        socket,
        &ServerFrame::Error {
            code: code.into(),
            message: message.into(),
        },
    )
    .await;
    let _ = socket.send(Message::Close(None)).await;
}

async fn send_server_frame(socket: &mut WebSocket, frame: &ServerFrame) -> Result<(), ()> {
    let serialized = serde_json::to_string(frame).map_err(|_| ())?;
    socket
        .send(Message::Text(serialized.into()))
        .await
        .map_err(|_| ())
}

async fn send_frame_to_sink<S>(sink: &mut S, frame: &ServerFrame) -> Result<(), ()>
where
    S: futures_util::Sink<Message> + Unpin,
{
    let serialized = serde_json::to_string(frame).map_err(|_| ())?;
    sink.send(Message::Text(serialized.into()))
        .await
        .map_err(|_| ())
}

async fn send_protocol_error_to_sink<S>(sink: &mut S, code: &'static str, message: &str)
where
    S: futures_util::Sink<Message> + Unpin,
{
    let _ = send_frame_to_sink(
        sink,
        &ServerFrame::Error {
            code: code.into(),
            message: message.into(),
        },
    )
    .await;
    let _ = sink.send(Message::Close(None)).await;
}

async fn publish_frame(
    redis: &RedisBackend,
    publisher: &mut redis::aio::MultiplexedConnection,
    channel: &str,
    frame: &PublishedFrame,
) -> Result<(), String> {
    let serialized = serde_json::to_string(frame).map_err(|error| error.to_string())?;
    let mut last_error = "Redis publish failed".to_owned();
    for attempt in 0..REDIS_RECONNECT_ATTEMPTS {
        if attempt > 0 {
            tokio::time::sleep(redis_reconnect_delay(attempt - 1)).await;
            match connect_publisher_once(redis).await {
                Ok(reconnected) => *publisher = reconnected,
                Err(error) => {
                    last_error = error;
                    continue;
                }
            }
        }
        match publisher.publish::<_, _, usize>(channel, &serialized).await {
            Ok(_) => return Ok(()),
            Err(error) => {
                last_error = error.to_string();
                if attempt + 1 < REDIS_RECONNECT_ATTEMPTS {
                    warn!(
                        attempt = attempt + 1,
                        error = %last_error,
                        "Redis signaling publish failed; resolving current master"
                    );
                }
            }
        }
    }
    Err(last_error)
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
            database_url: database_url_with_proxy(
                &required("DATABASE_URL")?,
                env::var("DATABASE_PROXY_HOST").ok().as_deref(),
                env::var("DATABASE_PROXY_PORT").ok().as_deref(),
            )
            .context("invalid PostgreSQL proxy configuration")?,
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

#[derive(Serialize, Deserialize)]
#[serde(tag = "event", rename_all = "snake_case")]
enum PublishedFrame {
    Signal {
        kind: SignalKind,
        sender: Uuid,
        target: Uuid,
        payload: Value,
        connection_id: Uuid,
        sent_at: chrono::DateTime<Utc>,
    },
    PeerJoined {
        peer: SignalingPeer,
    },
    PeerLeft {
        peer: SignalingPeer,
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
    use std::{
        collections::BTreeSet,
        env,
        net::SocketAddr,
        pin::Pin,
        sync::{Arc, Mutex},
        task::{Context, Poll},
        time::Duration,
    };

    use axum::{Router, extract::ws::Message, middleware, routing::get};
    use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
    use flow_auth::{PrincipalAuthenticator, SignedPrincipal};
    use flow_domain::{NewRoom, RoomState, ServiceInstanceReconcile, SessionMode};
    use flow_rate_limit::{IpRateLimiter, RateLimitPolicy, RedisBackend, TrustedProxies};
    use flow_store::PgStore;
    use futures_util::{Sink, SinkExt, StreamExt};
    use hmac::{Hmac, Mac};
    use serde_json::json;
    use sha2::Sha256;
    use tokio::net::TcpListener;
    use tokio_tungstenite::{
        WebSocketStream, connect_async, tungstenite::Message as ClientMessage,
    };
    use uuid::Uuid;

    use super::{
        AppState, REDIS_RECONNECT_ATTEMPTS, SignalKind, enforce_ip_rate_limit, live, parse_signal,
        ready, redis_reconnect_delay, send_protocol_error_to_sink, signal_upgrade,
    };

    const PRINCIPAL_SECRET: &[u8] = b"signaling-test-principal-secret-at-least-32-bytes";

    #[derive(Clone, Default)]
    struct RecordingSink(Arc<Mutex<Vec<Message>>>);

    impl Sink<Message> for RecordingSink {
        type Error = ();

        fn poll_ready(
            self: Pin<&mut Self>,
            _context: &mut Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn start_send(self: Pin<&mut Self>, item: Message) -> Result<(), Self::Error> {
            self.0.lock().unwrap().push(item);
            Ok(())
        }

        fn poll_flush(
            self: Pin<&mut Self>,
            _context: &mut Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn poll_close(
            self: Pin<&mut Self>,
            _context: &mut Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }
    }

    #[test]
    fn redis_reconnect_backoff_is_bounded() {
        let delays = (0..REDIS_RECONNECT_ATTEMPTS - 1)
            .map(redis_reconnect_delay)
            .collect::<Vec<_>>();
        assert_eq!(
            delays,
            vec![
                Duration::from_millis(100),
                Duration::from_millis(200),
                Duration::from_millis(400),
            ]
        );
        assert_eq!(
            redis_reconnect_delay(usize::MAX),
            Duration::from_millis(800)
        );
    }

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

    #[tokio::test]
    async fn heartbeat_revocation_sends_generic_error_then_closes() {
        let mut sink = RecordingSink::default();
        let recorded = sink.0.clone();
        send_protocol_error_to_sink(
            &mut sink,
            "principal_context_revoked",
            "delegated principal context is no longer valid",
        )
        .await;

        let frames = recorded.lock().unwrap();
        assert_eq!(frames.len(), 2);
        let Message::Text(error) = &frames[0] else {
            panic!("first frame must be a text protocol error");
        };
        let error: serde_json::Value = serde_json::from_str(error).unwrap();
        assert_eq!(error["type"], "error");
        assert_eq!(error["code"], "principal_context_revoked");
        assert_eq!(
            error["message"],
            "delegated principal context is no longer valid"
        );
        assert!(matches!(frames[1], Message::Close(_)));
    }

    #[tokio::test]
    async fn websocket_relays_presence_and_signals_and_enforces_revocation() {
        let (Ok(database_url), Ok(redis_url)) =
            (env::var("TEST_DATABASE_URL"), env::var("TEST_REDIS_URL"))
        else {
            eprintln!(
                "TEST_DATABASE_URL or TEST_REDIS_URL is not set; skipping signaling integration test"
            );
            return;
        };
        let store = PgStore::connect(&database_url, 4).await.unwrap();
        store.migrate().await.unwrap();
        let organization_id = Uuid::new_v4();
        let project_id = Uuid::new_v4();
        let service_instance_id = Uuid::new_v4();
        let room_id = Uuid::now_v7();
        store
            .reconcile_service_instance(ServiceInstanceReconcile {
                jwt_id: Uuid::now_v7(),
                organization_id,
                project_id,
                service_instance_id,
                principal_id: Uuid::new_v4(),
                generation: 1,
                name: format!("signaling-revocation-{service_instance_id}"),
                spec: json!({}),
            })
            .await
            .unwrap();
        let now = chrono::Utc::now();
        store
            .create_room(NewRoom {
                id: room_id,
                organization_id,
                project_id,
                service_instance_id,
                created_by_principal_id: Uuid::new_v4(),
                name: format!("room-{room_id}"),
                provider_room_name: None,
                mode: SessionMode::P2p,
                state: RoomState::Ready,
                max_participants: 2,
                metadata: json!({}),
            })
            .await
            .unwrap();

        let redis = RedisBackend::direct(&redis_url).unwrap();
        redis.ping().await.unwrap();
        let state = AppState {
            store: store.clone(),
            principal_auth: PrincipalAuthenticator::new(
                "heterocloud",
                "heterocloud-flow-data",
                PRINCIPAL_SECRET,
                Duration::from_mins(5),
            )
            .unwrap(),
            redis: Arc::new(redis.clone()),
            rate_limiter: Arc::new(IpRateLimiter::new(
                redis,
                RateLimitPolicy::new(1_000, 1_000).unwrap(),
            )),
            trusted_proxies: TrustedProxies::from_csv("127.0.0.0/8").unwrap(),
            auth_timeout: Duration::from_secs(2),
            heartbeat_interval: Duration::from_millis(50),
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
            .with_state(state);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            axum::serve(
                listener,
                app.into_make_service_with_connect_info::<SocketAddr>(),
            )
            .await
            .unwrap();
        });
        let url = format!("ws://{address}/v1/signal/{room_id}");
        let expires_at = u64::try_from((now + chrono::Duration::minutes(5)).timestamp()).unwrap();

        let revoked_context_id = Uuid::now_v7();
        store
            .revoke_principal_context(
                organization_id,
                project_id,
                service_instance_id,
                revoked_context_id,
                i64::try_from(expires_at).unwrap(),
            )
            .await
            .unwrap();
        let (mut revoked_socket, _) = connect_async(&url).await.unwrap();
        let revoked_principal_id = Uuid::new_v4();
        revoked_socket
            .send(ClientMessage::Text(
                authentication_frame(
                    organization_id,
                    project_id,
                    service_instance_id,
                    revoked_context_id,
                    revoked_principal_id,
                    expires_at,
                )
                .into(),
            ))
            .await
            .unwrap();
        assert_protocol_error(
            &mut revoked_socket,
            "principal_context_revoked",
            Duration::from_secs(2),
        )
        .await;
        assert!(matches!(
            revoked_socket.next().await,
            Some(Ok(ClientMessage::Close(_)))
        ));

        let active_context_id = Uuid::now_v7();
        let active_principal_id = Uuid::new_v4();
        let (mut active_socket, _) = connect_async(&url).await.unwrap();
        active_socket
            .send(ClientMessage::Text(
                authentication_frame(
                    organization_id,
                    project_id,
                    service_instance_id,
                    active_context_id,
                    active_principal_id,
                    expires_at,
                )
                .into(),
            ))
            .await
            .unwrap();
        let authenticated = next_json_frame(&mut active_socket).await;
        assert_eq!(authenticated["type"], "authenticated");
        assert_eq!(
            authenticated["principal_id"],
            active_principal_id.to_string()
        );
        assert_eq!(authenticated["peers"].as_array().map(Vec::len), Some(0));

        let peer_context_id = Uuid::now_v7();
        let peer_principal_id = Uuid::new_v4();
        let (mut peer_socket, _) = connect_async(&url).await.unwrap();
        peer_socket
            .send(ClientMessage::Text(
                authentication_frame(
                    organization_id,
                    project_id,
                    service_instance_id,
                    peer_context_id,
                    peer_principal_id,
                    expires_at,
                )
                .into(),
            ))
            .await
            .unwrap();
        let peer_authenticated = next_json_frame(&mut peer_socket).await;
        assert_eq!(peer_authenticated["type"], "authenticated");
        assert_eq!(
            peer_authenticated["peers"][0]["principal_id"],
            active_principal_id.to_string()
        );
        let peer_connection_id = peer_authenticated["connection_id"].clone();

        let joined = next_json_frame(&mut active_socket).await;
        assert_eq!(joined["type"], "peer_joined");
        assert_eq!(
            joined["peer"]["principal_id"],
            peer_principal_id.to_string()
        );
        assert_eq!(joined["peer"]["connection_id"], peer_connection_id);

        peer_socket
            .send(ClientMessage::Text(
                json!({
                    "type": "offer",
                    "target": active_principal_id,
                    "payload": {"sdp": "v=0"}
                })
                .to_string()
                .into(),
            ))
            .await
            .unwrap();
        let offer = next_json_frame(&mut active_socket).await;
        assert_eq!(offer["type"], "signal");
        assert_eq!(offer["kind"], "offer");
        assert_eq!(offer["sender"], peer_principal_id.to_string());
        assert_eq!(offer["payload"]["sdp"], "v=0");

        peer_socket.close(None).await.unwrap();
        let left = next_json_frame(&mut active_socket).await;
        assert_eq!(left["type"], "peer_left");
        assert_eq!(left["peer"]["principal_id"], peer_principal_id.to_string());
        assert_eq!(left["peer"]["connection_id"], peer_connection_id);

        store
            .revoke_principal_context(
                organization_id,
                project_id,
                service_instance_id,
                active_context_id,
                i64::try_from(expires_at).unwrap(),
            )
            .await
            .unwrap();
        assert_protocol_error(
            &mut active_socket,
            "principal_context_revoked",
            Duration::from_secs(2),
        )
        .await;
        assert!(matches!(
            active_socket.next().await,
            Some(Ok(ClientMessage::Close(_)))
        ));
        server.abort();
    }

    fn authentication_frame(
        organization_id: Uuid,
        project_id: Uuid,
        service_instance_id: Uuid,
        context_id: Uuid,
        principal_id: Uuid,
        expires_at: u64,
    ) -> String {
        let issued_at = u64::try_from(chrono::Utc::now().timestamp()).unwrap();
        let signed = SignedPrincipal {
            issuer: "heterocloud".into(),
            audience: "heterocloud-flow-data".into(),
            organization_id,
            project_id,
            service_instance_id,
            principal_id,
            permissions: BTreeSet::from(["flow.signal.connect".into()]),
            issued_at,
            expires_at,
            context_id,
        };
        let principal_context = URL_SAFE_NO_PAD.encode(serde_json::to_vec(&signed).unwrap());
        let timestamp = issued_at.to_string();
        let mut mac = Hmac::<Sha256>::new_from_slice(PRINCIPAL_SECRET).unwrap();
        mac.update(timestamp.as_bytes());
        mac.update(b".");
        mac.update(principal_context.as_bytes());
        let signature = URL_SAFE_NO_PAD.encode(mac.finalize().into_bytes());
        json!({
            "type": "signed_context",
            "principal_context": principal_context,
            "timestamp": timestamp,
            "signature": signature,
        })
        .to_string()
    }

    async fn next_json_frame(
        socket: &mut WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>,
    ) -> serde_json::Value {
        let message = tokio::time::timeout(Duration::from_secs(2), socket.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let ClientMessage::Text(message) = message else {
            panic!("expected JSON text frame");
        };
        serde_json::from_str(&message).unwrap()
    }

    async fn assert_protocol_error(
        socket: &mut WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>,
        expected_code: &str,
        timeout: Duration,
    ) {
        let message = tokio::time::timeout(timeout, socket.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let ClientMessage::Text(message) = message else {
            panic!("expected text protocol error");
        };
        let error: serde_json::Value = serde_json::from_str(&message).unwrap();
        assert_eq!(error["type"], "error");
        assert_eq!(error["code"], expected_code);
    }
}
