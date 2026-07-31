use std::{env, net::SocketAddr, process::ExitCode, str::FromStr, time::Duration};

use anyhow::{Context, Result, bail};
use axum::{Json, Router, extract::State, routing::get};
use chrono::Utc;
use flow_domain::{MatchCandidate, NewAuditEvent, NewUsageEvent, SessionMode};
use flow_livekit::LiveKitClient;
use flow_store::PgStore;
use serde_json::json;
use tokio::{net::TcpListener, sync::watch, task::JoinHandle};
use tower_http::trace::TraceLayer;
use tracing::{error, info, warn};
use tracing_subscriber::{EnvFilter, fmt};
use uuid::Uuid;

const MATCHMAKER_PRINCIPAL_ID: Uuid = Uuid::from_u128(1);

#[derive(Clone)]
struct HealthState {
    store: PgStore,
}

struct Config {
    bind_addr: SocketAddr,
    database_url: String,
    database_max_connections: u32,
    migrate_on_start: bool,
    livekit: LiveKitClient,
    poll_interval: Duration,
    reservation_ttl: Duration,
}

#[tokio::main]
async fn main() -> ExitCode {
    init_tracing();
    match run().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            error!(error = ?error, "flow-matchmaker terminated");
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

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let worker = spawn_worker(
        store.clone(),
        config.livekit,
        config.poll_interval,
        config.reservation_ttl,
        shutdown_rx,
    );
    let health_router = Router::new()
        .route("/health/live", get(live))
        .route("/health/ready", get(ready))
        .layer(TraceLayer::new_for_http())
        .with_state(HealthState {
            store: store.clone(),
        });
    let listener = TcpListener::bind(config.bind_addr)
        .await
        .with_context(|| format!("bind {}", config.bind_addr))?;
    info!(bind_addr = %config.bind_addr, "flow-matchmaker listening");
    axum::serve(listener, health_router)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .context("serve health endpoint")?;
    let _ = shutdown_tx.send(true);
    worker.await.context("join matchmaker worker")??;
    Ok(())
}

fn spawn_worker(
    store: PgStore,
    livekit: LiveKitClient,
    poll_interval: Duration,
    reservation_ttl: Duration,
    mut shutdown: watch::Receiver<bool>,
) -> JoinHandle<Result<()>> {
    tokio::spawn(async move {
        loop {
            if *shutdown.borrow() {
                return Ok(());
            }
            match store.claim_match(reservation_ttl).await {
                Ok(Some(candidate)) => {
                    process_candidate(&store, &livekit, candidate).await;
                }
                Ok(None) => {
                    tokio::select! {
                        () = tokio::time::sleep(poll_interval) => {},
                        result = shutdown.changed() => {
                            if result.is_err() || *shutdown.borrow() {
                                return Ok(());
                            }
                        }
                    }
                }
                Err(error) => {
                    warn!(%error, "failed to claim matchmaking tickets");
                    tokio::select! {
                        () = tokio::time::sleep(Duration::from_secs(1)) => {},
                        result = shutdown.changed() => {
                            if result.is_err() || *shutdown.borrow() {
                                return Ok(());
                            }
                        }
                    }
                }
            }
        }
    })
}

async fn process_candidate(store: &PgStore, livekit: &LiveKitClient, candidate: MatchCandidate) {
    let provision_result = match candidate.room.mode {
        SessionMode::P2p => Ok(()),
        SessionMode::Sfu => livekit.create_room(&candidate.room).await,
    };
    if let Err(error) = provision_result {
        warn!(
            room_id = %candidate.room.id,
            %error,
            "failed to provision match room"
        );
        if let Err(release_error) = store
            .release_match(candidate.room.id, &error.to_string())
            .await
        {
            error!(
                room_id = %candidate.room.id,
                %release_error,
                "failed to release match reservation"
            );
        }
        return;
    }

    match store.complete_match(candidate.room.id).await {
        Ok(assignments) => {
            if let Err(error) = store
                .append_audit(NewAuditEvent {
                    id: Uuid::now_v7(),
                    organization_id: candidate.room.organization_id,
                    project_id: candidate.room.project_id,
                    service_instance_id: candidate.room.service_instance_id,
                    principal_id: MATCHMAKER_PRINCIPAL_ID,
                    principal_context_id: None,
                    request_id: Uuid::now_v7().to_string(),
                    action: "flow.match.complete".into(),
                    resource_type: "room".into(),
                    resource_id: Some(candidate.room.id.to_string()),
                    outcome: "allowed".into(),
                    details: json!({
                        "mode": candidate.room.mode,
                        "participants": assignments.len()
                    }),
                })
                .await
            {
                warn!(%error, "failed to persist match audit event");
            }
            if let Err(error) = store
                .record_usage(NewUsageEvent {
                    id: Uuid::now_v7(),
                    organization_id: candidate.room.organization_id,
                    project_id: candidate.room.project_id,
                    service_instance_id: candidate.room.service_instance_id,
                    principal_id: None,
                    event_type: "match_completed".into(),
                    resource_id: Some(candidate.room.id.to_string()),
                    quantity: i64::try_from(assignments.len()).unwrap_or(i64::MAX),
                    idempotency_key: format!("match-completed:{}", candidate.room.id),
                    dimensions: json!({"mode": candidate.room.mode}),
                    occurred_at: Utc::now(),
                })
                .await
            {
                warn!(%error, "failed to persist match usage event");
            }
            info!(
                room_id = %candidate.room.id,
                participants = assignments.len(),
                "match completed"
            );
        }
        Err(error) => {
            warn!(
                room_id = %candidate.room.id,
                %error,
                "failed to complete match"
            );
            if let Err(release_error) = store
                .release_match(candidate.room.id, &error.to_string())
                .await
            {
                error!(
                    room_id = %candidate.room.id,
                    %release_error,
                    "failed to release incomplete match"
                );
            }
        }
    }
}

async fn live() -> Json<serde_json::Value> {
    Json(json!({"status": "ok"}))
}

async fn ready(
    State(state): State<HealthState>,
) -> Result<Json<serde_json::Value>, axum::http::StatusCode> {
    state
        .store
        .health()
        .await
        .map_err(|_| axum::http::StatusCode::SERVICE_UNAVAILABLE)?;
    Ok(Json(json!({"status": "ready"})))
}

impl Config {
    fn from_env() -> Result<Self> {
        let poll_ms: u64 = parse_or("MATCHMAKER_POLL_INTERVAL_MS", "250")?;
        let reservation_seconds: u64 = parse_or("MATCH_RESERVATION_TTL_SECONDS", "30")?;
        if poll_ms == 0 || reservation_seconds < 15 {
            bail!("matchmaker intervals are invalid");
        }
        Ok(Self {
            bind_addr: parse_or("FLOW_MATCHMAKER_BIND_ADDR", "0.0.0.0:8081")?,
            database_url: required("DATABASE_URL")?,
            database_max_connections: parse_or("DATABASE_MAX_CONNECTIONS", "10")?,
            migrate_on_start: parse_or("MIGRATE_ON_START", "false")?,
            livekit: LiveKitClient::new(
                &required("LIVEKIT_URL")?,
                required("LIVEKIT_API_KEY")?,
                required("LIVEKIT_API_SECRET")?,
            )?,
            poll_interval: Duration::from_millis(poll_ms),
            reservation_ttl: Duration::from_secs(reservation_seconds),
        })
    }
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
