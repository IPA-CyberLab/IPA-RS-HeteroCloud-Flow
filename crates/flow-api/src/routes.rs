use std::{collections::HashMap, net::SocketAddr, sync::Arc, time::Duration};

use axum::{
    Json, Router,
    body::Body,
    extract::{ConnectInfo, DefaultBodyLimit, FromRequestParts, Path, Query, State},
    http::{HeaderMap, HeaderValue, Request, StatusCode, header::HeaderName, request::Parts},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{delete, get, post, put},
};
use chrono::Utc;
use flow_auth::{PROVIDER_DELETE_ACTION, PrincipalAuthenticator, ProviderAuthenticator};
use flow_domain::{
    FlowRoom, MatchAssignment, MatchmakingTicket, NewAuditEvent, NewRoom, NewTicket, NewUsageEvent,
    PrincipalContext, RoomState, SIGNALING_CONNECTION_STALE_AFTER, ServiceInstanceDelete,
    ServiceInstanceReconcile, ServiceInstanceStatus, SessionMode,
};
use flow_livekit::LiveKitClient;
use flow_rate_limit::{IpRateLimiter, RateLimitDecision, RateLimitPolicy, TrustedProxies};
use flow_store::PgStore;
use flow_turn::{TurnCredentialIssuer, TurnCredentials};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tower_http::trace::TraceLayer;
use tracing::warn;
use utoipa::{Modify, OpenApi, ToSchema};
use utoipa_swagger_ui::SwaggerUi;
use uuid::Uuid;

use crate::coturn_metrics::{CoturnMetricsClient, LiveKitMetricsClient};
use crate::error::{ApiError, ErrorBody, ErrorEnvelope};

const REQUEST_ID_HEADER: HeaderName = HeaderName::from_static("x-request-id");
const IDEMPOTENCY_KEY_HEADER: HeaderName = HeaderName::from_static("idempotency-key");
const RATE_LIMIT_HEADER: HeaderName = HeaderName::from_static("x-ratelimit-limit");
const RATE_LIMIT_REMAINING_HEADER: HeaderName = HeaderName::from_static("x-ratelimit-remaining");
const RATE_LIMIT_RESET_HEADER: HeaderName = HeaderName::from_static("x-ratelimit-reset");

#[derive(Clone)]
pub struct AppState {
    pub store: PgStore,
    pub principal_auth: PrincipalAuthenticator,
    pub provider_auth: ProviderAuthenticator,
    pub livekit: LiveKitClient,
    pub coturn_metrics: CoturnMetricsClient,
    pub livekit_metrics: LiveKitMetricsClient,
    pub api_urls: Vec<String>,
    pub livekit_ws_urls: Vec<String>,
    pub signaling_urls: Vec<String>,
    pub turn: TurnCredentialIssuer,
    pub participant_token_ttl: Duration,
    pub rate_limiter: Arc<IpRateLimiter>,
    pub trusted_proxies: TrustedProxies,
}

#[derive(Clone)]
struct AdmittedPrincipal(PrincipalContext);

pub fn router(state: AppState) -> Router {
    let api_document = public_openapi(&state.api_urls);
    let public_routes = Router::new()
        .route("/v1/service-overview", get(service_overview))
        .route(
            "/v1/queues/{queue_name}/tickets",
            post(create_ticket).get(list_tickets),
        )
        .route("/v1/tickets/{ticket_id}", get(get_ticket))
        .route("/v1/tickets/{ticket_id}", delete(cancel_ticket))
        .route("/v1/rooms", post(create_room).get(list_rooms))
        .route("/v1/rooms/{room_id}", get(get_room))
        .route("/v1/rooms/{room_id}/join", post(join_room))
        .route("/v1/turn-credentials", post(issue_turn_credentials))
        .route_layer(middleware::from_fn_with_state(
            state.clone(),
            enforce_ip_rate_limit,
        ));

    Router::new()
        .route("/health/live", get(live))
        .route("/health/ready", get(ready))
        .route(
            "/internal/v1/service-instances/{service_instance_id}",
            put(reconcile_service_instance).delete(delete_service_instance),
        )
        .merge(public_routes)
        .merge(SwaggerUi::new("/docs").url("/openapi.json", api_document))
        .layer(DefaultBodyLimit::max(256 * 1024))
        .layer(TraceLayer::new_for_http())
        .with_state(state)
}

async fn live() -> impl IntoResponse {
    Json(json!({"status": "ok"}))
}

async fn ready(State(state): State<AppState>) -> Result<impl IntoResponse, ApiError> {
    state.store.health().await?;
    state
        .rate_limiter
        .ping()
        .await
        .map_err(|_| ApiError::rate_limit_unavailable())?;
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
    let system_decision = match state.rate_limiter.check(client_ip).await {
        Ok(decision) => decision,
        Err(error) => {
            warn!(%error, %client_ip, "IP rate-limit backend is unavailable");
            return ApiError::rate_limit_unavailable().into_response();
        }
    };
    if !system_decision.allowed {
        return rate_limited_response(system_decision);
    }

    let mut effective_decision = system_decision;
    if let Ok(principal) = state.principal_auth.authenticate_headers(request.headers()) {
        let service_rate_limit = match state
            .store
            .ready_service_rate_limit(
                principal.organization_id,
                principal.project_id,
                principal.service_instance_id,
            )
            .await
        {
            Ok(rate_limit) => rate_limit,
            Err(error) => {
                warn!(%error, %client_ip, "failed to load service IP rate limit");
                return ApiError::rate_limit_unavailable().into_response();
            }
        };
        if let Some(service_rate_limit) = service_rate_limit {
            let policy = match RateLimitPolicy::new(
                service_rate_limit.requests_per_second,
                service_rate_limit.burst,
            ) {
                Ok(policy) => policy,
                Err(error) => {
                    warn!(%error, service_instance_id = %principal.service_instance_id, "invalid service IP rate limit");
                    return ApiError::rate_limit_unavailable().into_response();
                }
            };
            effective_decision = match state
                .rate_limiter
                .check_service(principal.service_instance_id, client_ip, policy)
                .await
            {
                Ok(decision) => decision,
                Err(error) => {
                    warn!(%error, %client_ip, service_instance_id = %principal.service_instance_id, "service IP rate-limit backend is unavailable");
                    return ApiError::rate_limit_unavailable().into_response();
                }
            };
            if !effective_decision.allowed {
                return rate_limited_response(effective_decision);
            }
            request
                .extensions_mut()
                .insert(AdmittedPrincipal(principal));
        }
    }

    let mut response = next.run(request).await;
    insert_rate_limit_headers(&mut response, effective_decision);
    response
}

fn rate_limited_response(decision: RateLimitDecision) -> Response {
    let mut response = ApiError::rate_limited().into_response();
    insert_rate_limit_headers(&mut response, decision);
    let retry_after = decision.retry_after_seconds.max(1).to_string();
    if let Ok(value) = HeaderValue::from_str(&retry_after) {
        response.headers_mut().insert("retry-after", value);
    }
    response
}

fn insert_rate_limit_headers(response: &mut Response, decision: RateLimitDecision) {
    for (name, value) in [
        (&RATE_LIMIT_HEADER, decision.limit.to_string()),
        (&RATE_LIMIT_REMAINING_HEADER, decision.remaining.to_string()),
        (
            &RATE_LIMIT_RESET_HEADER,
            decision.reset_after_seconds.to_string(),
        ),
    ] {
        if let Ok(value) = HeaderValue::from_str(&value) {
            response.headers_mut().insert(name, value);
        }
    }
}

/// Read current service capacity, usage, and connection endpoints.
#[utoipa::path(
    get,
    path = "/v1/service-overview",
    tag = "Overview",
    security(("flow_principal" = [], "flow_timestamp" = [], "flow_signature" = [])),
    responses(
        (status = 200, description = "Current Flow service overview", body = ServiceOverviewResponse),
        (status = 401, description = "Signed access context is missing or invalid", body = ErrorEnvelope),
        (status = 403, description = "The context lacks flow.metrics.read", body = ErrorEnvelope),
        (status = 429, description = "Source IP token bucket is empty", body = ErrorEnvelope,
            headers(
                ("Retry-After" = u64, description = "Seconds before retrying"),
                ("X-RateLimit-Limit" = u32, description = "Configured burst capacity"),
                ("X-RateLimit-Remaining" = u32, description = "Tokens remaining"),
                ("X-RateLimit-Reset" = u64, description = "Seconds until the bucket is full")
            )
        ),
        (status = 503, description = "Request admission backend is unavailable", body = ErrorEnvelope)
    )
)]
async fn service_overview(
    State(state): State<AppState>,
    context: RequestContext,
) -> Result<impl IntoResponse, ApiError> {
    context.require("flow.metrics.read")?;
    let snapshot = state
        .store
        .service_overview_snapshot(
            context.principal.organization_id,
            context.principal.project_id,
            context.principal.service_instance_id,
            SIGNALING_CONNECTION_STALE_AFTER,
        )
        .await?;
    let (livekit_result, coturn_result, livekit_metrics_result) = tokio::join!(
        state
            .livekit
            .participant_count(&snapshot.provider_room_names),
        state
            .coturn_metrics
            .scrape(context.principal.service_instance_id),
        state
            .livekit_metrics
            .scrape(context.principal.service_instance_id),
    );
    let sfu_participants =
        livekit_result.map_err(|error| ApiError::dependency(error.to_string()))?;
    let concurrent_connections = sfu_participants
        .checked_add(snapshot.p2p_connections)
        .ok_or_else(|| ApiError::internal("concurrent connection count overflowed"))?;
    let mut ingress_bytes = snapshot.ingress_bytes;
    let mut egress_bytes = snapshot.egress_bytes;
    let mut turn_allocations = None;
    match coturn_result {
        Ok(Some(metrics)) => {
            let measured_ingress = i64::try_from(metrics.ingress_bytes)
                .map_err(|_| ApiError::internal("coturn ingress byte count exceeded i64"))?;
            let measured_egress = i64::try_from(metrics.egress_bytes)
                .map_err(|_| ApiError::internal("coturn egress byte count exceeded i64"))?;
            ingress_bytes = ingress_bytes
                .checked_add(measured_ingress)
                .ok_or_else(|| ApiError::internal("ingress byte count overflowed"))?;
            egress_bytes = egress_bytes
                .checked_add(measured_egress)
                .ok_or_else(|| ApiError::internal("egress byte count overflowed"))?;
            turn_allocations = metrics.allocations;
        }
        Ok(None) => {}
        Err(error) => {
            warn!(
                %error,
                service_instance_id = %context.principal.service_instance_id,
                "coturn metrics scrape failed; returning database usage only"
            );
        }
    }
    match livekit_metrics_result {
        Ok(Some(metrics)) => {
            let measured_ingress = i64::try_from(metrics.ingress_bytes)
                .map_err(|_| ApiError::internal("LiveKit ingress byte count exceeded i64"))?;
            let measured_egress = i64::try_from(metrics.egress_bytes)
                .map_err(|_| ApiError::internal("LiveKit egress byte count exceeded i64"))?;
            ingress_bytes = ingress_bytes
                .checked_add(measured_ingress)
                .ok_or_else(|| ApiError::internal("ingress byte count overflowed"))?;
            egress_bytes = egress_bytes
                .checked_add(measured_egress)
                .ok_or_else(|| ApiError::internal("egress byte count overflowed"))?;
        }
        Ok(None) => {}
        Err(error) => {
            warn!(
                %error,
                service_instance_id = %context.principal.service_instance_id,
                "LiveKit metrics scrape failed; returning other usage measurements"
            );
        }
    }
    let transferred_bytes = ingress_bytes
        .checked_add(egress_bytes)
        .ok_or_else(|| ApiError::internal("transferred byte count overflowed"))?;

    Ok(Json(ServiceOverviewResponse {
        measured_at: Utc::now(),
        active_rooms: snapshot.active_rooms,
        concurrent_connections,
        sfu_participants,
        p2p_connections: snapshot.p2p_connections,
        ingress_bytes,
        egress_bytes,
        transferred_bytes,
        turn_allocations,
        endpoints: ServiceEndpoints {
            api: state.api_urls.clone(),
            signaling: state.signaling_urls.clone(),
            livekit: state.livekit_ws_urls.clone(),
            stun: state.turn.stun_urls(),
            turn: state.turn.urls().to_vec(),
        },
        room_limit: Some(u64::from(snapshot.room_limit)),
    }))
}

async fn reconcile_service_instance(
    State(state): State<AppState>,
    Path(service_instance_id): Path<Uuid>,
    headers: HeaderMap,
    Json(request): Json<ReconcileRequest>,
) -> Result<impl IntoResponse, ApiError> {
    let claims = state.provider_auth.authenticate_headers(&headers)?;
    let idempotency_key = provider_idempotency_key(&headers)?;
    if claims.jwt_id != idempotency_key
        || claims.service_instance_id != service_instance_id
        || claims.generation != request.generation
    {
        return Err(ApiError::forbidden());
    }
    let outcome = state
        .store
        .reconcile_service_instance(ServiceInstanceReconcile {
            jwt_id: claims.jwt_id,
            organization_id: claims.organization_id,
            project_id: claims.project_id,
            service_instance_id: claims.service_instance_id,
            principal_id: claims.subject,
            generation: request.generation,
            name: request.name,
            spec: request.spec,
        })
        .await?;
    let status = if outcome.created {
        StatusCode::ACCEPTED
    } else {
        StatusCode::OK
    };
    Ok((
        status,
        Json(ReconcileResponse {
            operation_id: outcome.operation_id,
            status: outcome.status,
        }),
    ))
}

async fn delete_service_instance(
    State(state): State<AppState>,
    Path(service_instance_id): Path<Uuid>,
    Query(query): Query<DeleteServiceInstanceQuery>,
    headers: HeaderMap,
) -> Result<impl IntoResponse, ApiError> {
    let claims = state
        .provider_auth
        .authenticate_headers_for_action(&headers, PROVIDER_DELETE_ACTION)?;
    let idempotency_key = provider_idempotency_key(&headers)?;
    if claims.jwt_id != idempotency_key
        || claims.service_instance_id != service_instance_id
        || claims.generation != query.generation
    {
        return Err(ApiError::forbidden());
    }
    let command = ServiceInstanceDelete {
        jwt_id: claims.jwt_id,
        organization_id: claims.organization_id,
        project_id: claims.project_id,
        service_instance_id: claims.service_instance_id,
        principal_id: claims.subject,
        generation: query.generation,
    };
    let preparation = state
        .store
        .prepare_delete_service_instance(&command)
        .await?;
    if preparation.completed {
        return Ok((
            StatusCode::OK,
            Json(ReconcileResponse {
                operation_id: preparation.operation_id,
                status: preparation.status,
            }),
        ));
    }

    state
        .livekit
        .delete_rooms(&preparation.provider_room_names)
        .await
        .map_err(|error| ApiError::dependency(error.to_string()))?;
    let outcome = state
        .store
        .complete_delete_service_instance(&command, preparation.operation_id)
        .await?;
    let response_status = if outcome.completed_now {
        StatusCode::ACCEPTED
    } else {
        StatusCode::OK
    };
    Ok((
        response_status,
        Json(ReconcileResponse {
            operation_id: outcome.operation_id,
            status: outcome.status,
        }),
    ))
}

fn provider_idempotency_key(headers: &HeaderMap) -> Result<Uuid, ApiError> {
    headers
        .get(&IDEMPOTENCY_KEY_HEADER)
        .ok_or_else(|| ApiError::bad_request("idempotency-key header is required"))?
        .to_str()
        .map_err(|_| ApiError::bad_request("idempotency-key header is invalid"))?
        .parse::<Uuid>()
        .map_err(|_| ApiError::bad_request("idempotency-key must be a UUID"))
}

/// Create a matchmaking ticket in a named queue.
#[utoipa::path(
    post,
    path = "/v1/queues/{queue_name}/tickets",
    tag = "Matchmaking",
    security(("flow_principal" = [], "flow_timestamp" = [], "flow_signature" = [])),
    params(("queue_name" = String, Path, max_length = 96, description = "Queue name")),
    request_body = CreateTicketRequest,
    responses(
        (status = 201, description = "Ticket created", body = MatchmakingTicket),
        (status = 400, description = "Ticket request is invalid", body = ErrorEnvelope),
        (status = 401, description = "Signed access context is missing or invalid", body = ErrorEnvelope),
        (status = 403, description = "The context lacks flow.queue.write", body = ErrorEnvelope),
        (status = 429, description = "Source IP request limit exceeded", body = ErrorEnvelope,
            headers(("Retry-After" = u64), ("X-RateLimit-Limit" = u32), ("X-RateLimit-Remaining" = u32), ("X-RateLimit-Reset" = u64))
        ),
        (status = 503, description = "Request admission backend is unavailable", body = ErrorEnvelope)
    )
)]
async fn create_ticket(
    State(state): State<AppState>,
    context: RequestContext,
    Path(queue_name): Path<String>,
    Json(request): Json<CreateTicketRequest>,
) -> Result<impl IntoResponse, ApiError> {
    context.require("flow.queue.write")?;
    let ttl = request.ttl_seconds.unwrap_or(300);
    if !(10..=3600).contains(&ttl) {
        return Err(ApiError::bad_request(
            "ttl_seconds must be between 10 and 3600",
        ));
    }
    let ticket = state
        .store
        .create_ticket(NewTicket {
            id: Uuid::now_v7(),
            organization_id: context.principal.organization_id,
            project_id: context.principal.project_id,
            service_instance_id: context.principal.service_instance_id,
            principal_id: context.principal.principal_id,
            queue_name,
            mode: request.mode,
            match_size: request.match_size,
            attributes: request.attributes,
            expires_at: Utc::now() + chrono::Duration::seconds(i64::from(ttl)),
        })
        .await?;
    audit(
        &state,
        &context,
        "flow.ticket.create",
        "matchmaking_ticket",
        Some(ticket.id.to_string()),
        json!({"queue": ticket.queue_name, "mode": ticket.mode}),
    )
    .await;
    usage(
        &state,
        &context,
        "matchmaking_ticket_created",
        Some(ticket.id.to_string()),
        format!("ticket-created:{}", ticket.id),
        json!({"queue": ticket.queue_name, "mode": ticket.mode}),
    )
    .await;
    Ok((StatusCode::CREATED, Json(ticket)))
}

/// List matchmaking tickets in one queue and service scope.
#[utoipa::path(
    get,
    path = "/v1/queues/{queue_name}/tickets",
    tag = "Matchmaking",
    security(("flow_principal" = [], "flow_timestamp" = [], "flow_signature" = [])),
    params(
        ("queue_name" = String, Path, max_length = 96, description = "Queue name"),
        ("limit" = Option<i64>, Query, minimum = 1, maximum = 200, description = "Maximum items; defaults to 50")
    ),
    responses(
        (status = 200, description = "Scoped ticket list", body = TicketListResponse),
        (status = 401, description = "Signed access context is missing or invalid", body = ErrorEnvelope),
        (status = 403, description = "The context lacks flow.queue.read", body = ErrorEnvelope),
        (status = 429, description = "Source IP request limit exceeded", body = ErrorEnvelope,
            headers(("Retry-After" = u64), ("X-RateLimit-Limit" = u32), ("X-RateLimit-Remaining" = u32), ("X-RateLimit-Reset" = u64))
        ),
        (status = 503, description = "Request admission backend is unavailable", body = ErrorEnvelope)
    )
)]
async fn list_tickets(
    State(state): State<AppState>,
    context: RequestContext,
    Path(queue_name): Path<String>,
    Query(query): Query<ListQuery>,
) -> Result<impl IntoResponse, ApiError> {
    context.require("flow.queue.read")?;
    let tickets = state
        .store
        .list_tickets(
            context.principal.organization_id,
            context.principal.project_id,
            context.principal.service_instance_id,
            &queue_name,
            query.limit.unwrap_or(50),
        )
        .await?;
    Ok(Json(ListResponse { items: tickets }))
}

/// Read a matchmaking ticket and its assignment.
#[utoipa::path(
    get,
    path = "/v1/tickets/{ticket_id}",
    tag = "Matchmaking",
    security(("flow_principal" = [], "flow_timestamp" = [], "flow_signature" = [])),
    params(("ticket_id" = Uuid, Path, description = "Ticket ID")),
    responses(
        (status = 200, description = "Ticket and optional assignment", body = TicketResponse),
        (status = 401, description = "Signed access context is missing or invalid", body = ErrorEnvelope),
        (status = 403, description = "The context lacks flow.queue.read", body = ErrorEnvelope),
        (status = 404, description = "Ticket was not found in this service scope", body = ErrorEnvelope),
        (status = 429, description = "Source IP request limit exceeded", body = ErrorEnvelope,
            headers(("Retry-After" = u64), ("X-RateLimit-Limit" = u32), ("X-RateLimit-Remaining" = u32), ("X-RateLimit-Reset" = u64))
        ),
        (status = 503, description = "Request admission backend is unavailable", body = ErrorEnvelope)
    )
)]
async fn get_ticket(
    State(state): State<AppState>,
    context: RequestContext,
    Path(ticket_id): Path<Uuid>,
) -> Result<impl IntoResponse, ApiError> {
    context.require("flow.queue.read")?;
    let ticket = state
        .store
        .get_ticket(
            context.principal.organization_id,
            context.principal.project_id,
            context.principal.service_instance_id,
            ticket_id,
        )
        .await?;
    let assignment = state
        .store
        .assignment_for_ticket(
            context.principal.organization_id,
            context.principal.project_id,
            context.principal.service_instance_id,
            ticket_id,
        )
        .await?;
    Ok(Json(TicketResponse { ticket, assignment }))
}

/// Cancel the caller's queued matchmaking ticket.
#[utoipa::path(
    delete,
    path = "/v1/tickets/{ticket_id}",
    tag = "Matchmaking",
    security(("flow_principal" = [], "flow_timestamp" = [], "flow_signature" = [])),
    params(("ticket_id" = Uuid, Path, description = "Ticket ID")),
    responses(
        (status = 200, description = "Cancelled ticket", body = MatchmakingTicket),
        (status = 401, description = "Signed access context is missing or invalid", body = ErrorEnvelope),
        (status = 403, description = "The context lacks flow.queue.write", body = ErrorEnvelope),
        (status = 404, description = "Ticket was not found in this service scope", body = ErrorEnvelope),
        (status = 409, description = "Ticket can no longer be cancelled", body = ErrorEnvelope),
        (status = 429, description = "Source IP request limit exceeded", body = ErrorEnvelope,
            headers(("Retry-After" = u64), ("X-RateLimit-Limit" = u32), ("X-RateLimit-Remaining" = u32), ("X-RateLimit-Reset" = u64))
        ),
        (status = 503, description = "Request admission backend is unavailable", body = ErrorEnvelope)
    )
)]
async fn cancel_ticket(
    State(state): State<AppState>,
    context: RequestContext,
    Path(ticket_id): Path<Uuid>,
) -> Result<impl IntoResponse, ApiError> {
    context.require("flow.queue.write")?;
    let ticket = state
        .store
        .cancel_ticket(
            context.principal.organization_id,
            context.principal.project_id,
            context.principal.service_instance_id,
            context.principal.principal_id,
            ticket_id,
        )
        .await?;
    audit(
        &state,
        &context,
        "flow.ticket.cancel",
        "matchmaking_ticket",
        Some(ticket.id.to_string()),
        json!({}),
    )
    .await;
    Ok(Json(ticket))
}

/// Create a P2P or SFU room.
#[utoipa::path(
    post,
    path = "/v1/rooms",
    tag = "Rooms",
    security(("flow_principal" = [], "flow_timestamp" = [], "flow_signature" = [])),
    request_body = CreateRoomRequest,
    responses(
        (status = 201, description = "Ready room", body = FlowRoom),
        (status = 400, description = "Room request is invalid", body = ErrorEnvelope),
        (status = 401, description = "Signed access context is missing or invalid", body = ErrorEnvelope),
        (status = 403, description = "The context lacks flow.room.create", body = ErrorEnvelope),
        (status = 409, description = "Configured concurrent room limit was reached", body = ErrorEnvelope),
        (status = 429, description = "Source IP request limit exceeded", body = ErrorEnvelope,
            headers(("Retry-After" = u64), ("X-RateLimit-Limit" = u32), ("X-RateLimit-Remaining" = u32), ("X-RateLimit-Reset" = u64))
        ),
        (status = 502, description = "SFU provider is unavailable", body = ErrorEnvelope),
        (status = 503, description = "Request admission backend is unavailable", body = ErrorEnvelope)
    )
)]
async fn create_room(
    State(state): State<AppState>,
    context: RequestContext,
    Json(request): Json<CreateRoomRequest>,
) -> Result<impl IntoResponse, ApiError> {
    context.require("flow.room.create")?;
    let id = Uuid::now_v7();
    let room_name = request.name.unwrap_or_else(|| format!("room-{id}"));
    let provider_room_name = (request.mode == SessionMode::Sfu)
        .then(|| livekit_room_name(context.principal.service_instance_id, id));
    let new_room = NewRoom {
        id,
        organization_id: context.principal.organization_id,
        project_id: context.principal.project_id,
        service_instance_id: context.principal.service_instance_id,
        name: room_name.clone(),
        provider_room_name: provider_room_name.clone(),
        mode: request.mode,
        state: RoomState::Provisioning,
        max_participants: request.max_participants,
        metadata: request.metadata.clone(),
    };
    new_room
        .validate()
        .map_err(|error| ApiError::bad_request(error.to_string()))?;
    let now = Utc::now();
    let preview = FlowRoom {
        id,
        organization_id: context.principal.organization_id,
        project_id: context.principal.project_id,
        service_instance_id: context.principal.service_instance_id,
        name: room_name.clone(),
        provider_room_name: provider_room_name.clone(),
        mode: request.mode,
        state: RoomState::Provisioning,
        max_participants: request.max_participants,
        metadata: request.metadata.clone(),
        failure_reason: None,
        created_at: now,
        updated_at: now,
    };
    state.store.create_room(new_room).await?;
    if request.mode == SessionMode::Sfu
        && let Err(error) = state.livekit.create_room(&preview).await
    {
        state.store.fail_room(id, &error.to_string()).await?;
        return Err(ApiError::dependency(error.to_string()));
    }
    let room = state.store.activate_room(id).await?;
    audit(
        &state,
        &context,
        "flow.room.create",
        "room",
        Some(room.id.to_string()),
        json!({"mode": room.mode}),
    )
    .await;
    usage(
        &state,
        &context,
        "room_created",
        Some(room.id.to_string()),
        format!("room-created:{}", room.id),
        json!({"mode": room.mode}),
    )
    .await;
    Ok((StatusCode::CREATED, Json(room)))
}

/// List rooms in the signed service scope.
#[utoipa::path(
    get,
    path = "/v1/rooms",
    tag = "Rooms",
    security(("flow_principal" = [], "flow_timestamp" = [], "flow_signature" = [])),
    params(("limit" = Option<i64>, Query, minimum = 1, maximum = 200, description = "Maximum items; defaults to 50")),
    responses(
        (status = 200, description = "Scoped room list", body = RoomListResponse),
        (status = 401, description = "Signed access context is missing or invalid", body = ErrorEnvelope),
        (status = 403, description = "The context lacks flow.room.read", body = ErrorEnvelope),
        (status = 429, description = "Source IP request limit exceeded", body = ErrorEnvelope,
            headers(("Retry-After" = u64), ("X-RateLimit-Limit" = u32), ("X-RateLimit-Remaining" = u32), ("X-RateLimit-Reset" = u64))
        ),
        (status = 503, description = "Request admission backend is unavailable", body = ErrorEnvelope)
    )
)]
async fn list_rooms(
    State(state): State<AppState>,
    context: RequestContext,
    Query(query): Query<ListQuery>,
) -> Result<impl IntoResponse, ApiError> {
    context.require("flow.room.read")?;
    let rooms = state
        .store
        .list_rooms(
            context.principal.organization_id,
            context.principal.project_id,
            context.principal.service_instance_id,
            query.limit.unwrap_or(50),
        )
        .await?;
    Ok(Json(ListResponse { items: rooms }))
}

/// Read one room in the signed service scope.
#[utoipa::path(
    get,
    path = "/v1/rooms/{room_id}",
    tag = "Rooms",
    security(("flow_principal" = [], "flow_timestamp" = [], "flow_signature" = [])),
    params(("room_id" = Uuid, Path, description = "Room ID")),
    responses(
        (status = 200, description = "Room", body = FlowRoom),
        (status = 401, description = "Signed access context is missing or invalid", body = ErrorEnvelope),
        (status = 403, description = "The context lacks flow.room.read", body = ErrorEnvelope),
        (status = 404, description = "Room was not found in this service scope", body = ErrorEnvelope),
        (status = 429, description = "Source IP request limit exceeded", body = ErrorEnvelope,
            headers(("Retry-After" = u64), ("X-RateLimit-Limit" = u32), ("X-RateLimit-Remaining" = u32), ("X-RateLimit-Reset" = u64))
        ),
        (status = 503, description = "Request admission backend is unavailable", body = ErrorEnvelope)
    )
)]
async fn get_room(
    State(state): State<AppState>,
    context: RequestContext,
    Path(room_id): Path<Uuid>,
) -> Result<impl IntoResponse, ApiError> {
    context.require("flow.room.read")?;
    let room = state
        .store
        .get_room(
            context.principal.organization_id,
            context.principal.project_id,
            context.principal.service_instance_id,
            room_id,
        )
        .await?;
    Ok(Json(room))
}

/// Issue mode-specific short-lived connection data for a room.
#[utoipa::path(
    post,
    path = "/v1/rooms/{room_id}/join",
    tag = "Connectivity",
    security(("flow_principal" = [], "flow_timestamp" = [], "flow_signature" = [])),
    params(("room_id" = Uuid, Path, description = "Room ID")),
    request_body = JoinRoomRequest,
    responses(
        (status = 200, description = "P2P signaling or SFU connection data", body = JoinRoomResponse),
        (status = 400, description = "Join request is invalid", body = ErrorEnvelope),
        (status = 401, description = "Signed access context is missing, invalid, or nearly expired", body = ErrorEnvelope),
        (status = 403, description = "The context lacks flow.room.join", body = ErrorEnvelope),
        (status = 404, description = "Room was not found in this service scope", body = ErrorEnvelope),
        (status = 409, description = "Room is not ready", body = ErrorEnvelope),
        (status = 429, description = "Source IP request limit exceeded", body = ErrorEnvelope,
            headers(("Retry-After" = u64), ("X-RateLimit-Limit" = u32), ("X-RateLimit-Remaining" = u32), ("X-RateLimit-Reset" = u64))
        ),
        (status = 502, description = "Connection provider is unavailable", body = ErrorEnvelope),
        (status = 503, description = "Request admission backend is unavailable", body = ErrorEnvelope)
    )
)]
async fn join_room(
    State(state): State<AppState>,
    context: RequestContext,
    Path(room_id): Path<Uuid>,
    Json(request): Json<JoinRoomRequest>,
) -> Result<impl IntoResponse, ApiError> {
    context.require("flow.room.join")?;
    let room = state
        .store
        .get_room(
            context.principal.organization_id,
            context.principal.project_id,
            context.principal.service_instance_id,
            room_id,
        )
        .await?;
    if room.state != RoomState::Ready {
        return Err(ApiError::conflict("room is not ready"));
    }

    let identity = format!(
        "{}:{}:{}:{}",
        context.principal.organization_id,
        context.principal.project_id,
        context.principal.service_instance_id,
        context.principal.principal_id
    );
    let remaining = (context.principal.expires_at - Utc::now())
        .to_std()
        .map_err(|_| ApiError::from(flow_auth::AuthError::InvalidToken))?;
    if remaining < Duration::from_secs(10) {
        return Err(flow_auth::AuthError::InvalidToken.into());
    }
    let turn = state
        .turn
        .issue_with_ttl(&identity, remaining)
        .map_err(|error| ApiError::bad_request(error.to_string()))?;
    let connection = match room.mode {
        SessionMode::P2p => RoomConnection::P2p {
            urls: signaling_room_urls(&state.signaling_urls, room.id),
            turn,
        },
        SessionMode::Sfu => {
            let ttl = state.participant_token_ttl.min(remaining);
            let token = state
                .livekit
                .issue_participant_token(
                    &room,
                    &context.principal,
                    request.display_name.as_deref().unwrap_or(""),
                    request.can_publish,
                    request.can_subscribe,
                    ttl,
                )
                .map_err(|error| ApiError::dependency(error.to_string()))?;
            RoomConnection::Sfu {
                urls: state.livekit_ws_urls.clone(),
                token,
                turn,
            }
        }
    };
    audit(
        &state,
        &context,
        "flow.room.join",
        "room",
        Some(room.id.to_string()),
        json!({"mode": room.mode}),
    )
    .await;
    usage(
        &state,
        &context,
        "room_join_credentials_issued",
        Some(room.id.to_string()),
        format!(
            "participant-token:{}:{}",
            room.id, context.principal.token_id
        ),
        json!({"mode": room.mode}),
    )
    .await;
    Ok(Json(JoinRoomResponse {
        room_id: room.id,
        mode: room.mode,
        connection,
    }))
}

/// Issue short-lived TURN REST credentials for the signed principal.
#[utoipa::path(
    post,
    path = "/v1/turn-credentials",
    tag = "Connectivity",
    security(("flow_principal" = [], "flow_timestamp" = [], "flow_signature" = [])),
    responses(
        (status = 200, description = "TURN URLs and short-lived credentials", body = TurnCredentials),
        (status = 400, description = "Credential request is invalid", body = ErrorEnvelope),
        (status = 401, description = "Signed access context is missing, invalid, or nearly expired", body = ErrorEnvelope),
        (status = 403, description = "The context lacks flow.turn.issue", body = ErrorEnvelope),
        (status = 429, description = "Source IP request limit exceeded", body = ErrorEnvelope,
            headers(("Retry-After" = u64), ("X-RateLimit-Limit" = u32), ("X-RateLimit-Remaining" = u32), ("X-RateLimit-Reset" = u64))
        ),
        (status = 503, description = "Request admission backend is unavailable", body = ErrorEnvelope)
    )
)]
async fn issue_turn_credentials(
    State(state): State<AppState>,
    context: RequestContext,
) -> Result<impl IntoResponse, ApiError> {
    context.require("flow.turn.issue")?;
    let identity = format!(
        "{}:{}:{}:{}",
        context.principal.organization_id,
        context.principal.project_id,
        context.principal.service_instance_id,
        context.principal.principal_id
    );
    let remaining = (context.principal.expires_at - Utc::now())
        .to_std()
        .map_err(|_| ApiError::from(flow_auth::AuthError::InvalidToken))?;
    if remaining < Duration::from_secs(10) {
        return Err(flow_auth::AuthError::InvalidToken.into());
    }
    let credentials = state
        .turn
        .issue_with_ttl(&identity, remaining)
        .map_err(|error| ApiError::bad_request(error.to_string()))?;
    audit(
        &state,
        &context,
        "flow.turn.issue",
        "turn_credential",
        None,
        json!({"expires_at": credentials.expires_at}),
    )
    .await;
    Ok(Json(credentials))
}

async fn audit(
    state: &AppState,
    context: &RequestContext,
    action: &str,
    resource_type: &str,
    resource_id: Option<String>,
    details: Value,
) {
    if let Err(error) = state
        .store
        .append_audit(NewAuditEvent {
            id: Uuid::now_v7(),
            organization_id: context.principal.organization_id,
            project_id: context.principal.project_id,
            service_instance_id: context.principal.service_instance_id,
            principal_id: context.principal.principal_id,
            principal_context_id: Some(context.principal.token_id),
            request_id: context.request_id.clone(),
            action: action.into(),
            resource_type: resource_type.into(),
            resource_id,
            outcome: "allowed".into(),
            details,
        })
        .await
    {
        warn!(%error, "failed to persist audit event");
    }
}

async fn usage(
    state: &AppState,
    context: &RequestContext,
    event_type: &str,
    resource_id: Option<String>,
    idempotency_key: String,
    dimensions: Value,
) {
    if let Err(error) = state
        .store
        .record_usage(NewUsageEvent {
            id: Uuid::now_v7(),
            organization_id: context.principal.organization_id,
            project_id: context.principal.project_id,
            service_instance_id: context.principal.service_instance_id,
            principal_id: Some(context.principal.principal_id),
            event_type: event_type.into(),
            resource_id,
            quantity: 1,
            idempotency_key,
            dimensions,
            occurred_at: Utc::now(),
        })
        .await
    {
        warn!(%error, "failed to persist usage event");
    }
}

pub struct RequestContext {
    pub principal: PrincipalContext,
    pub request_id: String,
}

impl RequestContext {
    fn require(&self, permission: &str) -> Result<(), ApiError> {
        if self.principal.allows(permission) {
            Ok(())
        } else {
            Err(ApiError::forbidden())
        }
    }
}

impl FromRequestParts<AppState> for RequestContext {
    type Rejection = ApiError;

    async fn from_request_parts(
        parts: &mut Parts,
        state: &AppState,
    ) -> Result<Self, Self::Rejection> {
        let principal = if let Some(admitted) = parts.extensions.get::<AdmittedPrincipal>() {
            admitted.0.clone()
        } else {
            let principal = state.principal_auth.authenticate_headers(&parts.headers)?;
            if !state
                .store
                .service_instance_is_ready(
                    principal.organization_id,
                    principal.project_id,
                    principal.service_instance_id,
                )
                .await?
            {
                return Err(ApiError::forbidden());
            }
            principal
        };
        let request_id = parts
            .headers
            .get(&REQUEST_ID_HEADER)
            .and_then(|value| value.to_str().ok())
            .filter(|value| !value.is_empty() && value.len() <= 128)
            .map_or_else(|| Uuid::now_v7().to_string(), ToOwned::to_owned);
        Ok(Self {
            principal,
            request_id,
        })
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ReconcileRequest {
    generation: i64,
    name: String,
    spec: Value,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct DeleteServiceInstanceQuery {
    generation: i64,
}

#[derive(Serialize)]
struct ReconcileResponse {
    operation_id: Uuid,
    status: ServiceInstanceStatus,
}

#[derive(Debug, Serialize, ToSchema)]
struct ServiceOverviewResponse {
    measured_at: chrono::DateTime<Utc>,
    active_rooms: u64,
    concurrent_connections: u64,
    sfu_participants: u64,
    p2p_connections: u64,
    ingress_bytes: i64,
    egress_bytes: i64,
    transferred_bytes: i64,
    turn_allocations: Option<u64>,
    endpoints: ServiceEndpoints,
    room_limit: Option<u64>,
}

#[derive(Debug, Serialize, ToSchema)]
struct ServiceEndpoints {
    api: Vec<String>,
    signaling: Vec<String>,
    livekit: Vec<String>,
    stun: Vec<String>,
    turn: Vec<String>,
}

#[derive(Deserialize, ToSchema)]
struct CreateTicketRequest {
    mode: SessionMode,
    #[schema(minimum = 2, maximum = 100)]
    match_size: i32,
    #[serde(default = "empty_object")]
    attributes: Value,
    ttl_seconds: Option<i32>,
}

#[derive(Deserialize, ToSchema)]
struct CreateRoomRequest {
    mode: SessionMode,
    #[schema(max_length = 160)]
    name: Option<String>,
    #[serde(default = "default_max_participants")]
    #[schema(default = 16, minimum = 2, maximum = 1000)]
    max_participants: i32,
    #[serde(default = "empty_object")]
    metadata: Value,
}

#[derive(Deserialize, ToSchema)]
struct JoinRoomRequest {
    #[schema(max_length = 128)]
    display_name: Option<String>,
    #[serde(default = "default_true")]
    can_publish: bool,
    #[serde(default = "default_true")]
    can_subscribe: bool,
}

#[derive(Deserialize)]
struct ListQuery {
    limit: Option<i64>,
    #[serde(flatten)]
    _extra: HashMap<String, String>,
}

#[derive(Serialize)]
struct ListResponse<T> {
    items: Vec<T>,
}

#[derive(ToSchema)]
#[allow(dead_code)]
struct RoomListResponse {
    items: Vec<FlowRoom>,
}

#[derive(ToSchema)]
#[allow(dead_code)]
struct TicketListResponse {
    items: Vec<MatchmakingTicket>,
}

#[derive(Serialize, ToSchema)]
struct TicketResponse {
    ticket: MatchmakingTicket,
    assignment: Option<MatchAssignment>,
}

#[derive(Serialize, ToSchema)]
struct JoinRoomResponse {
    room_id: Uuid,
    mode: SessionMode,
    connection: RoomConnection,
}

#[derive(Serialize, ToSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
enum RoomConnection {
    P2p {
        urls: Vec<String>,
        turn: TurnCredentials,
    },
    Sfu {
        urls: Vec<String>,
        token: String,
        turn: TurnCredentials,
    },
}

#[derive(OpenApi)]
#[openapi(
    info(
        title = "HeteroCloud Flow Public API",
        version = env!("CARGO_PKG_VERSION"),
        description = "Public room, matchmaking, participant connection, and TURN credential API. Obtain the three short-lived X-Flow-* header values from HeteroCloud and send all three on every REST request. Every request is subject to a system source-IP ceiling; authenticated requests also share the Flow service's console-configured source-IP token bucket across all API replicas."
    ),
    paths(
        service_overview,
        create_ticket,
        list_tickets,
        get_ticket,
        cancel_ticket,
        create_room,
        list_rooms,
        get_room,
        join_room,
        issue_turn_credentials
    ),
    components(schemas(
        ErrorEnvelope,
        ErrorBody,
        ServiceOverviewResponse,
        ServiceEndpoints,
        CreateTicketRequest,
        CreateRoomRequest,
        JoinRoomRequest,
        TicketResponse,
        JoinRoomResponse,
        RoomConnection,
        MatchmakingTicket,
        MatchAssignment,
        FlowRoom,
        SessionMode,
        TurnCredentials,
        RoomListResponse,
        TicketListResponse
    )),
    tags(
        (name = "Overview", description = "Service capacity and endpoint discovery"),
        (name = "Matchmaking", description = "Queue and ticket operations"),
        (name = "Rooms", description = "P2P and SFU room lifecycle"),
        (name = "Connectivity", description = "Short-lived participant and TURN connection data")
    ),
    modifiers(&FlowSecurity)
)]
struct PublicApiDoc;

struct FlowSecurity;

impl Modify for FlowSecurity {
    fn modify(&self, openapi: &mut utoipa::openapi::OpenApi) {
        use utoipa::openapi::security::{ApiKey, ApiKeyValue, SecurityScheme};

        let components = openapi
            .components
            .get_or_insert_with(utoipa::openapi::Components::new);
        components.add_security_scheme(
            "flow_principal",
            SecurityScheme::ApiKey(ApiKey::Header(ApiKeyValue::with_description(
                "X-Flow-Principal",
                "Base64url-encoded signed principal context returned by HeteroCloud",
            ))),
        );
        components.add_security_scheme(
            "flow_timestamp",
            SecurityScheme::ApiKey(ApiKey::Header(ApiKeyValue::with_description(
                "X-Flow-Timestamp",
                "Issued-at timestamp returned with the signed principal context",
            ))),
        );
        components.add_security_scheme(
            "flow_signature",
            SecurityScheme::ApiKey(ApiKey::Header(ApiKeyValue::with_description(
                "X-Flow-Signature",
                "HMAC signature returned with the signed principal context",
            ))),
        );
    }
}

fn public_openapi(api_urls: &[String]) -> utoipa::openapi::OpenApi {
    let mut document = PublicApiDoc::openapi();
    document.servers = Some(
        api_urls
            .iter()
            .cloned()
            .map(utoipa::openapi::Server::new)
            .collect(),
    );
    document
}

fn empty_object() -> Value {
    json!({})
}

fn signaling_room_urls(base_urls: &[String], room_id: Uuid) -> Vec<String> {
    base_urls
        .iter()
        .map(|base_url| format!("{base_url}/v1/signal/{room_id}"))
        .collect()
}

fn livekit_room_name(service_instance_id: Uuid, room_id: Uuid) -> String {
    format!("flow-{service_instance_id}-{room_id}")
}

const fn default_max_participants() -> i32 {
    16
}

const fn default_true() -> bool {
    true
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeSet, env, sync::Arc, time::Duration};

    use axum::{
        body::{Body, to_bytes},
        http::{Request, StatusCode, header::AUTHORIZATION},
    };
    use flow_auth::{
        PROVIDER_DELETE_ACTION, PROVIDER_RECONCILE_ACTION, PrincipalAuthenticator, ProviderClaims,
    };
    use flow_domain::PrincipalContext;
    use flow_livekit::LiveKitClient;
    use flow_rate_limit::{IpRateLimiter, RateLimitPolicy, RedisBackend, TrustedProxies};
    use flow_store::PgStore;
    use flow_turn::TurnCredentialIssuer;
    use jsonwebtoken::{Algorithm, EncodingKey, Header, encode};
    use serde_json::{Value, json};
    use tower::ServiceExt;
    use uuid::Uuid;

    use super::{
        AppState, RequestContext, RoomConnection, ServiceEndpoints, ServiceOverviewResponse,
        livekit_room_name, public_openapi, router, signaling_room_urls,
    };
    use crate::coturn_metrics::{CoturnMetricsClient, LiveKitMetricsClient};

    const PRIVATE_KEY: &[u8] = br"-----BEGIN PRIVATE KEY-----
MC4CAQAwBQYDK2VwBCIEIFTAxDs5JPZKnyxcfE0FA8mmr+9KN0LmQ1co4bxZ6Vq/
-----END PRIVATE KEY-----
";
    const PUBLIC_KEY: &str = "-----BEGIN PUBLIC KEY-----\nMCowBQYDK2VwAyEAQnTjC0+B/djS2k/sebsW6/7yCb+Am2NFtI1EzKH/ZTA=\n-----END PUBLIC KEY-----\n";
    const KEY_ID: &str = "provider-route-test";

    #[derive(Clone, Copy)]
    #[allow(clippy::struct_field_names)]
    struct CommandScope {
        principal_id: Uuid,
        organization_id: Uuid,
        project_id: Uuid,
        service_instance_id: Uuid,
    }

    #[test]
    fn appends_room_path_to_every_signaling_origin() {
        let room_id = Uuid::new_v4();
        assert_eq!(
            signaling_room_urls(
                &[
                    "wss://flow-a.example.test".into(),
                    "wss://flow-b.example.test".into(),
                    "wss://flow-c.example.test".into(),
                ],
                room_id,
            ),
            [
                format!("wss://flow-a.example.test/v1/signal/{room_id}"),
                format!("wss://flow-b.example.test/v1/signal/{room_id}"),
                format!("wss://flow-c.example.test/v1/signal/{room_id}"),
            ]
        );
    }

    #[test]
    fn scopes_livekit_room_name_to_service_instance() {
        let service_instance_id = Uuid::parse_str("019fbdfa-4920-70e0-899d-c5bed06903a0").unwrap();
        let room_id = Uuid::parse_str("019fbdfc-9891-7dc3-a57b-83637604d1dd").unwrap();
        assert_eq!(
            livekit_room_name(service_instance_id, room_id),
            "flow-019fbdfa-4920-70e0-899d-c5bed06903a0-019fbdfc-9891-7dc3-a57b-83637604d1dd"
        );
    }

    #[test]
    fn serializes_ordered_join_failover_urls() {
        let connection = RoomConnection::P2p {
            urls: vec![
                "wss://flow-a.example.test/v1/signal/room".into(),
                "wss://flow-b.example.test/v1/signal/room".into(),
                "wss://flow-c.example.test/v1/signal/room".into(),
            ],
            turn: TurnCredentialIssuer::new(
                vec!["turn:turn-a.example.test:3478?transport=udp".into()],
                b"route-test-turn-secret-at-least-32-bytes".to_vec(),
                Duration::from_mins(5),
            )
            .unwrap()
            .issue("principal")
            .unwrap(),
        };

        let rendered = serde_json::to_value(connection).unwrap();
        assert_eq!(rendered["type"], "p2p");
        assert_eq!(rendered["urls"].as_array().unwrap().len(), 3);
        assert_eq!(
            rendered["urls"][2],
            "wss://flow-c.example.test/v1/signal/room"
        );
        assert!(rendered.get("signaling_url").is_none());
    }

    #[test]
    fn service_overview_shape_includes_room_limit() {
        let rendered = serde_json::to_value(ServiceOverviewResponse {
            measured_at: chrono::Utc::now(),
            active_rooms: 3,
            concurrent_connections: 8,
            sfu_participants: 5,
            p2p_connections: 3,
            ingress_bytes: 120,
            egress_bytes: 80,
            transferred_bytes: 200,
            turn_allocations: None,
            endpoints: ServiceEndpoints {
                api: vec!["https://flow.example.test".into()],
                signaling: vec!["wss://flow.example.test".into()],
                livekit: vec!["wss://rtc.example.test".into()],
                stun: vec!["stun:turn.example.test:3478".into()],
                turn: vec!["turn:turn.example.test:3478?transport=udp".into()],
            },
            room_limit: Some(100),
        })
        .unwrap();
        assert_eq!(rendered["active_rooms"], 3);
        assert_eq!(rendered["transferred_bytes"], 200);
        assert_eq!(rendered["room_limit"], 100);
        assert!(rendered["turn_allocations"].is_null());
        assert_eq!(
            rendered["endpoints"]["stun"][0],
            "stun:turn.example.test:3478"
        );
    }

    #[test]
    fn public_openapi_contains_only_customer_routes_and_three_header_authentication() {
        let document = public_openapi(&["https://flow.example.test".into()]);
        let value = serde_json::to_value(document).unwrap();
        let paths = value["paths"].as_object().unwrap();
        assert_eq!(paths.len(), 7);
        assert!(paths.contains_key("/v1/rooms"));
        assert!(paths.contains_key("/v1/turn-credentials"));
        assert!(!paths.keys().any(|path| path.starts_with("/internal")));
        assert_eq!(value["servers"][0]["url"], "https://flow.example.test");
        assert_eq!(
            value["paths"]["/v1/rooms"]["post"]["security"][0]
                .as_object()
                .unwrap()
                .len(),
            3
        );
        assert_eq!(
            value["components"]["securitySchemes"]["flow_principal"]["name"],
            "X-Flow-Principal"
        );
        assert!(
            value["paths"]["/v1/rooms"]["post"]["responses"]["429"]["headers"]
                .get("Retry-After")
                .is_some()
        );
    }

    #[test]
    fn service_overview_requires_metrics_permission() {
        let now = chrono::Utc::now();
        let mut context = RequestContext {
            principal: PrincipalContext {
                organization_id: Uuid::new_v4(),
                project_id: Uuid::new_v4(),
                service_instance_id: Uuid::new_v4(),
                principal_id: Uuid::new_v4(),
                permissions: BTreeSet::new(),
                issued_at: now,
                expires_at: now + chrono::Duration::minutes(5),
                token_id: Uuid::new_v4(),
            },
            request_id: Uuid::now_v7().to_string(),
        };
        assert!(context.require("flow.metrics.read").is_err());
        context
            .principal
            .permissions
            .insert("flow.metrics.read".into());
        assert!(context.require("flow.metrics.read").is_ok());
    }

    #[tokio::test]
    async fn provider_reconcile_route_enforces_contract_and_idempotency() {
        let Some(state) = test_state().await else {
            return;
        };
        let scope = CommandScope {
            principal_id: Uuid::new_v4(),
            organization_id: Uuid::new_v4(),
            project_id: Uuid::new_v4(),
            service_instance_id: Uuid::new_v4(),
        };
        let body = json!({"generation": 1, "name": "route-test", "spec": {"mode": "p2p"}});
        let (token, jwt_id) = provider_token(scope, 1);

        let first = send_reconcile(&state, scope.service_instance_id, &token, jwt_id, &body).await;
        assert_eq!(first.status(), StatusCode::ACCEPTED);
        let first_body = response_json(first).await;
        let operation_id = first_body["operation_id"].as_str().unwrap().to_owned();
        assert_eq!(first_body["status"]["phase"], "ready");
        assert_eq!(first_body["status"]["observed_generation"], 1);
        assert_eq!(first_body["status"]["operation_id"], operation_id);

        let duplicate =
            send_reconcile(&state, scope.service_instance_id, &token, jwt_id, &body).await;
        assert_eq!(duplicate.status(), StatusCode::OK);
        assert_eq!(response_json(duplicate).await, first_body);

        let mismatched_path = send_reconcile(&state, Uuid::new_v4(), &token, jwt_id, &body).await;
        assert_eq!(mismatched_path.status(), StatusCode::FORBIDDEN);

        let mismatched_body = send_reconcile(
            &state,
            scope.service_instance_id,
            &token,
            jwt_id,
            &json!({"generation": 2, "name": "route-test", "spec": {}}),
        )
        .await;
        assert_eq!(mismatched_body.status(), StatusCode::FORBIDDEN);

        let (new_token, new_jwt_id) = provider_token(scope, 2);
        let generation_two = send_reconcile(
            &state,
            scope.service_instance_id,
            &new_token,
            new_jwt_id,
            &json!({"generation": 2, "name": "route-test", "spec": {"mode": "sfu"}}),
        )
        .await;
        assert_eq!(generation_two.status(), StatusCode::ACCEPTED);
        let generation_two_body = response_json(generation_two).await;
        assert_eq!(generation_two_body["status"]["phase"], "ready");
        assert_eq!(generation_two_body["status"]["observed_generation"], 2);
        assert_eq!(
            generation_two_body["status"]["operation_id"],
            generation_two_body["operation_id"]
        );

        let (stale_token, stale_jwt_id) = provider_token(scope, 1);
        let stale_response = send_reconcile(
            &state,
            scope.service_instance_id,
            &stale_token,
            stale_jwt_id,
            &body,
        )
        .await;
        assert_eq!(stale_response.status(), StatusCode::CONFLICT);
    }

    #[tokio::test]
    async fn provider_delete_route_enforces_query_action_and_idempotency() {
        let Some(state) = test_state().await else {
            return;
        };
        let scope = CommandScope {
            principal_id: Uuid::new_v4(),
            organization_id: Uuid::new_v4(),
            project_id: Uuid::new_v4(),
            service_instance_id: Uuid::new_v4(),
        };
        let body = json!({"generation": 1, "name": "delete-route-test", "spec": {}});
        let (reconcile_token, reconcile_jwt_id) = provider_token(scope, 1);
        assert_eq!(
            send_reconcile(
                &state,
                scope.service_instance_id,
                &reconcile_token,
                reconcile_jwt_id,
                &body,
            )
            .await
            .status(),
            StatusCode::ACCEPTED
        );

        let (delete_token, delete_jwt_id) =
            provider_token_for_action(scope, 2, PROVIDER_DELETE_ACTION);
        let first = send_delete(
            &state,
            scope.service_instance_id,
            2,
            &delete_token,
            delete_jwt_id,
        )
        .await;
        assert_eq!(first.status(), StatusCode::ACCEPTED);
        let first_body = response_json(first).await;
        assert_eq!(first_body["status"]["phase"], "deleted");
        assert_eq!(first_body["status"]["observed_generation"], 2);

        let duplicate = send_delete(
            &state,
            scope.service_instance_id,
            2,
            &delete_token,
            delete_jwt_id,
        )
        .await;
        assert_eq!(duplicate.status(), StatusCode::OK);
        assert_eq!(response_json(duplicate).await, first_body);

        let query_mismatch = send_delete(
            &state,
            scope.service_instance_id,
            3,
            &delete_token,
            delete_jwt_id,
        )
        .await;
        assert_eq!(query_mismatch.status(), StatusCode::FORBIDDEN);
        let wrong_action = send_delete(
            &state,
            scope.service_instance_id,
            1,
            &reconcile_token,
            reconcile_jwt_id,
        )
        .await;
        assert_eq!(wrong_action.status(), StatusCode::UNAUTHORIZED);
    }

    async fn test_state() -> Option<AppState> {
        let Ok(database_url) = env::var("TEST_DATABASE_URL") else {
            eprintln!("TEST_DATABASE_URL is not set; skipping API integration test");
            return None;
        };
        let store = PgStore::connect(&database_url, 4).await.unwrap();
        store.migrate().await.unwrap();
        Some(AppState {
            store,
            principal_auth: PrincipalAuthenticator::new(
                "heterocloud",
                "heterocloud-flow-data",
                b"route-test-principal-secret-at-least-32-bytes".to_vec(),
                Duration::from_mins(5),
            )
            .unwrap(),
            provider_auth: flow_auth::ProviderAuthenticator::from_public_keys_json(
                "heterocloud",
                "heterocloud-flow",
                &json!({KEY_ID: PUBLIC_KEY}).to_string(),
            )
            .unwrap(),
            livekit: LiveKitClient::new(
                "http://livekit.invalid:7880",
                "flow-test",
                "route-test-livekit-secret-at-least-32-bytes",
            )
            .unwrap(),
            coturn_metrics: CoturnMetricsClient::default(),
            livekit_metrics: LiveKitMetricsClient::default(),
            api_urls: vec![
                "https://flow-a.example.test".into(),
                "https://flow-b.example.test".into(),
            ],
            livekit_ws_urls: vec![
                "wss://rtc-a.example.test".into(),
                "wss://rtc-b.example.test".into(),
            ],
            signaling_urls: vec![
                "wss://flow-a.example.test".into(),
                "wss://flow-b.example.test".into(),
            ],
            turn: TurnCredentialIssuer::new(
                vec!["turn:turn.example.test:3478?transport=udp".into()],
                b"route-test-turn-secret-at-least-32-bytes".to_vec(),
                Duration::from_mins(5),
            )
            .unwrap(),
            participant_token_ttl: Duration::from_mins(5),
            rate_limiter: Arc::new(IpRateLimiter::new(
                RedisBackend::direct("redis://127.0.0.1:6379").unwrap(),
                RateLimitPolicy::new(20, 40).unwrap(),
            )),
            trusted_proxies: TrustedProxies::from_csv("127.0.0.0/8").unwrap(),
        })
    }

    fn provider_token(scope: CommandScope, generation: i64) -> (String, Uuid) {
        provider_token_for_action(scope, generation, PROVIDER_RECONCILE_ACTION)
    }

    fn provider_token_for_action(
        scope: CommandScope,
        generation: i64,
        action: &str,
    ) -> (String, Uuid) {
        let now = chrono::Utc::now().timestamp();
        let jwt_id = Uuid::now_v7();
        let claims = ProviderClaims {
            issuer: "heterocloud".into(),
            audience: "heterocloud-flow".into(),
            subject: scope.principal_id,
            organization_id: scope.organization_id,
            project_id: scope.project_id,
            service_instance_id: scope.service_instance_id,
            action: action.into(),
            generation,
            jwt_id,
            issued_at: now,
            not_before: now - 5,
            expires_at: now + 60,
        };
        let mut header = Header::new(Algorithm::EdDSA);
        header.kid = Some(KEY_ID.into());
        (
            encode(
                &header,
                &claims,
                &EncodingKey::from_ed_pem(PRIVATE_KEY).unwrap(),
            )
            .unwrap(),
            jwt_id,
        )
    }

    async fn send_delete(
        state: &AppState,
        service_instance_id: Uuid,
        generation: i64,
        token: &str,
        jwt_id: Uuid,
    ) -> axum::response::Response {
        router(state.clone())
            .oneshot(
                Request::delete(format!(
                    "/internal/v1/service-instances/{service_instance_id}?generation={generation}"
                ))
                .header(AUTHORIZATION, format!("Bearer {token}"))
                .header("idempotency-key", jwt_id.to_string())
                .body(Body::empty())
                .unwrap(),
            )
            .await
            .unwrap()
    }

    async fn send_reconcile(
        state: &AppState,
        service_instance_id: Uuid,
        token: &str,
        jwt_id: Uuid,
        body: &Value,
    ) -> axum::response::Response {
        router(state.clone())
            .oneshot(
                Request::put(format!(
                    "/internal/v1/service-instances/{service_instance_id}"
                ))
                .header(AUTHORIZATION, format!("Bearer {token}"))
                .header("idempotency-key", jwt_id.to_string())
                .header("content-type", "application/json")
                .body(Body::from(body.to_string()))
                .unwrap(),
            )
            .await
            .unwrap()
    }

    async fn response_json(response: axum::response::Response) -> Value {
        serde_json::from_slice(&to_bytes(response.into_body(), 64 * 1024).await.unwrap()).unwrap()
    }
}
