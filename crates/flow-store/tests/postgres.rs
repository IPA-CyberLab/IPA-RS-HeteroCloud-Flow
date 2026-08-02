use std::{env, time::Duration};

use chrono::Utc;
use flow_domain::{
    NewAuditEvent, NewRoom, NewSignalingConnection, NewTicket, NewUsageEvent,
    PRINCIPAL_CONTEXT_CLOCK_SKEW, RoomState, ServiceInstanceDelete, ServiceInstancePhase,
    ServiceInstanceReconcile, SessionMode,
};
use flow_store::{PgStore, StoreError};
use serde_json::json;
use tokio::sync::Mutex;
use uuid::Uuid;

static DATABASE_TEST_LOCK: Mutex<()> = Mutex::const_new(());

#[derive(Clone, Copy)]
#[allow(clippy::struct_field_names)]
struct Scope {
    organization_id: Uuid,
    project_id: Uuid,
    service_instance_id: Uuid,
}

#[tokio::test]
async fn reconcile_is_idempotent_and_rejects_replay_and_stale_generations() {
    let _guard = DATABASE_TEST_LOCK.lock().await;
    let Some(store) = test_store(4).await else {
        return;
    };
    let scope = Scope {
        organization_id: Uuid::new_v4(),
        project_id: Uuid::new_v4(),
        service_instance_id: Uuid::new_v4(),
    };
    let first = reconcile_command(scope, 1, Uuid::new_v4(), "flow-a", json!({"mode": "p2p"}));

    let accepted = store
        .reconcile_service_instance(first.clone())
        .await
        .unwrap();
    assert!(accepted.created);
    let duplicate = store
        .reconcile_service_instance(first.clone())
        .await
        .unwrap();
    assert!(!duplicate.created);
    assert_eq!(duplicate.operation_id, accepted.operation_id);
    assert_eq!(duplicate.status, accepted.status);
    assert_eq!(accepted.status.observed_generation, 1);
    assert_eq!(accepted.status.operation_id, accepted.operation_id);

    let same_generation =
        reconcile_command(scope, 1, Uuid::new_v4(), "flow-a", json!({"mode": "p2p"}));
    let same_generation_outcome = store
        .reconcile_service_instance(same_generation)
        .await
        .unwrap();
    assert_eq!(same_generation_outcome.operation_id, accepted.operation_id);
    assert_eq!(same_generation_outcome.status, accepted.status);

    let mut replay = first;
    replay.name = "flow-b".into();
    assert!(matches!(
        store.reconcile_service_instance(replay).await,
        Err(StoreError::Conflict(
            "provider token replayed with different command"
        ))
    ));

    store
        .reconcile_service_instance(reconcile_command(
            scope,
            2,
            Uuid::new_v4(),
            "flow-a",
            json!({"mode": "sfu"}),
        ))
        .await
        .unwrap();
    assert!(matches!(
        store
            .reconcile_service_instance(reconcile_command(
                scope,
                1,
                Uuid::new_v4(),
                "flow-a",
                json!({"mode": "p2p"}),
            ))
            .await,
        Err(StoreError::StaleGeneration {
            current: 2,
            requested: 1
        })
    ));
}

#[tokio::test]
async fn ready_service_rate_limit_tracks_reconciled_spec() {
    let _guard = DATABASE_TEST_LOCK.lock().await;
    let Some(store) = test_store(4).await else {
        return;
    };
    let scope = Scope {
        organization_id: Uuid::new_v4(),
        project_id: Uuid::new_v4(),
        service_instance_id: Uuid::new_v4(),
    };
    store
        .reconcile_service_instance(reconcile_command(
            scope,
            1,
            Uuid::new_v4(),
            "rate-limited-flow",
            json!({
                "rate_limit": {"requests_per_second": 75, "burst": 150}
            }),
        ))
        .await
        .unwrap();

    let policy = store
        .ready_service_rate_limit(
            scope.organization_id,
            scope.project_id,
            scope.service_instance_id,
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(policy.requests_per_second, 75);
    assert_eq!(policy.burst, 150);
}

#[tokio::test]
async fn p2p_match_is_claimed_and_completed() {
    let _guard = DATABASE_TEST_LOCK.lock().await;
    let Some(store) = test_store(4).await else {
        return;
    };
    let scope = provision_scope(&store, None).await;
    let queue = format!("queue-{}", Uuid::new_v4().simple());

    create_ticket(&store, scope, &queue).await;
    create_ticket(&store, scope, &queue).await;

    let candidate = store
        .claim_match(Duration::from_secs(30))
        .await
        .unwrap()
        .expect("candidate");
    assert_eq!(candidate.tickets.len(), 2);
    assert_eq!(
        candidate.room.service_instance_id,
        scope.service_instance_id
    );
    let assignments = store.complete_match(candidate.room.id).await.unwrap();
    assert_eq!(assignments.len(), 2);
}

#[tokio::test]
async fn room_limit_is_atomic_across_room_creation_and_matchmaking() {
    let _guard = DATABASE_TEST_LOCK.lock().await;
    let Some(store) = test_store(8).await else {
        return;
    };
    let scope = Scope {
        organization_id: Uuid::new_v4(),
        project_id: Uuid::new_v4(),
        service_instance_id: Uuid::new_v4(),
    };
    store
        .reconcile_service_instance(reconcile_command(
            scope,
            1,
            Uuid::new_v4(),
            "limited-flow",
            json!({"max_rooms": 1}),
        ))
        .await
        .unwrap();

    let make_room = || {
        let id = Uuid::now_v7();
        NewRoom {
            id,
            organization_id: scope.organization_id,
            project_id: scope.project_id,
            service_instance_id: scope.service_instance_id,
            name: format!("room-{id}"),
            provider_room_name: None,
            mode: SessionMode::P2p,
            state: RoomState::Ready,
            max_participants: 2,
            metadata: json!({}),
        }
    };
    let first_store = store.clone();
    let second_store = store.clone();
    let (first, second) = tokio::join!(
        first_store.create_room(make_room()),
        second_store.create_room(make_room())
    );
    let results = [first, second];
    assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
    assert_eq!(
        results
            .iter()
            .filter(|result| matches!(result, Err(StoreError::RoomLimitExceeded { limit: 1 })))
            .count(),
        1
    );

    let snapshot = store
        .service_overview_snapshot(
            scope.organization_id,
            scope.project_id,
            scope.service_instance_id,
            Duration::from_secs(45),
        )
        .await
        .unwrap();
    assert_eq!(snapshot.active_rooms, 1);
    assert_eq!(snapshot.room_limit, 1);

    let queue = format!("limited-{}", Uuid::new_v4().simple());
    create_ticket(&store, scope, &queue).await;
    create_ticket(&store, scope, &queue).await;
    assert!(
        store
            .claim_match(Duration::from_secs(30))
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn rooms_and_matchmaking_are_service_instance_scoped() {
    let _guard = DATABASE_TEST_LOCK.lock().await;
    let Some(store) = test_store(4).await else {
        return;
    };
    let organization_id = Uuid::new_v4();
    let project_id = Uuid::new_v4();
    let first = provision_scope(&store, Some((organization_id, project_id))).await;
    let second = provision_scope(&store, Some((organization_id, project_id))).await;
    let room_id = Uuid::now_v7();
    store
        .create_room(NewRoom {
            id: room_id,
            organization_id,
            project_id,
            service_instance_id: first.service_instance_id,
            name: format!("room-{room_id}"),
            provider_room_name: None,
            mode: SessionMode::P2p,
            state: RoomState::Ready,
            max_participants: 2,
            metadata: json!({}),
        })
        .await
        .unwrap();

    assert!(matches!(
        store
            .get_room(
                organization_id,
                project_id,
                second.service_instance_id,
                room_id,
            )
            .await,
        Err(StoreError::NotFound)
    ));

    let queue = format!("isolated-{}", Uuid::new_v4().simple());
    create_ticket(&store, first, &queue).await;
    create_ticket(&store, second, &queue).await;
    assert!(
        store
            .claim_match(Duration::from_secs(30))
            .await
            .unwrap()
            .is_none()
    );

    create_ticket(&store, first, &queue).await;
    let candidate = store
        .claim_match(Duration::from_secs(30))
        .await
        .unwrap()
        .expect("first service instance should now match");
    assert!(
        candidate
            .tickets
            .iter()
            .all(|ticket| ticket.service_instance_id == first.service_instance_id)
    );
}

#[tokio::test]
async fn concurrent_workers_do_not_claim_the_same_tickets() {
    let _guard = DATABASE_TEST_LOCK.lock().await;
    let Some(store) = test_store(8).await else {
        return;
    };
    let scope = provision_scope(&store, None).await;
    let queue = format!("concurrent-{}", Uuid::new_v4().simple());
    create_ticket(&store, scope, &queue).await;
    create_ticket(&store, scope, &queue).await;

    let first_store = store.clone();
    let second_store = store.clone();
    let (first, second) = tokio::join!(
        first_store.claim_match(Duration::from_secs(30)),
        second_store.claim_match(Duration::from_secs(30))
    );
    let claimed = [first.unwrap(), second.unwrap()]
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();
    assert_eq!(claimed.len(), 1);
    assert_eq!(claimed[0].tickets.len(), 2);
}

#[tokio::test]
async fn audit_records_principal_context_id() {
    let _guard = DATABASE_TEST_LOCK.lock().await;
    let Some(store) = test_store(4).await else {
        return;
    };
    let scope = provision_scope(&store, None).await;
    let audit_id = Uuid::now_v7();
    let principal_context_id = Uuid::now_v7();
    store
        .append_audit(NewAuditEvent {
            id: audit_id,
            organization_id: scope.organization_id,
            project_id: scope.project_id,
            service_instance_id: scope.service_instance_id,
            principal_id: Uuid::new_v4(),
            principal_context_id: Some(principal_context_id),
            request_id: Uuid::now_v7().to_string(),
            action: "flow.room.read".into(),
            resource_type: "room".into(),
            resource_id: None,
            outcome: "allowed".into(),
            details: json!({}),
        })
        .await
        .unwrap();

    let stored: Option<Uuid> =
        sqlx::query_scalar("SELECT principal_context_id FROM audit_events WHERE id = $1")
            .bind(audit_id)
            .fetch_one(store.pool())
            .await
            .unwrap();
    assert_eq!(stored, Some(principal_context_id));
}

#[tokio::test]
async fn principal_context_revocation_is_scoped_idempotent_bounded_and_expires() {
    let _guard = DATABASE_TEST_LOCK.lock().await;
    let Some(store) = test_store(4).await else {
        return;
    };
    let scope = provision_scope(&store, None).await;
    let other_scope = provision_scope(&store, None).await;
    let context_id = Uuid::now_v7();
    let now = Utc::now().timestamp();

    assert!(
        !store
            .principal_context_is_revoked(
                scope.organization_id,
                scope.project_id,
                scope.service_instance_id,
                context_id,
            )
            .await
            .unwrap()
    );
    store
        .revoke_principal_context(
            scope.organization_id,
            scope.project_id,
            scope.service_instance_id,
            context_id,
            now + 300,
        )
        .await
        .unwrap();
    store
        .revoke_principal_context(
            scope.organization_id,
            scope.project_id,
            scope.service_instance_id,
            context_id,
            now + 240,
        )
        .await
        .unwrap();
    assert!(
        store
            .principal_context_is_revoked(
                scope.organization_id,
                scope.project_id,
                scope.service_instance_id,
                context_id,
            )
            .await
            .unwrap()
    );
    let (count, stored_expiry): (i64, chrono::DateTime<Utc>) = sqlx::query_as(
        r"
        SELECT count(*) OVER (), expires_at
        FROM flow_principal_context_revocations
        WHERE context_id = $1
        ",
    )
    .bind(context_id)
    .fetch_one(store.pool())
    .await
    .unwrap();
    assert_eq!(count, 1);
    assert_eq!(
        stored_expiry.timestamp(),
        now + 300 + i64::try_from(PRINCIPAL_CONTEXT_CLOCK_SKEW.as_secs()).unwrap()
    );

    assert!(matches!(
        store
            .revoke_principal_context(
                Uuid::new_v4(),
                scope.project_id,
                scope.service_instance_id,
                Uuid::now_v7(),
                now + 300,
            )
            .await,
        Err(StoreError::Conflict(
            "service instance scope does not match provider claims"
        ))
    ));
    assert!(matches!(
        store
            .revoke_principal_context(
                other_scope.organization_id,
                other_scope.project_id,
                other_scope.service_instance_id,
                context_id,
                now + 300,
            )
            .await,
        Err(StoreError::Conflict(
            "principal context is already revoked in another scope"
        ))
    ));
    assert!(matches!(
        store
            .revoke_principal_context(
                scope.organization_id,
                scope.project_id,
                scope.service_instance_id,
                Uuid::now_v7(),
                now + 360,
            )
            .await,
        Err(StoreError::RevocationExpiryTooDistant)
    ));

    let delayed_context_id = Uuid::now_v7();
    store
        .revoke_principal_context(
            scope.organization_id,
            scope.project_id,
            scope.service_instance_id,
            delayed_context_id,
            now - 1,
        )
        .await
        .unwrap();
    let delayed_rows: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM flow_principal_context_revocations WHERE context_id = $1",
    )
    .bind(delayed_context_id)
    .fetch_one(store.pool())
    .await
    .unwrap();
    assert_eq!(delayed_rows, 1);
    assert!(
        store
            .principal_context_is_revoked(
                scope.organization_id,
                scope.project_id,
                scope.service_instance_id,
                delayed_context_id,
            )
            .await
            .unwrap()
    );

    let fully_expired_context_id = Uuid::now_v7();
    store
        .revoke_principal_context(
            scope.organization_id,
            scope.project_id,
            scope.service_instance_id,
            fully_expired_context_id,
            now - 60,
        )
        .await
        .unwrap();
    let fully_expired_rows: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM flow_principal_context_revocations WHERE context_id = $1",
    )
    .bind(fully_expired_context_id)
    .fetch_one(store.pool())
    .await
    .unwrap();
    assert_eq!(fully_expired_rows, 0);

    let expired_context_id = Uuid::now_v7();
    sqlx::query(
        r"
        INSERT INTO flow_principal_context_revocations (
            context_id, organization_id, project_id, service_instance_id, expires_at
        )
        VALUES ($1, $2, $3, $4, now() - interval '1 second')
        ",
    )
    .bind(expired_context_id)
    .bind(scope.organization_id)
    .bind(scope.project_id)
    .bind(scope.service_instance_id)
    .execute(store.pool())
    .await
    .unwrap();
    assert!(
        !store
            .principal_context_is_revoked(
                scope.organization_id,
                scope.project_id,
                scope.service_instance_id,
                expired_context_id,
            )
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn delete_is_scoped_idempotent_and_leaves_a_tombstone() {
    let _guard = DATABASE_TEST_LOCK.lock().await;
    let Some(store) = test_store(4).await else {
        return;
    };
    let scope = provision_scope(&store, None).await;
    let principal_id = Uuid::new_v4();
    let room_id = Uuid::now_v7();
    store
        .create_room(NewRoom {
            id: room_id,
            organization_id: scope.organization_id,
            project_id: scope.project_id,
            service_instance_id: scope.service_instance_id,
            name: format!("room-{room_id}"),
            provider_room_name: Some(format!("flow-{room_id}")),
            mode: SessionMode::Sfu,
            state: RoomState::Ready,
            max_participants: 16,
            metadata: json!({}),
        })
        .await
        .unwrap();

    let first = delete_command(scope, 2, Uuid::now_v7(), principal_id);
    let prepared = store.prepare_delete_service_instance(&first).await.unwrap();
    assert!(!prepared.completed);
    assert_eq!(prepared.status.phase, ServiceInstancePhase::Deleting);
    assert_eq!(prepared.provider_room_names, [format!("flow-{room_id}")]);

    let duplicate = store.prepare_delete_service_instance(&first).await.unwrap();
    assert_eq!(duplicate.operation_id, prepared.operation_id);
    let refreshed_token = delete_command(scope, 2, Uuid::now_v7(), principal_id);
    let refreshed = store
        .prepare_delete_service_instance(&refreshed_token)
        .await
        .unwrap();
    assert_eq!(refreshed.operation_id, prepared.operation_id);

    let completed = store
        .complete_delete_service_instance(&refreshed_token, prepared.operation_id)
        .await
        .unwrap();
    assert!(completed.completed_now);
    assert_eq!(completed.status.phase, ServiceInstancePhase::Deleted);
    assert!(
        !store
            .service_instance_is_ready(
                scope.organization_id,
                scope.project_id,
                scope.service_instance_id,
            )
            .await
            .unwrap()
    );
    let remaining_rooms: i64 =
        sqlx::query_scalar("SELECT count(*) FROM flow_rooms WHERE service_instance_id = $1")
            .bind(scope.service_instance_id)
            .fetch_one(store.pool())
            .await
            .unwrap();
    assert_eq!(remaining_rooms, 0);

    let retried = store.prepare_delete_service_instance(&first).await.unwrap();
    assert!(retried.completed);
    assert_eq!(retried.operation_id, prepared.operation_id);
    let completed_again = store
        .complete_delete_service_instance(&first, prepared.operation_id)
        .await
        .unwrap();
    assert!(!completed_again.completed_now);
    assert_eq!(completed_again.status.phase, ServiceInstancePhase::Deleted);

    assert!(matches!(
        store
            .reconcile_service_instance(reconcile_command(
                scope,
                3,
                Uuid::now_v7(),
                "cannot-recreate",
                json!({}),
            ))
            .await,
        Err(StoreError::Conflict(
            "service instance is deleting or has been deleted"
        ))
    ));
}

#[tokio::test]
async fn overview_uses_ready_rooms_active_connections_and_measured_bytes() {
    let _guard = DATABASE_TEST_LOCK.lock().await;
    let Some(store) = test_store(4).await else {
        return;
    };
    let scope = provision_scope(&store, None).await;
    let p2p_room_id = Uuid::now_v7();
    let sfu_room_id = Uuid::now_v7();
    for (room_id, mode, provider_room_name, state) in [
        (p2p_room_id, SessionMode::P2p, None, RoomState::Ready),
        (
            sfu_room_id,
            SessionMode::Sfu,
            Some(format!("flow-{sfu_room_id}")),
            RoomState::Ready,
        ),
        (Uuid::now_v7(), SessionMode::P2p, None, RoomState::Failed),
    ] {
        store
            .create_room(NewRoom {
                id: room_id,
                organization_id: scope.organization_id,
                project_id: scope.project_id,
                service_instance_id: scope.service_instance_id,
                name: format!("room-{room_id}"),
                provider_room_name,
                mode,
                state,
                max_participants: 16,
                metadata: json!({}),
            })
            .await
            .unwrap();
    }
    let connection_id = Uuid::now_v7();
    store
        .open_signaling_connection(NewSignalingConnection {
            connection_id,
            organization_id: scope.organization_id,
            project_id: scope.project_id,
            service_instance_id: scope.service_instance_id,
            room_id: p2p_room_id,
            principal_id: Uuid::new_v4(),
        })
        .await
        .unwrap();
    for (event_type, quantity) in [
        ("ingress_bytes", 120_i64),
        ("egress_bytes", 80_i64),
        ("p2p_signaling_messages", 999_i64),
    ] {
        store
            .record_usage(NewUsageEvent {
                id: Uuid::now_v7(),
                organization_id: scope.organization_id,
                project_id: scope.project_id,
                service_instance_id: scope.service_instance_id,
                principal_id: None,
                event_type: event_type.into(),
                resource_id: None,
                quantity,
                idempotency_key: format!("{event_type}-{}", Uuid::now_v7()),
                dimensions: json!({}),
                occurred_at: Utc::now(),
            })
            .await
            .unwrap();
    }

    let current = store
        .service_overview_snapshot(
            scope.organization_id,
            scope.project_id,
            scope.service_instance_id,
            Duration::from_secs(45),
        )
        .await
        .unwrap();
    assert_eq!(current.active_rooms, 2);
    assert_eq!(current.p2p_connections, 1);
    assert_eq!(current.ingress_bytes, 120);
    assert_eq!(current.egress_bytes, 80);
    assert_eq!(current.provider_room_names, [format!("flow-{sfu_room_id}")]);

    sqlx::query(
        "UPDATE flow_signaling_connections SET last_seen_at = now() - interval '1 minute' WHERE connection_id = $1",
    )
    .bind(connection_id)
    .execute(store.pool())
    .await
    .unwrap();
    let stale = store
        .service_overview_snapshot(
            scope.organization_id,
            scope.project_id,
            scope.service_instance_id,
            Duration::from_secs(45),
        )
        .await
        .unwrap();
    assert_eq!(stale.p2p_connections, 0);
    assert!(
        store
            .heartbeat_signaling_connection(connection_id)
            .await
            .unwrap()
    );
    assert!(
        store
            .close_signaling_connection(connection_id)
            .await
            .unwrap()
    );
    assert!(
        !store
            .close_signaling_connection(connection_id)
            .await
            .unwrap()
    );
    let closed = store
        .service_overview_snapshot(
            scope.organization_id,
            scope.project_id,
            scope.service_instance_id,
            Duration::from_secs(45),
        )
        .await
        .unwrap();
    assert_eq!(closed.p2p_connections, 0);
}

async fn create_ticket(store: &PgStore, scope: Scope, queue_name: &str) {
    store
        .create_ticket(NewTicket {
            id: Uuid::now_v7(),
            organization_id: scope.organization_id,
            project_id: scope.project_id,
            service_instance_id: scope.service_instance_id,
            principal_id: Uuid::new_v4(),
            queue_name: queue_name.into(),
            mode: SessionMode::P2p,
            match_size: 2,
            attributes: json!({}),
            expires_at: Utc::now() + chrono::Duration::minutes(5),
        })
        .await
        .unwrap();
}

async fn provision_scope(store: &PgStore, parent: Option<(Uuid, Uuid)>) -> Scope {
    let (organization_id, project_id) = parent.unwrap_or_else(|| (Uuid::new_v4(), Uuid::new_v4()));
    let scope = Scope {
        organization_id,
        project_id,
        service_instance_id: Uuid::new_v4(),
    };
    let name = format!("flow-{}", scope.service_instance_id.simple());
    store
        .reconcile_service_instance(reconcile_command(
            scope,
            1,
            Uuid::new_v4(),
            &name,
            json!({}),
        ))
        .await
        .unwrap();
    scope
}

fn reconcile_command(
    scope: Scope,
    generation: i64,
    jwt_id: Uuid,
    name: &str,
    spec: serde_json::Value,
) -> ServiceInstanceReconcile {
    ServiceInstanceReconcile {
        jwt_id,
        organization_id: scope.organization_id,
        project_id: scope.project_id,
        service_instance_id: scope.service_instance_id,
        principal_id: Uuid::new_v4(),
        generation,
        name: name.into(),
        spec,
    }
}

fn delete_command(
    scope: Scope,
    generation: i64,
    jwt_id: Uuid,
    principal_id: Uuid,
) -> ServiceInstanceDelete {
    ServiceInstanceDelete {
        jwt_id,
        organization_id: scope.organization_id,
        project_id: scope.project_id,
        service_instance_id: scope.service_instance_id,
        principal_id,
        generation,
    }
}

async fn test_store(max_connections: u32) -> Option<PgStore> {
    let Ok(database_url) = env::var("TEST_DATABASE_URL") else {
        eprintln!("TEST_DATABASE_URL is not set; skipping PostgreSQL integration test");
        return None;
    };
    let store = PgStore::connect(&database_url, max_connections)
        .await
        .unwrap();
    store.migrate().await.unwrap();
    sqlx::query(
        r"
        TRUNCATE TABLE
            flow_principal_context_revocations,
            flow_delete_token_receipts,
            flow_delete_operations,
            flow_signaling_connections,
            usage_events,
            audit_events,
            match_assignments,
            matchmaking_tickets,
            flow_rooms,
            flow_provider_token_receipts,
            flow_reconcile_operations,
            flow_service_instances
        RESTART IDENTITY CASCADE
        ",
    )
    .execute(store.pool())
    .await
    .unwrap();
    Some(store)
}
