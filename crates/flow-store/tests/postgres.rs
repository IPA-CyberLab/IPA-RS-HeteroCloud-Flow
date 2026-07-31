use std::{env, time::Duration};

use chrono::Utc;
use flow_domain::{
    NewAuditEvent, NewRoom, NewTicket, RoomState, ServiceInstanceReconcile, SessionMode,
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
