use std::{str::FromStr, time::Duration};

use chrono::{DateTime, Utc};
use flow_domain::{
    FlowRoom, MatchAssignment, MatchCandidate, MatchmakingTicket, NewAuditEvent, NewRoom,
    NewSignalingConnection, NewTicket, NewUsageEvent, ReconcileOutcome, RoomState,
    ServiceInstanceDelete, ServiceInstanceReconcile, ServiceInstanceStatus, SessionMode,
    TicketState, ValidationError,
};
use serde_json::Value;
use sqlx::{
    PgPool, Postgres, Transaction,
    postgres::{PgPoolOptions, PgQueryResult},
};
use thiserror::Error;
use uuid::Uuid;

static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("../../migrations");

#[derive(Clone)]
pub struct PgStore {
    pool: PgPool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServiceOverviewSnapshot {
    pub active_rooms: u64,
    pub p2p_connections: u64,
    pub ingress_bytes: i64,
    pub egress_bytes: i64,
    pub provider_room_names: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeletePreparation {
    pub operation_id: Uuid,
    pub status: ServiceInstanceStatus,
    pub provider_room_names: Vec<String>,
    pub completed: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DeleteOutcome {
    pub operation_id: Uuid,
    pub status: ServiceInstanceStatus,
    pub completed_now: bool,
}

impl PgStore {
    pub async fn connect(database_url: &str, max_connections: u32) -> Result<Self, StoreError> {
        if database_url.is_empty() {
            return Err(StoreError::Configuration("DATABASE_URL is empty"));
        }
        let pool = PgPoolOptions::new()
            .max_connections(max_connections)
            .min_connections(1)
            .acquire_timeout(Duration::from_secs(5))
            .connect(database_url)
            .await?;
        Ok(Self { pool })
    }

    #[must_use]
    pub fn from_pool(pool: PgPool) -> Self {
        Self { pool }
    }

    #[must_use]
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    pub async fn migrate(&self) -> Result<(), StoreError> {
        MIGRATOR.run(&self.pool).await?;
        Ok(())
    }

    pub async fn health(&self) -> Result<(), StoreError> {
        sqlx::query("SELECT 1").execute(&self.pool).await?;
        Ok(())
    }

    pub async fn service_instance_is_ready(
        &self,
        organization_id: Uuid,
        project_id: Uuid,
        service_instance_id: Uuid,
    ) -> Result<bool, StoreError> {
        sqlx::query_scalar(
            r"
            SELECT EXISTS (
                SELECT 1
                FROM flow_service_instances
                WHERE id = $1
                  AND organization_id = $2
                  AND project_id = $3
                  AND observed_generation = desired_generation
                  AND status ->> 'phase' = 'ready'
            )
            ",
        )
        .bind(service_instance_id)
        .bind(organization_id)
        .bind(project_id)
        .fetch_one(&self.pool)
        .await
        .map_err(Into::into)
    }

    pub async fn reconcile_service_instance(
        &self,
        command: ServiceInstanceReconcile,
    ) -> Result<ReconcileOutcome, StoreError> {
        command.validate()?;
        let mut transaction = self.pool.begin().await?;
        advisory_lock(
            &mut transaction,
            &format!("provider-jti:{}", command.jwt_id),
        )
        .await?;
        advisory_lock(
            &mut transaction,
            &format!("service-instance:{}", command.service_instance_id),
        )
        .await?;

        let deleted_or_deleting: bool = sqlx::query_scalar(
            "SELECT EXISTS (SELECT 1 FROM flow_delete_operations WHERE service_instance_id = $1)",
        )
        .bind(command.service_instance_id)
        .fetch_one(&mut *transaction)
        .await?;
        if deleted_or_deleting {
            return Err(StoreError::Conflict(
                "service instance is deleting or has been deleted",
            ));
        }

        if let Some(receipt) = sqlx::query_as::<_, ReconcileReceiptRow>(
            "SELECT * FROM flow_provider_token_receipts WHERE jwt_id = $1 FOR UPDATE",
        )
        .bind(command.jwt_id)
        .fetch_optional(&mut *transaction)
        .await?
        {
            if receipt.matches(&command) {
                transaction.commit().await?;
                return Ok(ReconcileOutcome {
                    operation_id: receipt.operation_id,
                    status: ServiceInstanceStatus::ready(command.generation, receipt.operation_id),
                    created: false,
                });
            }
            return Err(StoreError::Conflict(
                "provider token replayed with different command",
            ));
        }

        let existing = sqlx::query_as::<_, ServiceInstanceRow>(
            "SELECT * FROM flow_service_instances WHERE id = $1 FOR UPDATE",
        )
        .bind(command.service_instance_id)
        .fetch_optional(&mut *transaction)
        .await?;
        if let Some(instance) = &existing {
            if instance.organization_id != command.organization_id
                || instance.project_id != command.project_id
            {
                return Err(StoreError::Conflict(
                    "service instance scope does not match provider claims",
                ));
            }
            if command.generation < instance.desired_generation {
                return Err(StoreError::StaleGeneration {
                    current: instance.desired_generation,
                    requested: command.generation,
                });
            }
            if command.generation == instance.desired_generation {
                if command.name != instance.name || command.spec != instance.desired_spec {
                    return Err(StoreError::Conflict(
                        "generation was reused with different desired state",
                    ));
                }
                insert_reconcile_receipt(&mut transaction, &command, instance.current_operation_id)
                    .await?;
                let status: ServiceInstanceStatus = serde_json::from_value(instance.status.clone())
                    .map_err(|_| StoreError::CorruptData("service instance status"))?;
                if status
                    != ServiceInstanceStatus::ready(
                        instance.observed_generation,
                        instance.current_operation_id,
                    )
                    || status.observed_generation != command.generation
                {
                    return Err(StoreError::CorruptData("service instance status"));
                }
                transaction.commit().await?;
                return Ok(ReconcileOutcome {
                    operation_id: instance.current_operation_id,
                    status,
                    created: false,
                });
            }
        }

        let operation_id = Uuid::now_v7();
        let reconciled_status = ServiceInstanceStatus::ready(command.generation, operation_id);
        let status = serde_json::to_value(reconciled_status)
            .map_err(|_| StoreError::CorruptData("service instance status"))?;
        if existing.is_some() {
            sqlx::query(
                r"
                UPDATE flow_service_instances
                SET name = $2,
                    desired_generation = $3,
                    desired_spec = $4,
                    observed_generation = $3,
                    status = $5,
                    current_operation_id = $6,
                    updated_at = now()
                WHERE id = $1
                ",
            )
            .bind(command.service_instance_id)
            .bind(&command.name)
            .bind(command.generation)
            .bind(&command.spec)
            .bind(&status)
            .bind(operation_id)
            .execute(&mut *transaction)
            .await
            .map_err(map_database_error)?;
        } else {
            sqlx::query(
                r"
                INSERT INTO flow_service_instances (
                    id, organization_id, project_id, name, desired_generation,
                    desired_spec, observed_generation, status, current_operation_id
                )
                VALUES ($1, $2, $3, $4, $5, $6, $5, $7, $8)
                ",
            )
            .bind(command.service_instance_id)
            .bind(command.organization_id)
            .bind(command.project_id)
            .bind(&command.name)
            .bind(command.generation)
            .bind(&command.spec)
            .bind(&status)
            .bind(operation_id)
            .execute(&mut *transaction)
            .await
            .map_err(map_database_error)?;
        }
        sqlx::query(
            r"
            INSERT INTO flow_reconcile_operations (
                id, service_instance_id, organization_id, project_id,
                principal_id, generation, name, spec, state
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 'succeeded')
            ",
        )
        .bind(operation_id)
        .bind(command.service_instance_id)
        .bind(command.organization_id)
        .bind(command.project_id)
        .bind(command.principal_id)
        .bind(command.generation)
        .bind(&command.name)
        .bind(&command.spec)
        .execute(&mut *transaction)
        .await
        .map_err(map_database_error)?;
        insert_reconcile_receipt(&mut transaction, &command, operation_id).await?;
        transaction.commit().await?;
        Ok(ReconcileOutcome {
            operation_id,
            status: reconciled_status,
            created: true,
        })
    }

    pub async fn prepare_delete_service_instance(
        &self,
        command: &ServiceInstanceDelete,
    ) -> Result<DeletePreparation, StoreError> {
        command.validate()?;
        let mut transaction = self.pool.begin().await?;
        advisory_lock(
            &mut transaction,
            &format!("provider-jti:{}", command.jwt_id),
        )
        .await?;
        advisory_lock(
            &mut transaction,
            &format!("service-instance:{}", command.service_instance_id),
        )
        .await?;

        if let Some(receipt) = sqlx::query_as::<_, DeleteReceiptRow>(
            "SELECT * FROM flow_delete_token_receipts WHERE jwt_id = $1 FOR UPDATE",
        )
        .bind(command.jwt_id)
        .fetch_optional(&mut *transaction)
        .await?
        {
            if !receipt.matches(command) {
                return Err(StoreError::Conflict(
                    "provider token replayed with different command",
                ));
            }
            let operation = sqlx::query_as::<_, DeleteOperationRow>(
                "SELECT * FROM flow_delete_operations WHERE id = $1 FOR UPDATE",
            )
            .bind(receipt.operation_id)
            .fetch_one(&mut *transaction)
            .await?;
            let preparation = operation.try_into_preparation(command)?;
            transaction.commit().await?;
            return Ok(preparation);
        }

        if let Some(operation) = sqlx::query_as::<_, DeleteOperationRow>(
            "SELECT * FROM flow_delete_operations WHERE service_instance_id = $1 FOR UPDATE",
        )
        .bind(command.service_instance_id)
        .fetch_optional(&mut *transaction)
        .await?
        {
            if !operation.matches(command) {
                if command.generation < operation.generation {
                    return Err(StoreError::StaleGeneration {
                        current: operation.generation,
                        requested: command.generation,
                    });
                }
                return Err(StoreError::Conflict(
                    "service instance delete scope or generation does not match",
                ));
            }
            insert_delete_receipt(&mut transaction, command, operation.id).await?;
            let preparation = operation.try_into_preparation(command)?;
            transaction.commit().await?;
            return Ok(preparation);
        }

        let instance = sqlx::query_as::<_, ServiceInstanceRow>(
            "SELECT * FROM flow_service_instances WHERE id = $1 FOR UPDATE",
        )
        .bind(command.service_instance_id)
        .fetch_optional(&mut *transaction)
        .await?
        .ok_or(StoreError::NotFound)?;
        if instance.organization_id != command.organization_id
            || instance.project_id != command.project_id
        {
            return Err(StoreError::Conflict(
                "service instance scope does not match provider claims",
            ));
        }
        let expected_generation =
            instance
                .desired_generation
                .checked_add(1)
                .ok_or(StoreError::Configuration(
                    "service instance generation overflow",
                ))?;
        if command.generation < expected_generation {
            return Err(StoreError::StaleGeneration {
                current: instance.desired_generation,
                requested: command.generation,
            });
        }
        if command.generation != expected_generation {
            return Err(StoreError::Conflict(
                "delete generation must immediately follow desired generation",
            ));
        }

        let provider_room_names = sqlx::query_scalar::<_, String>(
            r"
            SELECT provider_room_name
            FROM flow_rooms
            WHERE service_instance_id = $1
              AND mode = 'sfu'
              AND provider_room_name IS NOT NULL
            ORDER BY created_at
            ",
        )
        .bind(command.service_instance_id)
        .fetch_all(&mut *transaction)
        .await?;
        let operation_id = Uuid::now_v7();
        let deleting_status = ServiceInstanceStatus::deleting(command.generation, operation_id);
        let status = serde_json::to_value(deleting_status)
            .map_err(|_| StoreError::CorruptData("delete operation status"))?;
        let provider_room_names_json = serde_json::to_value(&provider_room_names)
            .map_err(|_| StoreError::CorruptData("provider room names"))?;

        sqlx::query(
            r"
            INSERT INTO flow_delete_operations (
                id, service_instance_id, organization_id, project_id,
                principal_id, generation, state, status, provider_room_names
            )
            VALUES ($1, $2, $3, $4, $5, $6, 'deleting', $7, $8)
            ",
        )
        .bind(operation_id)
        .bind(command.service_instance_id)
        .bind(command.organization_id)
        .bind(command.project_id)
        .bind(command.principal_id)
        .bind(command.generation)
        .bind(&status)
        .bind(&provider_room_names_json)
        .execute(&mut *transaction)
        .await
        .map_err(map_database_error)?;
        sqlx::query(
            r"
            UPDATE flow_service_instances
            SET desired_generation = $2,
                observed_generation = $2,
                status = $3,
                current_operation_id = $4,
                updated_at = now()
            WHERE id = $1
            ",
        )
        .bind(command.service_instance_id)
        .bind(command.generation)
        .bind(&status)
        .bind(operation_id)
        .execute(&mut *transaction)
        .await?;
        insert_delete_receipt(&mut transaction, command, operation_id).await?;
        transaction.commit().await?;

        Ok(DeletePreparation {
            operation_id,
            status: deleting_status,
            provider_room_names,
            completed: false,
        })
    }

    pub async fn complete_delete_service_instance(
        &self,
        command: &ServiceInstanceDelete,
        operation_id: Uuid,
    ) -> Result<DeleteOutcome, StoreError> {
        command.validate()?;
        let mut transaction = self.pool.begin().await?;
        advisory_lock(
            &mut transaction,
            &format!("service-instance:{}", command.service_instance_id),
        )
        .await?;
        let operation = sqlx::query_as::<_, DeleteOperationRow>(
            "SELECT * FROM flow_delete_operations WHERE id = $1 FOR UPDATE",
        )
        .bind(operation_id)
        .fetch_optional(&mut *transaction)
        .await?
        .ok_or(StoreError::NotFound)?;
        if !operation.matches(command) {
            return Err(StoreError::Conflict(
                "service instance delete scope or generation does not match",
            ));
        }
        if operation.state == "succeeded" {
            let status = operation.status()?;
            transaction.commit().await?;
            return Ok(DeleteOutcome {
                operation_id,
                status,
                completed_now: false,
            });
        }
        if operation.state != "deleting" {
            return Err(StoreError::CorruptData("delete operation state"));
        }

        let instance = sqlx::query_as::<_, ServiceInstanceRow>(
            "SELECT * FROM flow_service_instances WHERE id = $1 FOR UPDATE",
        )
        .bind(command.service_instance_id)
        .fetch_optional(&mut *transaction)
        .await?
        .ok_or(StoreError::CorruptData(
            "deleting service instance is missing",
        ))?;
        if instance.organization_id != command.organization_id
            || instance.project_id != command.project_id
            || instance.desired_generation != command.generation
            || instance.current_operation_id != operation_id
        {
            return Err(StoreError::Conflict(
                "service instance changed while delete was in progress",
            ));
        }

        sqlx::query("DELETE FROM flow_service_instances WHERE id = $1")
            .bind(command.service_instance_id)
            .execute(&mut *transaction)
            .await?;
        let deleted_status = ServiceInstanceStatus::deleted(command.generation, operation_id);
        let status = serde_json::to_value(deleted_status)
            .map_err(|_| StoreError::CorruptData("delete operation status"))?;
        sqlx::query(
            r"
            UPDATE flow_delete_operations
            SET state = 'succeeded', status = $2, updated_at = now()
            WHERE id = $1
            ",
        )
        .bind(operation_id)
        .bind(status)
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        Ok(DeleteOutcome {
            operation_id,
            status: deleted_status,
            completed_now: true,
        })
    }

    pub async fn service_overview_snapshot(
        &self,
        organization_id: Uuid,
        project_id: Uuid,
        service_instance_id: Uuid,
        signaling_stale_after: Duration,
    ) -> Result<ServiceOverviewSnapshot, StoreError> {
        let active_rooms: i64 = sqlx::query_scalar(
            r"
            SELECT count(*)
            FROM flow_rooms
            WHERE organization_id = $1
              AND project_id = $2
              AND service_instance_id = $3
              AND state = 'ready'
            ",
        )
        .bind(organization_id)
        .bind(project_id)
        .bind(service_instance_id)
        .fetch_one(&self.pool)
        .await?;
        let provider_room_names = sqlx::query_scalar::<_, String>(
            r"
            SELECT provider_room_name
            FROM flow_rooms
            WHERE organization_id = $1
              AND project_id = $2
              AND service_instance_id = $3
              AND mode = 'sfu'
              AND state = 'ready'
              AND provider_room_name IS NOT NULL
            ORDER BY created_at
            ",
        )
        .bind(organization_id)
        .bind(project_id)
        .bind(service_instance_id)
        .fetch_all(&self.pool)
        .await?;
        let stale_seconds = i64::try_from(signaling_stale_after.as_secs())
            .map_err(|_| StoreError::Configuration("signaling stale TTL is too large"))?;
        let p2p_connections: i64 = sqlx::query_scalar(
            r"
            SELECT count(*)
            FROM flow_signaling_connections
            WHERE organization_id = $1
              AND project_id = $2
              AND service_instance_id = $3
              AND closed_at IS NULL
              AND last_seen_at >= now() - ($4 * interval '1 second')
            ",
        )
        .bind(organization_id)
        .bind(project_id)
        .bind(service_instance_id)
        .bind(stale_seconds)
        .fetch_one(&self.pool)
        .await?;
        let (ingress_bytes, egress_bytes): (i64, i64) = sqlx::query_as(
            r"
            SELECT
                COALESCE(sum(quantity) FILTER (WHERE event_type = 'ingress_bytes'), 0)::bigint,
                COALESCE(sum(quantity) FILTER (WHERE event_type = 'egress_bytes'), 0)::bigint
            FROM usage_events
            WHERE organization_id = $1
              AND project_id = $2
              AND service_instance_id = $3
            ",
        )
        .bind(organization_id)
        .bind(project_id)
        .bind(service_instance_id)
        .fetch_one(&self.pool)
        .await?;

        Ok(ServiceOverviewSnapshot {
            active_rooms: u64::try_from(active_rooms)
                .map_err(|_| StoreError::CorruptData("active room count"))?,
            p2p_connections: u64::try_from(p2p_connections)
                .map_err(|_| StoreError::CorruptData("P2P connection count"))?,
            ingress_bytes,
            egress_bytes,
            provider_room_names,
        })
    }

    pub async fn open_signaling_connection(
        &self,
        connection: NewSignalingConnection,
    ) -> Result<(), StoreError> {
        let inserted = sqlx::query(
            r"
            INSERT INTO flow_signaling_connections (
                connection_id, organization_id, project_id, service_instance_id,
                room_id, principal_id
            )
            SELECT $1, $2, $3, $4, id, $6
            FROM flow_rooms
            WHERE id = $5
              AND organization_id = $2
              AND project_id = $3
              AND service_instance_id = $4
              AND mode = 'p2p'
              AND state = 'ready'
            ON CONFLICT (connection_id) DO NOTHING
            ",
        )
        .bind(connection.connection_id)
        .bind(connection.organization_id)
        .bind(connection.project_id)
        .bind(connection.service_instance_id)
        .bind(connection.room_id)
        .bind(connection.principal_id)
        .execute(&self.pool)
        .await
        .map_err(map_database_error)?;
        if inserted.rows_affected() == 1 {
            Ok(())
        } else {
            Err(StoreError::Conflict(
                "signaling connection already exists or room is unavailable",
            ))
        }
    }

    pub async fn heartbeat_signaling_connection(
        &self,
        connection_id: Uuid,
    ) -> Result<bool, StoreError> {
        let updated = sqlx::query(
            r"
            UPDATE flow_signaling_connections
            SET last_seen_at = now()
            WHERE connection_id = $1 AND closed_at IS NULL
            ",
        )
        .bind(connection_id)
        .execute(&self.pool)
        .await?;
        Ok(updated.rows_affected() == 1)
    }

    pub async fn close_signaling_connection(
        &self,
        connection_id: Uuid,
    ) -> Result<bool, StoreError> {
        let updated = sqlx::query(
            r"
            UPDATE flow_signaling_connections
            SET last_seen_at = now(), closed_at = now()
            WHERE connection_id = $1 AND closed_at IS NULL
            ",
        )
        .bind(connection_id)
        .execute(&self.pool)
        .await?;
        Ok(updated.rows_affected() == 1)
    }

    pub async fn create_ticket(&self, ticket: NewTicket) -> Result<MatchmakingTicket, StoreError> {
        ticket.validate()?;
        let row = sqlx::query_as::<_, TicketRow>(
            r"
            INSERT INTO matchmaking_tickets (
                id, organization_id, project_id, service_instance_id,
                principal_id, queue_name, mode, match_size, state, attributes,
                expires_at
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 'queued', $9, $10)
            RETURNING *
            ",
        )
        .bind(ticket.id)
        .bind(ticket.organization_id)
        .bind(ticket.project_id)
        .bind(ticket.service_instance_id)
        .bind(ticket.principal_id)
        .bind(ticket.queue_name)
        .bind(ticket.mode.to_string())
        .bind(ticket.match_size)
        .bind(ticket.attributes)
        .bind(ticket.expires_at)
        .fetch_one(&self.pool)
        .await
        .map_err(map_database_error)?;
        row.try_into()
    }

    pub async fn get_ticket(
        &self,
        organization_id: Uuid,
        project_id: Uuid,
        service_instance_id: Uuid,
        ticket_id: Uuid,
    ) -> Result<MatchmakingTicket, StoreError> {
        let row = sqlx::query_as::<_, TicketRow>(
            r"
            SELECT * FROM matchmaking_tickets
            WHERE id = $1
              AND organization_id = $2
              AND project_id = $3
              AND service_instance_id = $4
            ",
        )
        .bind(ticket_id)
        .bind(organization_id)
        .bind(project_id)
        .bind(service_instance_id)
        .fetch_optional(&self.pool)
        .await?
        .ok_or(StoreError::NotFound)?;
        row.try_into()
    }

    pub async fn list_tickets(
        &self,
        organization_id: Uuid,
        project_id: Uuid,
        service_instance_id: Uuid,
        queue_name: &str,
        limit: i64,
    ) -> Result<Vec<MatchmakingTicket>, StoreError> {
        let rows = sqlx::query_as::<_, TicketRow>(
            r"
            SELECT * FROM matchmaking_tickets
            WHERE organization_id = $1
              AND project_id = $2
              AND service_instance_id = $3
              AND queue_name = $4
            ORDER BY created_at DESC
            LIMIT $5
            ",
        )
        .bind(organization_id)
        .bind(project_id)
        .bind(service_instance_id)
        .bind(queue_name)
        .bind(limit.clamp(1, 200))
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter().map(TryInto::try_into).collect()
    }

    pub async fn cancel_ticket(
        &self,
        organization_id: Uuid,
        project_id: Uuid,
        service_instance_id: Uuid,
        principal_id: Uuid,
        ticket_id: Uuid,
    ) -> Result<MatchmakingTicket, StoreError> {
        let row = sqlx::query_as::<_, TicketRow>(
            r"
            UPDATE matchmaking_tickets
            SET state = 'cancelled', updated_at = now()
            WHERE id = $1
              AND organization_id = $2
              AND project_id = $3
              AND service_instance_id = $4
              AND principal_id = $5
              AND state = 'queued'
            RETURNING *
            ",
        )
        .bind(ticket_id)
        .bind(organization_id)
        .bind(project_id)
        .bind(service_instance_id)
        .bind(principal_id)
        .fetch_optional(&self.pool)
        .await?;

        if let Some(row) = row {
            return row.try_into();
        }
        let existing = self
            .get_ticket(organization_id, project_id, service_instance_id, ticket_id)
            .await?;
        if existing.principal_id != principal_id {
            return Err(StoreError::NotFound);
        }
        Err(StoreError::Conflict("ticket can no longer be cancelled"))
    }

    pub async fn assignment_for_ticket(
        &self,
        organization_id: Uuid,
        project_id: Uuid,
        service_instance_id: Uuid,
        ticket_id: Uuid,
    ) -> Result<Option<MatchAssignment>, StoreError> {
        let row = sqlx::query_as::<_, AssignmentRow>(
            r"
            SELECT a.*
            FROM match_assignments a
            JOIN matchmaking_tickets t ON t.id = a.ticket_id
            WHERE a.ticket_id = $1
              AND t.organization_id = $2
              AND t.project_id = $3
              AND t.service_instance_id = $4
            ",
        )
        .bind(ticket_id)
        .bind(organization_id)
        .bind(project_id)
        .bind(service_instance_id)
        .fetch_optional(&self.pool)
        .await?;
        row.map(TryInto::try_into).transpose()
    }

    pub async fn create_room(&self, room: NewRoom) -> Result<FlowRoom, StoreError> {
        room.validate()?;
        let row = insert_room(&self.pool, room).await?;
        row.try_into()
    }

    pub async fn get_room(
        &self,
        organization_id: Uuid,
        project_id: Uuid,
        service_instance_id: Uuid,
        room_id: Uuid,
    ) -> Result<FlowRoom, StoreError> {
        let row = sqlx::query_as::<_, RoomRow>(
            r"
            SELECT * FROM flow_rooms
            WHERE id = $1
              AND organization_id = $2
              AND project_id = $3
              AND service_instance_id = $4
            ",
        )
        .bind(room_id)
        .bind(organization_id)
        .bind(project_id)
        .bind(service_instance_id)
        .fetch_optional(&self.pool)
        .await?
        .ok_or(StoreError::NotFound)?;
        row.try_into()
    }

    pub async fn list_rooms(
        &self,
        organization_id: Uuid,
        project_id: Uuid,
        service_instance_id: Uuid,
        limit: i64,
    ) -> Result<Vec<FlowRoom>, StoreError> {
        let rows = sqlx::query_as::<_, RoomRow>(
            r"
            SELECT * FROM flow_rooms
            WHERE organization_id = $1
              AND project_id = $2
              AND service_instance_id = $3
            ORDER BY created_at DESC
            LIMIT $4
            ",
        )
        .bind(organization_id)
        .bind(project_id)
        .bind(service_instance_id)
        .bind(limit.clamp(1, 200))
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter().map(TryInto::try_into).collect()
    }

    pub async fn claim_match(
        &self,
        reservation_ttl: Duration,
    ) -> Result<Option<MatchCandidate>, StoreError> {
        let mut transaction = self.pool.begin().await?;
        requeue_abandoned_matches(&mut transaction).await?;
        expire_waiting_tickets(&mut transaction).await?;

        let group = sqlx::query_as::<_, (Uuid, Uuid, Uuid, String, String, i32)>(
            r"
            SELECT organization_id, project_id, service_instance_id,
                   queue_name, mode, match_size
            FROM matchmaking_tickets
            WHERE state = 'queued' AND expires_at > now()
            GROUP BY organization_id, project_id, service_instance_id,
                     queue_name, mode, match_size
            HAVING count(*) >= match_size
            ORDER BY min(created_at)
            LIMIT 1
            ",
        )
        .fetch_optional(&mut *transaction)
        .await?;

        let Some((
            organization_id,
            project_id,
            service_instance_id,
            queue_name,
            mode_name,
            match_size,
        )) = group
        else {
            transaction.commit().await?;
            return Ok(None);
        };

        let group_lock_key = serde_json::to_string(&(
            organization_id,
            project_id,
            service_instance_id,
            &queue_name,
            &mode_name,
            match_size,
        ))
        .map_err(|_| StoreError::Configuration("match group is not serializable"))?;
        advisory_lock(&mut transaction, &group_lock_key).await?;

        let rows = sqlx::query_as::<_, TicketRow>(
            r"
            SELECT *
            FROM matchmaking_tickets
            WHERE organization_id = $1
              AND project_id = $2
              AND service_instance_id = $3
              AND queue_name = $4
              AND mode = $5
              AND match_size = $6
              AND state = 'queued'
              AND expires_at > now()
            ORDER BY created_at
            FOR UPDATE
            LIMIT $6
            ",
        )
        .bind(organization_id)
        .bind(project_id)
        .bind(service_instance_id)
        .bind(&queue_name)
        .bind(&mode_name)
        .bind(match_size)
        .fetch_all(&mut *transaction)
        .await?;

        if rows.len() != usize::try_from(match_size).unwrap_or_default() {
            transaction.rollback().await?;
            return Ok(None);
        }

        let room_id = Uuid::now_v7();
        let reservation_seconds = i64::try_from(reservation_ttl.as_secs())
            .map_err(|_| StoreError::Configuration("reservation TTL is too large"))?;
        let ticket_ids: Vec<Uuid> = rows.iter().map(|row| row.id).collect();
        sqlx::query(
            r"
            UPDATE matchmaking_tickets
            SET state = 'matching',
                reservation_id = $1,
                reservation_expires_at = now() + ($2 * interval '1 second'),
                updated_at = now()
            WHERE id = ANY($3)
            ",
        )
        .bind(room_id)
        .bind(reservation_seconds)
        .bind(&ticket_ids)
        .execute(&mut *transaction)
        .await?;

        let mode = SessionMode::from_str(&mode_name)?;
        let room_name = format!("match-{room_id}");
        let new_room = NewRoom {
            id: room_id,
            organization_id,
            project_id,
            service_instance_id,
            name: room_name,
            provider_room_name: (mode == SessionMode::Sfu).then(|| format!("flow-{room_id}")),
            mode,
            state: RoomState::Provisioning,
            max_participants: match_size,
            metadata: serde_json::json!({
                "queue": queue_name,
                "source": "matchmaker"
            }),
        };
        new_room.validate()?;
        let room_row = insert_room(&mut *transaction, new_room).await?;
        transaction.commit().await?;

        Ok(Some(MatchCandidate {
            room: room_row.try_into()?,
            tickets: rows
                .into_iter()
                .map(TryInto::try_into)
                .collect::<Result<_, _>>()?,
        }))
    }

    pub async fn complete_match(&self, room_id: Uuid) -> Result<Vec<MatchAssignment>, StoreError> {
        let mut transaction = self.pool.begin().await?;
        let room =
            sqlx::query_as::<_, RoomRow>("SELECT * FROM flow_rooms WHERE id = $1 FOR UPDATE")
                .bind(room_id)
                .fetch_optional(&mut *transaction)
                .await?
                .ok_or(StoreError::NotFound)?;
        if room.state != "provisioning" {
            return Err(StoreError::Conflict("room is not provisioning"));
        }

        let tickets = sqlx::query_as::<_, TicketRow>(
            r"
            SELECT * FROM matchmaking_tickets
            WHERE reservation_id = $1 AND state = 'matching'
            ORDER BY created_at
            FOR UPDATE
            ",
        )
        .bind(room_id)
        .fetch_all(&mut *transaction)
        .await?;
        if tickets.is_empty() {
            return Err(StoreError::Conflict("match reservation is empty"));
        }
        let peers: Vec<Uuid> = tickets.iter().map(|ticket| ticket.principal_id).collect();
        let peers_json = serde_json::to_value(&peers)
            .map_err(|_| StoreError::Configuration("peer identifiers are not serializable"))?;
        let mut assignments = Vec::with_capacity(tickets.len());
        for ticket in &tickets {
            let row = sqlx::query_as::<_, AssignmentRow>(
                r"
                INSERT INTO match_assignments (
                    id, ticket_id, room_id, peer_principal_ids
                )
                VALUES ($1, $2, $3, $4)
                RETURNING *
                ",
            )
            .bind(Uuid::now_v7())
            .bind(ticket.id)
            .bind(room_id)
            .bind(&peers_json)
            .fetch_one(&mut *transaction)
            .await?;
            assignments.push(row.try_into()?);
        }

        sqlx::query(
            r"
            UPDATE matchmaking_tickets
            SET state = 'assigned',
                reservation_id = NULL,
                reservation_expires_at = NULL,
                updated_at = now()
            WHERE reservation_id = $1 AND state = 'matching'
            ",
        )
        .bind(room_id)
        .execute(&mut *transaction)
        .await?;
        sqlx::query("UPDATE flow_rooms SET state = 'ready', updated_at = now() WHERE id = $1")
            .bind(room_id)
            .execute(&mut *transaction)
            .await?;
        transaction.commit().await?;
        Ok(assignments)
    }

    pub async fn release_match(&self, room_id: Uuid, reason: &str) -> Result<(), StoreError> {
        let mut transaction = self.pool.begin().await?;
        sqlx::query(
            r"
            UPDATE matchmaking_tickets
            SET state = CASE WHEN expires_at <= now() THEN 'expired' ELSE 'queued' END,
                reservation_id = NULL,
                reservation_expires_at = NULL,
                updated_at = now()
            WHERE reservation_id = $1 AND state = 'matching'
            ",
        )
        .bind(room_id)
        .execute(&mut *transaction)
        .await?;
        sqlx::query(
            r"
            UPDATE flow_rooms
            SET state = 'failed', failure_reason = $2, updated_at = now()
            WHERE id = $1 AND state = 'provisioning'
            ",
        )
        .bind(room_id)
        .bind(truncate(reason, 1000))
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        Ok(())
    }

    pub async fn append_audit(&self, event: NewAuditEvent) -> Result<(), StoreError> {
        sqlx::query(
            r"
            INSERT INTO audit_events (
                id, organization_id, project_id, service_instance_id,
                principal_id, principal_context_id, request_id, action,
                resource_type, resource_id, outcome, details
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
            ",
        )
        .bind(event.id)
        .bind(event.organization_id)
        .bind(event.project_id)
        .bind(event.service_instance_id)
        .bind(event.principal_id)
        .bind(event.principal_context_id)
        .bind(event.request_id)
        .bind(event.action)
        .bind(event.resource_type)
        .bind(event.resource_id)
        .bind(event.outcome)
        .bind(event.details)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn record_usage(&self, event: NewUsageEvent) -> Result<bool, StoreError> {
        let result: PgQueryResult = sqlx::query(
            r"
            INSERT INTO usage_events (
                id, organization_id, project_id, service_instance_id,
                principal_id, event_type, resource_id, quantity, idempotency_key,
                dimensions, occurred_at
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
            ON CONFLICT (service_instance_id, idempotency_key) DO NOTHING
            ",
        )
        .bind(event.id)
        .bind(event.organization_id)
        .bind(event.project_id)
        .bind(event.service_instance_id)
        .bind(event.principal_id)
        .bind(event.event_type)
        .bind(event.resource_id)
        .bind(event.quantity)
        .bind(event.idempotency_key)
        .bind(event.dimensions)
        .bind(event.occurred_at)
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected() == 1)
    }
}

async fn insert_room<'e, E>(executor: E, room: NewRoom) -> Result<RoomRow, StoreError>
where
    E: sqlx::Executor<'e, Database = Postgres>,
{
    sqlx::query_as::<_, RoomRow>(
        r"
        INSERT INTO flow_rooms (
            id, organization_id, project_id, service_instance_id, name,
            provider_room_name, mode, state, max_participants, metadata
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
        RETURNING *
        ",
    )
    .bind(room.id)
    .bind(room.organization_id)
    .bind(room.project_id)
    .bind(room.service_instance_id)
    .bind(room.name)
    .bind(room.provider_room_name)
    .bind(room.mode.to_string())
    .bind(room.state.to_string())
    .bind(room.max_participants)
    .bind(room.metadata)
    .fetch_one(executor)
    .await
    .map_err(map_database_error)
}

async fn requeue_abandoned_matches(
    transaction: &mut Transaction<'_, Postgres>,
) -> Result<(), StoreError> {
    sqlx::query(
        r"
        WITH abandoned AS MATERIALIZED (
            SELECT DISTINCT reservation_id
            FROM matchmaking_tickets
            WHERE state = 'matching' AND reservation_expires_at <= now()
        ),
        requeued AS (
            UPDATE matchmaking_tickets
            SET state = CASE WHEN expires_at <= now() THEN 'expired' ELSE 'queued' END,
                reservation_id = NULL,
                reservation_expires_at = NULL,
                updated_at = now()
            WHERE state = 'matching' AND reservation_expires_at <= now()
            RETURNING id
        )
        UPDATE flow_rooms
        SET state = 'failed',
            failure_reason = 'matchmaker reservation expired',
            updated_at = now()
        WHERE state = 'provisioning'
          AND id IN (SELECT reservation_id FROM abandoned)
        ",
    )
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

async fn expire_waiting_tickets(
    transaction: &mut Transaction<'_, Postgres>,
) -> Result<(), StoreError> {
    sqlx::query(
        r"
        UPDATE matchmaking_tickets
        SET state = 'expired', updated_at = now()
        WHERE state = 'queued' AND expires_at <= now()
        ",
    )
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

fn truncate(value: &str, max: usize) -> &str {
    value.get(..max).unwrap_or(value)
}

async fn advisory_lock(
    transaction: &mut Transaction<'_, Postgres>,
    key: &str,
) -> Result<(), StoreError> {
    sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))")
        .bind(key)
        .execute(&mut **transaction)
        .await?;
    Ok(())
}

async fn insert_reconcile_receipt(
    transaction: &mut Transaction<'_, Postgres>,
    command: &ServiceInstanceReconcile,
    operation_id: Uuid,
) -> Result<(), StoreError> {
    sqlx::query(
        r"
        INSERT INTO flow_provider_token_receipts (
            jwt_id, service_instance_id, organization_id, project_id,
            generation, name, spec, operation_id
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        ",
    )
    .bind(command.jwt_id)
    .bind(command.service_instance_id)
    .bind(command.organization_id)
    .bind(command.project_id)
    .bind(command.generation)
    .bind(&command.name)
    .bind(&command.spec)
    .bind(operation_id)
    .execute(&mut **transaction)
    .await
    .map_err(map_database_error)?;
    Ok(())
}

async fn insert_delete_receipt(
    transaction: &mut Transaction<'_, Postgres>,
    command: &ServiceInstanceDelete,
    operation_id: Uuid,
) -> Result<(), StoreError> {
    sqlx::query(
        r"
        INSERT INTO flow_delete_token_receipts (
            jwt_id, service_instance_id, organization_id, project_id,
            principal_id, generation, operation_id
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        ",
    )
    .bind(command.jwt_id)
    .bind(command.service_instance_id)
    .bind(command.organization_id)
    .bind(command.project_id)
    .bind(command.principal_id)
    .bind(command.generation)
    .bind(operation_id)
    .execute(&mut **transaction)
    .await
    .map_err(map_database_error)?;
    Ok(())
}

fn map_database_error(error: sqlx::Error) -> StoreError {
    if let sqlx::Error::Database(database) = &error
        && database.is_unique_violation()
    {
        return StoreError::Conflict("resource already exists");
    }
    StoreError::Database(error)
}

#[derive(Debug, sqlx::FromRow)]
struct TicketRow {
    id: Uuid,
    organization_id: Uuid,
    project_id: Uuid,
    service_instance_id: Uuid,
    principal_id: Uuid,
    queue_name: String,
    mode: String,
    match_size: i32,
    state: String,
    attributes: Value,
    reservation_id: Option<Uuid>,
    #[allow(dead_code)]
    reservation_expires_at: Option<DateTime<Utc>>,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
    expires_at: DateTime<Utc>,
}

impl TryFrom<TicketRow> for MatchmakingTicket {
    type Error = StoreError;

    fn try_from(row: TicketRow) -> Result<Self, Self::Error> {
        Ok(Self {
            id: row.id,
            organization_id: row.organization_id,
            project_id: row.project_id,
            service_instance_id: row.service_instance_id,
            principal_id: row.principal_id,
            queue_name: row.queue_name,
            mode: SessionMode::from_str(&row.mode)?,
            match_size: row.match_size,
            state: TicketState::from_str(&row.state)?,
            attributes: row.attributes,
            reservation_id: row.reservation_id,
            created_at: row.created_at,
            updated_at: row.updated_at,
            expires_at: row.expires_at,
        })
    }
}

#[derive(Debug, sqlx::FromRow)]
struct RoomRow {
    id: Uuid,
    organization_id: Uuid,
    project_id: Uuid,
    service_instance_id: Uuid,
    name: String,
    provider_room_name: Option<String>,
    mode: String,
    state: String,
    max_participants: i32,
    metadata: Value,
    failure_reason: Option<String>,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
}

impl TryFrom<RoomRow> for FlowRoom {
    type Error = StoreError;

    fn try_from(row: RoomRow) -> Result<Self, Self::Error> {
        Ok(Self {
            id: row.id,
            organization_id: row.organization_id,
            project_id: row.project_id,
            service_instance_id: row.service_instance_id,
            name: row.name,
            provider_room_name: row.provider_room_name,
            mode: SessionMode::from_str(&row.mode)?,
            state: RoomState::from_str(&row.state)?,
            max_participants: row.max_participants,
            metadata: row.metadata,
            failure_reason: row.failure_reason,
            created_at: row.created_at,
            updated_at: row.updated_at,
        })
    }
}

#[derive(Debug, sqlx::FromRow)]
struct ServiceInstanceRow {
    #[allow(dead_code)]
    id: Uuid,
    organization_id: Uuid,
    project_id: Uuid,
    name: String,
    desired_generation: i64,
    desired_spec: Value,
    #[allow(dead_code)]
    observed_generation: i64,
    #[allow(dead_code)]
    status: Value,
    current_operation_id: Uuid,
    #[allow(dead_code)]
    created_at: DateTime<Utc>,
    #[allow(dead_code)]
    updated_at: DateTime<Utc>,
}

#[derive(Debug, sqlx::FromRow)]
struct ReconcileReceiptRow {
    #[allow(dead_code)]
    jwt_id: Uuid,
    service_instance_id: Uuid,
    organization_id: Uuid,
    project_id: Uuid,
    generation: i64,
    name: String,
    spec: Value,
    operation_id: Uuid,
    #[allow(dead_code)]
    created_at: DateTime<Utc>,
}

impl ReconcileReceiptRow {
    fn matches(&self, command: &ServiceInstanceReconcile) -> bool {
        self.service_instance_id == command.service_instance_id
            && self.organization_id == command.organization_id
            && self.project_id == command.project_id
            && self.generation == command.generation
            && self.name == command.name
            && self.spec == command.spec
    }
}

#[derive(Debug, sqlx::FromRow)]
struct DeleteOperationRow {
    id: Uuid,
    service_instance_id: Uuid,
    organization_id: Uuid,
    project_id: Uuid,
    principal_id: Uuid,
    generation: i64,
    state: String,
    status: Value,
    provider_room_names: Value,
    #[allow(dead_code)]
    created_at: DateTime<Utc>,
    #[allow(dead_code)]
    updated_at: DateTime<Utc>,
}

impl DeleteOperationRow {
    fn matches(&self, command: &ServiceInstanceDelete) -> bool {
        self.service_instance_id == command.service_instance_id
            && self.organization_id == command.organization_id
            && self.project_id == command.project_id
            && self.principal_id == command.principal_id
            && self.generation == command.generation
    }

    fn status(&self) -> Result<ServiceInstanceStatus, StoreError> {
        serde_json::from_value(self.status.clone())
            .map_err(|_| StoreError::CorruptData("delete operation status"))
    }

    fn try_into_preparation(
        self,
        command: &ServiceInstanceDelete,
    ) -> Result<DeletePreparation, StoreError> {
        if !self.matches(command) {
            return Err(StoreError::Conflict(
                "service instance delete scope or generation does not match",
            ));
        }
        let status = self.status()?;
        let completed = match self.state.as_str() {
            "deleting" if status == ServiceInstanceStatus::deleting(self.generation, self.id) => {
                false
            }
            "succeeded" if status == ServiceInstanceStatus::deleted(self.generation, self.id) => {
                true
            }
            _ => return Err(StoreError::CorruptData("delete operation status")),
        };
        let provider_room_names = serde_json::from_value::<Vec<String>>(self.provider_room_names)
            .map_err(|_| StoreError::CorruptData("provider room names"))?;
        if provider_room_names.iter().any(String::is_empty) {
            return Err(StoreError::CorruptData("provider room names"));
        }
        Ok(DeletePreparation {
            operation_id: self.id,
            status,
            provider_room_names,
            completed,
        })
    }
}

#[derive(Debug, sqlx::FromRow)]
struct DeleteReceiptRow {
    #[allow(dead_code)]
    jwt_id: Uuid,
    service_instance_id: Uuid,
    organization_id: Uuid,
    project_id: Uuid,
    principal_id: Uuid,
    generation: i64,
    operation_id: Uuid,
    #[allow(dead_code)]
    created_at: DateTime<Utc>,
}

impl DeleteReceiptRow {
    fn matches(&self, command: &ServiceInstanceDelete) -> bool {
        self.service_instance_id == command.service_instance_id
            && self.organization_id == command.organization_id
            && self.project_id == command.project_id
            && self.principal_id == command.principal_id
            && self.generation == command.generation
    }
}

#[derive(Debug, sqlx::FromRow)]
struct AssignmentRow {
    id: Uuid,
    ticket_id: Uuid,
    room_id: Uuid,
    peer_principal_ids: Value,
    created_at: DateTime<Utc>,
}

impl TryFrom<AssignmentRow> for MatchAssignment {
    type Error = StoreError;

    fn try_from(row: AssignmentRow) -> Result<Self, Self::Error> {
        Ok(Self {
            id: row.id,
            ticket_id: row.ticket_id,
            room_id: row.room_id,
            peer_principal_ids: serde_json::from_value(row.peer_principal_ids)
                .map_err(|_| StoreError::CorruptData("assignment peers"))?,
            created_at: row.created_at,
        })
    }
}

#[derive(Debug, Error)]
pub enum StoreError {
    #[error("resource was not found")]
    NotFound,
    #[error("resource conflict: {0}")]
    Conflict(&'static str),
    #[error("service instance generation {requested} is stale; current generation is {current}")]
    StaleGeneration { current: i64, requested: i64 },
    #[error("store configuration error: {0}")]
    Configuration(&'static str),
    #[error("stored data is invalid: {0}")]
    CorruptData(&'static str),
    #[error(transparent)]
    Validation(#[from] ValidationError),
    #[error(transparent)]
    Database(#[from] sqlx::Error),
    #[error(transparent)]
    Migration(#[from] sqlx::migrate::MigrateError),
}
