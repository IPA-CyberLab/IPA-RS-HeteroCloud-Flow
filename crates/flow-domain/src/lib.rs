use std::{collections::BTreeSet, fmt, str::FromStr};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;
use uuid::Uuid;

const MAX_EXTERNAL_ID_LEN: usize = 128;
const MAX_QUEUE_NAME_LEN: usize = 96;
const MAX_ROOM_NAME_LEN: usize = 160;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PrincipalContext {
    pub organization_id: Uuid,
    pub project_id: Uuid,
    pub service_instance_id: Uuid,
    pub principal_id: Uuid,
    #[serde(default)]
    pub permissions: BTreeSet<String>,
    pub issued_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
    pub token_id: Uuid,
}

impl PrincipalContext {
    /// Validates mandatory external identifiers and delegated lifetime.
    ///
    /// # Errors
    ///
    /// Returns [`ValidationError`] when an identifier or time range is invalid.
    pub fn validate(&self) -> Result<(), ValidationError> {
        if self.expires_at <= self.issued_at {
            return Err(ValidationError::InvalidTimeRange);
        }
        Ok(())
    }

    #[must_use]
    pub fn allows(&self, permission: &str) -> bool {
        self.permissions.contains(permission)
            || self.permissions.contains("flow.*")
            || self.permissions.contains("*")
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionMode {
    P2p,
    Sfu,
}

impl fmt::Display for SessionMode {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::P2p => formatter.write_str("p2p"),
            Self::Sfu => formatter.write_str("sfu"),
        }
    }
}

impl FromStr for SessionMode {
    type Err = ValidationError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "p2p" => Ok(Self::P2p),
            "sfu" => Ok(Self::Sfu),
            other => Err(ValidationError::InvalidEnum {
                field: "mode",
                value: other.to_owned(),
            }),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TicketState {
    Queued,
    Matching,
    Assigned,
    Cancelled,
    Expired,
}

impl fmt::Display for TicketState {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Queued => formatter.write_str("queued"),
            Self::Matching => formatter.write_str("matching"),
            Self::Assigned => formatter.write_str("assigned"),
            Self::Cancelled => formatter.write_str("cancelled"),
            Self::Expired => formatter.write_str("expired"),
        }
    }
}

impl FromStr for TicketState {
    type Err = ValidationError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "queued" => Ok(Self::Queued),
            "matching" => Ok(Self::Matching),
            "assigned" => Ok(Self::Assigned),
            "cancelled" => Ok(Self::Cancelled),
            "expired" => Ok(Self::Expired),
            other => Err(ValidationError::InvalidEnum {
                field: "ticket_state",
                value: other.to_owned(),
            }),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RoomState {
    Provisioning,
    Ready,
    Failed,
    Closed,
}

impl fmt::Display for RoomState {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Provisioning => formatter.write_str("provisioning"),
            Self::Ready => formatter.write_str("ready"),
            Self::Failed => formatter.write_str("failed"),
            Self::Closed => formatter.write_str("closed"),
        }
    }
}

impl FromStr for RoomState {
    type Err = ValidationError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "provisioning" => Ok(Self::Provisioning),
            "ready" => Ok(Self::Ready),
            "failed" => Ok(Self::Failed),
            "closed" => Ok(Self::Closed),
            other => Err(ValidationError::InvalidEnum {
                field: "room_state",
                value: other.to_owned(),
            }),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MatchmakingTicket {
    pub id: Uuid,
    pub organization_id: Uuid,
    pub project_id: Uuid,
    pub service_instance_id: Uuid,
    pub principal_id: Uuid,
    pub queue_name: String,
    pub mode: SessionMode,
    pub match_size: i32,
    pub state: TicketState,
    pub attributes: Value,
    pub reservation_id: Option<Uuid>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct NewTicket {
    pub id: Uuid,
    pub organization_id: Uuid,
    pub project_id: Uuid,
    pub service_instance_id: Uuid,
    pub principal_id: Uuid,
    pub queue_name: String,
    pub mode: SessionMode,
    pub match_size: i32,
    pub attributes: Value,
    pub expires_at: DateTime<Utc>,
}

impl NewTicket {
    /// Validates a ticket before it crosses the persistence boundary.
    ///
    /// # Errors
    ///
    /// Returns [`ValidationError`] for invalid scope, queue, size, expiry, or
    /// attributes.
    pub fn validate(&self) -> Result<(), ValidationError> {
        validate_name("queue_name", &self.queue_name, MAX_QUEUE_NAME_LEN)?;
        if !(2..=100).contains(&self.match_size) {
            return Err(ValidationError::OutOfRange {
                field: "match_size",
                min: 2,
                max: 100,
            });
        }
        if self.expires_at <= Utc::now() {
            return Err(ValidationError::InvalidTimeRange);
        }
        ensure_json_object("attributes", &self.attributes)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FlowRoom {
    pub id: Uuid,
    pub organization_id: Uuid,
    pub project_id: Uuid,
    pub service_instance_id: Uuid,
    pub name: String,
    pub provider_room_name: Option<String>,
    pub mode: SessionMode,
    pub state: RoomState,
    pub max_participants: i32,
    pub metadata: Value,
    pub failure_reason: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct NewRoom {
    pub id: Uuid,
    pub organization_id: Uuid,
    pub project_id: Uuid,
    pub service_instance_id: Uuid,
    pub name: String,
    pub provider_room_name: Option<String>,
    pub mode: SessionMode,
    pub state: RoomState,
    pub max_participants: i32,
    pub metadata: Value,
}

impl NewRoom {
    /// Validates a room before provider or database side effects occur.
    ///
    /// # Errors
    ///
    /// Returns [`ValidationError`] for invalid scope, name, capacity, or
    /// metadata.
    pub fn validate(&self) -> Result<(), ValidationError> {
        validate_name("name", &self.name, MAX_ROOM_NAME_LEN)?;
        if !(2..=1000).contains(&self.max_participants) {
            return Err(ValidationError::OutOfRange {
                field: "max_participants",
                min: 2,
                max: 1000,
            });
        }
        ensure_json_object("metadata", &self.metadata)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MatchAssignment {
    pub id: Uuid,
    pub ticket_id: Uuid,
    pub room_id: Uuid,
    pub peer_principal_ids: Vec<Uuid>,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct MatchCandidate {
    pub room: FlowRoom,
    pub tickets: Vec<MatchmakingTicket>,
}

#[derive(Debug, Clone)]
pub struct NewAuditEvent {
    pub id: Uuid,
    pub organization_id: Uuid,
    pub project_id: Uuid,
    pub service_instance_id: Uuid,
    pub principal_id: Uuid,
    pub principal_context_id: Option<Uuid>,
    pub request_id: String,
    pub action: String,
    pub resource_type: String,
    pub resource_id: Option<String>,
    pub outcome: String,
    pub details: Value,
}

#[derive(Debug, Clone)]
pub struct NewUsageEvent {
    pub id: Uuid,
    pub organization_id: Uuid,
    pub project_id: Uuid,
    pub service_instance_id: Uuid,
    pub principal_id: Option<Uuid>,
    pub event_type: String,
    pub resource_id: Option<String>,
    pub quantity: i64,
    pub idempotency_key: String,
    pub dimensions: Value,
    pub occurred_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct ServiceInstanceReconcile {
    pub jwt_id: Uuid,
    pub organization_id: Uuid,
    pub project_id: Uuid,
    pub service_instance_id: Uuid,
    pub principal_id: Uuid,
    pub generation: i64,
    pub name: String,
    pub spec: Value,
}

impl ServiceInstanceReconcile {
    /// Validates a provider command before it crosses the persistence boundary.
    ///
    /// # Errors
    ///
    /// Returns [`ValidationError`] for invalid generation, name, or spec.
    pub fn validate(&self) -> Result<(), ValidationError> {
        if self.generation <= 0 {
            return Err(ValidationError::OutOfRange {
                field: "generation",
                min: 1,
                max: i64::MAX,
            });
        }
        validate_display_name("name", &self.name, 120)?;
        ensure_json_object("spec", &self.spec)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ServiceInstancePhase {
    Ready,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServiceInstanceStatus {
    pub phase: ServiceInstancePhase,
    pub observed_generation: i64,
    pub operation_id: Uuid,
}

impl ServiceInstanceStatus {
    #[must_use]
    pub const fn ready(observed_generation: i64, operation_id: Uuid) -> Self {
        Self {
            phase: ServiceInstancePhase::Ready,
            observed_generation,
            operation_id,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReconcileOutcome {
    pub operation_id: Uuid,
    pub status: ServiceInstanceStatus,
    pub created: bool,
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum ValidationError {
    #[error("{field} must not be empty and must be at most {max} characters")]
    InvalidIdentifier { field: &'static str, max: usize },
    #[error("{field} contains unsupported characters")]
    InvalidCharacters { field: &'static str },
    #[error("{field} must be a JSON object")]
    ExpectedObject { field: &'static str },
    #[error("{field} must be between {min} and {max}")]
    OutOfRange {
        field: &'static str,
        min: i64,
        max: i64,
    },
    #[error("expiration must be after issuance")]
    InvalidTimeRange,
    #[error("unsupported {field} value: {value}")]
    InvalidEnum { field: &'static str, value: String },
}

/// Validates an externally supplied organization, project, principal, or token ID.
///
/// # Errors
///
/// Returns [`ValidationError`] when the value is empty, oversized, or contains
/// unsupported characters.
pub fn validate_external_id(field: &'static str, value: &str) -> Result<(), ValidationError> {
    validate_name(field, value, MAX_EXTERNAL_ID_LEN)
}

/// Validates a bounded protocol name.
///
/// # Errors
///
/// Returns [`ValidationError`] when the value is empty, oversized, or contains
/// unsupported characters.
pub fn validate_name(field: &'static str, value: &str, max: usize) -> Result<(), ValidationError> {
    if value.is_empty() || value.len() > max {
        return Err(ValidationError::InvalidIdentifier { field, max });
    }
    if !value.bytes().all(|byte| {
        byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b':' | b'/')
    }) {
        return Err(ValidationError::InvalidCharacters { field });
    }
    Ok(())
}

fn validate_display_name(
    field: &'static str,
    value: &str,
    max: usize,
) -> Result<(), ValidationError> {
    let length = value.trim().chars().count();
    if length == 0 || length > max {
        return Err(ValidationError::InvalidIdentifier { field, max });
    }
    Ok(())
}

fn ensure_json_object(field: &'static str, value: &Value) -> Result<(), ValidationError> {
    if value.is_object() {
        Ok(())
    } else {
        Err(ValidationError::ExpectedObject { field })
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use chrono::{Duration, Utc};
    use serde_json::json;
    use uuid::Uuid;

    use super::{
        NewTicket, PrincipalContext, ServiceInstanceReconcile, SessionMode, ValidationError,
    };

    #[test]
    fn principal_wildcard_permission_is_honored() {
        let now = Utc::now();
        let context = PrincipalContext {
            organization_id: Uuid::new_v4(),
            project_id: Uuid::new_v4(),
            service_instance_id: Uuid::new_v4(),
            principal_id: Uuid::new_v4(),
            permissions: BTreeSet::from(["flow.*".into()]),
            issued_at: now,
            expires_at: now + Duration::minutes(2),
            token_id: Uuid::new_v4(),
        };

        assert!(context.validate().is_ok());
        assert!(context.allows("flow.room.create"));
    }

    #[test]
    fn ticket_rejects_unsafe_queue_name() {
        let ticket = NewTicket {
            id: Uuid::new_v4(),
            organization_id: Uuid::new_v4(),
            project_id: Uuid::new_v4(),
            service_instance_id: Uuid::new_v4(),
            principal_id: Uuid::new_v4(),
            queue_name: "queue with spaces".into(),
            mode: SessionMode::P2p,
            match_size: 2,
            attributes: json!({}),
            expires_at: Utc::now() + Duration::minutes(1),
        };

        assert_eq!(
            ticket.validate(),
            Err(ValidationError::InvalidCharacters {
                field: "queue_name"
            })
        );
    }

    #[test]
    fn service_instance_accepts_heterocloud_display_name() {
        let reconcile = ServiceInstanceReconcile {
            jwt_id: Uuid::new_v4(),
            organization_id: Uuid::new_v4(),
            project_id: Uuid::new_v4(),
            service_instance_id: Uuid::new_v4(),
            principal_id: Uuid::new_v4(),
            generation: 1,
            name: "Flow E2E".into(),
            spec: json!({}),
        };

        assert!(reconcile.validate().is_ok());
    }

    #[test]
    fn service_instance_rejects_blank_display_name() {
        let reconcile = ServiceInstanceReconcile {
            jwt_id: Uuid::new_v4(),
            organization_id: Uuid::new_v4(),
            project_id: Uuid::new_v4(),
            service_instance_id: Uuid::new_v4(),
            principal_id: Uuid::new_v4(),
            generation: 1,
            name: "   ".into(),
            spec: json!({}),
        };

        assert_eq!(
            reconcile.validate(),
            Err(ValidationError::InvalidIdentifier {
                field: "name",
                max: 120
            })
        );
    }
}
