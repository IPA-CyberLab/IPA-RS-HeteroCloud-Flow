use std::{collections::BTreeSet, sync::Arc, time::Duration};

use flow_domain::{FlowRoom, PrincipalContext, SessionMode};
use livekit_api::{
    access_token::{AccessToken, VideoGrants},
    services::{LiveKitApi, room::CreateRoomOptions},
};
use thiserror::Error;

const ROOM_API_BATCH_SIZE: usize = 100;

#[derive(Clone)]
pub struct LiveKitClient {
    api: Arc<LiveKitApi>,
    api_key: String,
    api_secret: String,
}

impl LiveKitClient {
    pub fn new(
        host: &str,
        api_key: impl Into<String>,
        api_secret: impl Into<String>,
    ) -> Result<Self, LiveKitError> {
        let api_key = api_key.into();
        let api_secret = api_secret.into();
        if host.is_empty() || api_key.is_empty() || api_secret.len() < 32 {
            return Err(LiveKitError::InvalidConfiguration);
        }
        Ok(Self {
            api: Arc::new(
                LiveKitApi::with_api_key(host, &api_key, &api_secret)
                    .with_request_timeout(Duration::from_secs(10)),
            ),
            api_key,
            api_secret,
        })
    }

    pub async fn create_room(&self, room: &FlowRoom) -> Result<(), LiveKitError> {
        if room.mode != SessionMode::Sfu {
            return Ok(());
        }
        let provider_name = room
            .provider_room_name
            .as_deref()
            .ok_or(LiveKitError::MissingProviderRoom)?;
        self.api
            .room()
            .create_room(
                provider_name,
                CreateRoomOptions {
                    empty_timeout: 300,
                    departure_timeout: 30,
                    max_participants: u32::try_from(room.max_participants)
                        .map_err(|_| LiveKitError::InvalidParticipantLimit)?,
                    metadata: serde_json::to_string(&room.metadata)
                        .map_err(|_| LiveKitError::InvalidMetadata)?,
                    ..Default::default()
                },
            )
            .await
            .map_err(|error| LiveKitError::Service(error.to_string()))?;
        Ok(())
    }

    pub async fn participant_count(&self, room_names: &[String]) -> Result<u64, LiveKitError> {
        let mut count = 0_u64;
        let mut seen_rooms = BTreeSet::new();
        for names in room_names.chunks(ROOM_API_BATCH_SIZE) {
            let rooms = self
                .api
                .room()
                .list_rooms(names.to_vec())
                .await
                .map_err(|error| LiveKitError::Service(error.to_string()))?;
            for room in rooms {
                if !names.iter().any(|name| name == &room.name) {
                    return Err(LiveKitError::UnexpectedProviderRoom(room.name));
                }
                if !seen_rooms.insert(room.name) {
                    return Err(LiveKitError::DuplicateProviderRoom);
                }
                count = count
                    .checked_add(u64::from(room.num_participants))
                    .ok_or(LiveKitError::ParticipantCountOverflow)?;
            }
        }
        Ok(count)
    }

    pub async fn delete_rooms(&self, room_names: &[String]) -> Result<(), LiveKitError> {
        let mut seen_rooms = BTreeSet::new();
        for names in room_names.chunks(ROOM_API_BATCH_SIZE) {
            // Listing first makes retries safe when an earlier attempt deleted only a prefix.
            let rooms = self
                .api
                .room()
                .list_rooms(names.to_vec())
                .await
                .map_err(|error| LiveKitError::Service(error.to_string()))?;
            for room in rooms {
                if !names.iter().any(|name| name == &room.name) {
                    return Err(LiveKitError::UnexpectedProviderRoom(room.name));
                }
                if !seen_rooms.insert(room.name.clone()) {
                    return Err(LiveKitError::DuplicateProviderRoom);
                }
                self.api
                    .room()
                    .delete_room(&room.name)
                    .await
                    .map_err(|error| LiveKitError::Service(error.to_string()))?;
            }
        }
        Ok(())
    }

    pub fn issue_participant_token(
        &self,
        room: &FlowRoom,
        principal: &PrincipalContext,
        display_name: &str,
        can_publish: bool,
        can_subscribe: bool,
        ttl: Duration,
    ) -> Result<String, LiveKitError> {
        if room.mode != SessionMode::Sfu {
            return Err(LiveKitError::WrongMode);
        }
        let provider_name = room
            .provider_room_name
            .as_deref()
            .ok_or(LiveKitError::MissingProviderRoom)?;
        let identity = format!(
            "{}:{}:{}:{}",
            principal.organization_id,
            principal.project_id,
            principal.service_instance_id,
            principal.principal_id
        );
        let metadata = serde_json::json!({
            "organization_id": principal.organization_id,
            "project_id": principal.project_id,
            "service_instance_id": principal.service_instance_id,
            "principal_id": principal.principal_id,
            "flow_room_id": room.id,
        });
        AccessToken::with_api_key(&self.api_key, &self.api_secret)
            .with_ttl(ttl)
            .with_identity(&identity)
            .with_name(display_name)
            .with_metadata(
                &serde_json::to_string(&metadata).map_err(|_| LiveKitError::InvalidMetadata)?,
            )
            .with_grants(VideoGrants {
                room_join: true,
                room: provider_name.to_owned(),
                can_publish,
                can_subscribe,
                can_publish_data: true,
                ..Default::default()
            })
            .to_jwt()
            .map_err(|error| LiveKitError::Token(error.to_string()))
    }
}

#[derive(Debug, Error)]
pub enum LiveKitError {
    #[error("invalid LiveKit configuration")]
    InvalidConfiguration,
    #[error("SFU room has no provider room name")]
    MissingProviderRoom,
    #[error("operation requires an SFU room")]
    WrongMode,
    #[error("participant limit is invalid")]
    InvalidParticipantLimit,
    #[error("LiveKit participant count overflowed")]
    ParticipantCountOverflow,
    #[error("LiveKit returned an unrequested provider room: {0}")]
    UnexpectedProviderRoom(String),
    #[error("LiveKit returned a duplicate provider room")]
    DuplicateProviderRoom,
    #[error("room metadata is invalid")]
    InvalidMetadata,
    #[error("LiveKit service request failed: {0}")]
    Service(String),
    #[error("LiveKit token generation failed: {0}")]
    Token(String),
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeSet, time::Duration};

    use chrono::Utc;
    use flow_domain::{FlowRoom, PrincipalContext, RoomState, SessionMode};
    use livekit_api::access_token::TokenVerifier;
    use serde_json::json;
    use uuid::Uuid;

    use super::LiveKitClient;

    #[test]
    fn participant_token_is_scoped_to_room_and_identity() {
        let secret = "livekit-secret-with-at-least-thirty-two-bytes";
        let client = LiveKitClient::new("http://livekit:7880", "flow-key", secret).unwrap();
        let now = Utc::now();
        let organization_id = Uuid::new_v4();
        let project_id = Uuid::new_v4();
        let service_instance_id = Uuid::new_v4();
        let principal_id = Uuid::new_v4();
        let room = FlowRoom {
            id: Uuid::new_v4(),
            organization_id,
            project_id,
            service_instance_id,
            name: "room-a".into(),
            provider_room_name: Some("flow-room-a".into()),
            mode: SessionMode::Sfu,
            state: RoomState::Ready,
            max_participants: 10,
            metadata: json!({}),
            failure_reason: None,
            created_at: now,
            updated_at: now,
        };
        let principal = PrincipalContext {
            organization_id,
            project_id,
            service_instance_id,
            principal_id,
            permissions: BTreeSet::new(),
            issued_at: now,
            expires_at: now + chrono::Duration::minutes(2),
            token_id: Uuid::new_v4(),
        };

        let token = client
            .issue_participant_token(
                &room,
                &principal,
                "Alice",
                true,
                true,
                Duration::from_mins(2),
            )
            .unwrap();
        let claims = TokenVerifier::with_api_key("flow-key", secret)
            .verify(&token)
            .unwrap();
        assert_eq!(
            claims.sub,
            format!("{organization_id}:{project_id}:{service_instance_id}:{principal_id}")
        );
        assert_eq!(claims.video.room, "flow-room-a");
        assert!(claims.video.room_join);
    }
}
