use axum::{
    Json,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use flow_auth::AuthError;
use flow_store::StoreError;
use serde::Serialize;
use tracing::error;

#[derive(Debug)]
pub struct ApiError {
    status: StatusCode,
    code: &'static str,
    message: String,
}

impl ApiError {
    pub fn bad_request(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            code: "invalid_request",
            message: message.into(),
        }
    }

    pub fn forbidden() -> Self {
        Self {
            status: StatusCode::FORBIDDEN,
            code: "permission_denied",
            message: "principal does not have the required permission".into(),
        }
    }

    pub fn invalid_credentials() -> Self {
        Self {
            status: StatusCode::UNAUTHORIZED,
            code: "invalid_credentials",
            message: "signed principal context is invalid or revoked".into(),
        }
    }

    pub fn conflict(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::CONFLICT,
            code: "conflict",
            message: message.into(),
        }
    }

    pub fn dependency(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::BAD_GATEWAY,
            code: "provider_unavailable",
            message: message.into(),
        }
    }

    pub fn internal(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            code: "internal_error",
            message: message.into(),
        }
    }

    pub fn rate_limited() -> Self {
        Self {
            status: StatusCode::TOO_MANY_REQUESTS,
            code: "rate_limit_exceeded",
            message: "source IP request limit exceeded".into(),
        }
    }

    pub fn rate_limit_unavailable() -> Self {
        Self {
            status: StatusCode::SERVICE_UNAVAILABLE,
            code: "rate_limit_unavailable",
            message: "request admission service is unavailable".into(),
        }
    }

    pub fn credential_status_unavailable() -> Self {
        Self {
            status: StatusCode::SERVICE_UNAVAILABLE,
            code: "credential_status_unavailable",
            message: "credential status service is unavailable".into(),
        }
    }
}

impl From<AuthError> for ApiError {
    fn from(error: AuthError) -> Self {
        Self {
            status: StatusCode::UNAUTHORIZED,
            code: "invalid_credentials",
            message: error.to_string(),
        }
    }
}

impl From<StoreError> for ApiError {
    fn from(error: StoreError) -> Self {
        match error {
            StoreError::NotFound => Self {
                status: StatusCode::NOT_FOUND,
                code: "not_found",
                message: "resource was not found".into(),
            },
            StoreError::Conflict(message) => Self::conflict(message),
            StoreError::RoomLimitExceeded { limit } => Self {
                status: StatusCode::CONFLICT,
                code: "room_limit_exceeded",
                message: format!("room limit of {limit} has been reached"),
            },
            StoreError::StaleGeneration { current, requested } => Self::conflict(format!(
                "generation {requested} is stale; current generation is {current}"
            )),
            StoreError::RevocationExpiryTooDistant => Self::bad_request(
                "principal context revocation expiry cannot be more than 315 seconds in the future",
            ),
            StoreError::Validation(error) => Self::bad_request(error.to_string()),
            other => {
                error!(error = %other, "database operation failed");
                Self {
                    status: StatusCode::INTERNAL_SERVER_ERROR,
                    code: "internal_error",
                    message: "internal service error".into(),
                }
            }
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (
            self.status,
            Json(ErrorEnvelope {
                error: ErrorBody {
                    code: self.code,
                    message: self.message,
                },
            }),
        )
            .into_response()
    }
}

#[derive(Serialize, utoipa::ToSchema)]
pub(crate) struct ErrorEnvelope {
    pub(crate) error: ErrorBody,
}

#[derive(Serialize, utoipa::ToSchema)]
pub(crate) struct ErrorBody {
    pub(crate) code: &'static str,
    pub(crate) message: String,
}
