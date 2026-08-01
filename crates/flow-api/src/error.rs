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

#[derive(Serialize)]
struct ErrorEnvelope {
    error: ErrorBody,
}

#[derive(Serialize)]
struct ErrorBody {
    code: &'static str,
    message: String,
}
