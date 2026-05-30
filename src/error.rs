use axum::{
    Json,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use serde::Serialize;

use crate::pool::PoolError;

#[derive(Serialize)]
struct ErrorResponse {
    error: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    field: Option<String>,
}

#[derive(Debug)]
pub(crate) enum ApiError {
    Validation {
        field: Option<String>,
        error: String,
    },
    NotFound(String),
    Conflict(String),
    PoolExhausted(String),
    Backend(String),
}

impl From<PoolError> for ApiError {
    fn from(error: PoolError) -> Self {
        match error {
            PoolError::SlotNotFound(slot) => Self::NotFound(format!("slot {slot} does not exist")),
            PoolError::VmNotFound(name) => Self::NotFound(format!("VM '{name}' not found")),
            PoolError::VmAlreadyRunning(name) => {
                Self::Conflict(format!("VM '{name}' is already running"))
            }
            PoolError::NoAvailableSlot => Self::PoolExhausted("pool exhausted".to_string()),
            PoolError::InvalidTransition { slot, from, action } => Self::Conflict(format!(
                "slot {slot} cannot perform {action} from state {from:?}"
            )),
            PoolError::ChannelClosed => Self::Backend("slot worker channel is closed".to_string()),
            PoolError::Backend(error) => Self::Backend(error),
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (status, field, error) = match self {
            Self::Validation { field, error } => (StatusCode::BAD_REQUEST, field, error),
            Self::NotFound(error) => (StatusCode::NOT_FOUND, None, error),
            Self::Conflict(error) => (StatusCode::CONFLICT, None, error),
            Self::PoolExhausted(error) => (StatusCode::SERVICE_UNAVAILABLE, None, error),
            Self::Backend(error) => (StatusCode::INTERNAL_SERVER_ERROR, None, error),
        };

        (status, Json(ErrorResponse { error, field })).into_response()
    }
}
