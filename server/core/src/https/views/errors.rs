use axum::http::StatusCode;
use axum::response::{IntoResponse, Redirect, Response};
use axum_htmx::{HxEvent, HxResponseTrigger, HxReswap, HxRetarget, SwapOption};
use kanidmd_lib::idm::server::DomainInfoRead;
use utoipa::ToSchema;
use uuid::Uuid;

use kanidm_proto::internal::OperationError;

use crate::https::middleware::KOpId;
use crate::https::views::{ErrorToastPartial, UnrecoverableErrorView};
// #[derive(Template)]
// #[template(path = "recoverable_error_partial.html")]
// struct ErrorPartialView {
//     error_message: String,
//     operation_id: Uuid,
//     recovery_path: String,
//     recovery_boosted: bool,
// }

/// The web app's top level error type, this takes an `OperationError` and converts it into a HTTP response.
#[derive(Debug, ToSchema)]
pub(crate) enum HtmxError {
    /// For creating full page error screens on the webpage
    ErrorPage(HtmxErrorInfo),
    Notification(HtmxErrorInfo),
}
#[derive(Debug)]
pub(crate) struct HtmxErrorInfo(Uuid, OperationError, DomainInfoRead);

impl HtmxError {
    pub(crate) fn error_page(
        kopid: &KOpId,
        operr: OperationError,
        domain_info: DomainInfoRead,
    ) -> Self {
        HtmxError::ErrorPage(HtmxErrorInfo(kopid.eventid, operr, domain_info))
    }
    pub(crate) fn notification(
        kopid: &KOpId,
        operr: OperationError,
        domain_info: DomainInfoRead,
    ) -> Self {
        HtmxError::Notification(HtmxErrorInfo(kopid.eventid, operr, domain_info))
    }
}

impl IntoResponse for HtmxError {
    fn into_response(self) -> Response {
        match self {
            HtmxError::Notification(HtmxErrorInfo(eventid, operr, _)) => (ErrorToastPartial {
                err_code: operr,
                operation_id: eventid,
            })
            .into_response(),
            HtmxError::ErrorPage(HtmxErrorInfo(eventid, inner, domain_info)) => {
                let body = serde_json::to_string(&inner).unwrap_or(inner.to_string());
                match &inner {
                    OperationError::NotAuthenticated
                    | OperationError::SessionExpired
                    | OperationError::InvalidSessionState => Redirect::to("/ui").into_response(),
                    OperationError::SystemProtectedObject | OperationError::AccessDenied => {
                        let trigger = HxResponseTrigger::after_swap([HxEvent::new(
                            "permissionDenied".to_string(),
                        )]);
                        (
                            trigger,
                            HxRetarget("main".to_string()),
                            HxReswap(SwapOption::BeforeEnd),
                            (
                                StatusCode::FORBIDDEN,
                                ErrorToastPartial {
                                    err_code: inner,
                                    operation_id: eventid,
                                },
                            )
                                .into_response(),
                        )
                            .into_response()
                    }
                    OperationError::NoMatchingEntries => {
                        (StatusCode::NOT_FOUND, body).into_response()
                    }
                    OperationError::PasswordQuality(_)
                    | OperationError::EmptyRequest
                    | OperationError::SchemaViolation(_)
                    | OperationError::CU0003WebauthnUserNotVerified => {
                        (StatusCode::BAD_REQUEST, body).into_response()
                    }
                    _ => (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        HxRetarget("body".to_string()),
                        HxReswap(SwapOption::OuterHtml),
                        UnrecoverableErrorView {
                            err_code: inner,
                            operation_id: eventid,
                            domain_info,
                        },
                    )
                        .into_response(),
                }
            }
        }
    }
}
