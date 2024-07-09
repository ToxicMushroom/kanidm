use askama::Template;
use axum::Extension;
use axum::extract::State;
use axum::http::Uri;
use axum::response::{IntoResponse, Response};
use axum_htmx::{HxPushUrl, HxRequest, HxReswap, HxRetarget, SwapOption};

use crate::https::extractors::VerifiedClientInformation;
use crate::https::middleware::KOpId;
use crate::https::ServerState;
use crate::https::views::HtmlTemplate;

#[derive(Template)]
#[template(path = "profile.html")]
struct ProfileView {
    profile_partial: ProfilePartialView,
}

#[derive(Template)]
#[template(path = "profile_partial.html")]
struct ProfilePartialView {
}

pub(crate) async fn view_profile_get(
    State(_state): State<ServerState>,
    Extension(_kopid): Extension<KOpId>,
    HxRequest(hx_request): HxRequest,
    VerifiedClientInformation(_client_auth_info): VerifiedClientInformation,
) -> axum::response::Result<Response> {
    let profile_partial_view = ProfilePartialView {};
    let profile_view = ProfileView {
        profile_partial: profile_partial_view,
    };

    Ok(if hx_request {
        (
            HxPushUrl(Uri::from_static("/ui/profile")),
            HtmlTemplate(ProfilePartialView {}),
        ).into_response()
    } else {
        HtmlTemplate(profile_view).into_response()
    })
}
