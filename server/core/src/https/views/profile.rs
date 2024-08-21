use crate::https::extractors::VerifiedClientInformation;
use crate::https::middleware::KOpId;
use crate::https::views::errors::HtmxError;
use crate::https::views::HtmlTemplate;
use crate::https::ServerState;
use askama::Template;
use axum::extract::State;
use axum::http::Uri;
use axum::response::{IntoResponse, Response};
use axum::Extension;
use axum_extra::extract::cookie::CookieJar;
use axum_htmx::{HxPushUrl, HxRequest};
use futures_util::TryFutureExt;
use kanidm_proto::internal::UserAuthToken;
use kanidmd_lib::constants::{Attribute, AttributeProps, EntryClass};
use kanidmd_lib::filter::{f_and, f_eq, Filter};
use std::collections::BTreeSet;

#[derive(Template)]
#[template(path = "user_settings.html")]
struct ProfileView {
    profile_partial: ProfilePartialView,
}

#[derive(Template, Clone)]
#[template(path = "user_settings_profile_partial.html")]
struct ProfilePartialView {
    can_rw: bool,
    account_name: String,
    display_name: String,
    legal_name: String,
    email: Option<String>,
    posix_enabled: bool,
}

pub(crate) async fn view_profile_get(
    State(state): State<ServerState>,
    Extension(kopid): Extension<KOpId>,
    HxRequest(hx_request): HxRequest,
    VerifiedClientInformation(client_auth_info): VerifiedClientInformation,
) -> axum::response::Result<Response> {
    let uat: UserAuthToken = state
        .qe_r_ref
        .handle_whoami_uat(client_auth_info.clone(), kopid.eventid)
        .map_err(|op_err| HtmxError::new(&kopid, op_err))
        .await?;

    let filter = filter_all!(f_and(vec![f_eq(
        Attribute::Class,
        EntryClass::Account.into()
    )]));

    let attrs: Vec<(Attribute, Option<BTreeSet<String>>, AttributeProps)> = state
        .qe_w_ref
        .handle_getattributes(
            client_auth_info.clone(),
            "idm_admin".to_string(),
            filter.clone(),
            kopid.eventid,
        )
        .map_err(|op_err| HtmxError::new(&kopid, op_err))
        .await?;

    for (attr, value, props) in attrs {
        println!("{attr:?} ({props:?}): {value:?}")
    }

    let time = time::OffsetDateTime::now_utc() + time::Duration::new(60, 0);

    let can_rw = uat.purpose_readwrite_active(time);

    let profile_partial_view = ProfilePartialView {
        can_rw,
        account_name: uat.name().to_string(),
        display_name: uat.displayname.clone(),
        legal_name: uat.name().to_string(),
        email: uat.mail_primary.clone(),
        posix_enabled: false,
    };
    let profile_view = ProfileView {
        profile_partial: profile_partial_view.clone(),
    };

    Ok(if hx_request {
        (
            HxPushUrl(Uri::from_static("/ui/profile")),
            HtmlTemplate(profile_partial_view),
        )
            .into_response()
    } else {
        HtmlTemplate(profile_view).into_response()
    })
}

pub(crate) async fn view_profile_set(
    State(state): State<ServerState>,
    Extension(kopid): Extension<KOpId>,
    HxRequest(_hx_request): HxRequest,
    VerifiedClientInformation(client_auth_info): VerifiedClientInformation,
) -> axum::response::Result<Response> {
    let _uat: UserAuthToken = state
        .qe_r_ref
        .handle_whoami_uat(client_auth_info.clone(), kopid.eventid)
        .map_err(|op_err| HtmxError::new(&kopid, op_err))
        .await?;

    let filter = filter_all!(f_and(vec![f_eq(
        Attribute::Class,
        EntryClass::Account.into()
    )]));

    state
        .qe_w_ref
        .handle_setattribute(
            client_auth_info.clone(),
            "idm_admin".to_string(),
            Attribute::DisplayName.to_string(),
            vec!["amongus".to_string()],
            filter,
            kopid.eventid,
        )
        .map_err(|op_err| HtmxError::new(&kopid, op_err))
        .await?;

    Ok("hi".into_response())
}

// #[axum::debug_handler]
pub(crate) async fn view_profile_unlock_get(
    State(state): State<ServerState>,
    VerifiedClientInformation(client_auth_info): VerifiedClientInformation,
    Extension(kopid): Extension<KOpId>,
    jar: CookieJar,
) -> axum::response::Result<Response> {
    super::login::view_reauth_get(state, client_auth_info, kopid, jar, "/ui/profile").await
}
