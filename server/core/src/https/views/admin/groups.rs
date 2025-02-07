use crate::https::extractors::{AccessInfo, DomainInfo, VerifiedClientInformation};
use crate::https::middleware::KOpId;
use crate::https::views::errors::HtmxError;
use crate::https::views::login;
use crate::https::views::Urls;
use crate::https::ServerState;
use askama::Template;
use axum::extract::{Path, State};
use axum::http::{header, HeaderMap, StatusCode, Uri};
use axum::response::{ErrorResponse, IntoResponse, Response};
use axum::{Extension, Form};
use axum_extra::extract::CookieJar;
use axum_htmx::{HxPushUrl, HxRequest};
use futures_util::TryFutureExt;
use kanidm_proto::attribute::Attribute;

use crate::https::views::login::LoginDisplayCtx;
use crate::https::views::navbar::NavbarCtx;
use kanidm_proto::internal::{OperationError, UserAuthToken};
use kanidm_proto::scim_v1::server::{ScimEntryKanidm, ScimReference};
use kanidm_proto::scim_v1::{ScimEntryGetQuery, ScimMail};
use kanidmd_lib::constants::EntryClass;
use kanidmd_lib::filter::{f_and, f_eq, Filter, FC};
use kanidmd_lib::idm::server::DomainInfoRead;
use kanidmd_lib::idm::ClientAuthInfo;
use serde::{Deserialize, Serialize};
use std::str::FromStr;
use uuid::Uuid;

#[derive(Template)]
#[template(path = "admin/admin_overview.html")]
struct GroupsView {
    access_info: AccessInfo,
    partial: GroupsPartialView,
    navbar_ctx: NavbarCtx,
}

#[derive(Template)]
#[template(path = "admin/admin_groups_partial.html")]
struct GroupsPartialView {
    can_rw: bool,
    groups: Vec<GroupInfo>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct GroupInfo {
    uuid: Uuid,
    name: String,
    spn: String,
    description: Option<String>,
    entry_manager: Option<String>,
    acp: GroupACP,
    mails: Vec<ScimMail>,
    members: Vec<ScimReference>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct GroupACP {
    enabled: bool,
}

#[derive(Template)]
#[template(path = "admin/admin_overview.html")]
struct GroupCreateView {
    access_info: AccessInfo,
    partial: GroupCreatePartialView,
    navbar_ctx: NavbarCtx,
}

#[derive(Template)]
#[template(path = "admin/admin_group_create_partial.html")]
struct GroupCreatePartialView {
    can_rw: bool,
    groups: Vec<GroupInfo>,
}

#[derive(Serialize, Deserialize, Debug)]
pub(crate) struct GroupCreateFormData {
    name: String,
    guid: Option<String>,
    managed_by: Option<String>,
}

pub(crate) async fn view_group_create_get(
    State(state): State<ServerState>,
    HxRequest(is_htmx): HxRequest,
    Extension(kopid): Extension<KOpId>,
    VerifiedClientInformation(client_auth_info): VerifiedClientInformation,
    DomainInfo(domain_info): DomainInfo,
) -> axum::response::Result<Response> {
    let can_rw = get_can_rw(&state, &kopid, &client_auth_info, domain_info.clone()).await?;

    let groups = get_groups_info(state, &kopid, client_auth_info, domain_info.clone()).await?;
    let groups_partial = GroupCreatePartialView { can_rw, groups };

    let push_url = HxPushUrl(Uri::from_static("/ui/admin/group/create"));
    Ok(if is_htmx {
        (push_url, groups_partial).into_response()
    } else {
        (
            push_url,
            GroupCreateView {
                access_info: AccessInfo::new(),
                partial: groups_partial,
                navbar_ctx: NavbarCtx { domain_info },
            },
        )
            .into_response()
    })
}

pub(crate) async fn view_group_delete_post(
    State(state): State<ServerState>,
    HxRequest(is_htmx): HxRequest,
    Extension(kopid): Extension<KOpId>,
    Path(group_uuid): Path<Uuid>,
    VerifiedClientInformation(client_auth_info): VerifiedClientInformation,
    DomainInfo(domain_info): DomainInfo,
) -> axum::response::Result<Response> {
    dbg!(group_uuid);
    view_groups_get(
        State(state),
        HxRequest(is_htmx),
        Extension(kopid),
        VerifiedClientInformation(client_auth_info),
        DomainInfo(domain_info),
    )
    .await
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub(crate) struct GroupDetailsFormData {
    name: String,
    spn: String,
    entry_manager: String,
}

// #[derive(Template)]
// #[template(path = "admin/admin_group_details_partial.html")]
// struct GroupMemberDetailsPartialView {
//     group: GroupInfo,
//     can_edit: bool,
// }

pub(crate) async fn view_group_save_post(
    State(_state): State<ServerState>,
    HxRequest(_is_htmx): HxRequest,
    Extension(_kopid): Extension<KOpId>,
    VerifiedClientInformation(_client_auth_info): VerifiedClientInformation,
    Path(_guuid): Path<Uuid>,
    Form(_data): Form<GroupDetailsFormData>,
) -> axum::response::Result<Response> {
    // TODO: implement writes
    Ok(StatusCode::INTERNAL_SERVER_ERROR.into_response())
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub(crate) struct GroupAddMemberFormData {
    member: String,
}

// #[derive(Template)]
// #[template(path = "admin/admin_group_member_partial.html")]
// struct GroupMemberPartialView {
//     can_edit: bool,
//     group_uuid: Uuid,
//     member: MemberInfo,
// }

pub(crate) async fn view_group_new_member_post(
    State(_state): State<ServerState>,
    HxRequest(_is_htmx): HxRequest,
    Extension(_kopid): Extension<KOpId>,
    VerifiedClientInformation(_client_auth_info): VerifiedClientInformation,
    Path(_guuid): Path<Uuid>,
    Form(_data): Form<GroupAddMemberFormData>,
) -> axum::response::Result<Response> {
    // TODO: implement writes
    Ok(StatusCode::INTERNAL_SERVER_ERROR.into_response())
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub(crate) struct GroupAddMailFormData {
    mail: String,
}

// #[derive(Template)]
// #[template(path = "admin/admin_group_mail_partial.html")]
// struct GroupMailPartialView {
//     can_edit: bool,
//     group_uuid: Uuid,
//     mail: String,
// }

pub(crate) async fn view_group_new_mail_post(
    State(_state): State<ServerState>,
    HxRequest(_is_htmx): HxRequest,
    Extension(_kopid): Extension<KOpId>,
    VerifiedClientInformation(_client_auth_info): VerifiedClientInformation,
    Path(_guuid): Path<Uuid>,
    Form(_data): Form<GroupAddMailFormData>,
) -> axum::response::Result<Response> {
    // TODO: implement writes
    Ok(StatusCode::INTERNAL_SERVER_ERROR.into_response())
}

pub(crate) async fn view_group_create_post(
    State(_state): State<ServerState>,
    HxRequest(_is_htmx): HxRequest,
    Extension(_kopid): Extension<KOpId>,
    VerifiedClientInformation(_client_auth_info): VerifiedClientInformation,
    Form(_data): Form<GroupCreateFormData>,
) -> axum::response::Result<Response> {
    // TODO: implement writes
    Ok(StatusCode::INTERNAL_SERVER_ERROR.into_response())
}

pub(crate) async fn view_group_edit_get(
    State(state): State<ServerState>,
    HxRequest(is_htmx): HxRequest,
    Extension(kopid): Extension<KOpId>,
    VerifiedClientInformation(client_auth_info): VerifiedClientInformation,
    Path(uuid): Path<Uuid>,
    DomainInfo(domain_info): DomainInfo,
) -> axum::response::Result<Response> {
    let can_rw = get_can_rw(&state, &kopid, &client_auth_info, domain_info.clone()).await?;
    let group = get_group_info(uuid, state, &kopid, client_auth_info, domain_info.clone()).await?;
    let groups_partial = GroupViewPartial {
        can_rw,
        can_edit: true,
        group,
    };

    let path_string = format!("/ui/admin/group/{uuid}/edit").clone();
    let src = path_string.clone();
    let push_url = HxPushUrl(Uri::from_str(src.as_str()).expect("T"));
    Ok(if is_htmx {
        (push_url, groups_partial).into_response()
    } else {
        (
            push_url,
            GroupView {
                access_info: AccessInfo::new(),
                partial: groups_partial,
                navbar_ctx: NavbarCtx { domain_info },
            },
        )
            .into_response()
    })
}

#[derive(Template)]
#[template(path = "admin/admin_overview.html")]
struct GroupView {
    access_info: AccessInfo,
    partial: GroupViewPartial,
    navbar_ctx: NavbarCtx,
}

#[derive(Template)]
#[template(path = "admin/admin_group_view_partial.html")]
struct GroupViewPartial {
    can_rw: bool,
    can_edit: bool,
    group: GroupInfo,
}

pub(crate) async fn view_group_view_get(
    State(state): State<ServerState>,
    HxRequest(is_htmx): HxRequest,
    Extension(kopid): Extension<KOpId>,
    VerifiedClientInformation(client_auth_info): VerifiedClientInformation,
    Path(uuid): Path<Uuid>,
    DomainInfo(domain_info): DomainInfo,
) -> axum::response::Result<Response> {
    let can_rw = get_can_rw(&state, &kopid, &client_auth_info, domain_info.clone()).await?;
    let group = get_group_info(uuid, state, &kopid, client_auth_info, domain_info.clone()).await?;
    let groups_partial = GroupViewPartial {
        can_rw,
        can_edit: false,
        group,
    };

    let path_string = format!("/ui/admin/group/{uuid}/view").clone();
    let src = path_string.clone();
    let push_url = HxPushUrl(Uri::from_str(src.as_str()).expect("T"));
    Ok(if is_htmx {
        (push_url, groups_partial).into_response()
    } else {
        (
            push_url,
            GroupView {
                access_info: AccessInfo::new(),
                partial: groups_partial,
                navbar_ctx: NavbarCtx { domain_info },
            },
        )
            .into_response()
    })
}

pub(crate) async fn view_groups_get(
    State(state): State<ServerState>,
    HxRequest(is_htmx): HxRequest,
    Extension(kopid): Extension<KOpId>,
    VerifiedClientInformation(client_auth_info): VerifiedClientInformation,
    DomainInfo(domain_info): DomainInfo,
) -> axum::response::Result<Response> {
    let can_rw = get_can_rw(&state, &kopid, &client_auth_info, domain_info.clone()).await?;

    let groups = get_groups_info(state, &kopid, client_auth_info, domain_info.clone()).await?;
    let groups_partial = GroupsPartialView { can_rw, groups };

    let push_url = HxPushUrl(Uri::from_static("/ui/admin/groups"));
    Ok(if is_htmx {
        (push_url, groups_partial).into_response()
    } else {
        (
            push_url,
            GroupsView {
                access_info: AccessInfo::new(),
                partial: groups_partial,
                navbar_ctx: NavbarCtx { domain_info },
            },
        )
            .into_response()
    })
}

pub(crate) async fn view_groups_unlock_get(
    State(state): State<ServerState>,
    Extension(kopid): Extension<KOpId>,
    headers: HeaderMap,
    VerifiedClientInformation(client_auth_info): VerifiedClientInformation,
    DomainInfo(domain_info): DomainInfo,
    jar: CookieJar,
) -> axum::response::Result<Response> {
    let referrer = match headers.get(header::REFERER) {
        Some(header_value) => header_value.to_str().map_err(|x| {
            warn!("referer header couldn't be converted to string: {x}");
            HtmxError::OperationError(
                kopid.eventid,
                OperationError::InvalidRequestState,
                domain_info.clone(),
            )
        })?,
        None => "/ui/admin/groups",
    };

    Ok(login::view_reauth_get(
        state,
        client_auth_info,
        kopid,
        jar,
        referrer,
        LoginDisplayCtx {
            domain_info,
            reauth: None,
            oauth2: None,
            error: None,
        },
    )
    .await)
}

async fn get_can_rw(
    state: &ServerState,
    kopid: &KOpId,
    client_auth_info: &ClientAuthInfo,
    domain_info: DomainInfoRead,
) -> Result<bool, ErrorResponse> {
    let uat: UserAuthToken = state
        .qe_r_ref
        .handle_whoami_uat(client_auth_info.clone(), kopid.eventid)
        .map_err(|op_err| HtmxError::new(&kopid, op_err, domain_info))
        .await?;

    let time = time::OffsetDateTime::now_utc() + time::Duration::new(60, 0);

    let can_rw = uat.purpose_readwrite_active(time);
    Ok(can_rw)
}

async fn get_group_info(
    uuid: Uuid,
    state: ServerState,
    kopid: &KOpId,
    client_auth_info: ClientAuthInfo,
    domain_info: DomainInfoRead,
) -> Result<GroupInfo, ErrorResponse> {
    let scim_entry: ScimEntryKanidm = state
        .qe_r_ref
        .scim_entry_id_get(
            client_auth_info.clone(),
            kopid.eventid,
            uuid.to_string(),
            EntryClass::Group,
            ScimEntryGetQuery {
                attributes: None,
                ext_access_check: false,
            },
        )
        .map_err(|op_err| HtmxError::new(&kopid, op_err, domain_info.clone()))
        .await?;

    if let Some(group_info) = scimentry_into_groupinfo(&scim_entry) {
        Ok(group_info)
    } else {
        Err(HtmxError::new(kopid, OperationError::InvalidState, domain_info.clone()).into())
    }
}

async fn get_groups_info(
    state: ServerState,
    kopid: &KOpId,
    client_auth_info: ClientAuthInfo,
    domain_info: DomainInfoRead,
) -> Result<Vec<GroupInfo>, ErrorResponse> {
    let filter = filter_all!(f_and!([f_eq(Attribute::Class, EntryClass::Group.into())]));
    let base: Vec<_> = state
        .qe_r_ref
        .scim_entry_search(client_auth_info.clone(), filter, kopid.eventid)
        .map_err(|op_err| HtmxError::new(&kopid, op_err, domain_info.clone()))
        .await?;

    // TODO: inefficient to sort here
    let mut groups: Vec<_> = base
        .iter()
        // TODO: Filtering away unsuccessful entries may not be desired.
        .filter_map(|entry| scimentry_into_groupinfo(entry))
        .collect();
    groups.sort_by_key(|gi| gi.uuid.clone());
    groups.reverse();
    Ok(groups)
}

fn scimentry_into_groupinfo(scim_entry: &ScimEntryKanidm) -> Option<GroupInfo> {
    let name = scim_entry.attr_str(&Attribute::Name)?.to_string();
    let spn = scim_entry.attr_str(&Attribute::Spn)?.to_string();
    let description = scim_entry
        .attr_str(&Attribute::Description)
        .map(|t| t.to_string());
    let entry_manager = scim_entry
        .attr_str(&Attribute::EntryManagedBy)
        .map(|t| t.to_string());
    let mails = scim_entry.attr_mails().cloned().unwrap_or_default();
    let members = scim_entry
        .attr_references(&Attribute::Member)
        .cloned()
        .unwrap_or_default();

    Some(GroupInfo {
        uuid: scim_entry.header.id,
        name,
        spn,
        description,
        entry_manager,
        acp: GroupACP { enabled: false },
        mails,
        members,
    })
}
