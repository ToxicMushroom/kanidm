use base64urlsafedata::Base64UrlSafeData;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use utoipa::ToSchema;
use uuid::Uuid;

pub use scim_proto::prelude::{ScimAttr, ScimComplexAttr, ScimEntry, ScimError, ScimSimpleAttr};
pub use scim_proto::user::MultiValueAttr;
use scim_proto::*;

use crate::constants::{
    ATTR_ACCOUNT_EXPIRE, ATTR_ACCOUNT_VALID_FROM, ATTR_DESCRIPTION, ATTR_DISPLAYNAME,
    ATTR_GIDNUMBER, ATTR_LOGINSHELL, ATTR_MAIL, ATTR_MEMBER, ATTR_NAME, ATTR_PASSWORD_IMPORT,
    ATTR_SSH_PUBLICKEY, ATTR_TOTP_IMPORT, ATTR_UNIX_PASSWORD_IMPORT,
};

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, ToSchema)]
pub enum ScimSyncState {
    Refresh,
    Active { cookie: Base64UrlSafeData },
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, ToSchema)]
pub enum ScimSyncRetentionMode {
    /// No actions are to be taken - only update or create entries in the
    /// entries set.
    Ignore,
    /// All entries that have their uuid present in this set are retained.
    /// Anything not present will be deleted.
    Retain(Vec<Uuid>),
    /// Any entry with its UUID in this set will be deleted. Anything not
    /// present will be retained.
    Delete(Vec<Uuid>),
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, ToSchema)]
pub struct ScimSyncRequest {
    pub from_state: ScimSyncState,
    pub to_state: ScimSyncState,

    // How do I want to represent different entities to kani? Split by type? All in one?
    pub entries: Vec<ScimEntry>,

    pub retain: ScimSyncRetentionMode,
}

impl ScimSyncRequest {
    pub fn need_refresh(from_state: ScimSyncState) -> Self {
        ScimSyncRequest {
            from_state,
            to_state: ScimSyncState::Refresh,
            entries: Vec::default(),
            retain: ScimSyncRetentionMode::Ignore,
        }
    }
}

pub const SCIM_ALGO: &str = "algo";
pub const SCIM_DIGITS: &str = "digits";
pub const SCIM_SECRET: &str = "secret";
pub const SCIM_STEP: &str = "step";

pub const SCIM_SCHEMA_SYNC_1: &str = "urn:ietf:params:scim:schemas:kanidm:sync:1:";
pub const SCIM_SCHEMA_SYNC_ACCOUNT: &str = "urn:ietf:params:scim:schemas:kanidm:sync:1:account";
pub const SCIM_SCHEMA_SYNC_GROUP: &str = "urn:ietf:params:scim:schemas:kanidm:sync:1:group";
pub const SCIM_SCHEMA_SYNC_PERSON: &str = "urn:ietf:params:scim:schemas:kanidm:sync:1:person";
pub const SCIM_SCHEMA_SYNC_POSIXACCOUNT: &str =
    "urn:ietf:params:scim:schemas:kanidm:sync:1:posixaccount";
pub const SCIM_SCHEMA_SYNC_POSIXGROUP: &str =
    "urn:ietf:params:scim:schemas:kanidm:sync:1:posixgroup";

#[derive(Serialize, Debug, Clone)]
pub struct ScimTotp {
    /// maps to "label" in kanidm.
    pub external_id: String,
    pub secret: String,
    pub algo: String,
    pub step: u32,
    pub digits: u32,
}

// Need to allow this because clippy is broken and doesn't realise scimentry is out of crate
// so this can't be fulfilled
#[allow(clippy::from_over_into)]
impl Into<ScimComplexAttr> for ScimTotp {
    fn into(self) -> ScimComplexAttr {
        let ScimTotp {
            external_id,
            secret,
            algo,
            step,
            digits,
        } = self;
        let mut attrs = BTreeMap::default();

        attrs.insert(
            "external_id".to_string(),
            ScimSimpleAttr::String(external_id),
        );

        attrs.insert(SCIM_SECRET.to_string(), ScimSimpleAttr::String(secret));

        attrs.insert(SCIM_ALGO.to_string(), ScimSimpleAttr::String(algo));

        attrs.insert(SCIM_STEP.to_string(), ScimSimpleAttr::Number(step.into()));

        attrs.insert(
            SCIM_DIGITS.to_string(),
            ScimSimpleAttr::Number(digits.into()),
        );

        ScimComplexAttr { attrs }
    }
}

#[derive(Serialize, Debug, Clone)]
pub struct ScimSshPubKey {
    pub label: String,
    pub value: String,
}

#[allow(clippy::from_over_into)]
impl Into<ScimComplexAttr> for ScimSshPubKey {
    fn into(self) -> ScimComplexAttr {
        let ScimSshPubKey { label, value } = self;

        let mut attrs = BTreeMap::default();

        attrs.insert("label".to_string(), ScimSimpleAttr::String(label));

        attrs.insert("value".to_string(), ScimSimpleAttr::String(value));
        ScimComplexAttr { attrs }
    }
}

#[derive(Serialize, Debug, Clone)]
#[serde(into = "ScimEntry")]
pub struct ScimSyncPerson {
    pub id: Uuid,
    pub external_id: Option<String>,
    pub user_name: String,
    pub display_name: String,
    pub gidnumber: Option<u32>,
    pub password_import: Option<String>,
    pub unix_password_import: Option<String>,
    pub totp_import: Vec<ScimTotp>,
    pub login_shell: Option<String>,
    pub mail: Vec<MultiValueAttr>,
    pub ssh_publickey: Vec<ScimSshPubKey>,
    pub account_valid_from: Option<String>,
    pub account_expire: Option<String>,
}

// Need to allow this because clippy is broken and doesn't realise scimentry is out of crate
// so this can't be fulfilled
#[allow(clippy::from_over_into)]
impl Into<ScimEntry> for ScimSyncPerson {
    fn into(self) -> ScimEntry {
        let ScimSyncPerson {
            id,
            external_id,
            user_name,
            display_name,
            gidnumber,
            password_import,
            unix_password_import,
            totp_import,
            login_shell,
            mail,
            ssh_publickey,
            account_valid_from,
            account_expire,
        } = self;

        let schemas = if gidnumber.is_some() {
            vec![
                SCIM_SCHEMA_SYNC_PERSON.to_string(),
                SCIM_SCHEMA_SYNC_ACCOUNT.to_string(),
                SCIM_SCHEMA_SYNC_POSIXACCOUNT.to_string(),
            ]
        } else {
            vec![
                SCIM_SCHEMA_SYNC_PERSON.to_string(),
                SCIM_SCHEMA_SYNC_ACCOUNT.to_string(),
            ]
        };

        let mut attrs = BTreeMap::default();

        set_string!(attrs, ATTR_NAME, user_name);
        set_string!(attrs, ATTR_DISPLAYNAME, display_name);
        set_option_u32!(attrs, ATTR_GIDNUMBER, gidnumber);
        set_option_string!(attrs, ATTR_PASSWORD_IMPORT, password_import);
        set_option_string!(attrs, ATTR_UNIX_PASSWORD_IMPORT, unix_password_import);
        set_multi_complex!(attrs, ATTR_TOTP_IMPORT, totp_import);
        set_option_string!(attrs, ATTR_LOGINSHELL, login_shell);
        set_multi_complex!(attrs, ATTR_MAIL, mail);
        set_multi_complex!(attrs, ATTR_SSH_PUBLICKEY, ssh_publickey); // with the underscore
        set_option_string!(attrs, ATTR_ACCOUNT_EXPIRE, account_expire);
        set_option_string!(attrs, ATTR_ACCOUNT_VALID_FROM, account_valid_from);

        ScimEntry {
            schemas,
            id,
            external_id,
            meta: None,
            attrs,
        }
    }
}

#[derive(Serialize, Debug, Clone)]
pub struct ScimExternalMember {
    pub external_id: String,
}

// Need to allow this because clippy is broken and doesn't realise scimentry is out of crate
// so this can't be fulfilled
#[allow(clippy::from_over_into)]
impl Into<ScimComplexAttr> for ScimExternalMember {
    fn into(self) -> ScimComplexAttr {
        let ScimExternalMember { external_id } = self;
        let mut attrs = BTreeMap::default();

        attrs.insert(
            "external_id".to_string(),
            ScimSimpleAttr::String(external_id),
        );

        ScimComplexAttr { attrs }
    }
}

#[derive(Serialize, Debug, Clone)]
#[serde(into = "ScimEntry")]
pub struct ScimSyncGroup {
    pub id: Uuid,
    pub external_id: Option<String>,
    pub name: String,
    pub description: Option<String>,
    pub gidnumber: Option<u32>,
    pub members: Vec<ScimExternalMember>,
}

// Need to allow this because clippy is broken and doesn't realise scimentry is out of crate
// so this can't be fulfilled
#[allow(clippy::from_over_into)]
impl Into<ScimEntry> for ScimSyncGroup {
    fn into(self) -> ScimEntry {
        let ScimSyncGroup {
            id,
            external_id,
            name,
            description,
            gidnumber,
            members,
        } = self;

        let schemas = if gidnumber.is_some() {
            vec![
                SCIM_SCHEMA_SYNC_GROUP.to_string(),
                SCIM_SCHEMA_SYNC_POSIXGROUP.to_string(),
            ]
        } else {
            vec![SCIM_SCHEMA_SYNC_GROUP.to_string()]
        };

        let mut attrs = BTreeMap::default();

        set_string!(attrs, ATTR_NAME, name);
        set_option_u32!(attrs, ATTR_GIDNUMBER, gidnumber);
        set_option_string!(attrs, ATTR_DESCRIPTION, description);
        set_multi_complex!(attrs, ATTR_MEMBER, members);

        ScimEntry {
            schemas,
            id,
            external_id,
            meta: None,
            attrs,
        }
    }
}
