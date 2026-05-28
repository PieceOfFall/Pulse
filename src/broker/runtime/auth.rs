use std::collections::HashSet;

use serde::Deserialize;

use crate::protocol;

#[derive(Clone, Debug, Default, Deserialize)]
pub(crate) struct AuthConfig {
    #[serde(default)]
    pub(crate) enabled: bool,
    #[serde(default)]
    pub(crate) users: Vec<AuthUserConfig>,
    #[serde(default)]
    pub(crate) acl: Vec<AuthAclConfig>,
}

#[derive(Clone, Debug, Deserialize)]
pub(crate) struct AuthUserConfig {
    pub(crate) username: String,
    pub(crate) password: String,
}

#[derive(Clone, Debug, Deserialize)]
pub(crate) struct AuthAclConfig {
    pub(crate) username: String,
    pub(crate) action: AuthAction,
    pub(crate) topic_filter: String,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "lowercase")]
pub(crate) enum AuthAction {
    Publish,
    Subscribe,
}

#[derive(Debug)]
pub(crate) struct Authentication {
    pub(in crate::broker) principal: Option<String>,
}

pub(crate) trait Authenticator: Send + Sync {
    fn authenticate(
        &self,
        username: Option<&str>,
        password: Option<&[u8]>,
    ) -> Result<Authentication, u8>;

    fn authorize_publish(&self, principal: Option<&str>, topic_name: &str) -> bool;

    fn authorize_subscribe(&self, principal: Option<&str>, topic_filter: &str) -> bool;
}

#[derive(Clone, Debug)]
pub(crate) struct ConfiguredAuthenticator {
    enabled: bool,
    users: Vec<AuthUserConfig>,
    acl: Vec<AuthAclConfig>,
}

impl ConfiguredAuthenticator {
    pub(crate) fn new(config: AuthConfig) -> Self {
        Self {
            enabled: config.enabled,
            users: config.users,
            acl: config.acl,
        }
    }
}

impl Default for ConfiguredAuthenticator {
    fn default() -> Self {
        Self::new(AuthConfig::default())
    }
}

impl Authenticator for ConfiguredAuthenticator {
    fn authenticate(
        &self,
        username: Option<&str>,
        password: Option<&[u8]>,
    ) -> Result<Authentication, u8> {
        if !self.enabled {
            return if username.is_none() && password.is_none() {
                Ok(Authentication { principal: None })
            } else {
                Err(protocol::BAD_USER_NAME_OR_PASSWORD)
            };
        }

        let Some(username) = username else {
            return Err(protocol::BAD_USER_NAME_OR_PASSWORD);
        };
        let Some(password) = password.and_then(|password| std::str::from_utf8(password).ok())
        else {
            return Err(protocol::BAD_USER_NAME_OR_PASSWORD);
        };

        if self
            .users
            .iter()
            .any(|user| user.username == username && user.password == password)
        {
            Ok(Authentication {
                principal: Some(username.to_string()),
            })
        } else {
            Err(protocol::BAD_USER_NAME_OR_PASSWORD)
        }
    }

    fn authorize_publish(&self, principal: Option<&str>, topic_name: &str) -> bool {
        if !self.enabled {
            return true;
        }

        let Some(principal) = principal else {
            return false;
        };
        self.acl.iter().any(|rule| {
            rule.username == principal
                && rule.action == AuthAction::Publish
                && protocol::topic_matches(&rule.topic_filter, topic_name)
        })
    }

    fn authorize_subscribe(&self, principal: Option<&str>, topic_filter: &str) -> bool {
        if !self.enabled {
            return true;
        }

        let Some(principal) = principal else {
            return false;
        };
        let requested_filter =
            protocol::shared_subscription_filter(topic_filter).unwrap_or(topic_filter);
        self.acl.iter().any(|rule| {
            rule.username == principal
                && rule.action == AuthAction::Subscribe
                && topic_filter_covers(&rule.topic_filter, requested_filter)
        })
    }
}

impl AuthConfig {
    pub(crate) fn validate(&self) -> Result<(), String> {
        if !self.enabled {
            return Ok(());
        }
        if self.users.is_empty() {
            return Err(
                "auth.users must contain at least one user when auth.enabled is true".into(),
            );
        }

        let mut usernames = HashSet::new();
        for user in &self.users {
            if user.username.is_empty() {
                return Err("auth.users.username must not be empty".into());
            }
            if user.password.is_empty() {
                return Err("auth.users.password must not be empty".into());
            }
            if !usernames.insert(user.username.as_str()) {
                return Err(format!(
                    "auth.users contains duplicate username `{}`",
                    user.username
                ));
            }
        }

        for rule in &self.acl {
            if !usernames.contains(rule.username.as_str()) {
                return Err(format!(
                    "auth.acl references unknown username `{}`",
                    rule.username
                ));
            }
            if !protocol::is_valid_topic_filter(&rule.topic_filter) {
                return Err(format!(
                    "auth.acl topic_filter `{}` is not a valid MQTT topic filter",
                    rule.topic_filter
                ));
            }
        }

        Ok(())
    }
}

pub(in crate::broker) fn topic_filter_covers(allowed_filter: &str, requested_filter: &str) -> bool {
    let allowed_filter =
        protocol::shared_subscription_filter(allowed_filter).unwrap_or(allowed_filter);
    let requested_filter =
        protocol::shared_subscription_filter(requested_filter).unwrap_or(requested_filter);
    if !protocol::is_valid_topic_filter(allowed_filter)
        || !protocol::is_valid_topic_filter(requested_filter)
    {
        return false;
    }
    if requested_filter.starts_with('$') && !allowed_filter.starts_with('$') {
        return false;
    }

    let allowed: Vec<&str> = allowed_filter.split('/').collect();
    let requested: Vec<&str> = requested_filter.split('/').collect();
    let mut index = 0;

    loop {
        if index == allowed.len() && index == requested.len() {
            return true;
        }
        if index == allowed.len() {
            return false;
        }
        if allowed[index] == "#" {
            return true;
        }
        if index == requested.len() {
            return false;
        }

        match requested[index] {
            "#" => return false,
            "+" if allowed[index] != "+" => return false,
            "+" => {}
            literal => match allowed[index] {
                "+" => {}
                allowed if allowed == literal => {}
                _ => return false,
            },
        }

        index += 1;
    }
}
