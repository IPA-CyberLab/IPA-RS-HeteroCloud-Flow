use std::time::Duration;

use base64::{Engine, engine::general_purpose::STANDARD};
use chrono::{DateTime, Utc};
use hmac::{Hmac, Mac};
use serde::Serialize;
use sha1::Sha1;
use thiserror::Error;
use utoipa::ToSchema;

#[derive(Clone)]
pub struct TurnCredentialIssuer {
    urls: Vec<String>,
    shared_secret: Vec<u8>,
    ttl: Duration,
}

impl TurnCredentialIssuer {
    /// Builds an issuer for coturn's REST authentication mechanism.
    ///
    /// # Errors
    ///
    /// Returns [`TurnError::InvalidConfiguration`] when URLs, secret length, or
    /// credential lifetime are unsafe.
    pub fn new(
        urls: Vec<String>,
        shared_secret: impl Into<Vec<u8>>,
        ttl: Duration,
    ) -> Result<Self, TurnError> {
        let shared_secret = shared_secret.into();
        if urls.is_empty() || urls.iter().any(String::is_empty) {
            return Err(TurnError::InvalidConfiguration(
                "at least one TURN URL is required",
            ));
        }
        if shared_secret.len() < 32 {
            return Err(TurnError::InvalidConfiguration(
                "TURN secret must be at least 32 bytes",
            ));
        }
        if ttl.is_zero() || ttl > Duration::from_hours(1) {
            return Err(TurnError::InvalidConfiguration(
                "TURN TTL must be between 1 and 3600 seconds",
            ));
        }
        Ok(Self {
            urls,
            shared_secret,
            ttl,
        })
    }

    /// Issues credentials using the configured maximum lifetime.
    ///
    /// # Errors
    ///
    /// Returns [`TurnError::InvalidIdentity`] for an empty or oversized
    /// identity.
    pub fn issue(&self, identity: &str) -> Result<TurnCredentials, TurnError> {
        self.issue_with_ttl(identity, self.ttl)
    }

    #[must_use]
    pub fn urls(&self) -> &[String] {
        &self.urls
    }

    #[must_use]
    pub fn stun_urls(&self) -> Vec<String> {
        let mut urls = self
            .urls
            .iter()
            .filter_map(|url| {
                let authority = url
                    .strip_prefix("turn:")
                    .or_else(|| url.strip_prefix("turns:"))?
                    .split('?')
                    .next()?;
                (!authority.is_empty()).then(|| format!("stun:{authority}"))
            })
            .collect::<Vec<_>>();
        urls.sort();
        urls.dedup();
        urls
    }

    /// Issues credentials capped by both the configured and delegated lifetime.
    ///
    /// # Errors
    ///
    /// Returns [`TurnError`] when the identity or effective lifetime is
    /// invalid.
    pub fn issue_with_ttl(
        &self,
        identity: &str,
        maximum_ttl: Duration,
    ) -> Result<TurnCredentials, TurnError> {
        if identity.is_empty() || identity.len() > 384 {
            return Err(TurnError::InvalidIdentity);
        }
        let effective_ttl = self.ttl.min(maximum_ttl);
        if effective_ttl.is_zero() {
            return Err(TurnError::InvalidIdentity);
        }
        let ttl_seconds = i64::try_from(effective_ttl.as_secs())
            .map_err(|_| TurnError::InvalidConfiguration("TTL"))?;
        let expires_at = Utc::now() + chrono::Duration::seconds(ttl_seconds);
        let username = format!("{}:{identity}", expires_at.timestamp());
        let mut mac = Hmac::<Sha1>::new_from_slice(&self.shared_secret)
            .map_err(|_| TurnError::InvalidConfiguration("TURN secret"))?;
        mac.update(username.as_bytes());
        let password = STANDARD.encode(mac.finalize().into_bytes());

        Ok(TurnCredentials {
            urls: self.urls.clone(),
            username,
            password,
            expires_at,
        })
    }
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct TurnCredentials {
    pub urls: Vec<String>,
    pub username: String,
    pub password: String,
    pub expires_at: DateTime<Utc>,
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum TurnError {
    #[error("invalid TURN configuration: {0}")]
    InvalidConfiguration(&'static str),
    #[error("identity is invalid for TURN credentials")]
    InvalidIdentity,
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use base64::{Engine, engine::general_purpose::STANDARD};
    use hmac::{Hmac, Mac};
    use sha1::Sha1;

    use super::TurnCredentialIssuer;

    #[test]
    fn issues_coturn_rest_credentials() {
        let secret = b"turn-secret-with-at-least-thirty-two-bytes";
        let issuer = TurnCredentialIssuer::new(
            vec!["turn:turn.example.test:3478?transport=udp".into()],
            secret,
            Duration::from_mins(5),
        )
        .unwrap();

        let credentials = issuer.issue("principal-a").unwrap();
        let mut mac = Hmac::<Sha1>::new_from_slice(secret).unwrap();
        mac.update(credentials.username.as_bytes());
        assert_eq!(
            credentials.password,
            STANDARD.encode(mac.finalize().into_bytes())
        );
    }

    #[test]
    fn derives_deduplicated_stun_endpoints_from_turn_endpoints() {
        let issuer = TurnCredentialIssuer::new(
            vec![
                "turn:turn.example.test:3478?transport=udp".into(),
                "turn:turn.example.test:3478?transport=tcp".into(),
                "turns:turn.example.test:5349?transport=tcp".into(),
            ],
            b"turn-secret-with-at-least-thirty-two-bytes",
            Duration::from_mins(5),
        )
        .unwrap();

        assert_eq!(
            issuer.stun_urls(),
            ["stun:turn.example.test:3478", "stun:turn.example.test:5349"]
        );
    }

    #[test]
    fn delegated_lifetime_caps_turn_credential_expiry() {
        let issuer = TurnCredentialIssuer::new(
            vec!["turn:turn.example.test:3478?transport=udp".into()],
            b"turn-secret-with-at-least-thirty-two-bytes",
            Duration::from_mins(5),
        )
        .unwrap();
        let before = chrono::Utc::now();
        let credentials = issuer
            .issue_with_ttl("principal-a", Duration::from_secs(17))
            .unwrap();
        let after = chrono::Utc::now();

        assert!(credentials.expires_at >= before + chrono::Duration::seconds(17));
        assert!(credentials.expires_at <= after + chrono::Duration::seconds(17));
    }
}
