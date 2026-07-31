use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
    time::Duration,
};

use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use chrono::{TimeZone, Utc};
use flow_domain::PrincipalContext;
use hmac::{Hmac, Mac};
use http::{
    HeaderMap,
    header::{AUTHORIZATION, HeaderName},
};
use jsonwebtoken::{Algorithm, DecodingKey, Validation, decode, decode_header};
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use thiserror::Error;
use uuid::Uuid;

pub const PROVIDER_RECONCILE_ACTION: &str = "service-instance.reconcile";
const PROVIDER_MAX_TOKEN_TTL_SECONDS: i64 = 60;
const PROVIDER_NOT_BEFORE_OFFSET_SECONDS: i64 = 5;

pub const PRINCIPAL_HEADER: HeaderName = HeaderName::from_static("x-flow-principal");
pub const PRINCIPAL_TIMESTAMP_HEADER: HeaderName = HeaderName::from_static("x-flow-timestamp");
pub const PRINCIPAL_SIGNATURE_HEADER: HeaderName = HeaderName::from_static("x-flow-signature");

#[derive(Clone)]
pub struct ProviderAuthenticator {
    issuer: String,
    audience: String,
    keys: Arc<BTreeMap<String, DecodingKey>>,
    clock_skew_seconds: u64,
}

impl ProviderAuthenticator {
    /// Builds an Ed25519 verifier from a JSON object mapping `kid` to public PEM.
    ///
    /// # Errors
    ///
    /// Returns [`AuthError`] when the trust set is empty or malformed.
    pub fn from_public_keys_json(
        issuer: impl Into<String>,
        audience: impl Into<String>,
        public_keys_json: &str,
    ) -> Result<Self, AuthError> {
        let issuer = issuer.into();
        let audience = audience.into();
        if issuer.is_empty() || audience.is_empty() {
            return Err(AuthError::InvalidConfiguration(
                "provider issuer and audience are required",
            ));
        }
        let encoded_keys: BTreeMap<String, String> = serde_json::from_str(public_keys_json)
            .map_err(|_| AuthError::InvalidConfiguration("provider public key JSON is invalid"))?;
        if encoded_keys.is_empty() {
            return Err(AuthError::InvalidConfiguration(
                "at least one provider public key is required",
            ));
        }
        let keys = encoded_keys
            .into_iter()
            .map(|(key_id, pem)| {
                validate_key_id(&key_id)?;
                let key = DecodingKey::from_ed_pem(pem.as_bytes()).map_err(|_| {
                    AuthError::InvalidConfiguration("provider public key is invalid")
                })?;
                Ok((key_id, key))
            })
            .collect::<Result<_, AuthError>>()?;
        Ok(Self {
            issuer,
            audience,
            keys: Arc::new(keys),
            clock_skew_seconds: 5,
        })
    }

    /// Verifies an `EdDSA` bearer token from the `HeteroCloud` provider worker.
    ///
    /// # Errors
    ///
    /// Returns [`AuthError`] for missing, malformed, untrusted, or invalid
    /// provider credentials.
    pub fn authenticate_headers(&self, headers: &HeaderMap) -> Result<ProviderClaims, AuthError> {
        let value = headers
            .get(AUTHORIZATION)
            .ok_or(AuthError::MissingCredentials)?
            .to_str()
            .map_err(|_| AuthError::InvalidHeader)?;
        let token = value
            .strip_prefix("Bearer ")
            .filter(|token| !token.is_empty())
            .ok_or(AuthError::InvalidHeader)?;
        self.verify_token(token)
    }

    /// Verifies the `HeteroCloud` provider token and its exact command contract.
    ///
    /// # Errors
    ///
    /// Returns [`AuthError`] when signature, key ID, registered claims, action,
    /// generation, or short lifetime is invalid.
    pub fn verify_token(&self, token: &str) -> Result<ProviderClaims, AuthError> {
        let header = decode_header(token).map_err(|_| AuthError::InvalidToken)?;
        if header.alg != Algorithm::EdDSA {
            return Err(AuthError::InvalidToken);
        }
        let key_id = header.kid.ok_or(AuthError::MissingKeyId)?;
        let key = self.keys.get(&key_id).ok_or(AuthError::UnknownKeyId)?;

        let mut validation = Validation::new(Algorithm::EdDSA);
        validation.set_issuer(&[self.issuer.as_str()]);
        validation.set_audience(&[self.audience.as_str()]);
        validation.set_required_spec_claims(&["exp", "iat", "nbf", "iss", "aud", "sub", "jti"]);
        validation.leeway = self.clock_skew_seconds;
        validation.validate_exp = true;
        validation.validate_nbf = true;
        let claims = decode::<ProviderClaims>(token, key, &validation)
            .map_err(|_| AuthError::InvalidToken)?
            .claims;

        if claims.action != PROVIDER_RECONCILE_ACTION || claims.generation <= 0 {
            return Err(AuthError::InvalidProviderCommand);
        }
        if claims.expires_at <= claims.issued_at
            || claims.expires_at.saturating_sub(claims.issued_at) > PROVIDER_MAX_TOKEN_TTL_SECONDS
            || claims
                .issued_at
                .checked_sub(PROVIDER_NOT_BEFORE_OFFSET_SECONDS)
                != Some(claims.not_before)
        {
            return Err(AuthError::TokenLifetimeExceeded);
        }
        let now = Utc::now().timestamp();
        let skew = i64::try_from(self.clock_skew_seconds)
            .map_err(|_| AuthError::InvalidConfiguration("provider clock skew is invalid"))?;
        if claims.issued_at > now.saturating_add(skew) {
            return Err(AuthError::InvalidToken);
        }
        Ok(claims)
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ProviderClaims {
    #[serde(rename = "iss")]
    pub issuer: String,
    #[serde(rename = "aud")]
    pub audience: String,
    #[serde(rename = "sub")]
    pub subject: Uuid,
    pub organization_id: Uuid,
    pub project_id: Uuid,
    pub service_instance_id: Uuid,
    pub action: String,
    pub generation: i64,
    #[serde(rename = "jti")]
    pub jwt_id: Uuid,
    #[serde(rename = "iat")]
    pub issued_at: i64,
    #[serde(rename = "nbf")]
    pub not_before: i64,
    #[serde(rename = "exp")]
    pub expires_at: i64,
}

#[derive(Clone)]
pub struct PrincipalAuthenticator {
    issuer: String,
    audience: String,
    context_secret: Vec<u8>,
    max_context_ttl: Duration,
    clock_skew: Duration,
}

impl PrincipalAuthenticator {
    /// Builds the data-plane principal verifier.
    ///
    /// This HMAC boundary is distinct from provider command authentication and
    /// cannot authenticate the internal reconcile endpoint.
    ///
    /// # Errors
    ///
    /// Returns [`AuthError`] for missing identifiers, weak secrets, or zero TTL.
    pub fn new(
        issuer: impl Into<String>,
        audience: impl Into<String>,
        context_secret: impl Into<Vec<u8>>,
        max_context_ttl: Duration,
    ) -> Result<Self, AuthError> {
        let issuer = issuer.into();
        let audience = audience.into();
        let context_secret = context_secret.into();
        if issuer.is_empty() || audience.is_empty() {
            return Err(AuthError::InvalidConfiguration(
                "principal issuer and audience are required",
            ));
        }
        if context_secret.len() < 32 {
            return Err(AuthError::InvalidConfiguration(
                "principal context secret must be at least 32 bytes",
            ));
        }
        if max_context_ttl.is_zero() {
            return Err(AuthError::InvalidConfiguration(
                "maximum principal context TTL must be positive",
            ));
        }
        Ok(Self {
            issuer,
            audience,
            context_secret,
            max_context_ttl,
            clock_skew: Duration::from_secs(15),
        })
    }

    /// Verifies the separate signed data-plane principal headers.
    ///
    /// # Errors
    ///
    /// Returns [`AuthError`] for missing, expired, malformed, or invalid context.
    pub fn authenticate_headers(&self, headers: &HeaderMap) -> Result<PrincipalContext, AuthError> {
        let encoded = required_header(headers, &PRINCIPAL_HEADER)?;
        let timestamp_text = required_header(headers, &PRINCIPAL_TIMESTAMP_HEADER)?;
        let signature_text = required_header(headers, &PRINCIPAL_SIGNATURE_HEADER)?;
        let signed_at = timestamp_text
            .parse::<u64>()
            .map_err(|_| AuthError::InvalidHeader)?;

        let mut mac = Hmac::<Sha256>::new_from_slice(&self.context_secret)
            .map_err(|_| AuthError::InvalidConfiguration("invalid principal context secret"))?;
        mac.update(timestamp_text.as_bytes());
        mac.update(b".");
        mac.update(encoded.as_bytes());
        let signature = URL_SAFE_NO_PAD
            .decode(signature_text)
            .map_err(|_| AuthError::InvalidSignature)?;
        mac.verify_slice(&signature)
            .map_err(|_| AuthError::InvalidSignature)?;

        let serialized = URL_SAFE_NO_PAD
            .decode(encoded)
            .map_err(|_| AuthError::InvalidPrincipal)?;
        let signed: SignedPrincipal =
            serde_json::from_slice(&serialized).map_err(|_| AuthError::InvalidPrincipal)?;
        if signed_at != signed.issued_at {
            return Err(AuthError::PrincipalTimestampMismatch);
        }
        if signed.audience != self.audience || signed.issuer != self.issuer {
            return Err(AuthError::InvalidPrincipal);
        }
        if signed.expires_at <= signed.issued_at
            || signed.expires_at.saturating_sub(signed.issued_at) > self.max_context_ttl.as_secs()
        {
            return Err(AuthError::TokenLifetimeExceeded);
        }
        let now_u64 =
            u64::try_from(Utc::now().timestamp()).map_err(|_| AuthError::InvalidPrincipal)?;
        if signed.issued_at > now_u64.saturating_add(self.clock_skew.as_secs())
            || signed.expires_at.saturating_add(self.clock_skew.as_secs()) < now_u64
        {
            return Err(AuthError::InvalidToken);
        }

        let context = PrincipalContext {
            organization_id: signed.organization_id,
            project_id: signed.project_id,
            service_instance_id: signed.service_instance_id,
            principal_id: signed.principal_id,
            permissions: signed.permissions,
            issued_at: timestamp(signed.issued_at)?,
            expires_at: timestamp(signed.expires_at)?,
            token_id: signed.context_id,
        };
        context
            .validate()
            .map_err(|_| AuthError::InvalidPrincipal)?;
        Ok(context)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SignedPrincipal {
    pub issuer: String,
    pub audience: String,
    pub organization_id: Uuid,
    pub project_id: Uuid,
    pub service_instance_id: Uuid,
    pub principal_id: Uuid,
    #[serde(default)]
    pub permissions: BTreeSet<String>,
    pub issued_at: u64,
    pub expires_at: u64,
    pub context_id: Uuid,
}

fn required_header<'a>(headers: &'a HeaderMap, name: &HeaderName) -> Result<&'a str, AuthError> {
    headers
        .get(name)
        .ok_or(AuthError::MissingCredentials)?
        .to_str()
        .map_err(|_| AuthError::InvalidHeader)
}

fn timestamp(value: u64) -> Result<chrono::DateTime<Utc>, AuthError> {
    let seconds = i64::try_from(value).map_err(|_| AuthError::InvalidToken)?;
    Utc.timestamp_opt(seconds, 0)
        .single()
        .ok_or(AuthError::InvalidToken)
}

fn validate_key_id(value: &str) -> Result<(), AuthError> {
    if value.is_empty()
        || value.len() > 128
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
    {
        return Err(AuthError::InvalidConfiguration(
            "provider key ID is invalid",
        ));
    }
    Ok(())
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum AuthError {
    #[error("credentials are missing")]
    MissingCredentials,
    #[error("credential header is invalid")]
    InvalidHeader,
    #[error("token is invalid or expired")]
    InvalidToken,
    #[error("provider token has no key ID")]
    MissingKeyId,
    #[error("provider token key ID is not trusted")]
    UnknownKeyId,
    #[error("provider command claims are invalid")]
    InvalidProviderCommand,
    #[error("credential lifetime exceeds the service limit")]
    TokenLifetimeExceeded,
    #[error("principal context is invalid")]
    InvalidPrincipal,
    #[error("principal signature is invalid")]
    InvalidSignature,
    #[error("principal timestamp does not match signed issued_at")]
    PrincipalTimestampMismatch,
    #[error("invalid authentication configuration: {0}")]
    InvalidConfiguration(&'static str),
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeSet, time::Duration};

    use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
    use hmac::{Hmac, Mac};
    use http::{HeaderMap, HeaderValue, header::AUTHORIZATION};
    use jsonwebtoken::{Algorithm, EncodingKey, Header, encode};
    use sha2::Sha256;
    use uuid::Uuid;

    use super::{
        AuthError, PRINCIPAL_HEADER, PRINCIPAL_SIGNATURE_HEADER, PRINCIPAL_TIMESTAMP_HEADER,
        PROVIDER_RECONCILE_ACTION, PrincipalAuthenticator, ProviderAuthenticator, ProviderClaims,
        SignedPrincipal,
    };

    const PRINCIPAL_SECRET: &[u8] = b"principal-context-secret-with-at-least-thirty-two-bytes";
    const PRIVATE_KEY: &[u8] = br"-----BEGIN PRIVATE KEY-----
MC4CAQAwBQYDK2VwBCIEIFTAxDs5JPZKnyxcfE0FA8mmr+9KN0LmQ1co4bxZ6Vq/
-----END PRIVATE KEY-----
";
    const PUBLIC_KEY: &str = "-----BEGIN PUBLIC KEY-----\nMCowBQYDK2VwAyEAQnTjC0+B/djS2k/sebsW6/7yCb+Am2NFtI1EzKH/ZTA=\n-----END PUBLIC KEY-----\n";

    fn principal_authenticator() -> PrincipalAuthenticator {
        PrincipalAuthenticator::new(
            "heterocloud",
            "heterocloud-flow-data",
            PRINCIPAL_SECRET,
            Duration::from_mins(5),
        )
        .unwrap()
    }

    fn provider_authenticator() -> ProviderAuthenticator {
        ProviderAuthenticator::from_public_keys_json(
            "heterocloud",
            "heterocloud-flow",
            &serde_json::json!({"heterocloud-provider-1": PUBLIC_KEY}).to_string(),
        )
        .unwrap()
    }

    fn principal_headers(signed: &SignedPrincipal, header_timestamp: u64) -> HeaderMap {
        let encoded = URL_SAFE_NO_PAD.encode(serde_json::to_vec(signed).unwrap());
        let timestamp = header_timestamp.to_string();
        let mut mac = Hmac::<Sha256>::new_from_slice(PRINCIPAL_SECRET).unwrap();
        mac.update(timestamp.as_bytes());
        mac.update(b".");
        mac.update(encoded.as_bytes());
        let signature = URL_SAFE_NO_PAD.encode(mac.finalize().into_bytes());
        let mut headers = HeaderMap::new();
        headers.insert(PRINCIPAL_HEADER, HeaderValue::from_str(&encoded).unwrap());
        headers.insert(
            PRINCIPAL_TIMESTAMP_HEADER,
            HeaderValue::from_str(&timestamp).unwrap(),
        );
        headers.insert(
            PRINCIPAL_SIGNATURE_HEADER,
            HeaderValue::from_str(&signature).unwrap(),
        );
        headers
    }

    fn provider_token(action: &str) -> String {
        let now = chrono::Utc::now().timestamp();
        let claims = ProviderClaims {
            issuer: "heterocloud".into(),
            audience: "heterocloud-flow".into(),
            subject: Uuid::new_v4(),
            organization_id: Uuid::new_v4(),
            project_id: Uuid::new_v4(),
            service_instance_id: Uuid::new_v4(),
            action: action.into(),
            generation: 1,
            jwt_id: Uuid::now_v7(),
            issued_at: now,
            not_before: now - 5,
            expires_at: now + 60,
        };
        let mut header = Header::new(Algorithm::EdDSA);
        header.kid = Some("heterocloud-provider-1".into());
        encode(
            &header,
            &claims,
            &EncodingKey::from_ed_pem(PRIVATE_KEY).unwrap(),
        )
        .unwrap()
    }

    #[test]
    fn verifies_exact_provider_contract() {
        let token = provider_token(PROVIDER_RECONCILE_ACTION);
        let mut headers = HeaderMap::new();
        headers.insert(
            AUTHORIZATION,
            HeaderValue::from_str(&format!("Bearer {token}")).unwrap(),
        );

        let claims = provider_authenticator()
            .authenticate_headers(&headers)
            .unwrap();
        assert_eq!(claims.action, PROVIDER_RECONCILE_ACTION);
        assert_eq!(claims.expires_at - claims.issued_at, 60);
        assert_eq!(claims.issued_at - claims.not_before, 5);
    }

    #[test]
    fn rejects_wrong_provider_action() {
        let token = provider_token("room.join");
        assert_eq!(
            provider_authenticator().verify_token(&token),
            Err(AuthError::InvalidProviderCommand)
        );
    }

    #[test]
    fn verifies_service_instance_scoped_principal_context() {
        let now = u64::try_from(chrono::Utc::now().timestamp()).unwrap();
        let service_instance_id = Uuid::new_v4();
        let context_id = Uuid::now_v7();
        let signed = SignedPrincipal {
            issuer: "heterocloud".into(),
            audience: "heterocloud-flow-data".into(),
            organization_id: Uuid::new_v4(),
            project_id: Uuid::new_v4(),
            service_instance_id,
            principal_id: Uuid::new_v4(),
            permissions: BTreeSet::from(["flow.queue.write".into()]),
            issued_at: now,
            expires_at: now + 120,
            context_id,
        };
        let headers = principal_headers(&signed, now);

        let principal = principal_authenticator()
            .authenticate_headers(&headers)
            .unwrap();
        assert_eq!(principal.service_instance_id, service_instance_id);
        assert_eq!(principal.token_id, context_id);
    }

    #[test]
    fn reuses_bearer_context_near_expiry() {
        let now = u64::try_from(chrono::Utc::now().timestamp()).unwrap();
        let issued_at = now - 280;
        let signed = SignedPrincipal {
            issuer: "heterocloud".into(),
            audience: "heterocloud-flow-data".into(),
            organization_id: Uuid::new_v4(),
            project_id: Uuid::new_v4(),
            service_instance_id: Uuid::new_v4(),
            principal_id: Uuid::new_v4(),
            permissions: BTreeSet::from(["flow.queue.write".into()]),
            issued_at,
            expires_at: issued_at + 300,
            context_id: Uuid::now_v7(),
        };
        let headers = principal_headers(&signed, issued_at);
        let authenticator = principal_authenticator();

        let first = authenticator.authenticate_headers(&headers).unwrap();
        let reused = authenticator.authenticate_headers(&headers).unwrap();

        assert_eq!(first.token_id, signed.context_id);
        assert_eq!(reused.token_id, signed.context_id);
        assert_eq!(first, reused);
    }

    #[test]
    fn rejects_timestamp_context_mismatch_after_valid_mac() {
        let now = u64::try_from(chrono::Utc::now().timestamp()).unwrap();
        let signed = SignedPrincipal {
            issuer: "heterocloud".into(),
            audience: "heterocloud-flow-data".into(),
            organization_id: Uuid::new_v4(),
            project_id: Uuid::new_v4(),
            service_instance_id: Uuid::new_v4(),
            principal_id: Uuid::new_v4(),
            permissions: BTreeSet::new(),
            issued_at: now,
            expires_at: now + 300,
            context_id: Uuid::now_v7(),
        };
        let headers = principal_headers(&signed, now - 1);

        assert_eq!(
            principal_authenticator().authenticate_headers(&headers),
            Err(AuthError::PrincipalTimestampMismatch)
        );
    }
}
