//! Minimal JWT verification.
//!
//! `nebula-server` supports two authentication schemes in parallel:
//!
//! 1. **Static bearer allowlist** (`AppConfig::api_keys`) — for simple
//!    deployments, CI, and local dev.
//! 2. **JWT** (`AppConfig::jwt`) — symmetric HS256 verification against
//!    a shared secret. Validated claims: `exp` (required), `iss` and
//!    `aud` (enforced iff configured). A request passes auth if *either*
//!    scheme accepts it.
//!
//! We deliberately do not add JWKs / rotation / asymmetric keys here.
//! Those are a real request, but they are their own project (HTTP
//! fetch of a key set, cache invalidation, kid handling) and easy to
//! layer on later behind this config.

use jsonwebtoken::{decode, Algorithm, DecodingKey, Validation};
use serde::Deserialize;

#[derive(Debug, Clone)]
pub struct JwtConfig {
    /// HS256 shared secret. Base64 decoding is NOT applied — the raw
    /// bytes are the HMAC key. Generate with 32+ random bytes.
    pub secret: Vec<u8>,
    /// Required `iss` claim, if set.
    pub expected_issuer: Option<String>,
    /// Required `aud` claim, if set.
    pub expected_audience: Option<String>,
}

impl JwtConfig {
    pub fn hs256(secret: impl Into<Vec<u8>>) -> Self {
        Self {
            secret: secret.into(),
            expected_issuer: None,
            expected_audience: None,
        }
    }
}

#[derive(Debug, Deserialize)]
struct Claims {
    /// Subject — used as the rate-limit key when available. We decode
    /// it even if callers don't ask for it so request tracing can
    /// attribute a JWT to its principal.
    #[allow(dead_code)]
    sub: Option<String>,
    /// Expiration is required; `jsonwebtoken::Validation` enforces it
    /// by default but serde decoding will only accept tokens that
    /// carry the claim.
    #[allow(dead_code)]
    exp: usize,
    #[serde(default)]
    #[allow(dead_code)]
    iss: Option<String>,
    #[serde(default)]
    #[allow(dead_code)]
    aud: Option<String>,
}

/// Verify a JWT string. Returns `Ok(())` on success, `Err(reason)` on
/// failure. The reason string is logged server-side but NEVER echoed
/// to the caller — malformed-token diagnostics are a mild information
/// leak in auth contexts.
pub fn verify(cfg: &JwtConfig, token: &str) -> Result<(), String> {
    let mut validation = Validation::new(Algorithm::HS256);
    validation.leeway = 30; // small clock skew allowance
    if let Some(iss) = &cfg.expected_issuer {
        validation.set_issuer(&[iss]);
    }
    if let Some(aud) = &cfg.expected_audience {
        validation.set_audience(&[aud]);
    } else {
        // Without an explicit `aud` to validate, tell the library to
        // skip the check entirely rather than refusing tokens that
        // happen to carry an audience we don't recognize.
        validation.validate_aud = false;
    }
    let key = DecodingKey::from_secret(&cfg.secret);
    decode::<Claims>(token, &key, &validation)
        .map(|_| ())
        .map_err(|e| e.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use jsonwebtoken::{encode, EncodingKey, Header};
    use serde::Serialize;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[derive(Serialize)]
    struct Issue<'a> {
        sub: &'a str,
        exp: usize,
        #[serde(skip_serializing_if = "Option::is_none")]
        iss: Option<&'a str>,
        #[serde(skip_serializing_if = "Option::is_none")]
        aud: Option<&'a str>,
    }

    fn issue(secret: &[u8], claims: Issue<'_>) -> String {
        encode(
            &Header::new(Algorithm::HS256),
            &claims,
            &EncodingKey::from_secret(secret),
        )
        .unwrap()
    }

    fn now_plus(secs: i64) -> usize {
        let n = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;
        (n + secs).max(0) as usize
    }

    #[test]
    fn accepts_valid_token() {
        let secret = b"a-very-secret-secret-at-least-32b!";
        let tok = issue(
            secret,
            Issue {
                sub: "alice",
                exp: now_plus(60),
                iss: None,
                aud: None,
            },
        );
        let cfg = JwtConfig::hs256(secret.to_vec());
        assert!(verify(&cfg, &tok).is_ok());
    }

    #[test]
    fn rejects_expired_token() {
        let secret = b"another-secret-that-is-long-enough!!";
        let tok = issue(
            secret,
            Issue {
                sub: "alice",
                exp: now_plus(-60),
                iss: None,
                aud: None,
            },
        );
        let cfg = JwtConfig::hs256(secret.to_vec());
        assert!(verify(&cfg, &tok).is_err());
    }

    #[test]
    fn rejects_wrong_secret() {
        let tok = issue(
            b"issuer-side-secret",
            Issue {
                sub: "a",
                exp: now_plus(60),
                iss: None,
                aud: None,
            },
        );
        let cfg = JwtConfig::hs256(b"verifier-side-different".to_vec());
        assert!(verify(&cfg, &tok).is_err());
    }

    #[test]
    fn rejects_wrong_issuer() {
        let secret = b"shared-secret-xxxxxxxxxxxxxxxxxxx";
        let tok = issue(
            secret,
            Issue {
                sub: "a",
                exp: now_plus(60),
                iss: Some("evil-corp"),
                aud: None,
            },
        );
        let mut cfg = JwtConfig::hs256(secret.to_vec());
        cfg.expected_issuer = Some("nebuladb".into());
        assert!(verify(&cfg, &tok).is_err());
    }

    #[test]
    fn accepts_audience_match() {
        let secret = b"shared-secret-xxxxxxxxxxxxxxxxxxx";
        let tok = issue(
            secret,
            Issue {
                sub: "a",
                exp: now_plus(60),
                iss: None,
                aud: Some("nebula"),
            },
        );
        let mut cfg = JwtConfig::hs256(secret.to_vec());
        cfg.expected_audience = Some("nebula".into());
        assert!(verify(&cfg, &tok).is_ok());
    }
}
