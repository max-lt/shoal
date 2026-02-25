//! AWS Signature V4 verification for S3 data-plane requests.
//!
//! Implements the server-side of the [AWS Signature Version 4 signing process][spec]
//! so that standard S3 clients (AWS CLI, boto3, Bun S3Client) can authenticate
//! against Shoal.
//!
//! [spec]: https://docs.aws.amazon.com/AmazonS3/latest/API/sig-v4-authenticating-requests.html

use std::collections::HashMap;

use axum::extract::Request;
use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;

use crate::S3Error;

type HmacSha256 = Hmac<Sha256>;

/// Parsed components of an `Authorization: AWS4-HMAC-SHA256 ...` header.
struct SigV4Auth {
    access_key_id: String,
    date: String, // YYYYMMDD
    region: String,
    signed_headers: Vec<String>,
    signature: String, // hex
}

/// Parse the `Authorization` header value into its SigV4 components.
///
/// Expected format:
/// ```text
/// AWS4-HMAC-SHA256 Credential=<key>/<date>/<region>/s3/aws4_request, SignedHeaders=<list>, Signature=<hex>
/// ```
fn parse_authorization(header: &str) -> Result<SigV4Auth, S3Error> {
    let header = header.trim();
    let rest = header
        .strip_prefix("AWS4-HMAC-SHA256")
        .ok_or(S3Error::AccessDenied)?
        .trim();

    let mut credential = None;
    let mut signed_headers_raw = None;
    let mut signature = None;

    for part in rest.split(',') {
        let part = part.trim();

        if let Some(val) = part.strip_prefix("Credential=") {
            credential = Some(val.trim());
        } else if let Some(val) = part.strip_prefix("SignedHeaders=") {
            signed_headers_raw = Some(val.trim());
        } else if let Some(val) = part.strip_prefix("Signature=") {
            signature = Some(val.trim());
        }
    }

    let credential = credential.ok_or(S3Error::AccessDenied)?;
    let signed_headers_raw = signed_headers_raw.ok_or(S3Error::AccessDenied)?;
    let signature = signature.ok_or(S3Error::AccessDenied)?;

    // Credential = <access_key_id>/<date>/<region>/s3/aws4_request
    let cred_parts: Vec<&str> = credential.splitn(5, '/').collect();

    if cred_parts.len() != 5 || cred_parts[3] != "s3" || cred_parts[4] != "aws4_request" {
        return Err(S3Error::AccessDenied);
    }

    let signed_headers: Vec<String> = signed_headers_raw
        .split(';')
        .map(|s| s.to_owned())
        .collect();

    Ok(SigV4Auth {
        access_key_id: cred_parts[0].to_owned(),
        date: cred_parts[1].to_owned(),
        region: cred_parts[2].to_owned(),
        signed_headers,
        signature: signature.to_owned(),
    })
}

/// Build the canonical request string per the SigV4 spec.
///
/// ```text
/// <HTTPMethod>\n
/// <CanonicalURI>\n
/// <CanonicalQueryString>\n
/// <CanonicalHeaders>\n
/// <SignedHeaders>\n
/// <HashedPayload>
/// ```
fn build_canonical_request(
    method: &str,
    uri: &str,
    query: &str,
    headers: &[(String, String)],
    signed_headers: &[String],
    payload_hash: &str,
) -> String {
    // Canonical URI: the path component, already percent-encoded by the client.
    let canonical_uri = if uri.is_empty() { "/" } else { uri };

    // Canonical query string: sort by key name, then by value.
    let canonical_query = canonical_query_string(query);

    // Canonical headers: lowercase name, trimmed value, sorted, each line ends with \n.
    let mut canonical_headers = String::new();
    let mut header_map: Vec<(&str, &str)> = Vec::new();

    for (name, value) in headers {
        if signed_headers.contains(name) {
            header_map.push((name.as_str(), value.as_str()));
        }
    }

    header_map.sort_by_key(|(name, _)| *name);

    for (name, value) in &header_map {
        canonical_headers.push_str(name);
        canonical_headers.push(':');
        canonical_headers.push_str(value.trim());
        canonical_headers.push('\n');
    }

    let signed_headers_str = signed_headers.join(";");

    format!(
        "{}\n{}\n{}\n{}\n{}\n{}",
        method, canonical_uri, canonical_query, canonical_headers, signed_headers_str, payload_hash
    )
}

/// Sort query parameters by key for the canonical query string.
fn canonical_query_string(query: &str) -> String {
    if query.is_empty() {
        return String::new();
    }

    let mut pairs: Vec<(&str, &str)> = query
        .split('&')
        .filter(|s| !s.is_empty())
        .map(|pair| {
            let mut split = pair.splitn(2, '=');
            let key = split.next().unwrap_or("");
            let val = split.next().unwrap_or("");
            (key, val)
        })
        .collect();

    pairs.sort();

    pairs
        .iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<_>>()
        .join("&")
}

/// Build the "string to sign" for SigV4.
///
/// ```text
/// AWS4-HMAC-SHA256\n
/// <timestamp>\n
/// <scope>\n
/// <hex(SHA256(canonical_request))>
/// ```
fn build_string_to_sign(timestamp: &str, scope: &str, canonical_request: &str) -> String {
    let hash = hex::encode(Sha256::digest(canonical_request.as_bytes()));
    format!("AWS4-HMAC-SHA256\n{timestamp}\n{scope}\n{hash}")
}

/// Derive the signing key via the HMAC chain:
///
/// ```text
/// DateKey    = HMAC-SHA256("AWS4" + secret, date)
/// RegionKey  = HMAC-SHA256(DateKey, region)
/// ServiceKey = HMAC-SHA256(RegionKey, "s3")
/// SigningKey = HMAC-SHA256(ServiceKey, "aws4_request")
/// ```
fn derive_signing_key(secret: &str, date: &str, region: &str) -> Vec<u8> {
    let key = format!("AWS4{secret}");

    let date_key = hmac_sha256(key.as_bytes(), date.as_bytes());
    let region_key = hmac_sha256(&date_key, region.as_bytes());
    let service_key = hmac_sha256(&region_key, b"s3");
    hmac_sha256(&service_key, b"aws4_request")
}

/// Compute the hex-encoded HMAC-SHA256 signature.
fn compute_signature(signing_key: &[u8], string_to_sign: &str) -> String {
    hex::encode(hmac_sha256(signing_key, string_to_sign.as_bytes()))
}

/// HMAC-SHA256 helper.
fn hmac_sha256(key: &[u8], data: &[u8]) -> Vec<u8> {
    let mut mac = HmacSha256::new_from_slice(key).expect("HMAC can take key of any size");
    mac.update(data);
    mac.finalize().into_bytes().to_vec()
}

/// Verify an incoming request against stored API keys using AWS Signature V4.
///
/// Accepts `UNSIGNED-PAYLOAD` for the payload hash (matching MinIO / most
/// S3-compatible servers) since the middleware runs before body consumption.
pub(crate) fn verify_sigv4(
    request: &Request,
    api_keys: &HashMap<String, String>,
) -> Result<(), S3Error> {
    let auth_header = request
        .headers()
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .ok_or(S3Error::AccessDenied)?;

    let parsed = parse_authorization(auth_header)?;

    // Look up the secret for this access key.
    let secret = api_keys
        .get(&parsed.access_key_id)
        .ok_or(S3Error::AccessDenied)?;

    // Collect request headers as lowercase name → value pairs.
    let headers: Vec<(String, String)> = request
        .headers()
        .iter()
        .map(|(name, value)| {
            (
                name.as_str().to_lowercase(),
                value.to_str().unwrap_or("").to_owned(),
            )
        })
        .collect();

    // Payload hash: use x-amz-content-sha256 if present, otherwise UNSIGNED-PAYLOAD.
    let payload_hash = request
        .headers()
        .get("x-amz-content-sha256")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("UNSIGNED-PAYLOAD");

    // Extract URI path and query.
    let uri = request.uri();
    let path = uri.path();
    let query = uri.query().unwrap_or("");

    let method = request.method().as_str();

    let canonical_request = build_canonical_request(
        method,
        path,
        query,
        &headers,
        &parsed.signed_headers,
        payload_hash,
    );

    // Scope: date/region/s3/aws4_request
    let scope = format!("{}/{}/s3/aws4_request", parsed.date, parsed.region);

    // Timestamp from x-amz-date header.
    let timestamp = request
        .headers()
        .get("x-amz-date")
        .and_then(|v| v.to_str().ok())
        .ok_or(S3Error::AccessDenied)?;

    let string_to_sign = build_string_to_sign(timestamp, &scope, &canonical_request);

    let signing_key = derive_signing_key(secret, &parsed.date, &parsed.region);
    let expected_signature = compute_signature(&signing_key, &string_to_sign);

    // Constant-time comparison to prevent timing attacks.
    let sig_match: bool = expected_signature
        .as_bytes()
        .ct_eq(parsed.signature.as_bytes())
        .into();

    if !sig_match {
        return Err(S3Error::AccessDenied);
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_authorization_valid() {
        let header = "AWS4-HMAC-SHA256 Credential=SHOALTEST123456789012/20260225/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-content-sha256;x-amz-date, Signature=abcdef0123456789";
        let parsed = parse_authorization(header).unwrap();

        assert_eq!(parsed.access_key_id, "SHOALTEST123456789012");
        assert_eq!(parsed.date, "20260225");
        assert_eq!(parsed.region, "us-east-1");
        assert_eq!(
            parsed.signed_headers,
            vec!["host", "x-amz-content-sha256", "x-amz-date"]
        );
        assert_eq!(parsed.signature, "abcdef0123456789");
    }

    #[test]
    fn test_parse_authorization_bearer_rejected() {
        let result = parse_authorization("Bearer sometoken");
        assert!(result.is_err());
    }

    #[test]
    fn test_derive_signing_key_deterministic() {
        let key1 = derive_signing_key("secret", "20260225", "us-east-1");
        let key2 = derive_signing_key("secret", "20260225", "us-east-1");

        assert_eq!(key1, key2);
    }

    #[test]
    fn test_canonical_query_string_sorts() {
        assert_eq!(canonical_query_string("z=1&a=2&m=3"), "a=2&m=3&z=1");
    }

    #[test]
    fn test_canonical_query_string_empty() {
        assert_eq!(canonical_query_string(""), "");
    }
}
