//! XML response types for the S3 API, serialized via `quick-xml` + `serde`.

use serde::{Deserialize, Serialize};

const S3_XMLNS: &str = "http://s3.amazonaws.com/doc/2006-03-01/";

/// Helper: serialize a struct to an S3 XML string with `<?xml ...?>` header.
fn to_xml<T: Serialize>(value: &T) -> String {
    let body = quick_xml::se::to_string(value).expect("XML serialization cannot fail");
    format!("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n{body}")
}

// -----------------------------------------------------------------------
// S3 Error
// -----------------------------------------------------------------------

#[derive(Serialize)]
#[serde(rename = "Error")]
pub(crate) struct S3ErrorXml<'a> {
    #[serde(rename = "Code")]
    pub code: &'a str,
    #[serde(rename = "Message")]
    pub message: &'a str,
}

pub(crate) fn error_xml(code: &str, message: &str) -> String {
    to_xml(&S3ErrorXml { code, message })
}

// -----------------------------------------------------------------------
// ListAllMyBucketsResult (GET /)
// -----------------------------------------------------------------------

#[derive(Serialize)]
#[serde(rename = "ListAllMyBucketsResult")]
struct ListAllMyBucketsResult {
    #[serde(rename = "@xmlns")]
    xmlns: &'static str,
    #[serde(rename = "Owner")]
    owner: Owner,
    #[serde(rename = "Buckets")]
    buckets: Buckets,
}

#[derive(Serialize)]
struct Owner {
    #[serde(rename = "ID")]
    id: String,
}

#[derive(Serialize)]
struct Buckets {
    #[serde(rename = "Bucket", default)]
    bucket: Vec<BucketEntry>,
}

#[derive(Serialize)]
struct BucketEntry {
    #[serde(rename = "Name")]
    name: String,
}

pub(crate) fn list_all_my_buckets(owner_id: &str, buckets: &[String]) -> String {
    to_xml(&ListAllMyBucketsResult {
        xmlns: S3_XMLNS,
        owner: Owner {
            id: owner_id.to_string(),
        },
        buckets: Buckets {
            bucket: buckets
                .iter()
                .map(|name| BucketEntry { name: name.clone() })
                .collect(),
        },
    })
}

// -----------------------------------------------------------------------
// CopyObjectResult
// -----------------------------------------------------------------------

#[derive(Serialize)]
#[serde(rename = "CopyObjectResult")]
struct CopyObjectResultXml {
    #[serde(rename = "@xmlns")]
    xmlns: &'static str,
    #[serde(rename = "ETag")]
    etag: String,
}

pub(crate) fn copy_object_result(etag: &str) -> String {
    to_xml(&CopyObjectResultXml {
        xmlns: S3_XMLNS,
        etag: format!("\"{etag}\""),
    })
}

// -----------------------------------------------------------------------
// ListBucketResult (ListObjectsV2)
// -----------------------------------------------------------------------

#[derive(Serialize)]
#[serde(rename = "ListBucketResult")]
struct ListBucketResult {
    #[serde(rename = "@xmlns")]
    xmlns: &'static str,
    #[serde(rename = "Name")]
    name: String,
    #[serde(rename = "Prefix")]
    prefix: String,
    #[serde(rename = "KeyCount")]
    key_count: usize,
    #[serde(rename = "MaxKeys")]
    max_keys: usize,
    #[serde(rename = "IsTruncated")]
    is_truncated: bool,
    #[serde(rename = "Contents", default)]
    contents: Vec<Contents>,
}

#[derive(Serialize)]
struct Contents {
    #[serde(rename = "Key")]
    key: String,
}

pub(crate) fn list_objects_v2(
    bucket: &str,
    prefix: &str,
    keys: &[String],
    max_keys: usize,
    is_truncated: bool,
) -> String {
    to_xml(&ListBucketResult {
        xmlns: S3_XMLNS,
        name: bucket.to_string(),
        prefix: prefix.to_string(),
        key_count: keys.len(),
        max_keys,
        is_truncated,
        contents: keys.iter().map(|k| Contents { key: k.clone() }).collect(),
    })
}

// -----------------------------------------------------------------------
// InitiateMultipartUploadResult
// -----------------------------------------------------------------------

#[derive(Serialize)]
#[serde(rename = "InitiateMultipartUploadResult")]
struct InitiateMultipartUploadResult {
    #[serde(rename = "@xmlns")]
    xmlns: &'static str,
    #[serde(rename = "Bucket")]
    bucket: String,
    #[serde(rename = "Key")]
    key: String,
    #[serde(rename = "UploadId")]
    upload_id: String,
}

pub(crate) fn initiate_multipart_upload(bucket: &str, key: &str, upload_id: &str) -> String {
    to_xml(&InitiateMultipartUploadResult {
        xmlns: S3_XMLNS,
        bucket: bucket.to_string(),
        key: key.to_string(),
        upload_id: upload_id.to_string(),
    })
}

// -----------------------------------------------------------------------
// CompleteMultipartUploadResult
// -----------------------------------------------------------------------

#[derive(Serialize)]
#[serde(rename = "CompleteMultipartUploadResult")]
struct CompleteMultipartUploadResult {
    #[serde(rename = "@xmlns")]
    xmlns: &'static str,
    #[serde(rename = "Bucket")]
    bucket: String,
    #[serde(rename = "Key")]
    key: String,
    #[serde(rename = "ETag")]
    etag: String,
}

pub(crate) fn complete_multipart_upload(bucket: &str, key: &str, etag: &str) -> String {
    to_xml(&CompleteMultipartUploadResult {
        xmlns: S3_XMLNS,
        bucket: bucket.to_string(),
        key: key.to_string(),
        etag: format!("\"{etag}\""),
    })
}

// -----------------------------------------------------------------------
// CompleteMultipartUpload request parsing (incoming XML)
// -----------------------------------------------------------------------

#[derive(Deserialize)]
#[serde(rename = "CompleteMultipartUpload")]
struct CompleteMultipartUploadRequest {
    #[serde(rename = "Part", default)]
    parts: Vec<PartInfo>,
}

#[derive(Deserialize)]
struct PartInfo {
    #[serde(rename = "PartNumber")]
    part_number: u16,
}

/// Parse part numbers from a `CompleteMultipartUpload` XML request body.
pub(crate) fn parse_complete_multipart_request(body: &str) -> Vec<u16> {
    let Ok(req) = quick_xml::de::from_str::<CompleteMultipartUploadRequest>(body) else {
        return Vec::new();
    };

    let mut parts: Vec<u16> = req.parts.into_iter().map(|p| p.part_number).collect();
    parts.sort();
    parts
}
