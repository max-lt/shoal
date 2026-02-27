//! XML response types for the S3 API, serialized via `quick-xml` + `serde`.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use shoal_types::{BucketInfo, ObjectInfo};

const S3_XMLNS: &str = "http://s3.amazonaws.com/doc/2006-03-01/";

/// Convert unix seconds to ISO 8601 string (e.g. `2024-01-15T12:30:00Z`).
fn unix_to_iso8601(secs: u64) -> String {
    if secs == 0 {
        tracing::warn!("unix_to_iso8601 called with 0 — missing timestamp?");
        return "1970-01-01T00:00:00Z".to_string();
    }

    const SECS_PER_DAY: u64 = 86400;

    let days = secs / SECS_PER_DAY;
    let time_of_day = secs % SECS_PER_DAY;

    let hours = time_of_day / 3600;
    let minutes = (time_of_day % 3600) / 60;
    let seconds = time_of_day % 60;

    // Civil date from days since epoch (algorithm from Howard Hinnant).
    let z = days as i64;
    let era = if z >= 0 { z } else { z - 146096 } / 146097;
    let doe = (z - era * 146097) as u64;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };

    format!("{y:04}-{m:02}-{d:02}T{hours:02}:{minutes:02}:{seconds:02}Z")
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
    #[serde(rename = "CreationDate")]
    creation_date: String,
}

pub(crate) fn list_all_my_buckets(owner_id: &str, buckets: &[BucketInfo]) -> String {
    quick_xml::se::to_string(&ListAllMyBucketsResult {
        xmlns: S3_XMLNS,
        owner: Owner {
            id: owner_id.to_string(),
        },
        buckets: Buckets {
            bucket: buckets
                .iter()
                .map(|b| BucketEntry {
                    name: b.name.clone(),
                    creation_date: unix_to_iso8601(b.created_at),
                })
                .collect(),
        },
    })
    .expect("ListAllMyBucketsResult contains only safe strings")
}

// -----------------------------------------------------------------------
// VersioningConfiguration (GET /{bucket}?versioning)
// -----------------------------------------------------------------------

#[derive(Serialize)]
#[serde(rename = "VersioningConfiguration")]
struct VersioningConfiguration {
    #[serde(rename = "@xmlns")]
    xmlns: &'static str,
}

pub(crate) fn versioning_configuration() -> String {
    quick_xml::se::to_string(&VersioningConfiguration { xmlns: S3_XMLNS })
        .expect("VersioningConfiguration contains only safe strings")
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
    quick_xml::se::to_string(&CopyObjectResultXml {
        xmlns: S3_XMLNS,
        etag: format!("\"{etag}\""),
    })
    .expect("CopyObjectResult contains only safe strings")
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
    #[serde(rename = "Size")]
    size: u64,
    #[serde(rename = "LastModified")]
    last_modified: String,
    #[serde(rename = "ETag")]
    etag: String,
}

/// Strip characters illegal in XML 1.0 (control chars except TAB, LF, CR).
fn sanitize_for_xml(s: &str) -> String {
    s.chars()
        .filter(|&c| matches!(c, '\t' | '\n' | '\r' | '\u{20}'..))
        .collect()
}

pub(crate) fn list_objects_v2(
    bucket: &str,
    prefix: &str,
    objects: &[ObjectInfo],
    max_keys: usize,
    is_truncated: bool,
) -> String {
    quick_xml::se::to_string(&ListBucketResult {
        xmlns: S3_XMLNS,
        name: bucket.to_string(),
        prefix: prefix.to_string(),
        key_count: objects.len(),
        max_keys,
        is_truncated,
        contents: objects
            .iter()
            .map(|o| Contents {
                key: sanitize_for_xml(&o.key),
                size: o.size,
                last_modified: unix_to_iso8601(o.last_modified),
                etag: format!("\"{}\"", o.etag),
            })
            .collect(),
    })
    .expect("ListBucketResult with sanitized keys cannot fail")
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
    quick_xml::se::to_string(&InitiateMultipartUploadResult {
        xmlns: S3_XMLNS,
        bucket: bucket.to_string(),
        key: key.to_string(),
        upload_id: upload_id.to_string(),
    })
    .expect("InitiateMultipartUploadResult contains only safe strings")
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
    quick_xml::se::to_string(&CompleteMultipartUploadResult {
        xmlns: S3_XMLNS,
        bucket: bucket.to_string(),
        key: key.to_string(),
        etag: format!("\"{etag}\""),
    })
    .expect("CompleteMultipartUploadResult contains only safe strings")
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

// -----------------------------------------------------------------------
// Tagging (GetObjectTagging / PutObjectTagging)
// -----------------------------------------------------------------------

#[derive(Serialize)]
#[serde(rename = "Tagging")]
struct TaggingXml {
    #[serde(rename = "TagSet")]
    tag_set: TagSet,
}

#[derive(Serialize)]
struct TagSet {
    #[serde(rename = "Tag", default)]
    tag: Vec<TagXml>,
}

#[derive(Serialize)]
struct TagXml {
    #[serde(rename = "Key")]
    key: String,
    #[serde(rename = "Value")]
    value: String,
}

pub(crate) fn tagging_xml(tags: &BTreeMap<String, String>) -> String {
    quick_xml::se::to_string(&TaggingXml {
        tag_set: TagSet {
            tag: tags
                .iter()
                .map(|(k, v)| TagXml {
                    key: k.clone(),
                    value: v.clone(),
                })
                .collect(),
        },
    })
    .expect("TaggingXml contains only safe strings")
}

#[derive(Deserialize)]
#[serde(rename = "Tagging")]
struct TaggingRequest {
    #[serde(rename = "TagSet")]
    tag_set: TagSetRequest,
}

#[derive(Deserialize)]
struct TagSetRequest {
    #[serde(rename = "Tag", default)]
    tag: Vec<TagRequest>,
}

#[derive(Deserialize)]
struct TagRequest {
    #[serde(rename = "Key")]
    key: String,
    #[serde(rename = "Value")]
    value: String,
}

/// Parse tags from a `PutObjectTagging` XML request body.
pub(crate) fn parse_tagging_request(body: &str) -> BTreeMap<String, String> {
    let Ok(req) = quick_xml::de::from_str::<TaggingRequest>(body) else {
        return BTreeMap::new();
    };

    req.tag_set
        .tag
        .into_iter()
        .map(|t| (t.key, t.value))
        .collect()
}

// -----------------------------------------------------------------------
// ListMultipartUploadsResult
// -----------------------------------------------------------------------

#[derive(Serialize)]
#[serde(rename = "ListMultipartUploadsResult")]
struct ListMultipartUploadsResultXml {
    #[serde(rename = "@xmlns")]
    xmlns: &'static str,
    #[serde(rename = "Bucket")]
    bucket: String,
    #[serde(rename = "Upload", default)]
    upload: Vec<UploadXml>,
}

#[derive(Serialize)]
struct UploadXml {
    #[serde(rename = "Key")]
    key: String,
    #[serde(rename = "UploadId")]
    upload_id: String,
}

pub(crate) fn list_multipart_uploads(
    bucket: &str,
    uploads: &[(String, String)], // (key, upload_id)
) -> String {
    quick_xml::se::to_string(&ListMultipartUploadsResultXml {
        xmlns: S3_XMLNS,
        bucket: bucket.to_string(),
        upload: uploads
            .iter()
            .map(|(key, upload_id)| UploadXml {
                key: key.clone(),
                upload_id: upload_id.clone(),
            })
            .collect(),
    })
    .expect("ListMultipartUploadsResult contains only safe strings")
}

// -----------------------------------------------------------------------
// ListPartsResult
// -----------------------------------------------------------------------

#[derive(Serialize)]
#[serde(rename = "ListPartsResult")]
struct ListPartsResultXml {
    #[serde(rename = "@xmlns")]
    xmlns: &'static str,
    #[serde(rename = "Bucket")]
    bucket: String,
    #[serde(rename = "Key")]
    key: String,
    #[serde(rename = "UploadId")]
    upload_id: String,
    #[serde(rename = "Part", default)]
    part: Vec<PartXml>,
}

#[derive(Serialize)]
struct PartXml {
    #[serde(rename = "PartNumber")]
    part_number: u16,
    #[serde(rename = "Size")]
    size: usize,
    #[serde(rename = "ETag")]
    etag: String,
}

pub(crate) fn list_parts(
    bucket: &str,
    key: &str,
    upload_id: &str,
    parts: &[(u16, usize, String)], // (part_number, size, etag)
) -> String {
    quick_xml::se::to_string(&ListPartsResultXml {
        xmlns: S3_XMLNS,
        bucket: bucket.to_string(),
        key: key.to_string(),
        upload_id: upload_id.to_string(),
        part: parts
            .iter()
            .map(|(num, size, etag)| PartXml {
                part_number: *num,
                size: *size,
                etag: etag.clone(),
            })
            .collect(),
    })
    .expect("ListPartsResult contains only safe strings")
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
