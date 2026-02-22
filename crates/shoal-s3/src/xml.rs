//! XML response generation for S3 API responses.
//!
//! Hand-written XML builders for the small set of S3 XML formats we support.
//! No XML library dependency needed.

/// Escape XML special characters in a string.
pub(crate) fn xml_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}

/// Build a ListObjectsV2 XML response.
pub(crate) fn list_objects_v2(bucket: &str, prefix: &str, keys: &[String]) -> String {
    let bucket = xml_escape(bucket);
    let prefix = xml_escape(prefix);
    let count = keys.len();

    let mut contents = String::new();
    for key in keys {
        let key = xml_escape(key);
        contents.push_str(&format!(
            "  <Contents>\n    <Key>{key}</Key>\n  </Contents>\n"
        ));
    }

    format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
         <ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\n\
         \x20 <Name>{bucket}</Name>\n\
         \x20 <Prefix>{prefix}</Prefix>\n\
         \x20 <KeyCount>{count}</KeyCount>\n\
         \x20 <MaxKeys>1000</MaxKeys>\n\
         \x20 <IsTruncated>false</IsTruncated>\n\
         {contents}\
         </ListBucketResult>"
    )
}

/// Build an InitiateMultipartUpload XML response.
pub(crate) fn initiate_multipart_upload(bucket: &str, key: &str, upload_id: &str) -> String {
    let bucket = xml_escape(bucket);
    let key = xml_escape(key);
    format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
         <InitiateMultipartUploadResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\n\
         \x20 <Bucket>{bucket}</Bucket>\n\
         \x20 <Key>{key}</Key>\n\
         \x20 <UploadId>{upload_id}</UploadId>\n\
         </InitiateMultipartUploadResult>"
    )
}

/// Build a CompleteMultipartUpload XML response.
pub(crate) fn complete_multipart_upload(bucket: &str, key: &str, etag: &str) -> String {
    let bucket = xml_escape(bucket);
    let key = xml_escape(key);
    format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
         <CompleteMultipartUploadResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\n\
         \x20 <Bucket>{bucket}</Bucket>\n\
         \x20 <Key>{key}</Key>\n\
         \x20 <ETag>\"{etag}\"</ETag>\n\
         </CompleteMultipartUploadResult>"
    )
}

/// Parse part numbers from a CompleteMultipartUpload XML request body.
///
/// Handles both single-line and multi-line XML by searching for
/// `<PartNumber>N</PartNumber>` tags anywhere in the body.
pub(crate) fn parse_complete_multipart_request(body: &str) -> Vec<u16> {
    let mut parts = Vec::new();
    let mut remaining = body;

    while let Some(start) = remaining.find("<PartNumber>") {
        remaining = &remaining[start + "<PartNumber>".len()..];
        if let Some(end) = remaining.find("</PartNumber>") {
            let num_str = &remaining[..end];
            if let Ok(n) = num_str.parse::<u16>() {
                parts.push(n);
            }
            remaining = &remaining[end + "</PartNumber>".len()..];
        } else {
            break;
        }
    }

    parts.sort();
    parts
}
