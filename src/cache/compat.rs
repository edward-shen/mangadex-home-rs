//! These structs have alternative deserialize and serializations
//! implementations to assist reading from the official client file format.

use std::str::FromStr;

use chrono::{DateTime, FixedOffset};
use serde::de::{Unexpected, Visitor};
use serde::{Deserialize, Serialize};

use super::ImageContentType;

#[derive(Copy, Clone, Deserialize)]
pub struct LegacyImageMetadata {
    pub(crate) content_type: Option<LegacyImageContentType>,
    pub(crate) size: Option<u32>,
    pub(crate) last_modified: Option<LegacyDateTime>,
}

#[derive(Copy, Clone, Serialize)]
pub struct LegacyDateTime(pub DateTime<FixedOffset>);

impl<'de> Deserialize<'de> for LegacyDateTime {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct LegacyDateTimeVisitor;

        impl<'de> Visitor<'de> for LegacyDateTimeVisitor {
            type Value = LegacyDateTime;

            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                write!(f, "a valid image type")
            }

            fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                DateTime::parse_from_rfc2822(v)
                    .map(LegacyDateTime)
                    .map_err(|_| E::invalid_value(Unexpected::Str(v), &"a valid image type"))
            }
        }

        deserializer.deserialize_str(LegacyDateTimeVisitor)
    }
}

#[derive(Copy, Clone)]
pub struct LegacyImageContentType(pub ImageContentType);

impl<'de> Deserialize<'de> for LegacyImageContentType {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct LegacyImageContentTypeVisitor;

        impl<'de> Visitor<'de> for LegacyImageContentTypeVisitor {
            type Value = LegacyImageContentType;

            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                write!(f, "a valid image type")
            }

            fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                ImageContentType::from_str(v)
                    .map(LegacyImageContentType)
                    .map_err(|_| E::invalid_value(Unexpected::Str(v), &"a valid image type"))
            }
        }

        deserializer.deserialize_str(LegacyImageContentTypeVisitor)
    }
}

#[cfg(test)]
mod parse {
    use std::error::Error;

    use chrono::DateTime;

    use crate::cache::ImageContentType;

    use super::LegacyImageMetadata;

    #[test]
    fn from_valid_legacy_format() -> Result<(), Box<dyn Error>> {
        let legacy_header = r#"{"content_type":"image/jpeg","last_modified":"Sat, 10 Apr 2021 10:55:22 GMT","size":117888}"#;
        let metadata: LegacyImageMetadata = serde_json::from_str(legacy_header)?;

        assert_eq!(
            metadata.content_type.map(|v| v.0),
            Some(ImageContentType::Jpeg)
        );
        assert_eq!(metadata.size, Some(117888));
        assert_eq!(
            metadata.last_modified.map(|v| v.0),
            Some(DateTime::parse_from_rfc2822(
                "Sat, 10 Apr 2021 10:55:22 GMT"
            )?)
        );

        Ok(())
    }

    #[test]
    fn empty_metadata() -> Result<(), Box<dyn Error>> {
        let legacy_header = "{}";
        let metadata: LegacyImageMetadata = serde_json::from_str(legacy_header)?;

        assert!(metadata.content_type.is_none());
        assert!(metadata.size.is_none());
        assert!(metadata.last_modified.is_none());

        Ok(())
    }

    #[test]
    fn invalid_image_mime_value() {
        let legacy_header = r#"{"content_type":"image/not-a-real-image"}"#;
        assert!(serde_json::from_str::<LegacyImageMetadata>(legacy_header).is_err());
    }

    #[test]
    fn invalid_date_time() {
        let legacy_header = r#"{"last_modified":"idk last tuesday?"}"#;
        assert!(serde_json::from_str::<LegacyImageMetadata>(legacy_header).is_err());
    }

    #[test]
    fn invalid_size() {
        let legacy_header = r#"{"size":-1}"#;
        assert!(serde_json::from_str::<LegacyImageMetadata>(legacy_header).is_err());
    }

    #[test]
    fn wrong_image_type() {
        let legacy_header = r#"{"content_type":25}"#;
        assert!(serde_json::from_str::<LegacyImageMetadata>(legacy_header).is_err());
    }

    #[test]
    fn wrong_date_time_type() {
        let legacy_header = r#"{"last_modified":false}"#;
        assert!(serde_json::from_str::<LegacyImageMetadata>(legacy_header).is_err());
    }
}
