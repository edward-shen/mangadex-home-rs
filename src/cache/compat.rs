//! These structs have alternative deserialize and serializations
//! implementations to assist reading from the official client file format.

use std::str::FromStr;

use chrono::{DateTime, FixedOffset};
use serde::de::{Unexpected, Visitor};
use serde::{Deserialize, Serialize};

use super::ImageContentType;

#[derive(Copy, Clone, Serialize, Deserialize)]
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

impl Serialize for LegacyImageContentType {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(self.0.as_ref())
    }
}

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
