use base64;

use std::borrow::Cow;

use serde::{Serialize,Serializer};
use serde_json;


#[derive(Debug)]
/// Wrapper for the SQL BLOB (Binary Large OBject) and TEXT types
///
/// Serializes as Base64
pub struct Blob(pub Vec<u8>);

impl From<Vec<u8>> for Blob {
    fn from(v: Vec<u8>) -> Self {
        Blob(v)
    }
}

impl Serialize for Blob {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where S: Serializer
    {
        let serialized = base64::encode(&self.0);
        serializer.serialize_str(&serialized)
    }
}


#[derive(Debug, Serialize)]
/// Normalized representation of types which are present in MySQL
pub enum MySQLValue {
    SignedInteger(i64),
    Float(f32),
    Double(f64),
    String(String),
    Enum(i16),
    Blob(Blob),
    Year(u32),
    Date { year: u32, month: u32, day: u32 },
    Time { hours: u32, minutes: u32, seconds: u32, subseconds: u32},
    DateTime { year: u32, month: u32, day: u32, hour: u32, minute: u32, second: u32, subsecond: u32 },
    Json(serde_json::Value),
    Decimal(bigdecimal::BigDecimal),
    Timestamp { unix_time: i32, subsecond: u32 },
    Null
}


impl MySQLValue {
    /// Turn this type into a serde_json::Value
    ///
    /// Tries to avoid round-tripping through Serialize if it can
    pub(crate) fn as_value(&self) -> Result<Cow<serde_json::Value>, serde_json::error::Error> {
        match *self {
            MySQLValue::Json(ref j) => Ok(Cow::Borrowed(j)),
            MySQLValue::Null => Ok(Cow::Owned(serde_json::Value::Null)),
            ref j => {
                Ok(Cow::Owned(serde_json::to_value(j)?))
            }
        }
    }
}
