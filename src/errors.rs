use crate::column_types;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum EventParseError {
    #[error("unable to parse column: {0:?}")]
    ColumnParseError(#[from] ColumnParseError),
    #[error("I/O error reading column: {0:?}")]
    Io(#[from] ::std::io::Error),
    #[error("unexpected EOF")]
    EofError,
    #[error("bad UUID in Gtid Event: {0:?}")]
    Uuid(#[from] uuid::Error),
}

#[derive(Debug, Error)]
pub enum JsonbParseError {
    #[error("invalid type byte (got {0})")]
    InvalidTypeByte(u8),
    #[error("invalid type literal (got {0})")]
    InvalidLiteral(u16),
    #[error("I/O error reading JSONB value: {0:?}")]
    Io(#[from] ::std::io::Error),
    #[error("invalid JSON")]
    Json(#[from] serde_json::error::Error),
    #[error("error parsing opaque column in json record: {inner:?}")]
    OpaqueColumnParseError {
        #[source]
        inner: Box<ColumnParseError>,
    },
}

impl From<ColumnParseError> for JsonbParseError {
    fn from(e: ColumnParseError) -> Self {
        JsonbParseError::OpaqueColumnParseError { inner: Box::new(e) }
    }
}

#[derive(Debug, Error)]
pub enum ColumnParseError {
    #[error("unimplemented column type: {column_type:?}")]
    UnimplementedTypeError {
        column_type: column_types::ColumnType,
    },
    #[error("error parsing JSON column")]
    Json(#[from] JsonbParseError),
    #[error("error parcing Decimal column")]
    Decimal(#[from] DecimalParseError),
    #[error("I/O error reading column")]
    Io(#[from] std::io::Error),
}

#[derive(Debug, Error)]
pub enum BinlogParseError {
    #[error("error parsing event")]
    EventParseError(#[from] EventParseError),
    #[error("bad magic value at start of binlog: got {0:?}")]
    BadMagic([u8; 4]),
    #[error("bad first record in binlog")]
    BadFirstRecord,
    #[error("error opening binlog file")]
    OpenError(std::io::Error),
    #[error("other I/O error reading binlog file")]
    Io(#[from] std::io::Error),
}

#[derive(Debug, Error)]
pub enum DecimalParseError {
    #[error("I/O error reading decimal")]
    Io(#[from] std::io::Error),
    #[error("Decimal parse error")]
    BigDecimalParse(#[from] bigdecimal::ParseBigDecimalError),
}
