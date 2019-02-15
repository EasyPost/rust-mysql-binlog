use crate::column_types;

#[derive(Debug, Fail)]
pub enum EventParseError {
    #[fail(display="column parse error")]
    ColumnParseError(ColumnParseError),
    #[fail(display="unexpected EOF")]
    EofError,
}

impl From<ColumnParseError> for EventParseError {
    fn from(e: ColumnParseError) -> Self {
        EventParseError::ColumnParseError(e)
    }
}

#[derive(Debug, Fail)]
pub enum JsonbParseError {
    #[fail(display="invalid type byte")]
    InvalidTypeByte(u8),
    #[fail(display="invalid type literal byte")]
    InvalidLiteral(u16),
    #[fail(display="error parsing opaque column in json record: {:?}", inner)]
    OpaqueColumnParseError { inner: Box<ColumnParseError> },
}

impl From<ColumnParseError> for JsonbParseError {
    fn from(e: ColumnParseError) -> Self {
        JsonbParseError::OpaqueColumnParseError { inner: Box::new(e) }
    }
}


#[derive(Debug, Fail)]
pub enum ColumnParseError {
    #[fail(display="unimplemented column type: {:?}", column_type)]
    UnimplementedTypeError { column_type: column_types::ColumnType },
    #[fail(display="error parsing JSON column")]
    JsonError(JsonbParseError)
}

impl From<JsonbParseError> for ColumnParseError {
    fn from(e: JsonbParseError) -> Self {
        ColumnParseError::JsonError(e)
    }
}


#[derive(Debug, Fail)]
pub enum BinlogParseError {
    #[fail(display="error parsing event")]
    EventParseError(EventParseError),
    #[fail(display="bad magic value at start of binlog")]
    BadMagic([u8;4]),
    #[fail(display="bad first record in binlog")]
    BadFirstRecord,
}


impl From<EventParseError> for BinlogParseError {
    fn from(e: EventParseError) -> Self {
        BinlogParseError::EventParseError(e)
    }
}
