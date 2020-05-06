//! Parser for the MySQL binary log format.
//!
//! # Limitations
//!
//! - Targets Percona and Oracle MySQL 5.6 and 5.7. Has not been tested with MariaDB, MySQL 8.0, or older versions of MySQL
//! - Like all 5.6/5.7 MySQL implementations, UNSIGNED BIGINT cannot safely represent numbers between `2^63` and `2^64` because `i64` is used internally for all integral data types
//!
//! # Example
//!
//! A simple command line event parser and printer
//!
//! ```no_run
//! fn main() {
//!     for event in mysql_binlog::parse_file("bin-log.000001").unwrap() {
//!         println!("{:?}", event.unwrap());
//!     }
//! }
//! ```

use std::fs::File;
use std::io::{Read, Seek};
use std::path::Path;

pub mod binlog_file;
mod bit_set;
pub mod column_types;
pub mod errors;
pub mod event;
mod jsonb;
mod packet_helpers;
pub mod table_map;
mod tell;
pub mod value;

use event::EventData;
use serde_derive::Serialize;

use errors::{BinlogParseError, EventParseError};

#[derive(Debug, Clone, Copy)]
/// Global Transaction ID
pub struct Gtid(uuid::Uuid, u64);

impl serde::Serialize for Gtid {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let serialized = format!("{}:{}", self.0.to_hyphenated(), self.1);
        serializer.serialize_str(&serialized)
    }
}

impl ToString for Gtid {
    fn to_string(&self) -> String {
        format!("{}:{}", self.0.to_hyphenated(), self.1)
    }
}

#[derive(Debug, Clone, Copy, Serialize)]
pub struct LogicalTimestamp {
    last_committed: u64,
    sequence_number: u64,
}

#[derive(Debug, Serialize)]
/// A binlog event as returned by [`EventIterator`]. Filters out internal events
/// like the TableMapEvent and simplifies mapping GTIDs to individual events.
pub struct BinlogEvent {
    pub type_code: event::TypeCode,
    // warning: Y2038 Problem ahead
    pub timestamp: u32,
    pub gtid: Option<Gtid>,
    pub logical_timestamp: Option<LogicalTimestamp>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub table_name: Option<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub rows: Vec<event::RowEvent>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub query: Option<String>,
    pub offset: u64,
}

/// Iterator over [`BinlogEvent`]s
pub struct EventIterator<BR: Read + Seek> {
    events: binlog_file::BinlogEvents<BR>,
    table_map: table_map::TableMap,
    current_gtid: Option<Gtid>,
    logical_timestamp: Option<LogicalTimestamp>,
}

impl<BR: Read + Seek> EventIterator<BR> {
    fn new(bf: binlog_file::BinlogFile<BR>, start_offset: Option<u64>) -> Self {
        EventIterator {
            events: bf.events(start_offset),
            table_map: table_map::TableMap::new(),
            current_gtid: None,
            logical_timestamp: None,
        }
    }
}

impl<BR: Read + Seek> Iterator for EventIterator<BR> {
    type Item = Result<BinlogEvent, EventParseError>;

    fn next(&mut self) -> Option<Self::Item> {
        while let Some(event) = self.events.next() {
            let event = match event {
                Ok(event) => event,
                Err(e) => return Some(Err(e)),
            };
            let offset = event.offset();
            match event.inner(Some(&self.table_map)) {
                Ok(Some(e)) => match e {
                    EventData::GtidLogEvent {
                        uuid,
                        coordinate,
                        last_committed,
                        sequence_number,
                        ..
                    } => {
                        self.current_gtid = Some(Gtid(uuid, coordinate));
                        if let (Some(last_committed), Some(sequence_number)) =
                            (last_committed, sequence_number)
                        {
                            self.logical_timestamp = Some(LogicalTimestamp {
                                last_committed,
                                sequence_number,
                            });
                        } else {
                            self.logical_timestamp = None;
                        }
                    }
                    EventData::TableMapEvent {
                        table_id,
                        schema_name,
                        table_name,
                        columns,
                        ..
                    } => {
                        self.table_map
                            .handle(table_id, schema_name, table_name, columns);
                    }
                    EventData::QueryEvent { query, .. } => {
                        return Some(Ok(BinlogEvent {
                            offset,
                            type_code: event.type_code(),
                            timestamp: event.timestamp(),
                            gtid: self.current_gtid,
                            logical_timestamp: self.logical_timestamp,
                            table_name: None,
                            schema_name: None,
                            rows: Vec::new(),
                            query: Some(query),
                        }))
                    }
                    EventData::WriteRowsEvent { table_id, rows }
                    | EventData::UpdateRowsEvent { table_id, rows }
                    | EventData::DeleteRowsEvent { table_id, rows } => {
                        let maybe_table = self.table_map.get(table_id);
                        let message = BinlogEvent {
                            offset,
                            type_code: event.type_code(),
                            timestamp: event.timestamp(),
                            gtid: self.current_gtid,
                            logical_timestamp: self.logical_timestamp,
                            table_name: maybe_table.as_ref().map(|a| a.table_name.to_owned()),
                            schema_name: maybe_table.as_ref().map(|a| a.schema_name.to_owned()),
                            rows,
                            query: None,
                        };
                        return Some(Ok(message));
                    }
                    u => {
                        eprintln!("unhandled event: {:?}", u);
                    }
                },
                Ok(None) => {
                    // this event doesn't have an inner type, which means we don't currently
                    // care about it. Example: PreviousGtidEvent
                }
                Err(e) => return Some(Err(e)),
            }
        }
        None
    }
}

/// Builder to configure Binary Log reading
pub struct BinlogFileParserBuilder<BR: Read + Seek> {
    bf: binlog_file::BinlogFile<BR>,
    start_position: Option<u64>,
}

impl BinlogFileParserBuilder<File> {
    /// Construct a new BinlogFileParserBuilder from some path
    pub fn try_from_path<P: AsRef<Path>>(file_name: P) -> Result<Self, BinlogParseError> {
        let bf = binlog_file::BinlogFile::try_from_path(file_name.as_ref())?;
        Ok(BinlogFileParserBuilder {
            bf: bf,
            start_position: None,
        })
    }
}

impl<BR: Read + Seek> BinlogFileParserBuilder<BR> {
    /// Construct a new BinlogFileParserBuilder from some object implementing Read and Seek
    pub fn try_from_reader(r: BR) -> Result<Self, BinlogParseError> {
        let bf = binlog_file::BinlogFile::try_from_reader(r)?;
        Ok(BinlogFileParserBuilder {
            bf: bf,
            start_position: None,
        })
    }

    /// Set the start position to begin emitting events. NOTE: The beginning of the binlog will
    /// always be read first for the FDE. NOTE: Column mappings may be incorrect if you use this
    /// functionality, as TMEs may be missed.
    pub fn start_position(mut self, pos: u64) -> Self {
        self.start_position = Some(pos);
        self
    }

    /// Consume this builder, returning an iterator of [`BinlogEvent`] structs
    pub fn build(self) -> EventIterator<BR> {
        EventIterator::new(self.bf, self.start_position)
    }
}

/// Parse events from an object implementing the [`std::io::Read`] trait
///
/// ## Errors
///
/// - returns an immediate error if the Read does not begin with a valid Format Descriptor Event
/// - each call to the iterator can return an error if there is an I/O or parsing error
pub fn parse_reader<R: Read + Seek + 'static>(r: R) -> Result<EventIterator<R>, BinlogParseError> {
    BinlogFileParserBuilder::try_from_reader(r).map(|b| b.build())
}

/// parse all events in the file living at a given path
///
/// ## Errors
///
/// - returns an immediate error if the file could not be opened or if it does not contain a valid Format Desciptor Event
/// - each call to the iterator can return an error if there is an I/O or parsing error
pub fn parse_file<P: AsRef<Path>>(file_name: P) -> Result<EventIterator<File>, BinlogParseError> {
    BinlogFileParserBuilder::try_from_path(file_name).map(|b| b.build())
}

#[cfg(test)]
mod tests {
    use assert_matches::assert_matches;

    use bigdecimal::BigDecimal;

    use super::{parse_file, parse_reader};
    use crate::event::TypeCode;
    use crate::value::MySQLValue;

    #[test]
    fn test_parse_file() {
        let results = parse_file("test_data/bin-log.000001")
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(results.len(), 5);
        assert_eq!(results[0].type_code, TypeCode::QueryEvent);
        assert_eq!(results[0].query, Some("CREATE TABLE foo(id BIGINT AUTO_INCREMENT PRIMARY KEY, val_decimal DECIMAL(10, 5) NOT NULL, comment VARCHAR(255) NOT NULL)".to_owned()));
        assert_eq!(results[2].timestamp, 1550192291);
        assert_eq!(
            results[2].gtid.unwrap().to_string(),
            "87cee3a4-6b31-11e7-bdfd-0d98d6698870:14918"
        );
        assert_eq!(
            results[2].schema_name.as_ref().map(|s| s.as_str()),
            Some("bltest")
        );
        assert_eq!(
            results[2].table_name.as_ref().map(|s| s.as_str()),
            Some("foo")
        );
        let cols = results[2].rows[0].cols().unwrap();
        assert_matches!(cols[0], Some(MySQLValue::SignedInteger(1)));
        assert_matches!(cols[1], Some(MySQLValue::Decimal(_)));
        if let Some(MySQLValue::Decimal(ref d)) = cols[1] {
            assert_eq!(*d, "0.1".parse::<BigDecimal>().unwrap());
        }
        assert_matches!(cols[2], Some(MySQLValue::String(_)));
    }

    #[test]
    fn test_parse_reader() {
        let f = std::fs::File::open("test_data/bin-log.000001").unwrap();
        let results = parse_reader(f)
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(results.len(), 5);
        assert_eq!(results[0].type_code, TypeCode::QueryEvent);
        assert_eq!(results[0].query, Some("CREATE TABLE foo(id BIGINT AUTO_INCREMENT PRIMARY KEY, val_decimal DECIMAL(10, 5) NOT NULL, comment VARCHAR(255) NOT NULL)".to_owned()));
    }
}
