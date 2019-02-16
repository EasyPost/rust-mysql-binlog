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
//!     for event in mysql_binlog::parse_file("bin-log.000001").unwrap().events() {
//!         println!("{:?}", event.unwrap());
//!     }
//! }
//! ```
extern crate byteorder;
extern crate uuid;
extern crate base64;
#[macro_use] extern crate failure;
extern crate serde;
#[macro_use] extern crate serde_derive;
#[cfg_attr(test, macro_use)]
extern crate serde_json;

use std::fs::File;
use std::io::{Read, Seek};
use std::path::Path;

pub mod binlog_file;
pub mod errors;
pub mod event;
mod bit_set;
pub mod column_types;
pub mod value;
pub mod table_map;
mod packet_helpers;
mod tell;
mod jsonb;

use event::EventData;

pub use event::TypeCode;


#[derive(Debug, Clone, Copy)]
/// Global Transaction ID
pub struct Gtid(uuid::Uuid, u64);


impl serde::Serialize for Gtid {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where S: serde::Serializer
    {
        let serialized = format!("{}:{}", self.0.hyphenated(), self.1);
        serializer.serialize_str(&serialized)
    }
}


#[derive(Debug, Serialize)]
/// A binlog event as returned by [`EventIterator`]
pub struct BinlogEvent {
    pub type_code: event::TypeCode,
    // warning: Y2038 Problem ahead
    pub timestamp: u32,
    pub gtid: Option<Gtid>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub table_name: Option<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub rows: Vec<event::RowEvent>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub query: Option<String>,
}


/// Iterator over [`BinlogEvent`]s
pub struct EventIterator<BR: Read+Seek> {
    events: binlog_file::BinlogEvents<BR>,
    table_map: table_map::TableMap,
    current_gtid: Option<Gtid>,
}

impl<BR: Read+Seek> EventIterator<BR> {
    fn new(bf: binlog_file::BinlogFile<BR>, start_offset: Option<u64>) -> Self {
        EventIterator {
            events: bf.events(start_offset),
            table_map: table_map::TableMap::new(),
            current_gtid: None,
        }
    }
}

impl<BR: Read+Seek> Iterator for EventIterator<BR> {
    type Item = Result<BinlogEvent, failure::Error>;

    fn next(&mut self) -> Option<Self::Item> {
        while let Some(event) = self.events.next() {
            let event = match event {
                Ok(event) => event,
                Err(e) => return Some(Err(e))
            };
            match event.inner(Some(&self.table_map)) {
                Ok(Some(e)) => {
                    match e {
                        EventData::GtidLogEvent { uuid, coordinate, .. } => {
                            self.current_gtid = Some(Gtid(uuid, coordinate));
                        },
                        EventData::TableMapEvent { table_id, schema_name, table_name, columns, .. } => {
                            self.table_map.handle(table_id, schema_name, table_name, columns);
                        },
                        EventData::QueryEvent { query, .. } => {
                            return Some(Ok(BinlogEvent {
                                type_code: event.type_code(),
                                timestamp: event.timestamp(),
                                gtid: self.current_gtid,
                                table_name: None,
                                schema_name: None,
                                rows: Vec::new(),
                                query: Some(query)
                            }))
                        },
                        EventData::WriteRowsEvent { table_id, rows } | EventData::UpdateRowsEvent { table_id, rows } | EventData::DeleteRowsEvent { table_id, rows } => {
                            let maybe_table = self.table_map.get(table_id);
                            let message = BinlogEvent {
                                type_code: event.type_code(),
                                timestamp: event.timestamp(),
                                gtid: self.current_gtid,
                                table_name: maybe_table.as_ref().map(|a| a.table_name.to_owned()),
                                schema_name: maybe_table.as_ref().map(|a| a.schema_name.to_owned()),
                                rows,
                                query: None,
                            };
                            return Some(Ok(message))
                        },
                        u => {
                            eprintln!("unhandled event: {:?}", u);
                        }
                    }
                },
                Ok(None) => {
                    // this event doesn't have an inner type, which means we don't currently
                    // care about it. Example: PreviousGtidEvent
                },
                Err(e) => return Some(Err(e))
            }
        }
        None
    }
}


/// Builder to configure Binary Log reading
pub struct BinlogFileParserBuilder<BR: Read+Seek> {
    bf: binlog_file::BinlogFile<BR>,
    start_position: Option<u64>,
}

impl<BR: Read+Seek> BinlogFileParserBuilder<BR> {
    /// Set the start position to begin emitting events. NOTE: The beginning of the binlog will
    /// always be read first for the FDE. NOTE: Column mappings may be incorrect if you use this
    /// functionality, as TMEs may be missed.
    pub fn start_position(mut self, pos: u64) -> Self {
        self.start_position = Some(pos);
        self
    }

    /// Consume this builder, returning an iterator of [`BinlogEvent`] structs
    pub fn events(self) -> EventIterator<BR> {
        EventIterator::new(self.bf, self.start_position)
    }
}


/// Parse events from an object implementing the [`std::io::Read`] trait
///
/// ## Errors
///
/// - returns an immediate error if the Read does not begin with a valid Format Descriptor Event
/// - each call to the iterator can return an error if there is an I/O or parsing error
pub fn parse_reader<R: Read + Seek + 'static>(r: R) -> Result<BinlogFileParserBuilder<R>, failure::Error> {
    let bf = binlog_file::BinlogFile::from_reader(r)?;
    Ok(BinlogFileParserBuilder { bf: bf, start_position: None })
}


/// parse all events in the file living at a given path
///
/// ## Errors
///
/// - returns an immediate error if the file could not be opened or if it does not contain a valid Format Desciptor Event
/// - each call to the iterator can return an error if there is an I/O or parsing error
pub fn parse_file<P: AsRef<Path>>(file_name: P) -> Result<BinlogFileParserBuilder<File>, failure::Error> {
    let bf = binlog_file::BinlogFile::try_from_path(file_name.as_ref())?;
    Ok(BinlogFileParserBuilder { bf: bf, start_position: None })
}


#[cfg(test)]
mod tests{
    use super::{parse_file, parse_reader, TypeCode};

    #[test]
    fn test_parse_file() {
        let results = parse_file("test_data/bin-log.000001").unwrap().events().collect::<Result<Vec<_>, _>>().unwrap();
        assert_eq!(results.len(), 5);
        assert_eq!(results[0].type_code, TypeCode::QueryEvent);
        assert_eq!(results[0].query, Some("CREATE TABLE foo(id BIGINT AUTO_INCREMENT PRIMARY KEY, val_decimal DECIMAL(10, 5) NOT NULL, comment VARCHAR(255) NOT NULL)".to_owned()));
    }

    #[test]
    fn test_parse_reader() {
        let f = std::fs::File::open("test_data/bin-log.000001").unwrap();
        let results = parse_reader(f).unwrap().events().collect::<Result<Vec<_>, _>>().unwrap();
        assert_eq!(results.len(), 5);
        assert_eq!(results[0].type_code, TypeCode::QueryEvent);
        assert_eq!(results[0].query, Some("CREATE TABLE foo(id BIGINT AUTO_INCREMENT PRIMARY KEY, val_decimal DECIMAL(10, 5) NOT NULL, comment VARCHAR(255) NOT NULL)".to_owned()));
    }
}
