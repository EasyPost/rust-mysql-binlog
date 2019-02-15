extern crate byteorder;
extern crate uuid;
extern crate base64;
#[macro_use] extern crate failure;
#[macro_use] extern crate serde;
#[macro_use] extern crate serde_derive;
#[macro_use] extern crate serde_json;

use std::io::{Read, Seek};
use std::path::Path;

pub mod binlog_file;
pub mod errors;
pub mod event;
mod bit_set;
pub mod column_types;
mod value;
pub mod table_map;
mod packet_helpers;
mod tell;
mod jsonb;

use event::EventData;

pub use event::TypeCode;


#[derive(Debug, Clone, Copy)]
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
pub struct BinlogEvent {
    type_code: event::TypeCode,
    timestamp: u32,
    gtid: Option<Gtid>,
    #[serde(skip_serializing_if = "Option::is_none")]
    schema_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    table_name: Option<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    rows: Vec<event::RowEvent>,
    #[serde(skip_serializing_if = "Option::is_none")]
    query: Option<String>,
}


struct EventIterator<BR: Read+Seek> {
    events: binlog_file::BinlogEvents<BR>,
    table_map: table_map::TableMap,
    current_gtid: Option<Gtid>,
}

impl<BR: Read+Seek> EventIterator<BR> {
    fn new(bf: binlog_file::BinlogFile<BR>) -> Self {
        EventIterator {
            events: bf.events(None),
            table_map: table_map::TableMap::new(),
            current_gtid: None,
        }
    }
}

impl<BR: Read+Seek> Iterator for EventIterator<BR> {
    type Item = Result<BinlogEvent, failure::Error>;

    fn next(&mut self) -> Option<Self::Item> {
        while let Some(event) = self.events.next() {
            println!("event: {:?}", event);
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


/// parse all events in a given Read instance
pub fn parse_reader<R: Read + Seek + 'static>(r: R) -> Result<impl Iterator<Item=Result<BinlogEvent, failure::Error>>, failure::Error> {
    let bf = binlog_file::BinlogFile::from_reader(r)?;
    Ok(EventIterator::new(bf))
}


/// parse all events in the file living at a given path
pub fn parse_file<P: AsRef<Path>>(file_name: P) -> Result<impl Iterator<Item=Result<BinlogEvent, failure::Error>>, failure::Error> {
    let bf = binlog_file::BinlogFile::try_from_path(file_name.as_ref())?;
    Ok(EventIterator::new(bf))
}


#[cfg(test)]
mod tests{
    use failure::Error;

    use super::{parse_reader, parse_file, TypeCode};

    #[test]
    fn test_parse_file() {
        let results = parse_file("test_data/bin-log.000001").unwrap().collect::<Result<Vec<_>, _>>().unwrap();
        assert_eq!(results.len(), 5);
        assert_eq!(results[0].type_code, TypeCode::QueryEvent);
        assert_eq!(results[0].query, Some("CREATE TABLE foo(id BIGINT AUTO_INCREMENT PRIMARY KEY, val_decimal DECIMAL(10, 5) NOT NULL, comment VARCHAR(255) NOT NULL)".to_owned()));
    }
}
