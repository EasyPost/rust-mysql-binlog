use std::fmt;
use std::io::{self, Cursor, ErrorKind, Read, Seek};

use byteorder::{ByteOrder, LittleEndian, ReadBytesExt};
use serde_derive::Serialize;
use uuid::Uuid;

use crate::bit_set::BitSet;
use crate::column_types::ColumnType;
use crate::errors::{ColumnParseError, EventParseError};
use crate::packet_helpers::*;
use crate::table_map::{SingleTableMap, TableMap};
use crate::tell::Tell;
use crate::value::MySQLValue;

#[derive(Debug, PartialEq, Eq, Clone, Copy, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum TypeCode {
    Unknown,
    StartEventV3,
    QueryEvent,
    StopEvent,
    RotateEvent,
    IntvarEvent,
    LoadEvent,
    SlaveEvent,
    CreateFileEvent,
    AppendBlockEvent,
    ExecLoadEvent,
    DeleteFileEvent,
    NewLoadEvent,
    RandEvent,
    UserVarEvent,
    FormatDescriptionEvent,
    XidEvent,
    BeginLoadQueryEvent,
    ExecuteLoadQueryEvent,
    TableMapEvent,
    PreGaWriteRowsEvent,
    PreGaUpdateRowsEvent,
    PreGaDeleteRowsEvent,
    WriteRowsEventV1,
    UpdateRowsEventV1,
    DeleteRowsEventV1,
    IncidentEvent,
    HeartbeatLogEvent,
    IgnorableLogEvent,
    RowsQueryLogEvent,
    WriteRowsEventV2,
    UpdateRowsEventV2,
    DeleteRowsEventV2,
    GtidLogEvent,
    AnonymousGtidLogEvent,
    PreviousGtidsLogEvent,
    OtherUnknown(u8),
}

impl TypeCode {
    fn from_byte(b: u8) -> Self {
        match b {
            0 => TypeCode::Unknown,
            1 => TypeCode::StartEventV3,
            2 => TypeCode::QueryEvent,
            3 => TypeCode::StopEvent,
            4 => TypeCode::RotateEvent,
            5 => TypeCode::IntvarEvent,
            6 => TypeCode::LoadEvent,
            7 => TypeCode::SlaveEvent,
            8 => TypeCode::CreateFileEvent,
            9 => TypeCode::AppendBlockEvent,
            10 => TypeCode::ExecLoadEvent,
            11 => TypeCode::DeleteFileEvent,
            12 => TypeCode::NewLoadEvent,
            13 => TypeCode::RandEvent,
            14 => TypeCode::UserVarEvent,
            15 => TypeCode::FormatDescriptionEvent,
            16 => TypeCode::XidEvent,
            17 => TypeCode::BeginLoadQueryEvent,
            18 => TypeCode::ExecuteLoadQueryEvent,
            19 => TypeCode::TableMapEvent,
            20 => TypeCode::PreGaWriteRowsEvent,
            21 => TypeCode::PreGaUpdateRowsEvent,
            22 => TypeCode::PreGaDeleteRowsEvent,
            23 => TypeCode::WriteRowsEventV1,
            24 => TypeCode::UpdateRowsEventV1,
            25 => TypeCode::DeleteRowsEventV1,
            26 => TypeCode::IncidentEvent,
            27 => TypeCode::HeartbeatLogEvent,
            28 => TypeCode::IgnorableLogEvent,
            29 => TypeCode::RowsQueryLogEvent,
            30 => TypeCode::WriteRowsEventV2,
            31 => TypeCode::UpdateRowsEventV2,
            32 => TypeCode::DeleteRowsEventV2,
            33 => TypeCode::GtidLogEvent,
            34 => TypeCode::AnonymousGtidLogEvent,
            35 => TypeCode::PreviousGtidsLogEvent,
            i => TypeCode::OtherUnknown(i),
        }
    }
}

#[derive(Debug, Serialize)]
pub enum ChecksumAlgorithm {
    None,
    CRC32,
    Other(u8),
}

impl From<u8> for ChecksumAlgorithm {
    fn from(byte: u8) -> Self {
        match byte {
            0x00 => ChecksumAlgorithm::None,
            0x01 => ChecksumAlgorithm::CRC32,
            other => ChecksumAlgorithm::Other(other),
        }
    }
}

pub type RowData = Vec<Option<MySQLValue>>;

#[derive(Debug)]
pub enum EventData {
    GtidLogEvent {
        flags: u8,
        uuid: Uuid,
        coordinate: u64,
        last_committed: Option<u64>,
        sequence_number: Option<u64>,
    },
    QueryEvent {
        thread_id: u32,
        exec_time: u32,
        error_code: i16,
        schema: String,
        query: String,
    },
    FormatDescriptionEvent {
        binlog_version: u16,
        server_version: String,
        create_timestamp: u32,
        common_header_len: u8,
        checksum_algorithm: ChecksumAlgorithm,
    },
    TableMapEvent {
        table_id: u64,
        schema_name: String,
        table_name: String,
        columns: Vec<ColumnType>,
        null_bitmap: BitSet,
    },
    WriteRowsEvent {
        table_id: u64,
        rows: Vec<RowEvent>,
    },
    UpdateRowsEvent {
        table_id: u64,
        rows: Vec<RowEvent>,
    },
    DeleteRowsEvent {
        table_id: u64,
        rows: Vec<RowEvent>,
    },
}

struct RowsEvent {
    table_id: u64,
    rows: Vec<RowEvent>,
}

fn parse_one_row<R: Read + Seek>(
    mut cursor: &mut R,
    this_table_map: &SingleTableMap,
    present_bitmask: &BitSet,
) -> Result<RowData, ColumnParseError> {
    let num_set_columns = present_bitmask.bits_set();
    let null_bitmask_size = (num_set_columns + 7) >> 3;
    let mut row = Vec::with_capacity(this_table_map.columns.len());
    let null_bitmask = BitSet::from_slice(
        num_set_columns,
        &read_nbytes(&mut cursor, null_bitmask_size)?,
    )
    .unwrap();
    let mut null_index = 0;
    for (i, column_definition) in this_table_map.columns.iter().enumerate() {
        if !present_bitmask.is_set(i) {
            row.push(None);
            continue;
        }
        let is_null = null_bitmask.is_set(null_index);
        let val = if is_null {
            MySQLValue::Null
        } else {
            //println!("parsing column {} ({:?})", i, column_definition);
            column_definition.read_value(&mut cursor)?
        };
        row.push(Some(val));
        null_index += 1;
    }
    //println!("finished row: {:?}", row);
    Ok(row)
}

#[derive(Debug, Serialize)]
#[serde(untagged)]
pub enum RowEvent {
    NewRow {
        cols: RowData,
    },
    DeletedRow {
        cols: RowData,
    },
    UpdatedRow {
        before_cols: RowData,
        after_cols: RowData,
    },
}

impl RowEvent {
    pub fn cols(&self) -> Option<&RowData> {
        match self {
            RowEvent::NewRow { cols } => Some(cols),
            RowEvent::DeletedRow { cols } => Some(cols),
            RowEvent::UpdatedRow { .. } => None,
        }
    }
}

fn parse_rows_event<R: Read + Seek>(
    type_code: TypeCode,
    data_len: usize,
    mut cursor: &mut R,
    table_map: Option<&TableMap>,
) -> Result<RowsEvent, ColumnParseError> {
    let mut table_id_buf = [0u8; 8];
    cursor.read_exact(&mut table_id_buf[0..6])?;
    let table_id = LittleEndian::read_u64(&table_id_buf);
    // two-byte reserved value
    cursor.seek(io::SeekFrom::Current(2))?;
    match type_code {
        TypeCode::WriteRowsEventV2 | TypeCode::UpdateRowsEventV2 | TypeCode::DeleteRowsEventV2 => {
            let _ = cursor.read_i16::<LittleEndian>()?;
        }
        _ => {}
    }
    let num_columns = read_variable_length_integer(&mut cursor)? as usize;
    let bitmask_size = (num_columns + 7) >> 3;
    let before_column_bitmask =
        BitSet::from_slice(num_columns, &read_nbytes(&mut cursor, bitmask_size)?).unwrap();
    let after_column_bitmask = match type_code {
        TypeCode::UpdateRowsEventV1 | TypeCode::UpdateRowsEventV2 => {
            Some(BitSet::from_slice(num_columns, &read_nbytes(&mut cursor, bitmask_size)?).unwrap())
        }
        _ => None,
    };
    let mut rows = Vec::with_capacity(1);
    if let Some(table_map) = table_map {
        if let Some(this_table_map) = table_map.get(table_id) {
            loop {
                let pos = cursor.tell()? as usize;
                if data_len - pos < 1 {
                    break;
                }
                match type_code {
                    TypeCode::WriteRowsEventV1 | TypeCode::WriteRowsEventV2 => {
                        rows.push(RowEvent::NewRow {
                            cols: parse_one_row(
                                &mut cursor,
                                this_table_map,
                                &before_column_bitmask,
                            )?,
                        });
                    }
                    TypeCode::UpdateRowsEventV1 | TypeCode::UpdateRowsEventV2 => {
                        rows.push(RowEvent::UpdatedRow {
                            before_cols: parse_one_row(
                                &mut cursor,
                                this_table_map,
                                &before_column_bitmask,
                            )?,
                            after_cols: parse_one_row(
                                &mut cursor,
                                this_table_map,
                                after_column_bitmask.as_ref().unwrap(),
                            )?,
                        })
                    }
                    TypeCode::DeleteRowsEventV1 | TypeCode::DeleteRowsEventV2 => {
                        rows.push(RowEvent::DeletedRow {
                            cols: parse_one_row(
                                &mut cursor,
                                this_table_map,
                                &before_column_bitmask,
                            )?,
                        });
                    }
                    _ => unimplemented!(),
                }
            }
        }
    }
    Ok(RowsEvent { table_id, rows })
}

impl EventData {
    fn from_data(
        type_code: TypeCode,
        data: &[u8],
        table_map: Option<&TableMap>,
    ) -> Result<Option<Self>, EventParseError> {
        let mut cursor = Cursor::new(data);
        match type_code {
            TypeCode::FormatDescriptionEvent => {
                let binlog_version = cursor.read_u16::<LittleEndian>()?;
                if binlog_version != 4 {
                    unimplemented!("can only parse a version 4 binary log");
                }
                let mut server_version_buf = [0u8; 50];
                cursor.read_exact(&mut server_version_buf)?;
                let server_version = ::std::str::from_utf8(
                    server_version_buf
                        .split(|c| *c == 0x00)
                        .next()
                        .unwrap_or(&[]),
                )
                .unwrap()
                .to_owned();
                let create_timestamp = cursor.read_u32::<LittleEndian>()?;
                let common_header_len = cursor.read_u8()?;
                let event_types = data.len() - 2 - 50 - 4 - 1 - 5;
                let mut event_sizes_tables = vec![0u8; event_types];
                cursor.read_exact(&mut event_sizes_tables)?;
                let checksum_algo = ChecksumAlgorithm::from(cursor.read_u8()?);
                let mut checksum_buf = [0u8; 4];
                cursor.read_exact(&mut checksum_buf)?;
                Ok(Some(EventData::FormatDescriptionEvent {
                    binlog_version,
                    server_version,
                    create_timestamp,
                    common_header_len,
                    checksum_algorithm: checksum_algo,
                }))
            }
            TypeCode::GtidLogEvent => {
                let flags = cursor.read_u8()?;
                let mut uuid_buf = [0u8; 16];
                cursor.read_exact(&mut uuid_buf)?;
                let uuid = Uuid::from_slice(&uuid_buf)?;
                let offset = cursor.read_u64::<LittleEndian>()?;
                let (last_committed, sequence_number) = match cursor.read_u8() {
                    Ok(0x02) => {
                        let last_committed = cursor.read_u64::<LittleEndian>()?;
                        let sequence_number = cursor.read_u64::<LittleEndian>()?;
                        (Some(last_committed), Some(sequence_number))
                    }
                    _ => (None, None),
                };
                Ok(Some(EventData::GtidLogEvent {
                    flags,
                    uuid,
                    coordinate: offset,
                    last_committed,
                    sequence_number,
                }))
            }
            TypeCode::QueryEvent => {
                let thread_id = cursor.read_u32::<LittleEndian>()?;
                let execution_time = cursor.read_u32::<LittleEndian>()?;
                let schema_len = cursor.read_u8()?;
                let error_code = cursor.read_i16::<LittleEndian>()?;
                let _status_vars = read_two_byte_length_prefixed_bytes(&mut cursor)?;
                let schema =
                    String::from_utf8_lossy(&read_nbytes(&mut cursor, schema_len)?).into_owned();
                cursor.seek(io::SeekFrom::Current(1))?;
                let mut statement = String::new();
                cursor.read_to_string(&mut statement)?;
                Ok(Some(EventData::QueryEvent {
                    thread_id,
                    exec_time: execution_time,
                    error_code,
                    schema,
                    query: statement,
                }))
            }
            TypeCode::TableMapEvent => {
                let mut table_id_buf = [0u8; 8];
                cursor.read_exact(&mut table_id_buf[0..6])?;
                let table_id = LittleEndian::read_u64(&table_id_buf);
                // two-byte reserved value
                cursor.seek(io::SeekFrom::Current(2))?;
                let schema_name = read_one_byte_length_prefixed_string(&mut cursor)?;
                // nul byte
                cursor.seek(io::SeekFrom::Current(1))?;
                let table_name = read_one_byte_length_prefixed_string(&mut cursor)?;
                // nul byte
                cursor.seek(io::SeekFrom::Current(1))?;
                //println!("parsing table map for {}.{}", schema_name, table_name);
                let column_count = read_variable_length_integer(&mut cursor)? as usize;
                let mut columns = Vec::with_capacity(column_count);
                for _ in 0..column_count {
                    let column_type = ColumnType::from_byte(cursor.read_u8()?);
                    columns.push(column_type);
                }
                //let pos = cursor.tell()? as usize;
                //println!("column types: {:?}", columns);
                //println!("top of metadata: remaining table map data: {:?}", &data[pos..]);
                let _metadata_length = read_variable_length_integer(&mut cursor)? as usize;
                let final_columns = columns
                    .into_iter()
                    .map(|c| c.read_metadata(&mut cursor))
                    .collect::<Result<Vec<_>, _>>()?;
                //println!("finished decoding metadata; columns: {:?}", final_columns);
                //let end_of_map_pos = cursor.seek(io::SeekFrom::Current(0))? as usize;
                let num_columns = final_columns.len();
                let null_bitmask_size = (num_columns + 7) >> 3;
                let null_bitmap_source = read_nbytes(&mut cursor, null_bitmask_size)?;
                let nullable_bitmap = BitSet::from_slice(num_columns, &null_bitmap_source).unwrap();
                Ok(Some(EventData::TableMapEvent {
                    table_id,
                    schema_name,
                    table_name,
                    columns: final_columns,
                    null_bitmap: nullable_bitmap,
                }))
            }
            TypeCode::WriteRowsEventV1 | TypeCode::WriteRowsEventV2 => {
                let ev = parse_rows_event(type_code, data.len(), &mut cursor, table_map)?;
                Ok(Some(EventData::WriteRowsEvent {
                    table_id: ev.table_id,
                    rows: ev.rows,
                }))
            }
            TypeCode::UpdateRowsEventV1 | TypeCode::UpdateRowsEventV2 => {
                let ev = parse_rows_event(type_code, data.len(), &mut cursor, table_map)?;
                Ok(Some(EventData::UpdateRowsEvent {
                    table_id: ev.table_id,
                    rows: ev.rows,
                }))
            }
            TypeCode::DeleteRowsEventV1 | TypeCode::DeleteRowsEventV2 => {
                let ev = parse_rows_event(type_code, data.len(), &mut cursor, table_map)?;
                Ok(Some(EventData::DeleteRowsEvent {
                    table_id: ev.table_id,
                    rows: ev.rows,
                }))
            }
            _ => Ok(None),
        }
    }
}

pub struct Event {
    timestamp: u32,
    type_code: TypeCode,
    server_id: u32,
    event_length: u32,
    next_position: u32,
    flags: u16,
    data: Vec<u8>,
    offset: u64,
}

impl fmt::Debug for Event {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Event {{ timestamp: {:?}, type_code: {:?}, server_id: {:?}, data_len: {:?}, offset: {:?} }}", self.timestamp, self.type_code, self.server_id, self.data.len(), self.offset)
    }
}

// TODO: determine this by examining the server version
const HAS_CHECKSUM: bool = true;

impl Event {
    pub fn read<R: Read>(reader: &mut R, offset: u64) -> Result<Self, EventParseError> {
        let mut header = [0u8; 19];
        match reader.read_exact(&mut header) {
            Ok(_) => {}
            Err(ref e) if e.kind() == ErrorKind::UnexpectedEof => {
                return Err(EventParseError::EofError)
            }
            Err(e) => return Err(e.into()),
        }
        let mut c = Cursor::new(header);
        let timestamp = c.read_u32::<LittleEndian>()?;
        let type_code = TypeCode::from_byte(c.read_u8()?);
        let server_id = c.read_u32::<LittleEndian>()?;
        let event_length = c.read_u32::<LittleEndian>()?;
        let next_position = c.read_u32::<LittleEndian>()?;
        let flags = c.read_u16::<LittleEndian>()?;
        let mut data_length: usize = (event_length - 19) as usize;
        if HAS_CHECKSUM {
            data_length -= 4;
        }
        //println!("finished reading event header with type_code {:?} event_length {} and next_position {}", type_code, event_length, next_position);
        let mut data = vec![0u8; data_length];
        reader.read_exact(&mut data)?;
        //println!("finished reading body");
        Ok(Event {
            timestamp,
            type_code,
            server_id,
            event_length,
            next_position,
            flags,
            data,
            offset,
        })
    }

    pub fn type_code(&self) -> TypeCode {
        self.type_code
    }

    pub fn timestamp(&self) -> u32 {
        self.timestamp
    }

    pub fn next_position(&self) -> u64 {
        u64::from(self.next_position)
    }

    pub fn inner(
        &self,
        table_map: Option<&TableMap>,
    ) -> Result<Option<EventData>, EventParseError> {
        EventData::from_data(self.type_code, &self.data, table_map).map_err(Into::into)
    }

    pub fn data(&self) -> &Vec<u8> {
        &self.data
    }

    pub fn flags(&self) -> u16 {
        self.flags
    }

    pub fn event_length(&self) -> u32 {
        self.event_length
    }

    pub fn offset(&self) -> u64 {
        self.offset
    }
}
