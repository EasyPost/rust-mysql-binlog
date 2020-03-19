use std::io::{self,Read};

use byteorder::{ReadBytesExt,LittleEndian, BigEndian};
use failure::Error;

use crate::value::MySQLValue;
use crate::packet_helpers::*;
use crate::jsonb;
use crate::errors::ColumnParseError;


#[derive(Debug, Eq, PartialEq, Clone)]
pub enum ColumnType {
    Decimal,
    Tiny,
    Short,
    Long,
    Float(u8),
    Double(u8),
    Null,
    Timestamp,
    LongLong,
    Int24,
    Date,
    Time,
    DateTime,
    Year,
    NewDate,
    Timestamp2(u8),
    DateTime2(u8),
    Time2(u8),
    VarChar(u16),
    Bit(u8, u8),
    NewDecimal(u8, u8),
    Enum(u16),
    Set(u16),
    TinyBlob,
    MediumBlob,
    LongBlob,
    Blob(u8),
    VarString,
    MyString,
    Geometry(u8),
    Json(u8),
}

impl ColumnType {
    pub(crate) fn from_byte(b: u8) -> Self {
        match b {
            0 => ColumnType::Decimal,
            1 => ColumnType::Tiny,
            2 => ColumnType::Short,
            3 => ColumnType::Long,
            4 => ColumnType::Float(0),
            5 => ColumnType::Double(0),
            6 => ColumnType::Null,
            7 => ColumnType::Timestamp,
            8 => ColumnType::LongLong,
            9 => ColumnType::Int24,
            10 => ColumnType::Date,
            11 => ColumnType::Time,
            12 => ColumnType::DateTime,
            13 => ColumnType::Year,
            14 => ColumnType::NewDate,   // not implemented (or documented)
            15 => ColumnType::VarChar(0),
            16 => ColumnType::Bit(0, 0),   // not implemented
            17 => ColumnType::Timestamp2(0),
            18 => ColumnType::DateTime2(0),
            19 => ColumnType::Time2(0),
            245 => ColumnType::Json(0),    // need to implement JsonB
            246 => ColumnType::NewDecimal(0, 0),
            247 => ColumnType::Enum(0),
            248 => ColumnType::Set(0),
            249 => ColumnType::TinyBlob,   // docs say this can't occur
            250 => ColumnType::MediumBlob,   // docs say this can't occur
            251 => ColumnType::LongBlob,    // docs say this can't occur
            252 => ColumnType::Blob(0),
            253 => ColumnType::VarString,   // not implemented
            254 => ColumnType::MyString,
            255 => ColumnType::Geometry(0),   // not implemented
            i => unimplemented!("unhandled column type {}", i),
        }
    }

    pub(crate) fn read_metadata<R: Read>(self, cursor: &mut R) -> Result<Self, io::Error> {
        Ok(match self {
            ColumnType::Float(_) => {
                let pack_length = cursor.read_u8()?;
                ColumnType::Float(pack_length)
            },
            ColumnType::Double(_) => {
                let pack_length = cursor.read_u8()?;
                ColumnType::Double(pack_length)
            },
            ColumnType::Blob(_) => {
                let pack_length = cursor.read_u8()?;
                ColumnType::Blob(pack_length)
            },
            ColumnType::Geometry(_) => {
                let pack_length = cursor.read_u8()?;
                ColumnType::Geometry(pack_length)
            },
            ColumnType::VarString | ColumnType::VarChar(_) => {
                let max_length = cursor.read_u16::<LittleEndian>()?;
                assert!(max_length != 0);
                ColumnType::VarChar(max_length)
            },
            ColumnType::Bit(..) => {
                unimplemented!()
            },
            ColumnType::NewDecimal(_, _) => {
                let precision = cursor.read_u8()?;
                let num_decimals = cursor.read_u8()?;
                ColumnType::NewDecimal(precision, num_decimals)
            },
            ColumnType::MyString => {
                // In Table_map_event, column type MYSQL_TYPE_STRING
                // can have the following real_type:
                // * MYSQL_TYPE_STRING (used for CHAR(n) and BINARY(n) SQL types with n <=255)
                // * MYSQL_TYPE_ENUM
                // * MYSQL_TYPE_SET
                let f1 = cursor.read_u8()?;
                let f2 = cursor.read_u8()?;
                let (real_type, max_length) = if f1 == 0 {
                    // not sure which version of mysql emits this,
                    // but log_event.cc checks this case
                    (ColumnType::MyString, f2 as u16)
                } else {
                    // The max length is in 0-1023,
                    // (since CHAR(255) CHARACTER SET utf8mb4 turns into max_length=1020)
                    // and the upper 4 bits of real_type are always set
                    // (in real_type = MYSQL_TYPE_ENUM, MYSQL_TYPE_SET, MYSQL_TYPE_STRING)
                    // So MySQL packs the upper bits of the length
                    // in the 0x30 bits of the type, inverted
                    let real_type = f1 | 0x30;
                    let max_length = (!f1 as u16) << 4 & 0x300 | f2 as u16;
                    (ColumnType::from_byte(real_type), max_length)
                };
                match real_type {
                    ColumnType::MyString => ColumnType::VarChar(max_length),
                    ColumnType::Set(_) => ColumnType::Set(max_length),
                    ColumnType::Enum(_) => ColumnType::Enum(max_length),
                    i => unimplemented!("unimplemented stringy type {:?}", i),
                }
            },
            ColumnType::Enum(_) => {
                let pack_length = cursor.read_u16::<LittleEndian>()?;
                ColumnType::Enum(pack_length)
            },
            ColumnType::DateTime2(..) => {
                ColumnType::DateTime2(cursor.read_u8()?)
            },
            ColumnType::Time2(..) => {
                ColumnType::Time2(cursor.read_u8()?)
            },
            ColumnType::Timestamp2(..)  => {
                ColumnType::Timestamp2(cursor.read_u8()?)
            },
            ColumnType::Json(..) => {
                ColumnType::Json(cursor.read_u8()?)
            }
            c => c,
        })
    }

    pub fn read_value<R: Read>(&self, r: &mut R) -> Result<MySQLValue, Error> {
        match self {
            &ColumnType::Tiny => {
                Ok(MySQLValue::SignedInteger(i64::from(r.read_i8()?)))
            },
            &ColumnType::Short => {
                Ok(MySQLValue::SignedInteger(i64::from(r.read_i16::<LittleEndian>()?)))
            },
            &ColumnType::Long => {
                Ok(MySQLValue::SignedInteger(i64::from(r.read_i32::<LittleEndian>()?)))
            },
            &ColumnType::Timestamp => {
                Ok(MySQLValue::Timestamp { unix_time: r.read_i32::<LittleEndian>()?, subsecond: 0 })
            },
            &ColumnType::LongLong => {
                Ok(MySQLValue::SignedInteger(r.read_i64::<LittleEndian>()?))
            },
            &ColumnType::Int24 => {
                let val = i64::from(read_int24(r)?);
                Ok(MySQLValue::SignedInteger(val))
            }
            &ColumnType::Null => {
                Ok(MySQLValue::Null)
            },
            &ColumnType::VarChar(max_len) => {
                // TODO: don't decode to String,
                // since type=real_type=MYSQL_TYPE_STRING is used for BINARY(n)
                // and type=MYSQL_TYPE_VARCHAR is used for VARBINARY(n)
                // and also the CHAR(n) and VARCHAR(n) encoding is not always utf-8
                let value = if max_len > 255 {
                    read_two_byte_length_prefixed_string(r)?
                } else {
                    read_one_byte_length_prefixed_string(r)?
                };
                Ok(MySQLValue::String(value))
            },
            &ColumnType::Year => {
                Ok(MySQLValue::Year(u32::from(r.read_u8()?) + 1900))
            }
            &ColumnType::Date => {
                let val = read_uint24(r)?;
                if val == 0 {
                    Ok(MySQLValue::Null)
                } else {
                    let year = (val & ((1 << 15) - 1) << 9) >> 9;
                    let month = (val & ((1 << 4) - 1) << 5) >> 5;
                    let day = val & ((1 << 5) - 1);
                    if year == 0 || month == 0 || day == 0 {
                        Ok(MySQLValue::Null)
                    } else {
                        Ok(MySQLValue::Date { year, month, day })
                    }
                }
            },
            &ColumnType::Time => {
                let val = read_uint24(r)?;
                let hours = val / 10000;
                let minutes = (val % 10000) / 100;
                let seconds = val % 100;
                Ok(MySQLValue::Time { hours, minutes, seconds, subseconds: 0 })
            },
            &ColumnType::DateTime => {
                let value = r.read_u64::<LittleEndian>()?;
                if value == 0 {
                    Ok(MySQLValue::Null)
                } else {
                    let date = value / 1000000;
                    let time = value % 1000000;
                    let year = (date / 10000) as u32;
                    let month = ((date % 10000) / 100) as u32;
                    let day = (date % 100) as u32;
                    let hour = (time / 10000) as u32;
                    let minute = ((time % 10000) / 100) as u32;
                    let second = (time % 100) as u32;
                    if year == 0 || month == 0 || day == 0 {
                        Ok(MySQLValue::Null)
                    } else {
                        Ok(MySQLValue::DateTime { year, month, day, hour, minute, second, subsecond: 0 })
                    }
                }
            }
            // the *2 functions are new in MySQL 5.6
            // docs are at
            // https://dev.mysql.com/doc/internals/en/date-and-time-data-type-representation.html
            &ColumnType::DateTime2(pack_length) => {
                let mut buf = [0u8; 5];
                r.read_exact(&mut buf)?;
                let subsecond = read_datetime_subsecond_part(r, pack_length)?;
                // one bit unused (sign, but always positive
                buf[0] &= 0x7f;
                // 17 bits of yearmonth (all of buf[0] and buf[1] and the top 2 bits of buf[2]
                let year_month: u32 = ((buf[2] as u32) >> 6) + ((buf[1] as u32) << 2) + ((buf[0] as u32) << 10);
                let year = year_month / 13;
                let month = year_month % 13;
                // 5 bits day (bits 3-7 of buf[2])
                let day = ((buf[2] & 0x3e) as u32) >> 1;
                // 5 bits hour (the last bit of buf[2] and the top 4 bits of buf[3]
                let hour = (((buf[3] & 0xf0) as u32) >> 4) + (((buf[2] & 0x01) as u32) << 4);
                // 6 bits minute (the bottom 4 bits of buf[3] and the top 2 bits of buf[4]
                let minute = (buf[4] >> 6) as u32 + (((buf[3] & 0x0f) as u32) << 2);
                // 6 bits second (the rest of buf[4])
                let second = (buf[4] & 0x3f) as u32;
                Ok(MySQLValue::DateTime { year, month, day, hour, minute, second, subsecond })
            },
            &ColumnType::Timestamp2(pack_length) => {
                let whole_part = r.read_i32::<BigEndian>()?;
                let frac_part = read_datetime_subsecond_part(r, pack_length)?;
                Ok(MySQLValue::Timestamp { unix_time: whole_part, subsecond: frac_part })
            },
            &ColumnType::Time2(pack_length) => {
                // one bit sign
                // one bit unused
                // 10 bits hour
                // 6 bits minute
                // 6 bits second
                let mut buf = [0u8; 3];
                r.read_exact(&mut buf)?;
                let hours = (((buf[0] & 0x3f) as u32) << 4) | (((buf[1] & 0xf0) as u32) >> 4);
                let minutes = (((buf[1] & 0x0f) as u32) << 2) | (((buf[2] & 0xb0) as u32) >> 6);
                let seconds = (buf[2] & 0x3f) as u32;
                let frac_part = read_datetime_subsecond_part(r, pack_length)?;
                Ok(MySQLValue::Time { hours, minutes, seconds, subseconds: frac_part })
            }
            &ColumnType::Blob(length_bytes) => {
                let val = read_var_byte_length_prefixed_bytes(r, length_bytes)?;
                Ok(MySQLValue::Blob(val.into()))
            },
            &ColumnType::Float(length) | &ColumnType::Double(length) => {
                if length == 4 {
                    Ok(MySQLValue::Float(r.read_f32::<LittleEndian>()?))
                } else if length == 8 {
                    Ok(MySQLValue::Double(r.read_f64::<LittleEndian>()?))
                } else {
                    unimplemented!("wtf is a {}-byte float?", length)
                }
            }
            &ColumnType::NewDecimal(precision, decimal_places) => {
                let body = read_new_decimal(r, precision, decimal_places)?;
                Ok(MySQLValue::Decimal(body))
            },
            &ColumnType::Enum(length_bytes) => {
                let enum_value = match (length_bytes & 0xff) as u8 {
                    0x01 => i16::from(r.read_i8()?),
                    0x02 => r.read_i16::<LittleEndian>()?,
                    i => unimplemented!("unhandled Enum pack_length {:?}", i),
                };
                Ok(MySQLValue::Enum(enum_value))
            },
            &ColumnType::Json(size) => {
                let body = read_var_byte_length_prefixed_bytes(r, size)?;
                Ok(MySQLValue::Json(jsonb::parse(body)?))
            },
            &ColumnType::TinyBlob | &ColumnType::MediumBlob | &ColumnType::LongBlob | &ColumnType::VarString | &ColumnType::MyString => {
                // the manual promises that these are never present in binlogs and are
                // not implemented by MySQL
                Err(ColumnParseError::UnimplementedTypeError { column_type: self.clone() }.into())
            },
            &ColumnType::Decimal | &ColumnType::NewDate | &ColumnType::Bit(..) | &ColumnType::Set(..) | &ColumnType::Geometry(..) => {
                unimplemented!("unhandled value type: {:?}", self);
            }
        }
    }
}
