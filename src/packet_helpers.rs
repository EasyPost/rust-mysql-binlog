use std::io::{self,Read,Cursor};

use bigdecimal::BigDecimal;
use byteorder::{ByteOrder,ReadBytesExt,LittleEndian,BigEndian};

// This module contains miscellaneous shitty functions for reading various
// MySQL data types out of a packet (or, well, a Read).
//
// It's all garbage all the way down.

pub(crate) fn read_variable_length_integer<R: Read>(r: &mut R) -> io::Result<i64> {
    let first = r.read_u8()?;
    if first < 0xfb {
        Ok(i64::from(first as i8))
    } else if first == 0xfc {
        Ok(i64::from(r.read_i16::<LittleEndian>()?))
    } else if first == 0xfd {
        // why are there three byte integers fucking mysql
        let mut buf = [0u8; 4];
        r.read_exact(&mut buf[0..3])?;
        // TODO: sign-extend to fill that top byte
        Ok(i64::from(LittleEndian::read_i32(&buf)))
    } else if first == 0xfe {
        r.read_i64::<LittleEndian>()
    } else {
        unreachable!();
    }
}

pub(crate) fn read_known_length_integer_be<R: Read>(r: &mut R, bytes: usize) -> io::Result<i64> {
    Ok(match bytes {
        1 => i64::from(r.read_i8()?),
        2 => i64::from(r.read_i16::<BigEndian>()?),
        3 => {
            let mut buf = [0u8; 3];
            r.read_exact(&mut buf)?;
            let is_neg = buf[0] & 0x80 != 0;
            buf[0] &= 0x7f;
            let num: i64 = (i64::from(buf[0]) << 16) | (i64::from(buf[1]) << 8) | i64::from(buf[2]);
            if is_neg {
                -1 * num
            } else {
                num
            }
        },
        4 => i64::from(r.read_i32::<BigEndian>()?),
        _ => unimplemented!(),
    })
}

pub(crate) fn read_uint24<R: Read>(r: &mut R) -> io::Result<u32> {
    let mut buf = [0u8; 4];
    r.read_exact(&mut buf[0..3])?;
    Ok(LittleEndian::read_u32(&buf))
}


pub(crate) fn read_int24<R: Read>(r: &mut R) -> io::Result<i32> {
    let mut buf = [0u8; 4];
    r.read_exact(&mut buf[0..3])?;
    Ok(LittleEndian::read_i32(&buf))
}

pub(crate) fn read_one_byte_length_prefixed_bytes<R: Read>(r: &mut R) -> io::Result<Vec<u8>> {
    let length = r.read_u8()?;
    read_nbytes(r, length)
}

pub(crate) fn read_two_byte_length_prefixed_bytes<R: Read>(r: &mut R) -> io::Result<Vec<u8>> {
    let length = r.read_u16::<LittleEndian>()? as usize;
    read_nbytes(r, length)
}

pub(crate) fn read_var_byte_length_prefixed_bytes<R: Read>(r: &mut R, pl: u8) -> io::Result<Vec<u8>> {
    let len = match pl {
        1 => r.read_u8()? as usize,  // tinytext, tinyblob
        2 => r.read_u16::<LittleEndian>()? as usize,  // text, blob
        3 => r.read_u24::<LittleEndian>()? as usize,  // mediumtext, mediumblob
        4 => r.read_u32::<LittleEndian>()? as usize,  // longtext, longblob, json
        i => unreachable!("Unexpected read_var_byte_length_prefixed_bytes {}", i)
    };
    read_nbytes(r, len)
}


pub(crate) fn read_one_byte_length_prefixed_string<R: Read>(r: &mut R) -> io::Result<String> {
    let buf = read_one_byte_length_prefixed_bytes(r)?;
    Ok(String::from_utf8_lossy(&buf).into_owned())
}


pub(crate) fn read_two_byte_length_prefixed_string<R: Read>(r: &mut R) -> io::Result<String> {
    let buf = read_two_byte_length_prefixed_bytes(r)?;
    Ok(String::from_utf8_lossy(&buf).into_owned())
}

pub(crate) fn read_nbytes<R: Read, S: Into<usize>>(r: &mut R, desired_bytes: S) -> io::Result<Vec<u8>> {
    let mut into = vec![0u8; desired_bytes.into()];
    r.read_exact(&mut into)?;
    Ok(into)
}

pub(crate) fn read_variable_length_bytes<R: Read>(r: &mut R) -> io::Result<Vec<u8>> {
    let mut byte = 0x80;
    let mut length = 0usize;
    let mut shbits = 0u32;
    while byte & 0x80 != 0 {
        byte = r.read_u8()?;
        length |= ((byte & 0x7f) as usize) << shbits;
        shbits += 7;
        if shbits >= 57 {
            panic!("illegal shift, shbits={}", shbits);
        }
    }
    read_nbytes(r, length)
}

pub(crate) fn read_variable_length_string<R: Read>(r: &mut R) -> io::Result<String> {
    let buf = read_variable_length_bytes(r)?;
    Ok(String::from_utf8_lossy(&buf).into_owned())
}

const DECIMAL_DIGITS_PER_INTEGER: u8 = 9;

pub(crate) fn read_new_decimal<R: Read>(r: &mut R, precision: u8, decimal: u8) -> Result<BigDecimal, failure::Error> {
    // like every other binlog parser's implementation, this code
    // is a transliteration of https://github.com/jeremycole/mysql_binlog/blob/master/lib/mysql_binlog/binlog_field_parser.rb#L233
    // because this format is bananas
    let compressed_byte_map = [0usize, 1, 1, 2, 2, 3, 3, 4, 4, 4];
    let integral = precision - decimal;
    let uncompressed_integers: usize = (integral / DECIMAL_DIGITS_PER_INTEGER).into();
    let uncompressed_decimals: usize = (decimal / DECIMAL_DIGITS_PER_INTEGER).into();
    let compressed_integers: usize = integral as usize - (uncompressed_integers * DECIMAL_DIGITS_PER_INTEGER as usize);
    let compressed_decimals: usize = decimal as usize - (uncompressed_decimals * DECIMAL_DIGITS_PER_INTEGER as usize);

    let bytes_to_read: usize = uncompressed_integers * 4 + compressed_byte_map[compressed_integers] + uncompressed_decimals * 4 + compressed_byte_map[compressed_decimals];

    let mut buf = read_nbytes(r, bytes_to_read)?;

    let mut components = Vec::new();

    let is_negative = (buf[0] & 0x80) == 0;
    buf[0] ^= 0x80;
    if is_negative {
        components.push("-".to_owned());
    }
    let mut r = Cursor::new(buf);
    // if there's a compressed integral part, read it
    if compressed_integers != 0 {
        let to_read = compressed_byte_map[compressed_integers];
        components.push(read_known_length_integer_be(&mut r, to_read)?.to_string())
    }
    for _ in 0..uncompressed_integers {
        components.push(format!("{:09}", r.read_u32::<BigEndian>()?));
    }
    components.push(".".to_owned());
    for _ in 0..uncompressed_decimals {
        components.push(format!("{:09}", r.read_u32::<LittleEndian>()?));
    }
    if compressed_decimals != 0 {
        components.push(read_known_length_integer_be(&mut r, compressed_byte_map[compressed_decimals])?.to_string())
    }
    components.join("").parse::<BigDecimal>().map_err(|e| failure::Error::from_boxed_compat(Box::new(e)))
}


pub(crate) fn read_datetime_subsecond_part<R: Read>(r: &mut R, pack_length: u8) -> io::Result<u32> {
    Ok(match pack_length {
        0 => 0u32,
        1 | 2 => read_known_length_integer_be(r, 1)? as u32,
        3 | 4 => read_known_length_integer_be(r, 2)? as u32,
        5 | 6 => read_known_length_integer_be(r, 3)? as u32,
        _ => 0u32,
    })
}


#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use bigdecimal::BigDecimal;

    use super::read_new_decimal;

    #[test]
    fn test_read_new_decimal() {
        let mut uut = Cursor::new(vec![0x80, 0x00, 0x00, 0x00, 0x01]);
        let one = "1.00".parse::<BigDecimal>().unwrap();
        assert_eq!(read_new_decimal(&mut uut, 10, 0).expect("should parse"), one);
        let mut uut = Cursor::new(vec![0x80, 0x00, 0x01, 0x00, 0x00]);
        let zero_point_one = "0.100".parse::<BigDecimal>().unwrap();
        assert_eq!(read_new_decimal(&mut uut, 5, 5).expect("should parse"), zero_point_one);
        let mut uut = Cursor::new(vec![128, 0, 5, 0, 212, 49]);
        let expected = "5.54321".parse::<BigDecimal>().unwrap();
        assert_eq!(read_new_decimal(&mut uut, 10, 5).expect("should parse"), expected);
    }
}
