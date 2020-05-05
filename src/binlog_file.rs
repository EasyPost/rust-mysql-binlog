use std::fs::File;
use std::io::{self, Read, Seek};
use std::path::{Path, PathBuf};

use crate::errors::{BinlogParseError, EventParseError};
use crate::event::{Event, TypeCode};

/// Low level wrapper around a single Binlog file. Use this if you
/// want to introspect all events (including internal events like the FDE
/// and TME)
pub struct BinlogFile<I: Seek + Read> {
    file_name: Option<PathBuf>,
    file: I,
    first_event_offset: u64,
}

pub struct BinlogEvents<I: Seek + Read> {
    file: BinlogFile<I>,
    // if the offset is None, it means that we can't read any more
    // for whatever reason
    offset: Option<u64>,
}

impl<I: Seek + Read> BinlogEvents<I> {
    pub fn new(mut bf: BinlogFile<I>, start_offset: u64) -> Self {
        bf.file.seek(io::SeekFrom::Start(start_offset)).unwrap();
        BinlogEvents {
            offset: Some(start_offset),
            file: bf,
        }
    }
}

impl<I: Seek + Read> Iterator for BinlogEvents<I> {
    type Item = Result<Event, EventParseError>;

    fn next(&mut self) -> Option<Self::Item> {
        let event = match self.offset {
            Some(offset) => match self.file.read_at(offset) {
                Ok(e) => e,
                Err(EventParseError::Io(_)) => return None,
                Err(EventParseError::EofError) => return None,
                Err(e) => return Some(Err(e)),
            },
            None => return None,
        };
        if event.type_code() == TypeCode::RotateEvent {
            self.offset = None;
        } else {
            self.offset = Some(event.next_position());
        }
        Some(Ok(event))
    }
}

impl BinlogFile<File> {
    /// Construct a new BinLogFile from the given path
    ///
    /// Opens the file and reads/parses the FDE at construction time
    pub fn try_from_path<R: AsRef<Path>>(path: R) -> Result<Self, BinlogParseError> {
        let p = path.as_ref();
        let fh = File::open(p).map_err(BinlogParseError::OpenError)?;
        Self::try_new_from_reader_name(fh, Some(p.to_owned()))
    }
}

impl<I: Seek + Read> BinlogFile<I> {
    pub fn try_from_reader(reader: I) -> Result<Self, BinlogParseError> {
        Self::try_new_from_reader_name(reader, None)
    }

    fn try_new_from_reader_name(
        mut fh: I,
        name: Option<PathBuf>,
    ) -> Result<Self, BinlogParseError> {
        // read the magic bytes
        let mut magic = [0u8; 4];
        fh.read_exact(&mut magic)?;
        if magic != [0xfeu8, 0x62, 0x69, 0x6e] {
            return Err(BinlogParseError::BadMagic(magic).into());
        }
        let fde = Event::read(&mut fh, 4)?;
        if fde.inner(None)?.is_some() {
            // XXX: todo: thread through common_header_len
        } else {
            return Err(BinlogParseError::BadFirstRecord.into());
        }
        Ok(BinlogFile {
            file_name: name,
            file: fh,
            first_event_offset: fde.next_position(),
        })
    }

    fn read_at(&mut self, offset: u64) -> Result<Event, EventParseError> {
        self.file.seek(io::SeekFrom::Start(offset))?;
        Event::read(&mut self.file, offset).map_err(|i| i.into())
    }

    /// Iterate throgh events in this BinLog file, optionally from the given
    /// starting offset.
    pub fn events(self, offset: Option<u64>) -> BinlogEvents<I> {
        let offset = offset.unwrap_or(self.first_event_offset);
        BinlogEvents::new(self, offset)
    }

    pub fn file_name(&self) -> Option<&Path> {
        self.file_name.as_ref().map(|a| a.as_ref())
    }
}
