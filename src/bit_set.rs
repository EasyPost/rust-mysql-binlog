use std::error::Error;
use std::fmt;

/// A simple set implemented by using a bit-mask stored in sequential bytes (no word-level
/// packing is done to maintain compatibility with MySQL's).
///
/// Could probably be replaced by one of the BitVec crates if any of them do the right thing.
pub struct BitSet {
    num_elems: usize,
    inner: Vec<u8>,
}

impl fmt::Debug for BitSet {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "BitSet {{ num_elems: {} }}", self.num_elems)
    }
}

#[derive(Debug)]
pub enum BitSetError {
    ItemOutOfRange,
    SliceTooSmall,
}

impl fmt::Display for BitSetError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        <Self as fmt::Debug>::fmt(self, f)
    }
}

impl Error for BitSetError {
    fn description(&self) -> &'static str {
        "bitset error"
    }
}

impl BitSet {
    pub fn new(num_elems: usize) -> Self {
        let vec_len = (num_elems + 7) >> 3;
        BitSet {
            num_elems,
            inner: vec![0u8; vec_len],
        }
    }

    pub fn from_slice(num_elems: usize, slice: &[u8]) -> Result<Self, BitSetError> {
        let vec_len = (num_elems + 7) >> 3;
        if slice.len() < vec_len {
            return Err(BitSetError::SliceTooSmall);
        }
        Ok(BitSet {
            num_elems,
            inner: slice[0..vec_len].to_owned(),
        })
    }

    fn get_byte_offset(&self, item: usize) -> usize {
        if item >= self.num_elems {
            panic!(
                "attempted to index bit_set out of range: {} >= {}",
                item, self.num_elems
            );
        }
        item >> 3
    }

    pub fn set_value(&mut self, item: usize, value: bool) -> () {
        let offset = self.get_byte_offset(item);
        if value {
            self.inner[offset] |= 1 << (item & 0x07);
        } else {
            self.inner[offset] &= !(1 << (item & 0x07));
        }
        ()
    }

    pub fn set(&mut self, item: usize) -> () {
        self.set_value(item, true)
    }

    pub fn unset(&mut self, item: usize) -> () {
        self.set_value(item, false)
    }

    pub fn is_set(&self, item: usize) -> bool {
        let byte = self.inner[self.get_byte_offset(item)];
        byte & (1 << (item & 0x07)) != 0
    }

    pub fn as_vec(&self) -> Vec<bool> {
        let mut out = Vec::new();
        for i in 0..self.num_elems {
            out.push(self.is_set(i));
        }
        out
    }

    pub fn bits_set(&self) -> usize {
        self.inner.iter().map(|c| c.count_ones() as usize).sum()
    }
}

#[cfg(test)]
mod tests {
    use super::BitSet;

    #[test]
    fn test_basic() {
        assert_eq!(BitSet::new(24).inner.len(), 3);
        assert_eq!(BitSet::new(25).inner.len(), 4);
        let mut b = BitSet::new(25);
        for i in 0..25 {
            assert!(!b.is_set(i));
        }
        b.set(0);
        assert!(b.is_set(0));
        for i in 1..25 {
            assert!(!b.is_set(i));
        }
        b.set(20);
        assert!(!b.is_set(19));
        assert!(b.is_set(20));
        assert!(!b.is_set(21));
        assert_eq!(b.bits_set(), 2);
    }

    #[test]
    fn test_from_slice() {
        let b = BitSet::from_slice(9, &[255u8, 0u8]).expect("should construct");
        assert!(b.is_set(0));
        assert!(!b.is_set(8));
    }
}
