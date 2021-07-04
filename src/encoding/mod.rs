use std::convert::TryInto;

pub mod bitpacking;
pub mod delta_bitpacked;
pub mod delta_byte_array;
pub mod delta_length_byte_array;
pub mod hybrid_rle;
pub mod plain_byte_array;
pub mod uleb128;
pub mod zigzag_leb128;

pub use parquet_format::Encoding;

/// # Panics
/// This function panics iff `values.len() < 4`.
pub fn get_length(values: &[u8]) -> u32 {
    u32::from_le_bytes(values[0..4].try_into().unwrap())
}

/// Returns floor(log2(x))
#[inline]
pub fn log2(x: u64) -> u32 {
    64 - x.leading_zeros()
}

/// Returns the ceil of value/divisor
#[inline]
pub fn ceil8(value: usize) -> usize {
    value / 8 + ((value % 8 != 0) as usize)
}


#[cfg(test)]
mod tests {
    use super::log2;

    #[test]
    fn test_log2() {
        assert_eq!(0, log2(0));
        assert_eq!(1, log2(1));
        assert_eq!(2, log2(2));
        assert_eq!(2, log2(3));
        assert_eq!(3, log2(4));
        assert_eq!(3, log2(5));
        assert_eq!(3, log2(6));
        assert_eq!(3, log2(7));
        assert_eq!(4, log2(8));
        assert_eq!(4, log2(15));

        assert_eq!(8, log2(255));
        assert_eq!(9, log2(256));
    }
}