use parquet_format::Encoding;

use crate::encoding::{bitpacking, get_length, hybrid_rle, log2};

#[inline]
fn get_bit_width(max_level: i16) -> u32 {
    log2(max_level as u64)
}

pub fn needed_bytes(values: &[u8], _length: u32, encoding: (&Encoding, i16)) -> usize {
    match encoding {
        (_, 0) => 0, // no levels
        (Encoding::Rle, _) => {
            let length = get_length(values);
            // 4 consumed to read `length`
            4 + length as usize
        }
        (Encoding::BitPacked, _) => {
            todo!()
        }
        _ => unreachable!(),
    }
}

#[inline]
pub fn decode(values: &[u8], length: u32, encoding: (&Encoding, i16)) -> Vec<u32> {
    match encoding {
        (_, 0) => vec![0; length as usize], // no levels => required => all zero
        (Encoding::Rle, max_length) => {
            let bit_width = get_bit_width(max_length);
            rle_decode(
                &values[4..4 + get_length(values) as usize],
                bit_width as u32,
                length,
            )
        }
        (Encoding::BitPacked, _) => {
            todo!()
        }
        _ => unreachable!(),
    }
}

#[inline]
pub fn rle_decode(values: &[u8], num_bits: u32, length: u32) -> Vec<u32> {
    let length = length as usize;
    let runner = hybrid_rle::Decoder::new(&values, num_bits, length as usize);

    let mut values = Vec::with_capacity(length);
    runner.for_each(|run| match run {
        hybrid_rle::HybridEncoded::Bitpacked {
            compressed,
            num_bits,
            run_length,
        } => {
            let previous_len = values.len();
            values.extend(bitpacking::Decoder::new(
                compressed,
                num_bits as u8,
                run_length,
            ));
            debug_assert_eq!(previous_len + run_length, values.len());
        }
        hybrid_rle::HybridEncoded::Rle { value, run_length } => {
            values.extend(std::iter::repeat(value).take(run_length))
        }
    });
    values
}

pub fn consume_level<'a>(
    values: &'a [u8],
    length: u32,
    level_encoding: (&Encoding, i16),
) -> (&'a [u8], Vec<u32>) {
    if level_encoding.1 > 0 {
        let offset = needed_bytes(values, length, level_encoding);
        let def_levels = decode(values, length, level_encoding);
        (&values[offset..], def_levels)
    } else {
        (values, vec![0; length as usize])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rle_decode() {
        let bit_width = 1;
        let length = 5;
        let values = vec![
            2, 0, 0, 0, // length
            0b00000011, 0b00001011, // data
        ];
        let expected = vec![1, 1, 0, 1, 0];

        let result = rle_decode(&values[4..], bit_width, length);

        assert_eq!(result, expected);
    }
}
