use crate::*;
use anyhow::{anyhow, Result};

pub trait BinArrayBitmapExtExtension {
    fn bitmap_range() -> (i32, i32);
    fn get_bitmap_offset(bin_array_index: i32) -> Result<usize>;
    fn bin_array_offset_in_bitmap(bin_array_index: i32) -> Result<usize>;
    fn to_bin_array_index(offset: usize, bin_array_offset: usize, is_positive: bool)
        -> Result<i32>;

    fn get_bitmap(&self, bin_array_index: i32) -> Result<(usize, [u64; 8])>;
    fn bit(&self, bin_array_index: i32) -> Result<bool>;
    fn iter_bitmap(&self, start_index: i32, end_index: i32) -> Result<Option<i32>>;
    fn next_bin_array_index_with_liquidity(
        &self,
        swap_for_y: bool,
        start_index: i32,
    ) -> Result<(i32, bool)>;
}

impl BinArrayBitmapExtExtension for BinArrayBitmapExtension {
    fn bitmap_range() -> (i32, i32) {
        (
            -BIN_ARRAY_BITMAP_SIZE * (EXTENSION_BINARRAY_BITMAP_SIZE as i32 + 1),
            BIN_ARRAY_BITMAP_SIZE * (EXTENSION_BINARRAY_BITMAP_SIZE as i32 + 1) - 1,
        )
    }

    fn next_bin_array_index_with_liquidity(
        &self,
        swap_for_y: bool,
        start_index: i32,
    ) -> Result<(i32, bool)> {
        let (min_bitmap_id, max_bit_map_id) = BinArrayBitmapExtension::bitmap_range();
        if start_index > 0 {
            if swap_for_y {
                match self.iter_bitmap(start_index, BIN_ARRAY_BITMAP_SIZE)? {
                    Some(value) => Ok((value, true)),
                    None => Ok((BIN_ARRAY_BITMAP_SIZE - 1, false)),
                }
            } else {
                match self.iter_bitmap(start_index, max_bit_map_id)? {
                    Some(value) => Ok((value, true)),
                    None => Err(anyhow!("Cannot find non-zero liquidity bin array id")),
                }
            }
        } else if swap_for_y {
            match self.iter_bitmap(start_index, min_bitmap_id)? {
                Some(value) => Ok((value, true)),
                None => Err(anyhow!("Cannot find non-zero liquidity bin array id")),
            }
        } else {
            match self.iter_bitmap(start_index, -BIN_ARRAY_BITMAP_SIZE - 1)? {
                Some(value) => Ok((value, true)),
                None => Ok((-BIN_ARRAY_BITMAP_SIZE, false)),
            }
        }
    }

    fn bit(&self, bin_array_index: i32) -> Result<bool> {
        let (_, bin_array_bitmap) = self.get_bitmap(bin_array_index)?;
        let bin_array_offset_in_bitmap = Self::bin_array_offset_in_bitmap(bin_array_index)?;
        let limb = bin_array_offset_in_bitmap
            .checked_div(64)
            .context("overflow")?;
        let bit = bin_array_offset_in_bitmap
            .checked_rem(64)
            .context("overflow")?;
        let word = *bin_array_bitmap.get(limb).context("overflow")?;
        Ok(((word >> bit) & 1) == 1)
    }

    fn get_bitmap(&self, bin_array_index: i32) -> Result<(usize, [u64; 8])> {
        let offset = Self::get_bitmap_offset(bin_array_index)?;
        if bin_array_index < 0 {
            Ok((offset, self.negative_bin_array_bitmap[offset]))
        } else {
            Ok((offset, self.positive_bin_array_bitmap[offset]))
        }
    }

    fn to_bin_array_index(
        offset: usize,
        bin_array_offset: usize,
        is_positive: bool,
    ) -> Result<i32> {
        let offset = offset as i32;
        let bin_array_offset = bin_array_offset as i32;
        if is_positive {
            Ok((offset + 1) * BIN_ARRAY_BITMAP_SIZE + bin_array_offset)
        } else {
            Ok(-((offset + 1) * BIN_ARRAY_BITMAP_SIZE + bin_array_offset) - 1)
        }
    }

    fn bin_array_offset_in_bitmap(bin_array_index: i32) -> Result<usize> {
        if bin_array_index > 0 {
            Ok(bin_array_index
                .checked_rem(BIN_ARRAY_BITMAP_SIZE)
                .context("overflow")? as usize)
        } else {
            Ok((-(bin_array_index + 1))
                .checked_rem(BIN_ARRAY_BITMAP_SIZE)
                .context("overflow")? as usize)
        }
    }

    fn get_bitmap_offset(bin_array_index: i32) -> Result<usize> {
        let offset = if bin_array_index > 0 {
            bin_array_index / BIN_ARRAY_BITMAP_SIZE - 1
        } else {
            -(bin_array_index + 1) / BIN_ARRAY_BITMAP_SIZE - 1
        };
        Ok(offset as usize)
    }

    fn iter_bitmap(&self, start_index: i32, end_index: i32) -> Result<Option<i32>> {
        let step: i32 = if end_index >= start_index { 1 } else { -1 };
        let mut idx = start_index;

        loop {
            if self.bit(idx)? {
                return Ok(Some(idx));
            }

            if idx == end_index {
                break;
            }

            idx = idx.checked_add(step).context("overflow")?;
        }

        Ok(None)
    }
}
