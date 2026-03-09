use super::error::ErrorCode;

// The minimum tick
pub const MIN_TICK: i32 = -443_636;
// The maximum tick
pub const MAX_TICK: i32 = -MIN_TICK;

// Equivalent to get_sqrt_price_at_tick(MIN_TICK)
pub const MIN_SQRT_PRICE_X64: u128 = 4_295_048_016;
// Equivalent to get_sqrt_price_at_tick(MAX_TICK)
pub const MAX_SQRT_PRICE_X64: u128 = 79_226_673_521_066_979_257_578_248_091u128;

const BIT_PRECISION: u32 = 16;

// Compute (a * b) >> 64 exactly, where b is u64.
// This matches `(U128(a) * U128([b,0])) >> 64` from your original code.
#[inline]
fn mul_u128_by_u64_shr_64(a: u128, b: u64) -> u128 {
    let a_lo = a as u64;
    let a_hi = (a >> 64) as u64;

    let p0 = (a_lo as u128) * (b as u128); // 128-bit
    let p1 = (a_hi as u128) * (b as u128); // 128-bit

    // (a*b)>>64 = p1 + (p0>>64)
    p1 + (p0 >> 64)
}

// Calculates 1.0001^(tick/2) as a Q64.64 sqrt price (u128).
// Same constants & behavior as the original Raydium-style implementation.
pub fn get_sqrt_price_at_tick(tick: i32) -> Result<u128, ErrorCode> {
    let abs_tick: u32 = tick.unsigned_abs();
    if abs_tick > MAX_TICK as u32 {
        return Err(ErrorCode::TickUpperOverflow);
    }

    // i = 0
    let mut ratio: u128 = if (abs_tick & 0x1) != 0 {
        0xfffcb933bd6fb800u128
    } else {
        1u128 << 64 // 2^64
    };

    // helper to keep it readable
    macro_rules! step {
        ($mask:expr, $mul:expr) => {
            if (abs_tick & $mask) != 0 {
                ratio = mul_u128_by_u64_shr_64(ratio, $mul);
            }
        };
    }

    step!(0x2, 0xfff97272373d4000u64);
    step!(0x4, 0xfff2e50f5f657000u64);
    step!(0x8, 0xffe5caca7e10f000u64);
    step!(0x10, 0xffcb9843d60f7000u64);
    step!(0x20, 0xff973b41fa98e800u64);
    step!(0x40, 0xff2ea16466c9b000u64);
    step!(0x80, 0xfe5dee046a9a3800u64);
    step!(0x100, 0xfcbe86c7900bb000u64);
    step!(0x200, 0xf987a7253ac65800u64);
    step!(0x400, 0xf3392b0822bb6000u64);
    step!(0x800, 0xe7159475a2caf000u64);
    step!(0x1000, 0xd097f3bdfd2f2000u64);
    step!(0x2000, 0xa9f746462d9f8000u64);
    step!(0x4000, 0x70d869a156f31c00u64);
    step!(0x8000, 0x31be135f97ed3200u64);
    step!(0x10000, 0x09aa508b5b85a500u64);
    step!(0x20000, 0x005d6af8dedc582cu64);
    step!(0x40000, 0x00002216e584f5fau64);

    // Invert for positive ticks: ratio = MAX / ratio
    if tick > 0 {
        ratio = u128::MAX / ratio;
    }

    Ok(ratio)
}

pub fn get_tick_at_sqrt_price(sqrt_price_x64: u128) -> Result<i32, ErrorCode> {
    // second inequality must be < because the price can never reach the price at the max tick
    if !(MIN_SQRT_PRICE_X64..MAX_SQRT_PRICE_X64).contains(&sqrt_price_x64) {
        return Err(ErrorCode::SqrtPriceX64);
    }

    // Determine log_b(sqrt_ratio). First by calculating integer portion (msb)
    let msb: u32 = 128 - sqrt_price_x64.leading_zeros() - 1;
    let log2p_integer_x32 = (msb as i128 - 64) << 32;

    // get fractional value (r/2^msb), msb always > 128
    // We begin the iteration from bit 63 (0.5 in Q64.64)
    let mut bit: i128 = 0x8000_0000_0000_0000i128;
    let mut precision = 0;
    let mut log2p_fraction_x64 = 0;

    // Log2 iterative approximation for the fractional part
    // Go through each 2^(j) bit where j < 64 in a Q64.64 number
    // Append current bit value to fraction result if r^2 Q2.126 is more than 2
    let mut r = if msb >= 64 {
        sqrt_price_x64 >> (msb - 63)
    } else {
        sqrt_price_x64 << (63 - msb)
    };

    while bit > 0 && precision < BIT_PRECISION {
        r *= r;
        let is_r_more_than_two = r >> 127_u32;
        r >>= 63 + is_r_more_than_two;
        log2p_fraction_x64 += bit * is_r_more_than_two as i128;
        bit >>= 1;
        precision += 1;
    }
    let log2p_fraction_x32 = log2p_fraction_x64 >> 32;
    let log2p_x32 = log2p_integer_x32 + log2p_fraction_x32;

    // 14 bit refinement gives an error margin of 2^-14 / log2 (√1.0001) = 0.8461 < 1
    // Since tick is a decimal, an error under 1 is acceptable

    // Change of base rule: multiply with 2^32 / log2 (√1.0001)
    let log_sqrt_10001_x64 = log2p_x32 * 59543866431248i128;

    // tick - 0.01
    let tick_low = ((log_sqrt_10001_x64 - 184467440737095516i128) >> 64) as i32;

    // tick + (2^-14 / log2(√1.0001)) + 0.01
    let tick_high = ((log_sqrt_10001_x64 + 15793534762490258745i128) >> 64) as i32;

    Ok(if tick_low == tick_high {
        tick_low
    } else if get_sqrt_price_at_tick(tick_high).unwrap() <= sqrt_price_x64 {
        tick_high
    } else {
        tick_low
    })
}
