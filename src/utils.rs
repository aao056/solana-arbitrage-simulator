use std::panic::{AssertUnwindSafe, catch_unwind};

use crate::raydium::clmm::core::derive_tick_array_window;
use raydium_amm_v3::states::{PoolState, TickArrayBitmapExtension, TickArrayState};
use solana_pubkey::Pubkey;

pub fn ticks_per_array(tick_spacing: u16) -> i32 {
    raydium_amm_v3::states::TICK_ARRAY_SIZE * (tick_spacing as i32)
}

fn div_floor_i32(a: i32, b: i32) -> i32 {
    let mut q = a / b;
    let r = a % b;
    if (r != 0) && ((r > 0) != (b > 0)) {
        q -= 1;
    }
    q
}

pub fn tick_array_start_index(tick: i32, tick_spacing: u16) -> i32 {
    let tpa = ticks_per_array(tick_spacing);
    div_floor_i32(tick, tpa) * tpa
}

pub fn window_has_pair(ps: &PoolState, win: &[TickArrayState], zero_for_one: bool) -> bool {
    let tpa = ticks_per_array(ps.tick_spacing);
    let cur = tick_array_start_index(ps.tick_current, ps.tick_spacing);
    let next = if zero_for_one { cur - tpa } else { cur + tpa };

    let mut has_cur = false;
    let mut has_next = false;

    for ta in win {
        if ta.start_tick_index == cur {
            has_cur = true;
        }
        if ta.start_tick_index == next {
            has_next = true;
        }
    }
    has_cur && has_next
}

pub fn destructure_start_pks_safe(
    program_id: Pubkey,
    pool_id: Pubkey,
    ps: &PoolState,
    bm: &TickArrayBitmapExtension,
    n: usize,
) -> (Vec<i32>, Vec<Pubkey>) {
    let res = catch_unwind(AssertUnwindSafe(|| {
        derive_tick_array_window(program_id, pool_id, ps, bm, n)
    }));

    let v: Vec<(i32, Pubkey)> = match res {
        Ok(v) => v,
        Err(_) => {
            tracing::warn!(pool=%pool_id, "derive_tick_array_window panicked; returning empty window");
            return (Vec::new(), Vec::new());
        }
    };

    let starts: Vec<i32> = v.iter().map(|(s, _)| *s).collect();
    let pks: Vec<Pubkey> = v.iter().map(|(_, pk)| *pk).collect();
    (starts, pks)
}
