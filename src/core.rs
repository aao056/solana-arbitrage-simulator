use crate::models::ClmmSnapshot;
use crate::raydium::clmm::core::swap_compute;
use crate::utils::{tick_array_start_index, ticks_per_array};
use raydium_amm_v3::states::TickArrayState;
use std::collections::{HashMap, VecDeque};

pub fn raydium_clmm_quote(
    snapshot: &ClmmSnapshot,
    amount_in: u64,
    zero_for_one: bool,
) -> Result<u64, &'static str> {
    let w = &snapshot.ticks_array_window;
    if w.is_empty() {
        return Err("empty tick window");
    }

    let tpa = ticks_per_array(snapshot.pool_state.tick_spacing);
    let cur_start = tick_array_start_index(
        snapshot.pool_state.tick_current,
        snapshot.pool_state.tick_spacing,
    );

    let mut by_start: HashMap<i32, TickArrayState> = HashMap::with_capacity(w.len());
    for ta in w.iter().cloned() {
        by_start.insert(ta.start_tick_index, ta);
    }

    let cur = by_start
        .get(&cur_start)
        .cloned()
        .ok_or("window missing current tick array")?;

    let next_start = if zero_for_one {
        cur_start - tpa
    } else {
        cur_start + tpa
    };
    let next_opt = by_start.get(&next_start).cloned();

    let mut tick_arrays: VecDeque<TickArrayState> = VecDeque::new();
    tick_arrays.push_back(cur);
    if let Some(next) = next_opt {
        tick_arrays.push_back(next);
    }

    let current_valid_start = tick_arrays.front().unwrap().start_tick_index;

    let (amount_out, _) = swap_compute(
        zero_for_one,
        true,
        true,
        snapshot.trade_fee_rate,
        amount_in,
        current_valid_start,
        0,
        &snapshot.pool_state,
        &snapshot.tick_array_bitmap_ext,
        &mut tick_arrays,
    )?;

    Ok(amount_out)
}
