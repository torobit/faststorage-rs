use std::{
    collections::BTreeMap,
    env,
    ffi::{c_void, CString},
    ptr,
    time::Instant,
};

use faststorage_native::*;
use ordered_float::OrderedFloat;

/* ─────────────── order‑book structs ───────────────────────── */

#[derive(Default)]
struct DepthBook {
    bids: BTreeMap<OrderedFloat<f64>, f64>,
    asks: BTreeMap<OrderedFloat<f64>, f64>,
}
impl DepthBook {
    fn update(&mut self, price: f64, vol: f64, flags: u8) {
        let mf = MarketFlag::from_bits_truncate(flags);
        if mf.contains(MarketFlag::CLEAR) {
            self.bids.clear();
            self.asks.clear();
        }
        let side = if mf.contains(MarketFlag::BUY) { &mut self.bids } else { &mut self.asks };
        if vol > 0.0 {
            side.insert(OrderedFloat(price), vol);
        } else {
            side.remove(&OrderedFloat(price));
        }
    }
    fn best_bid(&self) -> Option<(f64, f64)> {
        self.bids.iter().rev().next().map(|(p, v)| (p.0, *v))
    }
    fn best_ask(&self) -> Option<(f64, f64)> {
        self.asks.iter().next().map(|(p, v)| (p.0, *v))
    }
}

#[derive(Default)]
struct Trades(Vec<(i64, f64, f64)>);
impl Trades {
    fn push(&mut self, ts: i64, p: f64, v: f64) { self.0.push((ts, p, v)); }
}

fn main() -> anyhow::Result<()> {
    let path = env::args().nth(1).expect("usage: bench <file.bin.lz4>");
    println!("Benchmarking {path}");
    let start = Instant::now();

    /* open native reader */
    let c_path = CString::new(path)?;
    let mut h: *mut c_void = ptr::null_mut();
    anyhow::ensure!(faststorage_native::open_reader(c_path.as_ptr(), &mut h) == 0, "open_reader failed");

    let mut depth  = DepthBook::default();
    let mut trades = Trades::default();
    let mut msgs   = 0usize;
    let mut building_snapshot = true;   // block book inspection until first trade after CLEAR

    loop {
        let mut msg: *const c_void = ptr::null();
        let sz = unsafe { faststorage_native::read_message(h, &mut msg) };
        if sz == 0 { break; }

        let kind: i16 = unsafe { ptr::read_unaligned(msg as *const i16) };

        match kind {
            x if x == MessageKind::Depth as i16 => {
                let _seq = unsafe { ptr::read_unaligned((msg as *const u8).add(4) as *const i64) };
                let p  = unsafe { ptr::read_unaligned((msg as *const u8).add(12) as *const i64) } as f64 / 1e8;
                let v  = unsafe { ptr::read_unaligned((msg as *const u8).add(20) as *const i64) } as f64 / 1e8;
                let fl = unsafe { ptr::read_unaligned((msg as *const u8).add(28) as *const u8) };
                depth.update(p, v, fl);
            }
            x if x == MessageKind::Tick as i16 => {
                let ts = unsafe { ptr::read_unaligned((msg as *const u8).add(4)  as *const i64) };
                let p  = unsafe { ptr::read_unaligned((msg as *const u8).add(20) as *const i64) } as f64 / 1e8;
                let v  = unsafe { ptr::read_unaligned((msg as *const u8).add(28) as *const i64) } as f64 / 1e8;
                trades.push(ts, p, v);
                building_snapshot = false;      // snapshot finished
            }
            _ => {}
        }
        msgs += 1;
    }

    faststorage_native::close_reader(h);

    let dur = start.elapsed();
    println!(
        "Processed {msgs} msgs in {:.3}s ({:.1} msgs/s)",
        dur.as_secs_f64(),
        msgs as f64 / dur.as_secs_f64()
    );

    if !building_snapshot {
        if let (Some((bb_price, bb_vol)), Some((ba_price, ba_vol))) =
            (depth.best_bid(), depth.best_ask())
        {
            println!(
                "Final OB – levels: bids {}, asks {}  |  best bid {:.2} ({}), best ask {:.2} ({})",
                depth.bids.len(),
                depth.asks.len(),
                bb_price,
                bb_vol,
                ba_price,
                ba_vol
            );
        }
    } else {
        println!("Snapshot never completed – no trades encountered in file.");
    }

    println!("Trades captured: {}", trades.0.len());
    if let Some(t) = trades.0.last() {
        println!("Last trade: {:?}", t);
    }

    Ok(())
}
