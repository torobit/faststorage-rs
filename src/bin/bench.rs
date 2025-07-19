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
        if mf.contains(MarketFlag::CLEAR) { self.bids.clear(); self.asks.clear(); }
        let book = if mf.contains(MarketFlag::BUY) { &mut self.bids } else { &mut self.asks };
        if vol > 0.0 { book.insert(OrderedFloat(price), vol); } else { book.remove(&OrderedFloat(price)); }
    }
    fn print(&self) {
        let best_bid = self.bids.keys().rev().next().map(|o| o.0);
        let best_ask = self.asks.keys().next().map(|o| o.0);
        println!("Bids {:<5} Asks {:<5} BestBid {:<10} BestAsk {:<10}",
                 self.bids.len(), self.asks.len(),
                 best_bid.map_or("N/A".into(), |p| format!("{p:.2}")),
                 best_ask.map_or("N/A".into(), |p| format!("{p:.2}")));
    }
}

#[derive(Default)]
struct Trades { v: Vec<(i64,f64,f64)> }
impl Trades {
    fn push(&mut self, ts: i64, p: f64, v: f64) { self.v.push((ts,p,v)); }
    fn print(&self) {
        println!("Trades: {}", self.v.len());
        if let Some(t) = self.v.last() { println!("Last trade: {:?}", t); }
    }
}

/* ─────────────── main ───────────────────────── */

fn main() -> anyhow::Result<()> {
    let file = env::args().nth(1).expect("usage: bench <file.bin.lz4>");
    println!("Starting benchmark for: {file}");
    let start = Instant::now();

    /* open reader */
    let c_path = CString::new(file)?;
    let mut h: *mut c_void = ptr::null_mut();
    anyhow::ensure!(faststorage_native::open_reader(c_path.as_ptr(), &mut h) == 0, "open_reader failed");

    let mut depth = DepthBook::default();
    let mut trades = Trades::default();
    let mut n = 0usize;

    loop {
        let mut msg: *const c_void = ptr::null();
        let sz = unsafe { faststorage_native::read_message(h, &mut msg) };
        if sz == 0 { break; }
        anyhow::ensure!(sz > 0, "reader error {sz}");

        unsafe {
            let hdr: MessageHeader = ptr::read_unaligned(msg as *const _);
            if hdr.kind == MessageKind::Depth as i16 {
                let p  = ptr::read_unaligned((msg as *const u8).add(12) as *const i64) as f64 / 1e8;
                let v  = ptr::read_unaligned((msg as *const u8).add(20) as *const i64) as f64 / 1e8;
                let fl = ptr::read_unaligned((msg as *const u8).add(28) as *const u8);
                depth.update(p, v, fl);
            } else if hdr.kind == MessageKind::Tick as i16 {
                let ts = ptr::read_unaligned((msg as *const u8).add( 4) as *const i64);
                let p  = ptr::read_unaligned((msg as *const u8).add(20) as *const i64) as f64 / 1e8;
                let v  = ptr::read_unaligned((msg as *const u8).add(28) as *const i64) as f64 / 1e8;
                trades.push(ts, p, v);
            }
        }
        n += 1;
    }
    faststorage_native::close_reader(h);

    let dur = start.elapsed();
    println!("\nProcessed {n} msgs in {:.3}s  ({:.1} msgs/s)", dur.as_secs_f64(), n as f64/dur.as_secs_f64());
    depth.print();
    trades.print();
    Ok(())
}
