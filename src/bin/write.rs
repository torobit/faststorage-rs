use std::{
    collections::BTreeMap,
    env,
    ffi::{c_void, CString},
    fs::File,
    io::{BufWriter, Write},
    ptr,
    time::Instant,
};

use faststorage_native::*;
use ordered_float::OrderedFloat;

/* ─── Order‑book ─────────────────────────────────────────── */

#[derive(Default)]
struct Book {
    bids: BTreeMap<OrderedFloat<f64>, f64>,
    asks: BTreeMap<OrderedFloat<f64>, f64>,
}
impl Book {
    fn update(&mut self, p: f64, v: f64, flags: u8) {
        let mf = MarketFlag::from_bits_truncate(flags);
        if mf.contains(MarketFlag::CLEAR) {
            self.bids.clear();
            self.asks.clear();
        }
        let side = if mf.contains(MarketFlag::BUY) { &mut self.bids } else { &mut self.asks };
        if v > 0.0 {
            side.insert(OrderedFloat(p), v);
        } else {
            side.remove(&OrderedFloat(p));
        }
    }
    fn best_bid(&self) -> Option<(f64, f64)> { self.bids.iter().rev().next().map(|(p, v)| (p.0, *v)) }
    fn best_ask(&self) -> Option<(f64, f64)> { self.asks.iter().next().map(|(p, v)| (p.0, *v)) }
}

/* ─── CSV ──────────────────────────────────────────────── */

struct Csv { w: BufWriter<File> }
impl Csv {
    fn new(path: &str) -> anyhow::Result<Self> {
        let mut w = BufWriter::new(File::create(path)?);
        writeln!(w, "time;bestAskPrice;bestAskVolume;bestBidPrice;bestBidVolume")?;
        Ok(Self { w })
    }
    fn log(&mut self, ts: i64, ask: (f64, f64), bid: (f64, f64)) {
        let _ = writeln!(
            self.w,
            "{};{:.8};{:.8};{:.8};{:.8}",
            ts, ask.0, ask.1, bid.0, bid.1
        );
    }
}

/* ─── Main ─────────────────────────────────────────────── */

fn main() -> anyhow::Result<()> {
    let file = env::args().nth(1).expect("usage: bench <file.bin.lz4>");
    let start = Instant::now();

    // open native reader
    let mut rdr: *mut c_void = std::ptr::null_mut();
    let c_path = CString::new(file.clone())?;
    anyhow::ensure!(faststorage_native::open_reader(c_path.as_ptr(), &mut rdr) == 0, "open_reader failed");

    let mut book = Book::default();
    let mut csv  = Csv::new("best_book.csv")?;
    let mut building_snapshot = true; // true until first trade after CLEAR

    loop {
        let mut msg_ptr: *const c_void = std::ptr::null();
        let sz = unsafe { faststorage_native::read_message(rdr, &mut msg_ptr) };
        if sz == 0 { break; }

        // kind (i16) at offset 0 – use read_unaligned to avoid packed‑struct UB
        let kind: i16 = unsafe { ptr::read_unaligned(msg_ptr as *const i16) };

        match kind {
            x if x == MessageKind::Depth as i16 => {
                // Extract timestamp, price, volume, flags – layout: hh len (4) + ts(8) + price(8) + vol(8) + flags(1)
                let ts = unsafe { ptr::read_unaligned((msg_ptr as *const u8).add(4)  as *const i64) };
                let p  = unsafe { ptr::read_unaligned((msg_ptr as *const u8).add(12) as *const i64) } as f64 / 1e8;
                let v  = unsafe { ptr::read_unaligned((msg_ptr as *const u8).add(20) as *const i64) } as f64 / 1e8;
                let fl = unsafe { ptr::read_unaligned((msg_ptr as *const u8).add(28) as *const u8) };
                book.update(p, v, fl);

                if !building_snapshot {
                    if let (Some(ask), Some(bid)) = (book.best_ask(), book.best_bid()) {
                        csv.log(ts, ask, bid);
                    }
                }
            }
            x if x == MessageKind::Tick as i16 => {
                // end snapshot once the *first* trade tick arrives
                building_snapshot = false;
            }
            _ => {}
        }
    }

    faststorage_native::close_reader(rdr);
    csv.w.flush()?;

    println!("CSV saved → best_book.csv");
    println!("Done in {:.2?}", start.elapsed());
    Ok(())
}
