//! FastStorage.Native

use std::{
    ffi::{c_char, CStr},
    fs::File,
    io::{BufReader, Read},
    os::raw::c_void,
};

use anyhow::{Context, Result};
use bitflags::bitflags;
use byteorder::{ByteOrder, LittleEndian};

/* ────────────────  1. decoder  ────────────── */

mod k4os_pickler {
    use super::*;
    use lz4_flex::block;

    pub fn unpickle(src: &[u8]) -> Result<Vec<u8>> {
        if src.is_empty() {
            return Ok(Vec::new());
        }

        let b0 = src[0];
        anyhow::ensure!(b0 & 7 == 0, "unsupported version");

        let diff_len = match (b0 >> 6) & 3 { 0 => 0, 1 => 1, 2 => 2, _ => 4 };
        let data_off = 1 + diff_len;
        anyhow::ensure!(src.len() >= data_off, "header truncated");

        let diff = if diff_len == 0 {
            0
        } else {
            let mut tmp = [0u8; 4];
            tmp[..diff_len].copy_from_slice(&src[1..data_off]);
            LittleEndian::read_u32(&tmp) as usize
        };

        let payload = &src[data_off..];
        if diff == 0 {
            Ok(payload.to_vec())
        } else {
            let expected = payload.len() + diff;
            let out = block::decompress(payload, expected)?;
            Ok(out)
        }
    }
}

/* ────────────────  2. wire‑format structs  ─────────────────────────── */

#[repr(i16)]
#[derive(Clone, Copy)]
pub enum MessageKind { Depth = 0, Tick = 1, Symbol = 2, Candle = 3, CandleEnd = 4 }

bitflags! {
    #[repr(transparent)]
    #[derive(Default)]
    pub struct MarketFlag: u8 {
        const BUY       = 1;
        const SELL      = 2;
        const CLEAR     = 4;
        const END_OF_TX = 8;
    }
}

#[repr(C, packed)]
#[derive(Clone, Copy)]
pub struct MessageHeader { pub kind: i16, pub size: u16, pub time: i64 }

#[repr(C, packed)]
#[derive(Clone, Copy)]
pub struct DepthItem { pub header: MessageHeader, pub price: i64, pub volume: i64, pub flags: u8 }

#[repr(C, packed)]
#[derive(Clone, Copy)]
pub struct TickItem  { pub header: MessageHeader, pub id: i64, pub price: i64, pub volume: i64, pub side: u8 }

/* ────────────────  3. reader implementation  ───────────────────────── */

struct FastCacheReader {
    file:      BufReader<File>,
    src:       Vec<u8>,
    offset:    usize,
    block_len: usize,
}

impl FastCacheReader {
    fn open(path: &str) -> Result<Self> {
        let mut f = BufReader::new(File::open(path).with_context(|| format!("open {path}"))?);
        let mut hdr = [0u8; 4];
        f.read_exact(&mut hdr)?;
        let buf_len = LittleEndian::read_i32(&hdr);
        anyhow::ensure!(buf_len > 0, "invalid buffer length in file");
        Ok(Self { file: f, src: vec![0; buf_len as usize], offset: 0, block_len: 0 })
    }

    unsafe fn next_msg(&mut self) -> Result<Option<*const c_void>> {
        if self.offset >= self.block_len && !self.load_block()? {
            return Ok(None);
        }
        let ptr = self.src.as_ptr().add(self.offset);
        let h = &*(ptr as *const MessageHeader);
        if h.size == 0 { return Ok(None); }
        self.offset += h.size as usize;
        Ok(Some(ptr as *const c_void))
    }

    fn load_block(&mut self) -> Result<bool> {
        let mut hdr = [0u8; 4];
        if self.file.read_exact(&mut hdr).is_err() { return Ok(false); }
        let cmp_len = LittleEndian::read_i32(&hdr) as usize;
        anyhow::ensure!(cmp_len > 0, "compressed length 0");

        let mut cmp_buf = vec![0u8; cmp_len];
        self.file.read_exact(&mut cmp_buf)?;
        let block = k4os_pickler::unpickle(&cmp_buf)?;
        anyhow::ensure!(block.len() <= self.src.len(), "block larger than buffer");
        self.src[..block.len()].copy_from_slice(&block);
        self.block_len = block.len();
        self.offset = 0;
        Ok(true)
    }
}

/* ────────────────  4. C‑ABI exports  ───────────────────────────────── */

#[no_mangle]
pub extern "C" fn open_reader(path: *const c_char, out: *mut *mut c_void) -> i32 {
    if path.is_null() || out.is_null() { return -1; }
    let path = unsafe { CStr::from_ptr(path) }.to_string_lossy().into_owned();
    match FastCacheReader::open(&path) {
        Ok(r)  => { unsafe { *out = Box::into_raw(Box::new(r)) as *mut _ }; 0 }
        Err(_) => -1,
    }
}

#[no_mangle]
pub unsafe extern "C" fn read_message(handle: *mut c_void, out: *mut *const c_void) -> i32 {
    if handle.is_null() || out.is_null() { return -1; }
    let rdr = &mut *(handle as *mut FastCacheReader);
    match rdr.next_msg() {
        Ok(Some(p)) => { *out = p; (&*(p as *const MessageHeader)).size as i32 }
        Ok(None)    => 0,
        Err(_)      => -2,
    }
}

#[no_mangle]
pub extern "C" fn close_reader(h: *mut c_void) {
    if !h.is_null() {
        unsafe { drop(Box::from_raw(h as *mut FastCacheReader)) };
    }
}
