# faststorage‑rs

High‑throughput **Rust** reader for the binary “FastStorage” cache files
written by K4os’ `.NET` library  
([`K4os.Compression.LZ4`](https://github.com/MiloszKrajewski/K4os.Compression.LZ4))
— plus a minimal **C ABI** and a zero‑dependency **Python** wrapper.

| What? | Why? |
|-------|------|
| **~20 M msgs / s** on an M‑series Mac | Rust decoder avoids allocations, copies and exceptions |
| **Single `cdylib`** target | Easy FFI: load from C/C++, Python (via `ctypes`), etc. |
| Strictly **no extra crates** beyond `lz4_flex`, `anyhow`, and friends | Small build / binary footprint |

---

## Quick start

```bash
# 1. clone & build
git clone https://github.com/torobit/faststorage-rs
cd faststorage-rs
cargo build --release           # produces ./target/release/libfaststorage_native.{dylib|so|dll}

# 2. run the Rust benchmark (native speed test)
cargo run --release --bin bench /path/to/file.bin.lz4
```
Native (Rust) benchmark:

Processed 56 992 165 msgs in 2.516s  (22 656 218.1 msgs/s)



Using from Python
```bash
# 1. Build the shared library first (see above)
# 2. Drop python/bench_faststorage.py next to libfaststorage_native.dylib (or export FASTSTORAGE_NATIVE_PATH)
python python/bench_faststorage.py  /abs/path/to/file.bin.lz4
```

The Python benchmark is intentionally allocation‑free:

Processed 64 520 102 msgs in 122.8 s  (525 121 msgs/s)
That’s ~ 40× faster than the original pure‑Python reader and still
single‑threaded.






