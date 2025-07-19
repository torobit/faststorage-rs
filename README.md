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
git clone https://github.com/your‑org/faststorage-rs
cd faststorage-rs
cargo build --release           # produces ./target/release/libfaststorage_native.{dylib|so|dll}

# 2. run the Rust benchmark (native speed test)
cargo run --release --bin bench /path/to/file.bin.lz4
