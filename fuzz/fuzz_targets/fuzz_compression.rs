#![no_main]

use libfuzzer_sys::fuzz_target;
use rustfs_kafka::producer::Compression;

fuzz_target!(|data: &[u8]| {
    if data.is_empty() {
        return;
    }
    // Map first byte to a Compression variant and exercise its operations
    let variant = match data[0] % 5 {
        0 => Compression::NONE,
        1 => Compression::GZIP,
        2 => Compression::SNAPPY,
        3 => Compression::LZ4,
        4 => Compression::ZSTD,
        _ => return,
    };

    // These should never panic
    let _disc = variant as i32;
    let _debug = format!("{:?}", variant);
    let _clone = variant;
});
