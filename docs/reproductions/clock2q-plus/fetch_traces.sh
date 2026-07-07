#!/bin/sh
# Fetch CloudPhysics block-I/O traces (the Clock2Q+ paper's dataset).
#
# Source: cacheMon/cache_dataset public S3 mirror (Yang lab, CC BY 4.0)
#   https://github.com/cacheMon/cache_dataset
#   s3://cache-datasets/cache_dataset_oracleGeneral/2015_cloudphysics/
# The originals are the 106 one-week VMware ESXi vscsi traces from
# Waldspurger et al., FAST '15 (SHARDS), also held by SNIA IOTTA (which
# requires a browser click-through license; the S3 mirror does not).
#
# Format: oracleGeneral (libCacheSim), zstd-compressed. 24-byte records
# {clock_time u32, obj_id u64, obj_size u32, next_access_vtime i64},
# little-endian; obj_id is the raw vscsi LBN (verified record-for-record
# against the vscsi twin of libCacheSim's bundled sample trace).
# Full set: 106 traces, ~9.0 GB compressed. w01 is the largest (819 MB);
# w105/w87/w93 are the smallest (~12 MB each).
#
# Usage: fetch_traces.sh [w01 w02 ...]   (default: the small trio)
#
# Each trace is decompressed to data/traces/<w>.oracleGeneral.bin and its
# sha256 recorded in data/traces/MANIFEST.sha256 for reproducibility.
set -eu

BUCKET="https://cache-datasets.s3.amazonaws.com/cache_dataset_oracleGeneral/2015_cloudphysics"
DIR="$(dirname "$0")/../../../data/traces"
mkdir -p "$DIR"

TRACES="${*:-w105 w87 w93}"
for w in $TRACES; do
    if [ -f "$DIR/$w.oracleGeneral.bin" ]; then
        echo "$w: already present, skipping"
        continue
    fi
    echo "$w: downloading..."
    curl -sf -o "$DIR/$w.oracleGeneral.bin.zst" "$BUCKET/$w.oracleGeneral.bin.zst"
    zstd -d -q -f "$DIR/$w.oracleGeneral.bin.zst"
    rm "$DIR/$w.oracleGeneral.bin.zst"
    (cd "$DIR" && shasum -a 256 "$w.oracleGeneral.bin" >> MANIFEST.sha256)
    echo "$w: done ($(wc -c < "$DIR/$w.oracleGeneral.bin") bytes)"
done
echo "manifest:"
cat "$DIR/MANIFEST.sha256"
