#!/bin/sh
# Curve-recreation fleet, pass 2 — one CloudPhysics trace, three runs:
#   win/  Fig-13 window sweep   (fanout 200, clock2q+ window 10/30%)
#   crp/  LRU-2-CRP sweep       (fanout 200, A18/A19 experiment)
#   data/ Fig-8(b) data traces  (fanout 1, the paper-overlap policies)
# Downloads each trace once, deletes it after (disk-bounded pipeline).
# Resumable: all three CSVs non-empty => trace done.
#
# Driver (run from anywhere inside the repo; requires curl, zstd, and a
# release build of trace_sim):
#   cargo build --release --bin trace_sim
#   caffeinate -is xargs -P <workers> -n 1 \
#       docs/reproductions/clock2q-plus/scripts/fleet2_one.sh \
#       < docs/reproductions/clock2q-plus/scripts/fleet_order.txt
# Workers: cores - 1 is a good default; results are deterministic and
# machine-independent (verified byte-identical contended vs idle), so
# parallelism and hardware only affect wall-clock.
set -u

w="$1"
REPO="$(git -C "$(dirname "$0")" rev-parse --show-toplevel)"
DIR="$REPO/data/traces"
OUT="$REPO/data/traces/fleet2"
BUCKET="https://cache-datasets.s3.amazonaws.com/cache_dataset_oracleGeneral/2015_cloudphysics"
SIM="$REPO/target/release/trace_sim"

P_DATA="clock,2q,arc,s3fifo,s3fifo-2bit,clock2q,clock2q+"
P_WIN="clock2q+w10,clock2q+w30"
P_CRP="lruk-crp1,lruk-crp5,lruk-crp25"

mkdir -p "$OUT/win" "$OUT/crp" "$OUT/data"

if [ -s "$OUT/win/$w.csv" ] && [ -s "$OUT/crp/$w.csv" ] && [ -s "$OUT/data/$w.csv" ]; then
    echo "$w: already done"
    exit 0
fi

if [ ! -f "$DIR/$w.oracleGeneral.bin" ]; then
    curl -sf --retry 2 -o "$DIR/$w.zst.part" "$BUCKET/$w.oracleGeneral.bin.zst" \
        || { echo "$w FETCH-FAIL" >> "$OUT/failures.log"; rm -f "$DIR/$w.zst.part"; exit 0; }
    mv "$DIR/$w.zst.part" "$DIR/$w.oracleGeneral.bin.zst"
    zstd -d -q -f "$DIR/$w.oracleGeneral.bin.zst" \
        || { echo "$w ZSTD-FAIL" >> "$OUT/failures.log"; rm -f "$DIR/$w.oracleGeneral.bin.zst"; exit 0; }
    rm "$DIR/$w.oracleGeneral.bin.zst"
fi

run_pass() {
    # $1 = subdir, $2 = fanout, $3 = policies
    if [ ! -s "$OUT/$1/$w.csv" ]; then
        "$SIM" --trace "$DIR/$w.oracleGeneral.bin" --fanout "$2" --policies "$3" \
            > "$OUT/$1/$w.csv.part" 2> "$OUT/$1/$w.log" \
            && mv "$OUT/$1/$w.csv.part" "$OUT/$1/$w.csv" \
            || { echo "$w $1-FAIL" >> "$OUT/failures.log"; rm -f "$OUT/$1/$w.csv.part"; }
    fi
}

run_pass win 200 "$P_WIN"
run_pass crp 200 "$P_CRP"
run_pass data 1 "$P_DATA"

rm -f "$DIR/$w.oracleGeneral.bin"
echo "$w: done"
