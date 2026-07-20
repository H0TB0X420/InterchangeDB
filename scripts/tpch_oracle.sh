#!/usr/bin/env bash
#
# TPC-H oracle: generate the deterministic SF-0.01 dataset as CSVs, load them
# into DuckDB (a trusted engine), run every queries/tpch/*.sql, and write the
# reference answers to tests/tpch/expected/qNN.csv.
#
# The oracle is a one-time OFFLINE tool — never a runtime dependency of the
# database. It is idempotent and deterministic: same seed + scale factor ⇒
# byte-identical CSVs ⇒ byte-identical expected answers.
#
# Usage:  scripts/tpch_oracle.sh
#         DUCKDB=/path/to/duckdb scripts/tpch_oracle.sh   # override the binary
#
set -euo pipefail

# --- locate the repo and the DuckDB binary -------------------------------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# DuckDB is installed at a fixed path (NOT on PATH as a shell alias); fall back
# to whatever `which duckdb` finds if that path is absent.
DEFAULT_DUCKDB="$HOME/.duckdb/cli/latest/duckdb"
DUCKDB="${DUCKDB:-$DEFAULT_DUCKDB}"
if [[ ! -x "$DUCKDB" ]]; then
    DUCKDB="$(command -v duckdb || true)"
fi
if [[ -z "$DUCKDB" || ! -x "$DUCKDB" ]]; then
    echo "error: DuckDB binary not found (looked at $DEFAULT_DUCKDB and PATH)" >&2
    exit 1
fi
echo "using DuckDB: $DUCKDB ($("$DUCKDB" --version))"

SCALE_FACTOR="${SCALE_FACTOR:-0.01}"
QUERIES_DIR="$REPO_ROOT/queries/tpch"
EXPECTED_DIR="$REPO_ROOT/tests/tpch/expected"
CSV_DIR="$(mktemp -d "${TMPDIR:-/tmp}/tpch_oracle.XXXXXX")"
trap 'rm -rf "$CSV_DIR"' EXIT

mkdir -p "$EXPECTED_DIR"

# --- generate the CSVs + schema.sql --------------------------------------
echo "generating CSVs at SF $SCALE_FACTOR ..."
cargo run --release --quiet --manifest-path "$REPO_ROOT/Cargo.toml" --bin tpch -- \
    --scale-factor "$SCALE_FACTOR" --csv-out "$CSV_DIR"

# --- compose one DuckDB script: schema, load, then each query ------------
DDB_SCRIPT="$CSV_DIR/oracle.duckdb.sql"
{
    echo ".bail on"
    echo ".read $CSV_DIR/schema.sql"
    for name in region nation supplier part partsupp customer orders lineitem; do
        # HEADER matches the CSVs' first row; explicit schema (already created)
        # parses dates/decimals from text.
        echo "COPY $name FROM '$CSV_DIR/$name.csv' (HEADER, DELIMITER ',', QUOTE '\"');"
    done
    echo ".mode csv"
    echo ".headers on"
    for id in 01 02 03 04 05 06 07 08 09 10 11 12 13 14 15 16 17 18 19 20 21 22; do
        echo ".output $EXPECTED_DIR/q$id.csv"
        echo ".read $QUERIES_DIR/q$id.sql"
    done
    echo ".output /dev/stdout"
} > "$DDB_SCRIPT"

echo "running 22 queries through DuckDB ..."
"$DUCKDB" -init /dev/null < "$DDB_SCRIPT"

echo "wrote reference answers to $EXPECTED_DIR"
ls "$EXPECTED_DIR"
