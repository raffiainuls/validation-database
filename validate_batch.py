#!/usr/bin/env python3
"""
Batch validator: run the chunked validation across MANY tables in one go.

Designed for comparing two databases on the SAME engine (e.g. ClickHouse layer A
vs layer B on the same server), where each table may have a different primary key
(single or composite) and the key column is not always named "id".

Usage:
    python validate_batch.py <batch_config.yaml> [--mode missing|full]

Batch config format (YAML):

    source_alias: bronze          # any label for side A
    target_alias: batch           # any label for side B
    source_db: bronze_layer_v5_staging
    target_db: batching
    engine: clickhouse            # engine type for both sides
    creds: clickhouse             # credentials entry (creds/<creds>.json), shared
    mode: full                    # full | missing (CLI --mode overrides)
    id_chunk_size: 2000000
    clickhouse_final: no
    tables:
      - name: datamart_assets_v5
        keys: [id]
      - name: ws_order_item_stocks
        keys: [order_id, material_id]   # composite key
        chunk_column: order_id          # numeric column to chunk on (default keys[0])
        exclude_columns: [created_at]    # optional columns to skip in full mode

A per-table result CSV is written by each run (in output/result/), and a combined
batch summary is written to output/result/batch_summary_<timestamp>.csv.
"""
import os
import sys
import csv
import time
import yaml
import logging
from datetime import datetime

from running_validation import main as run_validation
from config import load_all_credentials


def emit(msg):
    """Print AND log to the run log file, so progress is visible live via
    `tail -f logs/data_validation_*.log` even when stdout is buffered."""
    print(msg, flush=True)
    logging.info(msg)


def load_batch_config(path):
    with open(path, 'r') as f:
        return yaml.safe_load(f)


def build_table_config(batch, table_entry, credentials):
    name = table_entry['name']
    keys = table_entry.get('keys') or table_entry.get('key') or ['id']
    if isinstance(keys, str):
        keys = [keys]
    src_alias = batch.get('source_alias', 'source')
    tgt_alias = batch.get('target_alias', 'target')
    # Per-side engine/creds (defaults keep same-engine batches working).
    src_engine = batch.get('source_engine', batch.get('engine', 'clickhouse'))
    tgt_engine = batch.get('target_engine', batch.get('engine', 'clickhouse'))
    src_creds = batch.get('source_creds', batch.get('creds', src_engine))
    tgt_creds = batch.get('target_creds', batch.get('creds', tgt_engine))
    # Table-name mapping: empty *_db = name as-is; target_prefix builds e.g. raw_X.
    src_db = batch.get('source_db', '')
    tgt_db = batch.get('target_db', '')
    src_name = table_entry.get('source_name', name)
    tgt_name = table_entry.get('target_name', f"{batch.get('target_prefix', '')}{name}")

    def qualify(db, tbl):
        return f"{db}.{tbl}" if db else tbl

    cfg = {
        'databases': [src_alias, tgt_alias],
        f'{src_alias}_type': src_engine,
        f'{src_alias}_creds': src_creds,
        f'{tgt_alias}_type': tgt_engine,
        f'{tgt_alias}_creds': tgt_creds,
        f'{src_alias}_table_name': qualify(src_db, src_name),
        f'{tgt_alias}_table_name': qualify(tgt_db, tgt_name),
        'chunk_by_id': 'yes',
        'id_chunk_size': batch.get('id_chunk_size', 2000000),
        'heartbeat_seconds': batch.get('heartbeat_seconds', 60),
        # ClickHouse FINAL is always applied internally (dedups ReplacingMergeTree).
        'composite_id_columns': keys,
        # Per-table mode overrides the batch/CLI mode (e.g. giant tables -> missing).
        'mode': table_entry.get('mode') or batch.get('mode', 'full'),
        'credentials': credentials,
    }
    if table_entry.get('chunk_column'):
        cfg['chunk_column'] = table_entry['chunk_column']
    if table_entry.get('exclude_columns'):
        cfg['exclude_columns'] = table_entry['exclude_columns']
    return cfg, name, keys


def main():
    if len(sys.argv) < 2:
        print("Usage: python validate_batch.py <batch_config.yaml> [--mode missing|full]")
        sys.exit(1)

    batch_path = sys.argv[1]
    cli_mode = None
    resume = False
    args = sys.argv[2:]
    i = 0
    while i < len(args):
        if args[i] == '--mode' and i + 1 < len(args):
            cli_mode = args[i + 1].lower(); i += 2
        elif args[i].startswith('--mode='):
            cli_mode = args[i].split('=', 1)[1].lower(); i += 1
        elif args[i] == '--resume':
            resume = True; i += 1
        else:
            i += 1

    batch = load_batch_config(batch_path)
    if cli_mode:
        batch['mode'] = cli_mode
    credentials = load_all_credentials()

    tables = batch.get('tables', [])
    if not tables:
        print("No tables listed in batch config under 'tables:'")
        sys.exit(1)

    # --resume: skip tables that already produced a summary .log (completed OK).
    # A completed table writes a file containing '_<source_table>_vs_'.
    done_tables = set()
    if resume:
        try:
            existing = os.listdir('output/result')
        except FileNotFoundError:
            existing = []
        logs = [f for f in existing if f.endswith('_summary.log')]
        for entry in tables:
            nm = entry['name']
            if any(f"_{nm}_vs_" in f for f in logs):
                done_tables.add(nm)
        emit(f"--resume: {len(done_tables)} tabel sudah selesai, akan dilewati.")

    ts = datetime.now().strftime('%Y%m%d_%H%M%S')
    os.makedirs('output/result', exist_ok=True)
    summary_path = f"output/result/batch_summary_{ts}.csv"

    rows = []
    total = len(tables)
    emit(f"Batch validation: {total} tables | mode={batch.get('mode', 'full')} | "
          f"{batch.get('source_db')} vs {batch.get('target_db')}")

    for idx, entry in enumerate(tables, 1):
        cfg, name, keys = build_table_config(batch, entry, credentials)
        if name in done_tables:
            emit(f"\n===== [{idx}/{total}] {name}  -> SKIP (sudah selesai, --resume) =====")
            continue
        emit(f"\n===== [{idx}/{total}] {name}  key={keys} =====")
        row = {'table': name, 'keys': "+".join(keys), 'status': 'ok',
               'missing_in_source': '', 'missing_in_target': '',
               'differing_values': '', 'error': ''}
        # Retry transient failures (e.g. brief VPN/connection drops) a few times
        # so one network blip doesn't fail the rest of the batch.
        attempts = 3
        last_err = None
        for attempt in range(1, attempts + 1):
            try:
                res = run_validation(cfg) or {}
                row['missing_in_source'] = res.get('missing_in_source', '')
                row['missing_in_target'] = res.get('missing_in_target', '')
                row['differing_values'] = res.get('differing_values', '')
                in_sync = (res.get('missing_in_source') == 0 and
                           res.get('missing_in_target') == 0 and
                           res.get('differing_values') == 0)
                row['status'] = 'IN_SYNC' if in_sync else 'DIFF'
                emit(f"  -> {row['status']}: missing_src={row['missing_in_source']} "
                      f"missing_tgt={row['missing_in_target']} diff={row['differing_values']}")
                last_err = None
                break
            except Exception as e:
                last_err = e
                msg = str(e).lower()
                transient = any(k in msg for k in
                                ('timed out', 'timeout', "can't connect", 'connection',
                                 'max retries', 'unreachable', 'broken'))
                if transient and attempt < attempts:
                    wait = 20 * attempt
                    emit(f"  -> attempt {attempt} failed (transient), retry in {wait}s: {str(e)[:120]}")
                    time.sleep(wait)
                    continue
                break
        if last_err is not None:
            row['status'] = 'ERROR'
            row['error'] = str(last_err)[:300]
            logging.error(f"Batch table {name} failed: {last_err}")
            emit(f"  -> ERROR: {str(last_err)[:200]}")
        rows.append(row)

    with open(summary_path, 'w', newline='') as f:
        w = csv.DictWriter(f, fieldnames=['table', 'keys', 'status', 'missing_in_source',
                                          'missing_in_target', 'differing_values', 'error'])
        w.writeheader()
        w.writerows(rows)

    n_sync = sum(1 for r in rows if r['status'] == 'IN_SYNC')
    n_diff = sum(1 for r in rows if r['status'] == 'DIFF')
    n_err = sum(1 for r in rows if r['status'] == 'ERROR')
    emit(f"\n===== BATCH DONE: {n_sync} in-sync, {n_diff} differ, {n_err} error =====")
    emit(f"Summary: {summary_path}")


if __name__ == "__main__":
    main()
