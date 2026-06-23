#!/usr/bin/env python3
"""Compile all per-table *_summary.log files into one Excel workbook.

Sheets:
  - ALL       : every validated table (latest run per table)
  - RAW_       : MySQL -> ClickHouse (raw_) tables
  - DATAMART   : ClickHouse bronze -> batching datamart tables
  - DIFF_only  : only tables that are not in sync

Usage: python make_summary_excel.py
"""
import os
import re
import glob
from datetime import datetime
import pandas as pd

RESULT_DIR = "output/result"


def parse_log(path):
    text = open(path).read()

    def g(key, cast=str, default=None):
        m = re.search(rf'{re.escape(key)}\s*:\s*(.*)', text)
        if not m:
            return default
        val = m.group(1).strip()
        try:
            return cast(val)
        except (ValueError, TypeError):
            return default

    # status / counts
    status = g("status")
    mode = g("mode")
    keys = g("key columns")
    chunk_col = g("chunk column")
    # missing lines look like "missing_in_mysql   : 0"
    m_src = re.search(r'missing_in_\S+\s*:\s*(\d+)', text)
    miss_all = re.findall(r'missing_in_(\S+?)\s*:\s*(\d+)', text)
    differing = g("differing_values", int, 0)
    vc = re.search(r'value columns \((\d+)\)', text)
    val_count = int(vc.group(1)) if vc else 0
    # source / target lines: "source (mysql): table"
    src = re.search(r'source \((\w+)\)\s*:\s*(.+)', text)
    tgt = re.search(r'target \((\w+)\)\s*:\s*(.+)', text)
    return {
        'status': status,
        'mode': mode,
        'source_engine': src.group(1) if src else '',
        'source_table': src.group(2).strip() if src else '',
        'target_engine': tgt.group(1) if tgt else '',
        'target_table': tgt.group(2).strip() if tgt else '',
        'key_columns': keys,
        'chunk_column': chunk_col,
        'value_columns_compared': val_count,
        'missing_in_source': int(miss_all[0][1]) if len(miss_all) > 0 else None,
        'missing_in_target': int(miss_all[1][1]) if len(miss_all) > 1 else None,
        'differing_values': differing,
        'result_csv': g("result_csv"),
        'detail_csv': g("detail_csv"),
        'mtime': os.path.getmtime(path),
        'logfile': os.path.basename(path),
    }


def main():
    files = glob.glob(os.path.join(RESULT_DIR, "*_summary.log"))
    if not files:
        print("Tidak ada *_summary.log di", RESULT_DIR)
        return

    # latest log per (source_table, target_table)
    latest = {}
    for f in files:
        rec = parse_log(f)
        key = (rec['source_table'], rec['target_table'])
        if key not in latest or rec['mtime'] > latest[key]['mtime']:
            latest[key] = rec

    rows = list(latest.values())
    for r in rows:
        r['batch'] = 'RAW_ (MySQL->ClickHouse)' if r['source_engine'] == 'mysql' \
                     else 'DATAMART (ClickHouse->ClickHouse)' if r['source_engine'] == 'bronze' \
                     else r['source_engine']
        r['last_run'] = datetime.fromtimestamp(r['mtime']).strftime('%Y-%m-%d %H:%M:%S')

    cols = ['batch', 'source_table', 'target_table', 'status', 'mode', 'key_columns',
            'chunk_column', 'value_columns_compared', 'missing_in_source',
            'missing_in_target', 'differing_values', 'last_run', 'detail_csv',
            'result_csv', 'logfile']
    df = pd.DataFrame(rows)[cols].sort_values(['batch', 'source_table']).reset_index(drop=True)

    raw_df = df[df['batch'].str.startswith('RAW_')]
    dm_df = df[df['batch'].str.startswith('DATAMART')]
    diff_df = df[df['status'] != 'IN_SYNC']

    ts = datetime.now().strftime('%Y%m%d_%H%M%S')
    out = os.path.join(RESULT_DIR, f"validation_summary_{ts}.xlsx")
    with pd.ExcelWriter(out, engine='openpyxl') as xw:
        df.to_excel(xw, sheet_name='ALL', index=False)
        raw_df.to_excel(xw, sheet_name='RAW_', index=False)
        dm_df.to_excel(xw, sheet_name='DATAMART', index=False)
        diff_df.to_excel(xw, sheet_name='DIFF_only', index=False)
        # auto-fit-ish column widths
        for sh in xw.sheets.values():
            for col in sh.columns:
                width = max((len(str(c.value)) for c in col if c.value is not None), default=10)
                sh.column_dimensions[col[0].column_letter].width = min(width + 2, 60)

    print(f"Excel dibuat: {out}")
    print(f"  ALL={len(df)}  RAW_={len(raw_df)}  DATAMART={len(dm_df)}  DIFF_only={len(diff_df)}")
    print(f"  RAW_     : IN_SYNC={len(raw_df[raw_df.status=='IN_SYNC'])} DIFF={len(raw_df[raw_df.status=='DIFF'])}")
    print(f"  DATAMART : IN_SYNC={len(dm_df[dm_df.status=='IN_SYNC'])} DIFF={len(dm_df[dm_df.status=='DIFF'])}")


if __name__ == "__main__":
    main()
