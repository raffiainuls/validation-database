#!/usr/bin/env python3
"""
Auto-generate a batch validation YAML — NO manual key entry.

It inspects the databases and fills, per table:
  - keys         : from ClickHouse ORDER BY (sorting_key) or MySQL PRIMARY KEY
  - chunk_column : first NUMERIC key column (else omitted -> full-scan)
  - mode         : 'missing' when the table exceeds --big-threshold rows, else full

Two scenarios:

1) ClickHouse -> ClickHouse (same server, different db). Tables matched by name in BOTH dbs:
     python gen_batch_config.py --out batch_bronze_vs_batching.yaml \
        --source-db bronze_layer_v5_staging --target-db batching \
        --prefixes datamart_,dim_,dashboard_,platinum_

2) MySQL -> ClickHouse raw_ (cross engine). MySQL table X matched to ClickHouse raw_X:
     python gen_batch_config.py --out batch_raw_vs_mysql.yaml \
        --source-engine mysql --target-engine clickhouse \
        --target-db batching --target-prefix raw_

Common options:
  --big-threshold 5000000   rows above which a table is validated in mode: missing
  --exclude-prefixes a_,b_  skip tables with these prefixes (source side)
  --creds clickhouse        credentials entry name (creds/<name>.json) for same-engine
  --source-creds / --target-creds   per-side creds for cross-engine
"""
import re
import sys
import json
import yaml


def load_creds(name):
    with open(f"creds/{name}.json") as f:
        return json.load(f)


def ch_client(creds_name):
    import clickhouse_connect
    c = load_creds(creds_name)
    return clickhouse_connect.get_client(
        host=c['host_clickhouse'], port=c['port_clickhouse'], database=c['database_clickhouse'],
        username=c['username_clickhouse'], password=c['password_clickhouse'], connect_timeout=20)


def mysql_conn(creds_name):
    import mysql.connector
    c = load_creds(creds_name)
    return mysql.connector.connect(
        host=c['hostname_mysql'], port=c['port_mysql'], database=c['database_mysql'],
        user=c['username_mysql'], password=c['password_mysql'], connection_timeout=20)


def parse_cols(sortkey):
    if not sortkey:
        return []
    return [c.strip() for c in sortkey.split(',') if re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]*', c.strip())]


def arg(flag, default=None):
    return sys.argv[sys.argv.index(flag) + 1] if flag in sys.argv else default


def main():
    out = arg('--out', 'batch_generated.yaml')
    src_engine = arg('--source-engine', arg('--engine', 'clickhouse'))
    tgt_engine = arg('--target-engine', arg('--engine', 'clickhouse'))
    src_creds = arg('--source-creds', arg('--creds', src_engine))
    tgt_creds = arg('--target-creds', arg('--creds', tgt_engine))
    src_db = arg('--source-db', '')
    tgt_db = arg('--target-db', '')
    tgt_prefix = arg('--target-prefix', '')
    big = int(arg('--big-threshold', '5000000'))
    prefixes = [p for p in (arg('--prefixes', '') or '').split(',') if p]
    excl = [p for p in (arg('--exclude-prefixes', 'raw_,_') or '').split(',') if p]
    mode_default = arg('--mode', 'full')

    # ---- gather source tables + metadata ----
    # source_keys[table] = [key cols]; numeric_cols[table] = set; rows[table] = count
    source_keys, numeric_cols, rows = {}, {}, {}
    src_tables = set()

    if src_engine == 'clickhouse':
        ch = ch_client(src_creds)
        src_tables = set(r[0] for r in ch.query(f"SHOW TABLES FROM {src_db}").result_rows)
        for name, sk in ch.query(f"SELECT name,sorting_key FROM system.tables WHERE database='{src_db}'").result_rows:
            source_keys[name] = parse_cols(sk)
        for t, n, ty in ch.query(f"SELECT table,name,type FROM system.columns WHERE database='{src_db}'").result_rows:
            if any(x in ty for x in ('Int', 'Float', 'Decimal', 'UInt')):
                numeric_cols.setdefault(t, set()).add(n)
        rows = {r[0]: (r[1] or 0) for r in ch.query(f"SELECT name,total_rows FROM system.tables WHERE database='{src_db}'").result_rows}
    elif src_engine == 'mysql':
        m = mysql_conn(src_creds); mc = m.cursor()
        db = load_creds(src_creds)['database_mysql']
        mc.execute("SHOW TABLES"); src_tables = set(r[0] for r in mc.fetchall())
        mc.execute(f"""SELECT TABLE_NAME, COLUMN_NAME FROM information_schema.KEY_COLUMN_USAGE
                       WHERE TABLE_SCHEMA='{db}' AND CONSTRAINT_NAME='PRIMARY' ORDER BY ORDINAL_POSITION""")
        for t, c in mc.fetchall():
            source_keys.setdefault(t, []).append(c)
        mc.execute(f"""SELECT TABLE_NAME, COLUMN_NAME FROM information_schema.COLUMNS
                       WHERE TABLE_SCHEMA='{db}' AND DATA_TYPE IN
                       ('int','bigint','smallint','tinyint','mediumint','decimal','float','double')""")
        for t, c in mc.fetchall():
            numeric_cols.setdefault(t, set()).add(c)
        mc.execute(f"SELECT TABLE_NAME, TABLE_ROWS FROM information_schema.TABLES WHERE TABLE_SCHEMA='{db}'")
        rows = {t: (n or 0) for t, n in mc.fetchall()}
        m.close()
    else:
        print(f"source engine tak didukung untuk auto-key: {src_engine}"); sys.exit(1)

    # ---- gather target tables (to intersect) ----
    if tgt_engine == 'clickhouse':
        ch2 = ch_client(tgt_creds)
        tgt_tables = set(r[0] for r in ch2.query(f"SHOW TABLES FROM {tgt_db}").result_rows)
    elif tgt_engine == 'mysql':
        m2 = mysql_conn(tgt_creds); mc2 = m2.cursor(); mc2.execute("SHOW TABLES")
        tgt_tables = set(r[0] for r in mc2.fetchall()); m2.close()
    else:
        tgt_tables = set()

    # ---- select candidate source tables ----
    def keep(name):
        if any(name.startswith(p) for p in excl):
            return False
        if prefixes and not any(name.startswith(p) for p in prefixes):
            return False
        # target must contain the mapped table
        return f"{tgt_prefix}{name}" in tgt_tables

    candidates = sorted(t for t in src_tables if keep(t))

    # ---- build entries ----
    entries, no_key, big_tables = [], [], []
    for t in candidates:
        keys = source_keys.get(t) or []
        # cross-engine fallback: use target ClickHouse sorting key if source has no PK
        if not keys and tgt_engine == 'clickhouse':
            res = ch_client(tgt_creds).query(
                f"SELECT sorting_key FROM system.tables "
                f"WHERE database='{tgt_db}' AND name='{tgt_prefix}{t}'").result_rows
            if res:
                keys = parse_cols(res[0][0])
        if not keys:
            no_key.append(t); continue
        e = {'name': t, 'keys': keys}
        chunk = next((k for k in keys if k in numeric_cols.get(t, set())), None)
        if chunk:
            e['chunk_column'] = chunk
        if rows.get(t, 0) > big:
            e['mode'] = 'missing'; big_tables.append(t)
        entries.append(e)

    cfg = {'source_alias': 'src', 'target_alias': 'tgt',
           'source_engine': src_engine, 'target_engine': tgt_engine,
           'source_creds': src_creds, 'target_creds': tgt_creds,
           'source_db': src_db, 'target_db': tgt_db,
           'mode': mode_default, 'id_chunk_size': 2000000, 'heartbeat_seconds': 30}
    if tgt_prefix:
        cfg['target_prefix'] = tgt_prefix
    cfg['tables'] = entries

    yaml.safe_dump(cfg, open(out, 'w'), sort_keys=False, width=200)
    print(f"OK -> {out}")
    print(f"  tabel: {len(entries)}  | mode:missing (>{big:,} baris): {len(big_tables)}")
    if no_key:
        print(f"  DILEWATI (tak ada key/PK): {no_key}")


if __name__ == "__main__":
    main()
