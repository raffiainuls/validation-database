# CLAUDE.md — Data Validation Tool (MySQL / ClickHouse)

Instruksi untuk agent (Claude Code) yang menjalankan/melanjutkan proyek ini di
laptop mana pun. Baca seluruhnya sebelum eksekusi.

## Tujuan proyek
Membandingkan data antar database untuk memastikan migrasi/ingestion konsisten:
- **RAW_ layer**: MySQL (source) vs ClickHouse `batching.raw_<table>` (target).
- **Mart layer**: ClickHouse `bronze_layer_v5_staging` vs `batching` (server sama,
  beda database) untuk tabel `datamart_*`, `dim_*`, `dashboard_*`, `platinum_*`.

Yang dideteksi per tabel: **missing key** (ada di satu sisi, tidak di sisi lain,
dua arah) dan **beda nilai** kolom (mode full).

## Environment
- Python venv: `.venv` (Python 3.8). Jalankan semua perintah pakai `.venv/bin/python`.
- Dependensi: pandas, mysql-connector-python, clickhouse-connect, PyYAML, boto3,
  cx_Oracle, psycopg2, pyodps, **openpyxl** (untuk Excel — kalau hilang:
  `.venv/bin/python -m ensurepip --upgrade && .venv/bin/python -m pip install openpyxl`).
- **Kredensial (WAJIB, tidak ada di git — `.gitignore`):**
  - `creds/mysql.json` dan `creds/clickhouse.json` harus ada.
  - Nilainya berasal dari file **`.env-staging`** (juga gitignored). Kalau pindah
    laptop, **salin `creds/` dan `.env-staging`** dari laptop lama, ATAU buat ulang
    `creds/*.json` dari `.env-staging`. Struktur:
    ```json
    // creds/mysql.json
    { "hostname_mysql": "...", "port_mysql": 3306, "database_mysql": "staging_smile5_20260212",
      "username_mysql": "...", "password_mysql": "..." }
    // creds/clickhouse.json
    { "host_clickhouse": "clickhouse-data.smile5.xyz", "port_clickhouse": 8123,
      "database_clickhouse": "batching", "username_clickhouse": "smile5", "password_clickhouse": "..." }
    ```
  - Butuh **VPN** ke host DB. Kalau VPN putus, koneksi timeout (lihat "Masalah umum").
- Jangan pernah commit `creds/`, `.env-staging`, `logs/`, `output/` (sudah di-`.gitignore`).

## File penting
- `running_validation.py` — engine inti. Berisi: koneksi tiap DB, jalur **chunked-by-id**,
  deteksi kolom otomatis, perbandingan, **Heartbeat**, penulisan output + summary.log.
- `config.py` — loader: `python config.py <config.yaml> [--mode missing|full]`. Memuat
  semua `creds/*.json` lalu memanggil engine. Untuk **validasi 1 tabel**.
- `validate_batch.py` — **batch runner** banyak tabel: `python validate_batch.py <batch.yaml> [--mode ...] [--resume]`.
- `make_summary_excel.py` — gabung semua `*_summary.log` jadi 1 Excel (sheet ALL / RAW_ /
  DATAMART / DIFF_only). Jalankan: `.venv/bin/python make_summary_excel.py`.
- Batch config:
  - `batch_raw_vs_mysql.yaml` — 105 tabel, **MySQL → ClickHouse raw_** (lintas engine).
  - `batch_bronze_vs_batching.yaml` — 71 tabel **CH↔CH** (datamart/dim/dashboard/platinum).
  - `batch_datamart_ch.yaml` — subset 30 tabel datamart saja (CH↔CH).
- Single-table contoh: `config_ws_transactions.yaml`, `config_ws_patients.yaml`,
  `config_ws_order_item_stocks.yaml`, `config_staging_test.yaml`.

## Konsep kunci (WAJIB paham sebelum mengubah)
- **Chunked-by-id**: tabel diproses per rentang kolom numerik (`chunk_column`) supaya
  memori aman DAN deteksi missing benar **lintas semua periode** (join pakai key, bukan
  filter tanggal). Jangan ganti ke filter tanggal — itu menimbulkan false "missing".
- **Mode** (`--mode` menimpa `mode:` di YAML):
  - `missing` — hanya fetch key, cek missing (cepat & ringan). Dipakai untuk tabel raksasa.
  - `full` — cek missing + banding **semua kolom yang sama** (auto-detect; kecuali key
    dan kolom meta ClickHouse `ingested_at, version, _dlt_load_id, _dlt_id`). Default.
- **Tabel >5 juta baris ditandai `mode: missing`** di batch config supaya tidak berat.
- **ClickHouse SELALU pakai `FINAL`** (dedup ReplacingMergeTree) — sudah dipaksa di kode.
- **Key per tabel**: `composite_id_columns: [..]` (boleh composite). Untuk batch, key diisi
  dari MySQL PRIMARY KEY (raw_) atau ORDER BY/sorting_key ClickHouse (mart). `chunk_column`
  = kolom key numerik pertama; kalau tidak ada yang numerik → otomatis full-scan 1 query.
- **Heartbeat**: tiap `heartbeat_seconds` (default 60; batch set 30) muncul
  `[heartbeat] alive <detik> | <tabel>: <fase>` di SEMUA fase termasuk comparing — bukti
  code hidup. Kalau baris heartbeat berhenti bertambah > beberapa menit = benar stuck;
  timeout ClickHouse 600s akan memutus lalu **retry** (3×) otomatis.
- **`--resume`**: skip tabel yang sudah punya `*_summary.log` (sukses). Tabel ERROR diulang.
  Selalu pakai `--resume` saat melanjutkan setelah interupsi.

## Cara menjalankan (dua eksekusi, boleh paralel di 2 terminal)
Pastikan VPN konek dulu. Jalankan di root proyek.

**Eksekusi A — RAW_ (MySQL → ClickHouse):**
```bash
nohup .venv/bin/python -u validate_batch.py batch_raw_vs_mysql.yaml --mode full --resume \
  > logs/batch_raw.out 2>&1 &
```

**Eksekusi B — Mart CH↔CH (datamart/dim/dashboard/platinum):**
```bash
nohup .venv/bin/python -u validate_batch.py batch_bronze_vs_batching.yaml --mode full --resume \
  > logs/batch_bronze_all.out 2>&1 &
```

Validasi 1 tabel saja (debug):
```bash
.venv/bin/python config.py config_ws_transactions.yaml --mode full
```

## Pantau & deteksi stuck
```bash
tail -f logs/batch_raw.out          # atau logs/batch_bronze_all.out
tail -f "$(ls -t logs/data_validation_*.log | head -1)"   # log engine detail (per-block fetch)
```
- `[phase] ...` = transisi fase; `[heartbeat] alive Ns | ...` = bukti hidup tiap interval.
- `-> IN_SYNC | DIFF | ERROR` = hasil per tabel.
- Cek proses: `pgrep -fl validate_batch`. Hentikan: `pkill -9 -f validate_batch`.

## Output
Di `output/result/` (gitignored), per tabel & ber-timestamp:
- `..._result.csv` — ringkasan (kolom: missing_in_<src>, missing_in_<tgt>, differing_values;
  **3 kolom ini list independen — jangan dibaca per-baris**).
- `..._result_differing_values.csv` — detail **long format**: `id, column, value_<src>, value_<tgt>`.
- `..._result_summary.log` — ringkasan 1 tabel (status, mode, key, jumlah missing/diff, path).
- Batch juga tulis `batch_summary_<ts>.csv`.
Buat Excel gabungan kapan saja: `.venv/bin/python make_summary_excel.py`
→ `output/result/validation_summary_<ts>.xlsx`.

## Masalah umum & penanganan
- **VPN/koneksi putus saat run** → tabel berikutnya `ERROR: Can't connect / timed out`.
  Penanganan: retry 3× otomatis; kalau gagal terus, hentikan, sambungkan VPN, jalankan
  ulang dengan **`--resume`** (yang sudah selesai di-skip). Laptop sleep saat lid ditutup
  juga memutus socket — hindari menutup lid saat run, atau resume setelahnya.
- **Proses menggantung (UN state, CPU ~0)** = socket DB mati. `pkill -9 -f validate_batch`
  lalu `--resume`.
- **`Unknown column 'id'`** = tabel tanpa PK & key default `id` tidak ada → isi `keys:` yang
  benar di batch config, atau keluarkan tabel itu (mis. `integration_ws_orders` tidak punya
  key sama sekali → dibuang).
- **`Length mismatch ... 0 elements`** = chunk kosong; SUDAH diperbaiki (empty-chunk guard).
- **Kolom reserved word (mis. `signal`)** = SUDAH diperbaiki (semua kolom di-backtick).

## Status terakhir / yang perlu dilanjutkan
- **RAW_ (MySQL→ClickHouse): SELESAI** sebelumnya — 105 tabel, 89 IN_SYNC, 16 DIFF, 0 error.
  (2 tabel `integration_ws_orders`, `integration_ws_order_item_stocks` dibuang: tanpa key.)
- **Mart CH↔CH (`batch_bronze_vs_batching.yaml`, 71 tabel): BELUM tuntas.** Sudah ~41 tabel
  selesai; sisanya perlu dilanjutkan. **Lanjutkan dengan `--resume`** (perintah Eksekusi B).
  17 tabel >5jt sudah ditandai `mode: missing`.
- Setelah dua eksekusi tuntas: jalankan `make_summary_excel.py` untuk Excel final, lalu
  laporkan tabel DIFF/ERROR (lihat sheet `DIFF_only`).

## Git
- Branch utama: `main`. Remote: `github.com/raffiainuls/validation-database` (push via SSH).
- Commit per topik. Jangan commit kredensial/log/output.
