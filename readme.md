# Data Validator Documentation

This repository provides a comprehensive data validation tool designed to ensure data consistency across multiple database systems. The tool was created based on real-world experience working on a data migration project from AWS Athena to Alibaba MaxCompute, where frequent data discrepancies were encountered.

## Project Overview

This validation tool is designed to:
- Validate and identify missing IDs between different database systems (AWS Athena, Alibaba MaxCompute, PostgreSQL, Oracle, MySQL, ClickHouse)
- Detect discrepancies in values based on unique IDs or composite IDs
- Support multiple data types (integer, string, date) with appropriate validation logic
- Scale to very large tables (tens of millions of rows) via memory-safe chunked-by-id processing
- Provide detailed reporting of validation results

## Features

- **Missing ID Detection**: Identifies any IDs that are missing between the compared databases and saves results to CSV files
- **Value Discrepancy Detection**: Compares values of records with the same ID across different databases and saves output to CSV files
- **Multi-Database Support**: Supports AWS Athena, Alibaba MaxCompute, Oracle, PostgreSQL, ClickHouse, and MySQL databases
- **Flexible Data Type Validation**: Handles integer, string, and date data types with appropriate comparison logic
- **Fuzzy String Matching**: For string data, uses configurable similarity thresholds to identify potential matches
- **Date Filtering**: Optional date range filtering for time-series data validation
- **Composite ID Support**: Handles tables with composite primary keys (multiple columns forming a unique identifier)
- **Batch Processing**: Efficiently processes large datasets using configurable batch sizes
- **Concurrent Processing**: Uses multi-threading for parallel data fetching from different databases
- **Chunked-by-ID Validation**: For very large tables (tens of millions of rows), processes the data in bounded id ranges so memory stays low — while keeping missing-ID detection correct across **all periods** (see [Chunked Validation for Large Tables](#chunked-validation-for-large-tables))
- **Streaming ClickHouse Fetch**: Reads ClickHouse results as a single streamed query (no `LIMIT/OFFSET` pagination), avoiding the O(n²) slowdown and read-timeouts on large tables
- **Checkpoint Logging**: Each chunk writes a progress checkpoint to the run log file, so you can monitor long runs live with `tail -f`
- **Timestamped, Self-Describing Output**: Result file names embed both table names, the checked column, and a timestamp, so runs never overwrite each other

## System Requirements

### Prerequisites

Before you begin, ensure you have the following:

- **Python 3.8 or higher**
- **Database Access**: Credentials and network access to the databases you want to validate
- **Required Python Packages**:
  - pandas
  - boto3 (for AWS Athena)
  - pyodps (for Alibaba MaxCompute)
  - psycopg2-binary (for PostgreSQL)
  - cx_Oracle (for Oracle)
  - clickhouse-connect (for ClickHouse)
  - mysql-connector-python (for MySQL)
  - PyYAML
  - concurrent.futures (built-in)
  - logging (built-in)
  - os, sys (built-in)

### Installation

#### Option 1: Using uv (Recommended - Faster Performance)
1. **Install uv** (if not already installed):
   ```bash
   # macOS/Linux
   curl -LsSf https://astral.sh/uv/install.sh | sh
   
   # Windows (PowerShell)
   irm https://astral.sh/uv/install.ps1 | iex
   ```

2. **Clone the repository:**
   ```bash
   git clone https://github.com/raffiainuls/validation-database.git
   cd validation-database
   ```

3. **Create virtual environment and install dependencies:**
   ```bash
   uv venv
   source .venv/bin/activate  # On Windows: .venv\Scripts\activate
   uv pip install pandas boto3 pyodps psycopg2-binary cx_Oracle clickhouse-connect mysql-connector-python PyYAML
   ```

#### Option 2: Using pip (Traditional)
1. **Clone the repository:**
   ```bash
   git clone https://github.com/raffiainuls/validation-database.git
   cd validation-database
   ```

2. **Install Python dependencies:**
   ```bash
   pip install pandas boto3 pyodps psycopg2-binary cx_Oracle clickhouse-connect mysql-connector-python PyYAML
   ```

3. **Install Oracle Client** (for Oracle database support):
   - Download and install Oracle Instant Client from [Oracle's website](https://www.oracle.com/database/technologies/instant-client/downloads.html)
   - Set the `ORACLE_HOME` environment variable to point to the Oracle client installation directory

## Project Structure

```
validation-database/
├── readme.md                 # This documentation file
├── config.py                 # Configuration loader and credential manager
├── running_validation.py     # Main validation logic and data processing
├── validation.ipynb          # Jupyter notebook for running validations
├── config.yaml               # Main configuration file
├── config_ws_transactions.yaml # Example chunked-by-id config for a large table
├── docs.md                   # Additional documentation
├── .gitignore                # Excludes creds/, .env*, logs/, output/ from git
├── creds/                    # Database credentials (gitignored — not committed)
│   ├── mysql.json            # MySQL credentials
│   ├── clickhouse.json       # ClickHouse credentials
│   ├── aws.json              # AWS credentials
│   ├── oracle.json           # Oracle credentials
│   ├── postgres.json         # PostgreSQL credentials
│   └── ali.json              # Alibaba MaxCompute credentials
├── output/                   # Directory for validation results
│   └── result/               # CSV output files
└── logs/                     # Log files directory (one timestamped log per run)
```

## Configuration Guide

### 1. Database Credentials Setup

> **Security:** the `creds/` directory and `.env*` files are listed in `.gitignore` and are **not** tracked by git. Never commit real credentials. Connection details (host, port, db, user) are logged for traceability, but **passwords and cloud access keys are never written to the logs**.

Create JSON files in the `creds/` directory for each database you plan to use:

**PostgreSQL (`creds/postgres.json`):**
```json
{
  "hostname_postgres": "your-postgres-host",
  "port_postgres": 5432,
  "database_postgres": "your-database-name",
  "username_postgres": "your-username",
  "password_postgres": "your-password"
}
```

**Oracle (`creds/oracle.json`):**
```json
{
  "dsn_oracle": "your-oracle-dsn",
  "username_oracle": "your-username",
  "password_oracle": "your-password"
}
```

**AWS Athena (`creds/aws.json`):**
```json
{
  "aws_database": "your-database-name",
  "output_location": "s3://your-bucket/output/",
  "aws_region": "your-region",
  "aws_access_key_id": "your-access-key",
  "aws_secret_access_key": "your-secret-key"
}
```

**Alibaba MaxCompute (`creds/ali.json`):**
```json
{
  "ali_access_id": "your-access-id",
  "ali_access_key": "your-access-key",
  "ali_project_name": "your-project-name",
  "ali_endpoint": "your-endpoint"
}
```

**ClickHouse (`creds/clickhouse.json`):**
```json
{
  "host_clickhouse": "your-clickhouse-host",
  "port_clickhouse": 8123,
  "database_clickhouse": "your-database-name",
  "username_clickhouse": "your-username",
  "password_clickhouse": "your-password"
}
```

### 2. Main Configuration (`config.yaml`)

Configure the validation parameters in `config.yaml`:

```yaml
# Basic Configuration
database1: postgres          # First database to compare (postgres, oracle, aws, ali, mysql, clickhouse)
database2: oracle           # Second database to compare (postgres, oracle, aws, ali, mysql, clickhouse)
databases: ["oracle", "postgres"]  # List of databases to validate
data_type: string           # Data type: integer, string, or date
is_using_manual_queries: no # Use manual queries or auto-generate (yes/no)
batch_size: 10000          # Batch size for processing large datasets
threshold: 1              # Fuzzy matching threshold for string data (0.0 to 1.0)

# Date Filtering (optional)
# start_date: "2014-01-01"
# end_date: "2024-12-30"

# Chunked-by-ID Validation (optional, recommended for very large tables)
# When enabled, the tool ignores manual queries and the full-fetch path, and
# instead processes the tables in id ranges (WHERE id BETWEEN ...). See the
# "Chunked Validation for Large Tables" section below.
# chunk_by_id: yes          # Enable chunked processing
# id_chunk_size: 2000000    # Number of ids per chunk fetched from each DB
# clickhouse_final: yes     # Apply FINAL on a ClickHouse ReplacingMergeTree table

# ID Configuration
composite_id_columns: ["ID"]  # Composite ID columns (use unique ID if single column)
check_column: "MODEL"        # Column to validate/compare

# Table Names
aws_table_name: "your_aws_table"
ali_table_name: "your_ali_table"
postgres_table_name: "public.orders"
oracle_table_name: "orders"
clickhouse_table_name: "your_clickhouse_table"

# Date Columns for Filtering
postgres_database_date_column: "month_id"
oracle_database_date_column: "month_id"
aws_database_date_column: "month_id"
ali_database_date_column: "month_id"
clickhouse_database_date_column: "month_id"

# Manual Queries (only if is_using_manual_queries: yes)
queries:
  first_query: |
    SELECT column1, column2 FROM table1 WHERE condition
  second_query: |
    SELECT column1, column2 FROM table2 WHERE condition
```

## Validation Workflow

### Step-by-Step Process

1. **Configuration Setup**
   - Set up database credentials in `creds/` directory
   - Configure validation parameters in `config.yaml`

2. **Data Extraction**
   - The tool connects to both specified databases
   - Executes queries to extract data based on configuration
   - Uses batch processing for large datasets
   - Applies date filtering if configured

3. **Data Processing**
   - Normalizes data types across databases
   - Creates composite IDs if multiple ID columns are specified
   - Sorts data by ID for efficient comparison

4. **Validation Logic**
   - **Missing ID Detection**: Identifies IDs present in one database but missing in the other
   - **Value Comparison**: Compares values for matching IDs using appropriate logic:
     - **Integer**: Direct comparison with NaN handling
     - **String**: Fuzzy matching with configurable similarity threshold
     - **Date**: Date comparison with NaT (Not a Time) handling

5. **Result Generation**
   - Creates summary CSV with missing IDs and differing values
   - Generates detailed CSV with row-by-row discrepancies
   - Saves results to `output/result/` directory

### Running the Validation

#### Method 1: Using Jupyter Notebook

1. **Open the notebook:**
   ```bash
   jupyter notebook validation.ipynb
   ```

2. **Install dependencies** (if not already installed):
   ```python
   !pip install -r requirements.txt
   ```

3. **Run the configuration:**
   ```python
   !python config.py config.yaml
   ```

4. **Execute the validation:**
   - Run the remaining cells in the notebook

#### Method 2: Command Line

1. **Run directly with Python:**
   ```bash
   python config.py config.yaml
   ```

2. **Or run the main validation script** (loads the config itself; defaults to `config.yaml`):
   ```bash
   python running_validation.py config.yaml
   ```

#### Monitoring a run live

Every run writes progress to a timestamped log file. Follow it in another terminal:

```bash
tail -f "$(ls -t logs/data_validation_*.log | head -1)"
```

For long, chunked runs you will see one checkpoint line per chunk, e.g.:

```
Chunked validation: id range [1, 60859676], chunk_size=2000000, total_chunks=31
[checkpoint] chunk 1/31 id[1-2000000] rows mysql=2000000 clickhouse=2000000 | this chunk: missing_mysql+=0 missing_clickhouse+=0 diff+=0 | running totals: missing_mysql=0 missing_clickhouse=0 diff=0
...
✅ Chunked validation done. missing_in_mysql=0, missing_in_clickhouse=13, differing_values=0
```

## Understanding the Output

### Output Files

The validation generates two output files in `output/result/`. File names embed both
table names, the checked column, and a run timestamp, so successive runs never
overwrite each other:

```
output_<db1>_<table1>_vs_<db2>_<table2>_<column>_<YYYYMMDD_HHMMSS>_result.csv
output_<db1>_<table1>_vs_<db2>_<table2>_<column>_<YYYYMMDD_HHMMSS>_result_differing_values.csv
```

1. **Main Results File** (`..._result.csv`):
   - `missing_in_{database1}`: IDs present in the other database but missing here
   - `missing_in_{database2}`: IDs present in the other database but missing here
   - `differing_values`: records whose checked value differs

   > **Note:** these three columns are **independent lists** padded to equal length —
   > do **not** read the file row-wise (the value in row *N* of one column is unrelated
   > to row *N* of another). Use the column-level contents and the detail file below.

2. **Detailed Discrepancies File** (`..._result_differing_values.csv`) — **always created**, with just a header row when there are no differences:
   - `id`: the (composite) ID of the record
   - `{check_column}_{database1}`: value from the first database
   - `{check_column}_{database2}`: value from the second database

### Log Files

Detailed logging is provided in `logs/data_validation_{timestamp}.log`:
- Connection status to each database (passwords/keys are **not** logged)
- Query execution details
- Batch/streaming/chunk progress (with per-chunk checkpoints)
- Validation results summary
- Error messages and troubleshooting information

## Validation Modes (`--mode`)

The chunked path supports two modes, selectable on the command line (this overrides any `mode:` in the YAML):

| Command | What it does |
|---------|--------------|
| `python config.py <cfg> --mode missing` | **Missing-ID only.** Fetches just the id column from both sides and reports IDs present in one database but not the other. Fast and light. |
| `python config.py <cfg> --mode full` | **Missing-ID + value differences.** Also compares column values. **This is the default.** |

### Auto-detecting columns to compare

In `--mode full`, if you **omit** `check_column`, the tool automatically compares **every column common to both tables**, excluding the id column and the ClickHouse/dlt meta columns (`ingested_at`, `version`, `_dlt_load_id`, `_dlt_id`). You no longer need to pick a single column.

- Set `check_column` to restrict the comparison to one column (legacy behaviour).
- Add `exclude_columns: [colA, colB]` to skip specific columns (e.g. noisy timestamps).
- Comparison is **type-aware**: numeric columns compared numerically, datetime columns as timestamps, everything else as strings; `(null, null)` counts as equal.

### Differing-values output (long format)

The detail file lists **one row per differing cell**, so it stays readable no matter how many columns are compared:

```
id,column,value_mysql,value_clickhouse
366403,name,CxuB...==,D2Kn...==
366403,village_id,1671061005.0,0
366403,updated_at,2026-04-23 09:22:24,2026-05-04 04:15:57
```

## Chunked Validation for Large Tables

For tables with tens of millions of rows, loading both sides fully into memory can
exhaust RAM (and ClickHouse `LIMIT/OFFSET` pagination becomes O(n²) and times out).
The **chunked-by-id** path solves both problems.

### How to enable

```yaml
chunk_by_id: yes
id_chunk_size: 2000000        # ids per chunk fetched from each database
clickhouse_final: yes         # apply FINAL on a ClickHouse ReplacingMergeTree
composite_id_columns: ["id"]  # the numeric id column to range over (single column)
# check_column: "activity_id" # OPTIONAL: omit in --mode full to compare ALL common columns
# exclude_columns: ["created_at", "updated_at"]  # OPTIONAL: skip noisy columns
# mode: full                  # OPTIONAL: missing | full (CLI --mode overrides this)
mysql_table_name: "ws_transactions"
clickhouse_table_name: "raw_ws_transactions"
```

> **Memory tip:** `--mode full` with auto-detected columns fetches *all* columns per
> chunk, which is heavier than a single column. For very wide or very large tables,
> lower `id_chunk_size` (e.g. `500000`) to keep each chunk within RAM.

When `chunk_by_id: yes`, the tool ignores `is_using_manual_queries` and the full-fetch
path. Instead it:

1. Reads `MIN(id)`/`MAX(id)` from both databases to find the global id range.
2. Iterates that range in steps of `id_chunk_size`, issuing `WHERE id BETWEEN lo AND hi`
   against **both** databases (in parallel threads) per chunk.
3. Compares each chunk (missing IDs both ways + value differences) and accumulates totals.
4. Logs a checkpoint per chunk and writes the same two output files at the end.

### Why it is correct across all periods

Chunking is done by **`id`**, never by a date/period column. An id always falls into the
same range on both sides regardless of its `created_at`/period, so an id is **never**
falsely reported as missing just because its timestamp differs between source and target.
This is the key advantage over filtering both sides by a date window.

### Requirements & notes

- The id column (`composite_id_columns[0]`) must be **numeric** to range over.
- Memory stays bounded by `id_chunk_size` (e.g. ~2M rows per chunk), not the full table.
- ClickHouse rows are read with a single **streamed** query per chunk (no `OFFSET`).
- Tune `id_chunk_size` down if memory is tight, or up to reduce the number of round-trips.

## Advanced Configuration

### Custom Queries

For complex validation scenarios, you can use custom SQL queries:

1. Set `is_using_manual_queries: yes` in `config.yaml`
2. Define custom queries in the `queries` section:
   ```yaml
   queries:
     first_query: |
       SELECT id, value_column FROM table1 WHERE complex_condition
     second_query: |
       SELECT id, value_column FROM table2 WHERE complex_condition
   ```

### Composite IDs

For tables with composite primary keys:

1. List all ID columns in `composite_id_columns`:
   ```yaml
   composite_id_columns: ["customer_id", "order_date", "product_id"]
   ```

2. The tool will automatically create a composite ID by concatenating these columns with underscores

### Fuzzy String Matching

For string data validation with tolerance for minor differences:

1. Set appropriate threshold (0.0 to 1.0):
   ```yaml
   data_type: string
   threshold: 0.85  # 85% similarity required
   ```

2. The tool uses SequenceMatcher to calculate similarity between strings

## Troubleshooting

### Common Issues

1. **Connection Errors**
   - Verify database credentials in `creds/` files
   - Check network connectivity to database servers
   - Ensure required Python packages are installed

2. **Query Errors**
   - Verify table names and column names in `config.yaml`
   - Check SQL syntax for manual queries
   - Ensure date column names match database schema

3. **Memory Issues with Large Datasets**
   - **Enable chunked-by-id validation** (`chunk_by_id: yes`) — the recommended fix for tables with millions of rows; see [Chunked Validation for Large Tables](#chunked-validation-for-large-tables)
   - Lower `id_chunk_size` (chunked mode) to reduce per-chunk memory
   - Reduce `batch_size` in configuration
   - Monitor system memory usage during validation

4. **ClickHouse read timeouts / very slow ClickHouse fetch**
   - This is the classic `LIMIT/OFFSET` problem on large tables; the tool now streams results and supports chunked-by-id processing — enable `chunk_by_id: yes`

5. **Oracle Client Issues**
   - Verify Oracle Instant Client installation
   - Check `ORACLE_HOME` environment variable
   - Ensure cx_Oracle package version compatibility

### Getting Help

- Check the detailed logs in `logs/` directory for error information
- Verify all configuration parameters in `config.yaml`
- Ensure all required dependencies are installed
- Test database connections independently before running validation

## Performance Optimization with uv

### Why Use uv?

`uv` is a fast Python package installer and resolver that provides significant performance improvements:

- **10-100x faster** than pip for package installation
- **Parallel dependency resolution** and downloading
- **Optimized virtual environments** with faster activation
- **Better caching** and package management
- **Improved dependency resolution** algorithms

### uv Performance Benefits for Data Validation

When working with large datasets and multiple database connections, `uv` can significantly improve:

1. **Faster startup times** for validation scripts
2. **Quicker dependency resolution** for complex package requirements
3. **Improved virtual environment creation** and management
4. **Better memory usage** during package installation

### uv Commands for Development

```bash
# Create and activate virtual environment
uv venv
source .venv/bin/activate  # Linux/macOS
# or
.venv\Scripts\activate     # Windows

# Install dependencies with uv
uv pip install -r requirements.txt

# Install development dependencies
uv pip install -e ".[dev]"

# Update all dependencies
uv pip compile pyproject.toml --output-file requirements.txt

# Check for outdated packages
uv pip list --outdated
```

### Traditional pip Alternative

If you prefer to use pip, the project still supports traditional installation:

```bash
# Create virtual environment with venv
python -m venv .venv
source .venv/bin/activate  # Linux/macOS
# or
.venv\Scripts\activate     # Windows

# Install dependencies
pip install -r requirements.txt
```

## Best Practices

1. **Security**: Never commit credential files to version control
2. **Performance**: Use appropriate batch sizes for your system memory
3. **Data Quality**: Always validate a small sample before processing large datasets
4. **Monitoring**: Check log files regularly during long-running validations
5. **Backup**: Keep backups of original data before making any corrections based on validation results
6. **Package Management**: Use `uv` for faster dependency management and virtual environment creation

## Contributing

To contribute to this project:

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests for new functionality
5. Submit a pull request

## License

This project is licensed under the MIT License - see the LICENSE file for details.
