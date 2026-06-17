# Data Validator Documentation

This repository provides a comprehensive data validation tool designed to ensure data consistency across multiple database systems. The tool was created based on real-world experience working on a data migration project from AWS Athena to Alibaba MaxCompute, where frequent data discrepancies were encountered.

## Project Overview

This validation tool is designed to:
- Validate and identify missing IDs between different database systems (AWS, Alibaba, Postgres, Oracle)
- Detect discrepancies in values based on unique IDs or composite IDs
- Support multiple data types (integer, string, date) with appropriate validation logic
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
   uv pip install pandas boto3 pyodps psycopg2-binary cx_Oracle clickhouse-connect PyYAML
   ```

#### Option 2: Using pip (Traditional)
1. **Clone the repository:**
   ```bash
   git clone https://github.com/raffiainuls/validation-database.git
   cd validation-database
   ```

2. **Install Python dependencies:**
   ```bash
   pip install pandas boto3 pyodps psycopg2-binary cx_Oracle clickhouse-connect PyYAML
   ```

3. **Install Oracle Client** (for Oracle database support):
   - Download and install Oracle Instant Client from [Oracle's website](https://www.oracle.com/database/technologies/instant-client/downloads.html)
   - Set the `ORACLE_HOME` environment variable to point to the Oracle client installation directory

## Project Structure

```
validation-database/
├── readme.md              # This documentation file
├── config.py             # Configuration loader and credential manager
├── running_validation.py # Main validation logic and data processing
├── validation.ipynb      # Jupyter notebook for running validations
├── config.yaml           # Main configuration file
├── docs.md              # Additional documentation
├── creds/               # Directory for database credentials
│   ├── aws.json         # AWS credentials
│   ├── oracle.json      # Oracle credentials
│   ├── postgres.json    # PostgreSQL credentials
│   └── alibaba.json     # Alibaba credentials (ali.json)
├── output/              # Directory for validation results
│   └── result/          # CSV output files
└── logs/                # Log files directory
```

## Configuration Guide

### 1. Database Credentials Setup

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
database1: postgres          # First database to compare (postgres, oracle, aws, ali, clickhouse)
database2: oracle           # Second database to compare (postgres, oracle, aws, ali, clickhouse)
databases: ["oracle", "postgres"]  # List of databases to validate
data_type: string           # Data type: integer, string, or date
is_using_manual_queries: no # Use manual queries or auto-generate (yes/no)
batch_size: 10000          # Batch size for processing large datasets
threshold: 1              # Fuzzy matching threshold for string data (0.0 to 1.0)

# Date Filtering (optional)
# start_date: "2014-01-01"
# end_date: "2024-12-30"

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

2. **Or run the main validation script:**
   ```bash
   python running_validation.py
   ```

## Understanding the Output

### Output Files

The validation generates two main output files in `output/result/`:

1. **Main Results File** (`output_{database1}_{database2}_{check_column}_result.csv`):
   - `missing_in_{database1}`: IDs missing in the first database
   - `missing_in_{database2}`: IDs missing in the second database
   - `differing_values`: Detailed information about value discrepancies

2. **Detailed Discrepancies File** (`output_{database1}_{database2}_{check_column}_result.csv_differing_values.csv`):
   - `id`: The composite ID of the record
   - `{check_column}_{database1}`: Value from the first database
   - `{check_column}_{database2}`: Value from the second database

### Log Files

Detailed logging is provided in `logs/data_validation_{timestamp}.log`:
- Connection status to each database
- Query execution details
- Batch processing progress
- Validation results summary
- Error messages and troubleshooting information

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
   - Reduce `batch_size` in configuration
   - Monitor system memory usage during validation
   - Consider running validation on smaller date ranges

4. **Oracle Client Issues**
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
