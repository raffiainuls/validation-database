# Setup Guide: Enhanced Data Validation Project

## Quick Start with uv (Recommended)

### 1. Install uv (if not already installed)
```bash
# macOS/Linux
curl -LsSf https://astral.sh/uv/install.sh | sh

# Windows (PowerShell)
irm https://astral.sh/uv/install.ps1 | iex
```

### 2. Setup the Project
```bash
# Clone the repository
git clone https://github.com/raffiainuls/validation-database.git
cd validation-database

# Create virtual environment and install dependencies
uv venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# Install all dependencies
uv pip install pandas boto3 pyodps psycopg2-binary cx_Oracle clickhouse-connect PyYAML
```

### 3. Install PostgreSQL (for psycopg2)
```bash
# macOS
brew install postgresql

# Ubuntu/Debian
sudo apt-get install postgresql postgresql-contrib libpq-dev

# CentOS/RHEL
sudo yum install postgresql postgresql-server postgresql-devel
```

### 4. Verify Installation
```bash
# Test all imports
python -c "
import pandas, boto3, pyodps, psycopg2, cx_Oracle, clickhouse_connect, yaml
print('✅ All dependencies imported successfully!')
print(f'pandas: {pandas.__version__}')
"
```

## Project Structure

```
validation-database/
├── readme.md              # Main documentation
├── SETUP_GUIDE.md         # This file
├── config.py             # Configuration loader
├── running_validation.py # Main validation logic
├── validation.ipynb      # Jupyter notebook
├── config.yaml           # Configuration file
├── pyproject.toml        # Modern Python project config
├── requirements.txt      # Traditional pip dependencies
├── creds/                # Database credentials
│   ├── aws.json         # AWS credentials
│   ├── oracle.json      # Oracle credentials
│   ├── postgres.json    # PostgreSQL credentials
│   ├── alibaba.json     # Alibaba credentials
│   └── clickhouse.json  # ClickHouse credentials
├── output/              # Validation results
└── logs/                # Log files
```

## Supported Databases

The enhanced project now supports **5 databases**:

1. **AWS Athena** - For cloud data warehouse validation
2. **Alibaba MaxCompute** - For Alibaba cloud data validation
3. **PostgreSQL** - For relational database validation
4. **Oracle** - For enterprise database validation
5. **ClickHouse** - For analytical database validation (NEW!)

## Performance Benefits with uv

Using `uv` provides significant performance improvements:

- **10-100x faster** package installation compared to pip
- **Parallel dependency resolution** and downloading
- **Optimized virtual environments** with faster activation
- **Better caching** and package management
- **Improved dependency resolution** algorithms

## Quick Validation Example

1. **Configure databases** in `creds/` directory
2. **Set up config.yaml** with your validation parameters
3. **Run validation**:
   ```bash
   python config.py config.yaml
   ```

## Troubleshooting

### Common Issues

1. **psycopg2 import errors**: Ensure PostgreSQL development headers are installed
2. **ClickHouse import errors**: Use compatible version (0.6.x for Python 3.8)
3. **Oracle client issues**: Install Oracle Instant Client and set ORACLE_HOME

### Getting Help

- Check the detailed logs in `logs/` directory
- Verify all configuration parameters in `config.yaml`
- Test database connections independently before running validation