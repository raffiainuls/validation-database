import pandas as pd
import mysql.connector
import clickhouse_connect
import yaml

# Load credentials
with open('creds/mysql.json', 'r') as f:
    mysql_creds = yaml.safe_load(f)

with open('creds/clickhouse.json', 'r') as f:
    clickhouse_creds = yaml.safe_load(f)

# IDs that are showing as missing in ClickHouse
missing_clickhouse_ids = ['81907', '81908', '81910', '81911', '81912', '81966', '81967']

print("=== DEBUGGING COMPOSITE ID ISSUE ===")

# Test what the auto-generated query would be
print("\n1. Testing auto-generated composite ID queries...")

# MySQL query (should be: COALESCE(CAST(id AS CHAR), '0') AS id)
print("\nMySQL auto-generated query:")
mysql_query = """
SELECT 
    COALESCE(CAST(id AS CHAR), '0') AS id,
    asset_type_id
FROM asset_inventories
"""
print(mysql_query)

# ClickHouse query (should be: CAST(COALESCE(CAST(id AS String), '0') AS String) AS id)
print("\nClickHouse auto-generated query:")
clickhouse_query = """
SELECT 
    CAST(COALESCE(CAST(id AS String), '0') AS String) AS id,
    asset_type_id
FROM raw_asset_inventories
"""
print(clickhouse_query)

# Test the actual data with these queries
print("\n2. Testing actual data with auto-generated queries...")

try:
    # Test MySQL with auto-generated query
    mysql_conn = mysql.connector.connect(
        host=mysql_creds['hostname_mysql'],
        port=mysql_creds['port_mysql'],
        database=mysql_creds['database_mysql'],
        user=mysql_creds['username_mysql'],
        password=mysql_creds['password_mysql']
    )
    
    mysql_df = pd.read_sql(mysql_query, mysql_conn)
    print(f"MySQL result shape: {mysql_df.shape}")
    print(f"MySQL columns: {list(mysql_df.columns)}")
    
    # Check if missing IDs exist in MySQL result
    mysql_ids = set(str(x) for x in mysql_df['id'].tolist())
    print(f"\nChecking missing ClickHouse IDs in MySQL result:")
    for missing_id in missing_clickhouse_ids:
        exists = missing_id in mysql_ids
        print(f"   ID {missing_id} in MySQL result: {exists}")
    
    mysql_conn.close()
    
    # Test ClickHouse with auto-generated query
    clickhouse_client = clickhouse_connect.get_client(
        host=clickhouse_creds['host_clickhouse'],
        port=clickhouse_creds['port_clickhouse'],
        database=clickhouse_creds['database_clickhouse'],
        username=clickhouse_creds['username_clickhouse'],
        password=clickhouse_creds['password_clickhouse']
    )
    
    clickhouse_result = clickhouse_client.query(clickhouse_query)
    try:
        clickhouse_df = clickhouse_result.df()
    except AttributeError:
        # Fallback for older versions of clickhouse_connect
        clickhouse_df = pd.DataFrame(clickhouse_result.result_rows, columns=clickhouse_result.column_names)
    
    print(f"\nClickHouse result shape: {clickhouse_df.shape}")
    print(f"ClickHouse columns: {list(clickhouse_df.columns)}")
    
    # Check if missing IDs exist in ClickHouse result
    clickhouse_ids = set(str(x) for x in clickhouse_df['id'].tolist())
    print(f"\nChecking missing ClickHouse IDs in ClickHouse result:")
    for missing_id in missing_clickhouse_ids:
        exists = missing_id in clickhouse_ids
        print(f"   ID {missing_id} in ClickHouse result: {exists}")
    
    clickhouse_client.close()
    
    # Test raw data without composite ID formatting
    print("\n3. Testing raw data without composite ID formatting...")
    
    # Raw MySQL query
    raw_mysql_query = "SELECT id, asset_type_id FROM asset_inventories"
    raw_mysql_df = pd.read_sql(raw_mysql_query, mysql_conn)
    print(f"Raw MySQL result shape: {raw_mysql_df.shape}")
    
    # Raw ClickHouse query  
    raw_clickhouse_result = clickhouse_client.query("SELECT id, asset_type_id FROM raw_asset_inventories")
    raw_clickhouse_df = raw_clickhouse_result.df()
    print(f"Raw ClickHouse result shape: {raw_clickhouse_df.shape}")
    
    # Compare composite vs raw IDs
    print(f"\n4. Comparing composite vs raw IDs...")
    print(f"MySQL composite IDs sample: {mysql_df['id'].head(5).tolist()}")
    print(f"MySQL raw IDs sample: {raw_mysql_df['id'].head(5).tolist()}")
    print(f"ClickHouse composite IDs sample: {clickhouse_df['id'].head(5).tolist()}")
    print(f"ClickHouse raw IDs sample: {raw_clickhouse_df['id'].head(5).tolist()}")
    
except Exception as e:
    print(f"Error during testing: {e}")

print("\n=== DEBUGGING COMPLETE ===")