import pandas as pd
import mysql.connector
import clickhouse_connect
import yaml

# Load credentials
with open('creds/mysql.json', 'r') as f:
    mysql_creds = yaml.safe_load(f)

with open('creds/clickhouse.json', 'r') as f:
    clickhouse_creds = yaml.safe_load(f)

# IDs that are showing as missing
missing_mysql_ids = ['81962', '81963']
missing_clickhouse_ids = ['81907', '81908', '81910', '81911', '81912', '81966', '81967']

print("=== DEBUGGING MISSING IDs ===")

# Test MySQL connection and check for missing IDs
print("\n1. Checking MySQL for missing IDs...")
try:
    mysql_conn = mysql.connector.connect(
        host=mysql_creds['hostname_mysql'],
        port=mysql_creds['port_mysql'],
        database=mysql_creds['database_mysql'],
        user=mysql_creds['username_mysql'],
        password=mysql_creds['password_mysql']
    )
    
    cursor = mysql_conn.cursor()
    
    for missing_id in missing_mysql_ids:
        query = f"SELECT id, asset_model_id FROM asset_inventories WHERE id = {missing_id}"
        cursor.execute(query)
        result = cursor.fetchall()
        
        if result:
            print(f"   MySQL: ID {missing_id} FOUND - Value: {result[0]}")
        else:
            print(f"   MySQL: ID {missing_id} NOT FOUND")
    
    cursor.close()
    mysql_conn.close()
    
except Exception as e:
    print(f"   MySQL connection error: {e}")

# Test ClickHouse connection and check for missing IDs
print("\n2. Checking ClickHouse for missing IDs...")
try:
    clickhouse_client = clickhouse_connect.get_client(
        host=clickhouse_creds['host_clickhouse'],
        port=clickhouse_creds['port_clickhouse'],
        database=clickhouse_creds['database_clickhouse'],
        username=clickhouse_creds['username_clickhouse'],
        password=clickhouse_creds['password_clickhouse']
    )
    
    for missing_id in missing_clickhouse_ids:
        query = f"SELECT id, asset_model_id FROM raw_asset_inventories WHERE id = {missing_id}"
        result = clickhouse_client.query(query)
        
        if result.result_rows:
            print(f"   ClickHouse: ID {missing_id} FOUND - Value: {result.result_rows[0]}")
        else:
            print(f"   ClickHouse: ID {missing_id} NOT FOUND")
    
    clickhouse_client.close()
    
except Exception as e:
    print(f"   ClickHouse connection error: {e}")

# Test the actual queries being used in validation
print("\n3. Testing actual validation queries...")
try:
    # Test MySQL query
    mysql_conn = mysql.connector.connect(
        host=mysql_creds['hostname_mysql'],
        port=mysql_creds['port_mysql'],
        database=mysql_creds['database_mysql'],
        user=mysql_creds['username_mysql'],
        password=mysql_creds['password_mysql']
    )
    
    cursor = mysql_conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM asset_inventories")
    total_mysql = cursor.fetchone()[0]
    print(f"   MySQL total records: {total_mysql}")
    
    # Check if missing IDs exist in the actual table
    for missing_id in missing_mysql_ids:
        cursor.execute(f"SELECT COUNT(*) FROM asset_inventories WHERE id = {missing_id}")
        count = cursor.fetchone()[0]
        print(f"   MySQL: ID {missing_id} exists in table: {count > 0}")
    
    cursor.close()
    mysql_conn.close()
    
    # Test ClickHouse query
    clickhouse_client = clickhouse_connect.get_client(
        host=clickhouse_creds['host_clickhouse'],
        port=clickhouse_creds['port_clickhouse'],
        database=clickhouse_creds['database_clickhouse'],
        username=clickhouse_creds['username_clickhouse'],
        password=clickhouse_creds['password_clickhouse']
    )
    
    total_clickhouse = clickhouse_client.query("SELECT COUNT(*) FROM raw_asset_inventories").result_rows[0][0]
    print(f"   ClickHouse total records: {total_clickhouse}")
    
    # Check if missing IDs exist in the actual table
    for missing_id in missing_clickhouse_ids:
        result = clickhouse_client.query(f"SELECT COUNT(*) FROM raw_asset_inventories WHERE id = {missing_id}")
        count = result.result_rows[0][0]
        print(f"   ClickHouse: ID {missing_id} exists in table: {count > 0}")
    
    clickhouse_client.close()
    
except Exception as e:
    print(f"   Error testing queries: {e}")

print("\n=== DEBUGGING COMPLETE ===")