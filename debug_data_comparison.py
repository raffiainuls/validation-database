#!/usr/bin/env python3
"""
Script diagnostic untuk memeriksa data yang sebenarnya di kedua database
dan memahami mengapa hasil missing ID tidak sesuai harapan.
"""

import pandas as pd
import yaml
import mysql.connector
import clickhouse_connect

def load_config():
    with open('config.yaml', 'r') as f:
        return yaml.safe_load(f)

def fetch_mysql_data():
    """Fetch data dari MySQL"""
    with open('creds/mysql.json', 'r') as f:
        mysql_creds = yaml.safe_load(f)
    
    conn = mysql.connector.connect(
        host=mysql_creds['hostname_mysql'],
        port=mysql_creds['port_mysql'],
        database=mysql_creds['database_mysql'],
        user=mysql_creds['username_mysql'],
        password=mysql_creds['password_mysql']
    )
    
    query = """
    SELECT id, asset_model_id
    FROM asset_inventories
    """
    
    cursor = conn.cursor()
    cursor.execute(query)
    columns = [desc[0] for desc in cursor.description]
    all_data = []
    
    while True:
        data = cursor.fetchmany(1000)
        if not data:
            break
        all_data.extend(data)
    
    cursor.close()
    conn.close()
    
    return pd.DataFrame(all_data, columns=columns)

def fetch_clickhouse_data():
    """Fetch data dari ClickHouse"""
    with open('creds/clickhouse.json', 'r') as f:
        ch_creds = yaml.safe_load(f)
    
    client = clickhouse_connect.get_client(
        host=ch_creds['host_clickhouse'],
        port=ch_creds['port_clickhouse'],
        database=ch_creds['database_clickhouse'],
        username=ch_creds['username_clickhouse'],
        password=ch_creds['password_clickhouse']
    )
    
    query = """
    SELECT id, asset_model_id
    FROM raw_asset_inventories final
    """
    
    count_query = f"SELECT COUNT(*) as total FROM ({query}) as subquery"
    count_result = client.query(count_query)
    total_rows = count_result.result_rows[0][0]
    
    all_data = []
    offset = 0
    
    while offset < total_rows:
        batch_query = f"{query} LIMIT 10000 OFFSET {offset}"
        batch_result = client.query(batch_query)
        
        # Fallback for older versions of clickhouse_connect
        try:
            batch_df = batch_result.df()
        except AttributeError:
            batch_df = pd.DataFrame(batch_result.result_rows, columns=batch_result.column_names)
        
        if batch_df.empty:
            break
            
        all_data.append(batch_df)
        offset += 10000
    
    final_df = pd.concat(all_data, ignore_index=True) if all_data else pd.DataFrame()
    return final_df

def analyze_data():
    print("=== ANALYSIS DATA COMPARISON ===")
    
    # Fetch data
    print("Fetching MySQL data...")
    mysql_df = fetch_mysql_data()
    print(f"MySQL data: {len(mysql_df)} rows")
    
    print("Fetching ClickHouse data...")
    ch_df = fetch_clickhouse_data()
    print(f"ClickHouse data: {len(ch_df)} rows")
    
    # Convert ID columns to string for comparison
    mysql_df['id'] = mysql_df['id'].astype(str)
    ch_df['id'] = ch_df['id'].astype(str)
    
    # Get unique IDs
    mysql_ids = set(mysql_df['id'])
    ch_ids = set(ch_df['id'])
    
    print(f"\n=== ID ANALYSIS ===")
    print(f"MySQL unique IDs: {len(mysql_ids)}")
    print(f"ClickHouse unique IDs: {len(ch_ids)}")
    
    # Find common IDs
    common_ids = mysql_ids.intersection(ch_ids)
    print(f"Common IDs: {len(common_ids)}")
    
    # Find missing IDs
    missing_in_ch = mysql_ids - ch_ids
    missing_in_mysql = ch_ids - mysql_ids
    
    print(f"\n=== MISSING ID ANALYSIS ===")
    print(f"IDs in MySQL but NOT in ClickHouse: {len(missing_in_ch)}")
    print(f"IDs in ClickHouse but NOT in MySQL: {len(missing_in_mysql)}")
    
    # Show some examples
    print(f"\n=== EXAMPLES ===")
    if common_ids:
        print(f"First 5 common IDs: {list(common_ids)[:5]}")
    
    if missing_in_ch:
        print(f"First 5 IDs missing in ClickHouse: {list(missing_in_ch)[:5]}")
    
    if missing_in_mysql:
        print(f"First 5 IDs missing in MySQL: {list(missing_in_mysql)[:5]}")
    
    # Check if there are any overlapping IDs
    if len(common_ids) > 0:
        print(f"\n=== OVERLAP ANALYSIS ===")
        print(f"Overlap percentage: {len(common_ids) / len(mysql_ids) * 100:.2f}%")
        
        # Check values for common IDs
        mysql_common = mysql_df[mysql_df['id'].isin(common_ids)]
        ch_common = ch_df[ch_df['id'].isin(common_ids)]
        
        # Merge to compare values
        merged = pd.merge(
            mysql_common[['id', 'asset_model_id']],
            ch_common[['id', 'asset_model_id']],
            on='id',
            suffixes=('_mysql', '_clickhouse')
        )
        
        # Check for differing values
        differing = merged[merged['asset_model_id_mysql'] != merged['asset_model_id_clickhouse']]
        print(f"Records with differing values: {len(differing)}")
        
        if len(differing) > 0:
            print("First 5 differing records:")
            print(differing.head())
    else:
        print("\n=== NO OVERLAP ===")
        print("Tidak ada ID yang sama di kedua database!")

if __name__ == "__main__":
    analyze_data()