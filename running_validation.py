import pandas as pd 
import boto3
from odps import ODPS 
import yaml 
import logging
from concurrent.futures import ThreadPoolExecutor
from difflib import SequenceMatcher
import os 
import psycopg2
from datetime import datetime
import cx_Oracle
import clickhouse_connect
import mysql.connector


# setup logging

# Buat timestamp dengan format YYYYMMDD_HHMMSS
timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
log_directory = "logs"
os.makedirs(log_directory, exist_ok=True)
# Gabungkan timestamp ke dalam nama file log
log_filename = f"{log_directory}/data_validation_{timestamp}.log"

logging.basicConfig(
    filename=log_filename,
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)


def fetch_data_postgres(query, host, port, database, user, password, batch_size=1000):
    print("try to connect database postgres")
    logging.info("try to connect database postgres")
    logging.info(f"hostname: {host}")
    logging.info(f"port: {port}")
    logging.info(f"database: {database}")
    logging.info(f"username: {user}")
    logging.info(f"Execute with query: ")
    logging.info(f"{query}")
    try:
        # Membuat koneksi ke PostgreSQL
        conn = psycopg2.connect(
            host=host,
            port=port,
            database=database,
            user=user,
            password=password
        )
        logging.info("Connected to PostgreSQL database.")
        print("Connected to PostgreSQL database.")

        # Menjalankan query dan membaca hasil secara bertahap
        with conn.cursor() as cursor:
            logging.info("Executing query on PostgreSQL...")
            cursor.execute(query)
            columns = [desc[0] for desc in cursor.description]  # Mendapatkan nama kolom
            all_data = []
            batch_counter = 1

            while True:
                data = cursor.fetchmany(batch_size)  # Mengambil batch data
                if not data:
                    break
                all_data.extend(data)  # Menambahkan data ke list
                logging.info(f"Fetched Batch {batch_counter}, rows so far: {len(all_data)}.")
                print(f"Fetched Batch {batch_counter}, rows so far: {len(all_data)}.")
                batch_counter += 1
        
        # Menutup koneksi
        conn.close()
        logging.info("Connection to PostgreSQL closed.")
        print("Connection to PostgreSQL closed.")
        
        return pd.DataFrame(all_data, columns=columns)

    except Exception as e:
        logging.error(f"Error fetching data from PostgreSQL: {str(e)}")
        raise

def fetch_data_oracle(query, dsn, user, password, batch_size):
     print("Try to connect database oracle....")
     logging.info("Try to connect database oracle....")
     logging.info(f"dsn: {dsn}")
     logging.info(f"username: {user}")
     logging.info(f"batch_size: {batch_size}")
     logging.info("execute with query:  ")
     logging.info(f"{query}")
     
     try:
          # create connection to oracle
          conn = cx_Oracle.connect(user=user, password=password, dsn=dsn)
          logging.info("Connected to Oracle database")
          print("Connected to Oracle database")

          # running query batching 
          with conn.cursor() as cursor:
               logging.info("Executing query on Oracle ....")
               cursor.execute(query)
               columns = [desc[0] for desc in cursor.description]
               all_data = []
               batch_counter = 1 

               while True:
                    data = cursor.fetchmany(batch_size)
                    if not data:
                         break
                    all_data.extend(data)
                    logging.info(f"Fetched Batch {batch_counter}, rows so far: {len(all_data)}.")
                    print(f"Fetched Batch {batch_counter}, rows so far: {len(all_data)}.")
                    batch_counter += 1
            
          conn.close()
          logging.info("Connection to Oracle closed.")
          print("Connection to Oracle closed.")

          return pd.DataFrame(all_data, columns=columns)
     
     except Exception as e:
          logging.error(f"Error fetching data from Oracle: {str(e)}")
          raise

def fetch_data_aws(query, database, output_location, region_name, aws_access_key_id, aws_secret_access_key, batch_size=None):
    print("Try to connect AWS Athnea.......")
    logging.info("Try to connect AWS Athena....")
    logging.info(f"Database: {database}")
    logging.info(f"output_location: {output_location}")
    logging.info(f"region_name: {region_name}")
    logging.info("Execute Using Query:  ")
    logging.info(f"{query}")

    client = boto3.client(
        'athena',
        region_name= region_name,
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key
    )
    logging.info("Starting AWS Athena query execution.....")
    print("Starting AWS Athena query execution......")

    response = client.start_query_execution(
        QueryString= query,
        QueryExecutionContext={'Database':database},
        ResultConfiguration={'OutputLocation': output_location}
    )
    query_execution_id = response['QueryExecutionId']
    logging.info(f"AWS query execution started with QueryExecutionId: {query_execution_id}. Polling for completion......")
    print(f"AWS query execution started with QueryExecutionId: {query_execution_id}. Polling for completion......")

    while True:
        status = client.get_query_execution(QueryExecutionId=query_execution_id)
        state = status['QueryExecution']['Status']['State']
        if state in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
            break

    if state != 'SUCCEEDED':
        raise Exception(f"AWS Athena Query {state}: {status['QueryExecution']['Status']['StateChangeReason']}")
    logging.info("AWS Athena query succeeded, Fetching results....")
    print("AWS Athena query succeeded. Fetching results.....")

    rows = []
    next_token = None
    batch_counter = 1
    while True:
        result = client.get_query_results(QueryExecutionId=query_execution_id, NextToken=next_token) if next_token else client.get_query_results(QueryExecutionId=query_execution_id)
        rows.extend(result['ResultSet']['Rows'])
        logging.info(f"Fetched Batch {batch_counter}, rows so far: {len(rows)}.")
        print(f"Fetched Batch {batch_counter}, rows so far: {len(rows)}.")
        batch_counter += 1
        next_token = result.get('NextToken')
        if not next_token:
            break
    
    headers = [col['VarCharValue'] for col in rows[0]['Data']]
    data = [[col.get('VarCharValue') for col in row['Data']] for row in rows[1:]]
    logging.info(f"Fineshed fetching AWS data. Total rows fetched: {len(data)}.")
    print(f"Fineshed fetching AWS data. Total rows fetched: {len(data)}.")
    return pd.DataFrame(data, columns=headers)

def fetch_data_alicloud(query, access_id, access_key, project_name, endpoint, batch_size=1000000):
    print("Try to connect Alibaba Max Compute....")
    logging.info("Try to connect Alibaba Max Compute.....")
    logging.info(f"Project Name: {project_name}")
    logging.info(f"Endpoint: {endpoint}")
    logging.info(f"Batch Size: {batch_size}")
    logging.info("Execute Using Query: ")
    logging.info(f"{query}")
    o = ODPS(access_id, access_key, project_name, endpoint=endpoint)
    logging.info("Starting Alibaba ODPS query execution.....")
    print("Starting Alibaba ODPS query execution.....")

    with o.execute_sql(query).open_reader() as reader:
        total_rows = reader.count
        logging.info(f"Alibaba query succeeded. Total rows to fetch: {total_rows}.")
        print(f"Alibaba query succeeded. Total rows to fetch: {total_rows}.")
        all_data = []
        batch_counter = 1
        for i in range(0, total_rows, batch_size):
            batch_data = reader[i:i+batch_size].to_pandas()
            logging.info(f"Fetched Batch {batch_counter}, rows so far: {len(batch_data)}.")
            print(f"Fetched Batch {batch_counter}, rows so far: {len(batch_data)}.")
            all_data.append(batch_data)
            batch_counter +=1
    
    result_df = pd.concat(all_data, ignore_index=True)
    logging.info(f"Finished fetching Alibaba data. Total rows fetched: {len(result_df)}.")
    print(f"Finished fetching Alibaba data. Total rows fetched: {len(result_df)}.")
    return result_df

def fetch_data_clickhouse(query, host, port, database, user, password, batch_size):
    print("Try to connect ClickHouse database....")
    logging.info("Try to connect ClickHouse database....")
    logging.info(f"host: {host}")
    logging.info(f"port: {port}")
    logging.info(f"database: {database}")
    logging.info(f"user: {user}")
    logging.info(f"batch_size: {batch_size}")
    logging.info("Execute Using Query: ")
    logging.info(f"{query}")

    try:
        # Create connection to ClickHouse. A long send/receive timeout keeps the
        # streaming request alive while the server produces large result sets.
        client = clickhouse_connect.get_client(
            host=host,
            port=port,
            database=database,
            username=user,
            password=password,
            connect_timeout=30,
            send_receive_timeout=3600
        )
        logging.info("Connected to ClickHouse database")
        print("Connected to ClickHouse database")

        # Stream the result of the query in native blocks using a single request.
        # This avoids LIMIT/OFFSET pagination (which is O(n^2) on ClickHouse and
        # times out on large tables) and never issues a separate COUNT query.
        logging.info("Executing streaming query on ClickHouse....")
        print("Executing streaming query on ClickHouse....")

        all_data = []
        batch_counter = 1
        rows_so_far = 0

        with client.query_df_stream(query) as stream:
            for batch_df in stream:
                if batch_df.empty:
                    continue
                all_data.append(batch_df)
                rows_so_far += len(batch_df)
                logging.info(f"Fetched Batch Clickhouse {batch_counter}, rows so far: {rows_so_far}.")
                print(f"Fetched Batch Clickhouse {batch_counter}, rows so far: {rows_so_far}.")
                batch_counter += 1

        # Combine all streamed blocks
        final_df = pd.concat(all_data, ignore_index=True) if all_data else pd.DataFrame()
        logging.info(f"Finished fetching ClickHouse data. Total rows fetched: {len(final_df)}.")
        print(f"Finished fetching ClickHouse data. Total rows fetched: {len(final_df)}.")

        return final_df

    except Exception as e:
        logging.error(f"Error fetching data from ClickHouse: {str(e)}")
        raise

def fetch_data_mysql(query, host, port, database, user, password, batch_size=10000000):
    print("Try to connect MySQL database....")
    logging.info("Try to connect MySQL database....")
    logging.info(f"host: {host}")
    logging.info(f"port: {port}")
    logging.info(f"database: {database}")
    logging.info(f"user: {user}")
    logging.info(f"batch_size: {batch_size}")
    logging.info("Execute Using Query: ")
    logging.info(f"{query}")

    try:
        # Create connection to MySQL
        conn = mysql.connector.connect(
            host=host,
            port=port,
            database=database,
            user=user,
            password=password
        )
        logging.info("Connected to MySQL database")
        print("Connected to MySQL database")

        # Execute query and fetch results
        logging.info("Executing query on MySQL....")
        cursor = conn.cursor()
        cursor.execute(query)
        
        # Get column names
        columns = [desc[0] for desc in cursor.description]
        
        # Fetch all data
        all_data = []
        batch_counter = 1
        
        while True:
            data = cursor.fetchmany(batch_size)
            if not data:
                break
            all_data.extend(data)
            logging.info(f"Fetched Batch Mysql {batch_counter}, rows so far: {len(all_data)}.")
            print(f"Fetched Batch Mysql {batch_counter}, rows so far: {len(all_data)}.")
            batch_counter += 1
        
        # Close connection
        cursor.close()
        conn.close()
        
        logging.info(f"MySQL query succeeded. Total rows fetched: {len(all_data)}.")
        print(f"MySQL query succeeded. Total rows fetched: {len(all_data)}.")
        
        return pd.DataFrame(all_data, columns=columns)
     
    except Exception as e:
        logging.error(f"Error fetching data from MySQL: {str(e)}")
        raise

def validate_data_integer(first_df, second_df, check_column, output_filename, id_column, database1, database2):

    logging.info("Starting Validate Data")
    print("Starting Validate Data")
    logging.info("Column Check is Integer, Validate Data Integer Start .......")
    
    # Keep original ID data types, only convert for comparison when needed
    # This preserves the original format while allowing proper numeric comparison
    
    # Ensure check columns are numeric for comparison
    first_df[check_column] = pd.to_numeric(first_df[check_column], errors='coerce')
    second_df[check_column] = pd.to_numeric(second_df[check_column], errors='coerce')
    
    # Log data sizes for debugging
    print(f"First database ({database1}) data size: {len(first_df)} rows")
    print(f"Second database ({database2}) data size: {len(second_df)} rows")
    logging.info(f"First database ({database1}) data size: {len(first_df)} rows")
    logging.info(f"Second database ({database2}) data size: {len(second_df)} rows")

    # Simple validation without batching
    print("Processing validation without batching...")
    logging.info("Processing validation without batching...")
    
    # Initialize results
    missing_in_database1 = []
    missing_in_database2 = []
    differing_values_list = []
    
    # Convert both dataframes to sets for faster lookup
    first_ids_set = set(first_df[id_column].astype(str))
    second_ids_set = set(second_df[id_column].astype(str))
    
    # Find IDs that exist in both databases
    common_ids = first_ids_set.intersection(second_ids_set)
    
    # Find missing IDs in second database (those in first but not in second)
    missing_in_database2 = list(first_ids_set - second_ids_set)
    
    # Find missing IDs in first database (those in second but not in first)
    missing_in_database1 = list(second_ids_set - first_ids_set)
    
    # Find differing values for common IDs.
    # Vectorized with a pandas merge instead of a per-id Python loop, which is
    # orders of magnitude faster on large tables (tens of millions of rows).
    if len(common_ids) > 0:
        col1 = f'{check_column}_{database1}'
        col2 = f'{check_column}_{database2}'

        # Normalized string id as the join key (matches the set logic above).
        # drop_duplicates(keep='last') mirrors dict(zip(...)) last-wins semantics.
        first_norm = first_df[[id_column, check_column]].copy()
        second_norm = second_df[[id_column, check_column]].copy()
        first_norm['_idstr'] = first_norm[id_column].astype(str)
        second_norm['_idstr'] = second_norm[id_column].astype(str)
        first_norm = first_norm.drop_duplicates('_idstr', keep='last')
        second_norm = second_norm.drop_duplicates('_idstr', keep='last')

        merged = pd.merge(
            first_norm[['_idstr', check_column]].rename(columns={check_column: col1}),
            second_norm[['_idstr', check_column]].rename(columns={check_column: col2}),
            on='_idstr', how='inner'
        )

        # Both values present and not equal.
        diff_mask = merged[col1].notna() & merged[col2].notna() & (merged[col1] != merged[col2])
        diff = merged.loc[diff_mask]

        if not diff.empty:
            differing_values_list = pd.DataFrame({
                f'{id_column}_{database1}': diff['_idstr'].values,
                col1: diff[col1].values,
                f'{id_column}_{database2}': diff['_idstr'].values,
                col2: diff[col2].values,
            }).to_dict('records')

    print(f"Found {len(missing_in_database1)} IDs missing in {database1}")
    print(f"Found {len(missing_in_database2)} IDs missing in {database2}")
    print(f"Found {len(differing_values_list)} records with differing values")
    logging.info(f"IDs missing in {database1}: {len(missing_in_database1)}")
    logging.info(f"IDs missing in {database2}: {len(missing_in_database2)}")
    logging.info(f"Differing values count: {len(differing_values_list)}")
    
    print("Processing Validate Missing Ids Done.")
    logging.info("Processing Validate Missing Ids Done.")

    # Keep an unpadded copy of the real differing records for the detail CSV,
    # so the None-padding below (needed only to align summary columns) does not
    # leak into the detail output.
    differing_values_records = list(differing_values_list)

    # menyesuaikan panjang with None or Nan
    max_len = max(len(missing_in_database1), len(missing_in_database2), len(differing_values_list))

    #ensure all list have same length
    missing_in_database1.extend([None] * (max_len - len(missing_in_database1)))
    missing_in_database2.extend([None] * (max_len - len(missing_in_database2)))
    differing_values_list.extend([None] * (max_len - len(differing_values_list)))

    # Create DataFrame for validation results 
    validation_df = pd.DataFrame({
        f'missing_in_{database1}': missing_in_database1,
        f'missing_in_{database2}': missing_in_database2,
        'differing_values': differing_values_list
    })

    print("Processing Validate Data Done")
    logging.info("Processing Validate Data Done")

    print("saving result to csv file ........")

    # save result to csv
    validation_df.to_csv(output_filename, index=False)
    logging.info(f"Validation results saved to {output_filename}.")
    print(f"Validation result saved to ... {output_filename}.")
    

    outputfile_id_differing_values = f"{output_filename[:-4] if output_filename.endswith('.csv') else output_filename}_differing_values.csv"

    # Always create a CSV for differing values (ID and check_column only),
    # writing just the header row when there are no differences.
    if differing_values_records:
        differing_values_df = pd.DataFrame(differing_values_records)
    else:
        differing_values_df = pd.DataFrame(columns=[
            f'{id_column}_{database1}', f'{check_column}_{database1}',
            f'{id_column}_{database2}', f'{check_column}_{database2}'
        ])
    differing_values_df.to_csv(outputfile_id_differing_values, index=False)
    logging.info(f"Id Differing Values csv file save into {outputfile_id_differing_values}")
    print(f"Id Differing Values csv file save into {outputfile_id_differing_values}")


def save_dataframe_to_csv(df, filename):
        try:
            df.to_csv(filename, index=False)
            logging.info(f"DataFrame saved to {filename}.")
        except Exception as e:
            logging.error(f"Error saving DataFrame to {filename}: {str(e)}")
            raise 
    
def fuzzy_match(str1, str2, threshold=0.9):
        """
        Membandingkan dua string dengan algoritma SequenceMatcher. 
        Mengembalikan True jika kemiripan >= threshold.
        """
        if pd.isna(str1) and pd.isna(str2):
             return True 
        if pd.isna(str1) or pd.isna(str2):
             return False 
        similarity  = SequenceMatcher(None, str1, str2).ratio()
        return similarity >= threshold

def validate_data_string(first_df, second_df, check_column, output_filename, id_column, threshold, database1, database2 ):
        print("Starting Validate Data")
        logging.info("Starting Validate Data")
        logging.info("Column Check is String")

        
        first_df[id_column] = first_df[id_column].astype(str)
        second_df[id_column] = second_df[id_column].astype(str)
        # step 1: Validate missing Ids
        print("Processing Find Missing IDs")
        logging.info("Processing Find Missing IDs")
        missing_in_first_database = second_df[~second_df[id_column].isin(first_df[id_column])][id_column].tolist()
        missing_in_second_database = first_df[~first_df[id_column].isin(second_df[id_column])][id_column].tolist()
        print("Processing Missing IDs Done")
        logging.info("Processing Missing IDs Done")


        # step 2: Validate differing values with fuzzy matching
        print("Processing Differing Values Start")
        logging.info("Processing Differing Values Start")
        merged_df = pd.merge(
            first_df[[id_column, check_column]],
            second_df[[id_column, check_column]],
            on=id_column,
            suffixes=(f'_{database1}', f'_{database2}'),
            how= 'inner'
        )

        differing_values = merged_df[
            ~merged_df.apply(
                lambda row: fuzzy_match(row[f'{check_column}_{database1}'], row[f'{check_column}_{database2}'], threshold),
                axis=1
            )
        ] 

        # log results 

        if not differing_values.empty:
            differing_values_list = differing_values.to_dict('records')
        else:
            differing_values_list = []

        max_len = max(len(missing_in_first_database), len(missing_in_second_database), len(differing_values_list))

        # ensure all list have same length 
        missing_in_first_database.extend([None] * (max_len - len(missing_in_first_database)))
        missing_in_second_database.extend([None] * (max_len - len(missing_in_second_database)))
        differing_values_list.extend([None] * (max_len - len(differing_values_list)))

        validation_df = pd.DataFrame({
        f'missing_in_{database1}': missing_in_first_database,
        f'missing_in_{database2}': missing_in_second_database,
        'differing_values': differing_values_list
        })

        print("Processing Validate Data Done")
        logging.info("Processing Validate Data Done")
        print("saving result to csv file ........")
        logging.info("saving result to csv file ........")

        outputfile_id_differing_values = f"{output_filename[:-4] if output_filename.endswith('.csv') else output_filename}_differing_values.csv"

        validation_df.to_csv(output_filename, index=False)
        logging.info(f"Validation results saved to {output_filename}.")
        # Always write the differing-values detail file (header-only if none).
        detail_columns = [id_column, f'{check_column}_{database1}', f'{check_column}_{database2}']
        if not differing_values.empty:
            differing_values_csv = differing_values[detail_columns]
        else:
            differing_values_csv = pd.DataFrame(columns=detail_columns)
        differing_values_csv.to_csv(outputfile_id_differing_values, index=False)
        logging.info(f"Id Differing Values csv file save into {outputfile_id_differing_values}")
        print(f"Id Differing Values csv file save into {outputfile_id_differing_values}")

        
def validate_data_date(first_df, second_df, check_column, output_filename, id_column, database1, database2):

        first_df[id_column] = first_df[id_column].astype(str)
        second_df[id_column] = second_df[id_column].astype(str)

        # konversi column date to datetime 
        first_df[check_column] = pd.to_datetime(first_df[check_column], errors='coerce')
        second_df[check_column] = pd.to_datetime(second_df[check_column], errors='coerce')

        # step 1: validate missing IDs
        missing_in_first_database = second_df[~second_df[id_column].isin(first_df[id_column])][id_column].tolist()
        missing_in_second_database = first_df[~first_df[id_column].isin(second_df[id_column])][id_column].tolist()

        # Step 2: Validate differing values
        merged_df = pd.merge(
            first_df[[id_column, check_column]],
            second_df[[id_column, check_column]],
            on=id_column,
            suffixes=(f'_{database1}', f'_{database2}'),
            how='inner'
        )

        # Filter differing values while ignoring rows where both are NaT
        differing_values = merged_df[
        (merged_df[f'{check_column}_{database1}'] != merged_df[f'{check_column}_{database2}']) &
        ~(merged_df[f'{check_column}_{database1}'].isna() & merged_df[f'{check_column}_{database2}'].isna())
        ]

        # Log results ........


        # Check if differing_values is empty and handle accordingly
        if not differing_values.empty:
            differing_values_list = differing_values.to_dict('records')
        else:
            differing_values_list = []


        # Menyesuaikan panjang dengan None atau NaN
        max_len = max(len(missing_in_first_database), len(missing_in_second_database), len(differing_values_list))

        # Pastikan semua list memiliki panjang yang sama
        missing_in_first_database.extend([None] * (max_len - len(missing_in_first_database)))
        missing_in_second_database.extend([None] * (max_len - len(missing_in_second_database)))
        differing_values_list.extend([None] * (max_len - len(differing_values_list)))

        # Create DataFrame for validation results
        validation_df = pd.DataFrame({
        f'missing_in_{database1}': missing_in_first_database,
        f'missing_in_{database2}': missing_in_second_database,
        'differing_values': differing_values_list
        })

        # Save results to CSV
        validation_df.to_csv(output_filename, index=False)
        logging.info(f"Validation results saved to {output_filename}.")

        outputfile_id_differing_values = f"{output_filename[:-4] if output_filename.endswith('.csv') else output_filename}_differing_values.csv"

        # Always write the differing-values detail file (header-only if none).
        detail_columns = [id_column, f'{check_column}_{database1}', f'{check_column}_{database2}']
        if not differing_values.empty:
            differing_values_csv = differing_values[detail_columns]
        else:
            differing_values_csv = pd.DataFrame(columns=detail_columns)
        differing_values_csv.to_csv(outputfile_id_differing_values, index=False)
        logging.info(f"Id Differing Values csv file save into {outputfile_id_differing_values}")
        print(f"Id Differing Values csv file save into {outputfile_id_differing_values}")

    
def build_minmax_query(database, id_column, table_name, use_final=False):
    """MIN/MAX of the numeric id column, used to derive chunk boundaries."""
    final = " FINAL" if (database == 'clickhouse' and use_final) else ""
    if database == 'postgres':
        return f'SELECT MIN("{id_column}") AS lo, MAX("{id_column}") AS hi FROM {table_name}'
    return f"SELECT MIN({id_column}) AS lo, MAX({id_column}) AS hi FROM {table_name}{final}"


def build_range_query(database, id_column, check_column, table_name, lo, hi, use_final=False):
    """Fetch (id, check_column) for a single id range [lo, hi]. Chunking by id
    keeps memory bounded and is period-agnostic: an id falls in the same range
    on both sides regardless of its created_at, so missing-id detection stays
    correct across all periods."""
    final = " FINAL" if (database == 'clickhouse' and use_final) else ""
    if database == 'postgres':
        return (f'SELECT "{id_column}" AS id, "{check_column}" '
                f'FROM {table_name} WHERE "{id_column}" BETWEEN {lo} AND {hi}')
    return (f"SELECT {id_column} AS id, {check_column} "
            f"FROM {table_name}{final} WHERE {id_column} BETWEEN {lo} AND {hi}")


def compare_chunk(first_df, second_df, check_column, data_type, threshold, database1, database2):
    """Compare one id-range chunk. Returns (missing_in_db1, missing_in_db2,
    differing_records) where differing_records are dicts: id, <col>_<db1>, <col>_<db2>."""
    # The range queries always select exactly two columns aliased (id, check_column).
    first_df.columns = ['id', check_column]
    second_df.columns = ['id', check_column]

    first_df['id'] = first_df['id'].astype(str)
    second_df['id'] = second_df['id'].astype(str)

    s1 = set(first_df['id'])
    s2 = set(second_df['id'])
    missing_in_db2 = list(s1 - s2)   # in first (db1) but not second (db2)
    missing_in_db1 = list(s2 - s1)   # in second (db2) but not first (db1)

    col1 = f'{check_column}_{database1}'
    col2 = f'{check_column}_{database2}'
    f = first_df.drop_duplicates('id', keep='last').rename(columns={check_column: col1})
    s = second_df.drop_duplicates('id', keep='last').rename(columns={check_column: col2})
    merged = pd.merge(f, s, on='id', how='inner')

    if merged.empty:
        return missing_in_db1, missing_in_db2, []

    dt = data_type.lower()
    if dt == 'integer':
        merged[col1] = pd.to_numeric(merged[col1], errors='coerce')
        merged[col2] = pd.to_numeric(merged[col2], errors='coerce')
        mask = merged[col1].notna() & merged[col2].notna() & (merged[col1] != merged[col2])
    elif dt == 'date':
        merged[col1] = pd.to_datetime(merged[col1], errors='coerce')
        merged[col2] = pd.to_datetime(merged[col2], errors='coerce')
        mask = (merged[col1] != merged[col2]) & ~(merged[col1].isna() & merged[col2].isna())
    elif dt == 'string':
        mask = ~merged.apply(lambda r: fuzzy_match(r[col1], r[col2], threshold), axis=1)
    else:
        mask = merged[col1] != merged[col2]

    diff = merged.loc[mask]
    records = []
    if not diff.empty:
        records = pd.DataFrame({
            'id': diff['id'].values,
            col1: diff[col1].values,
            col2: diff[col2].values,
        }).to_dict('records')
    return missing_in_db1, missing_in_db2, records


# ClickHouse / dlt internal columns that should never be compared as data.
CLICKHOUSE_META_COLUMNS = {'ingested_at', 'version', '_dlt_load_id', '_dlt_id'}


def detect_value_columns(fetch_data, database1, table1, database2, table2, id_column, extra_excludes=None):
    """Return the list of columns present in BOTH tables, excluding the id column
    and ClickHouse/dlt meta columns. Order follows the source table."""
    src_cols = list(fetch_data(database1, f"SELECT * FROM {table1} LIMIT 1").columns)
    tgt_cols = set(fetch_data(database2, f"SELECT * FROM {table2} LIMIT 1").columns)
    excludes = set(CLICKHOUSE_META_COLUMNS)
    excludes.add(id_column)
    if extra_excludes:
        excludes.update(extra_excludes)
    return [c for c in src_cols if c in tgt_cols and c not in excludes]


def build_range_query_multi(database, id_column, value_columns, table_name, lo, hi, use_final=False):
    """Build a per-id-range query selecting the id plus the given value columns.
    If value_columns is empty (missing-only mode), only the id is fetched."""
    final = " FINAL" if (database == 'clickhouse' and use_final) else ""
    if database == 'postgres':
        sel = f'"{id_column}" AS id'
        if value_columns:
            sel += ", " + ", ".join(f'"{c}"' for c in value_columns)
        return f'SELECT {sel} FROM {table_name} WHERE "{id_column}" BETWEEN {lo} AND {hi}'
    sel = f"{id_column} AS id"
    if value_columns:
        sel += ", " + ", ".join(value_columns)
    return f"SELECT {sel} FROM {table_name}{final} WHERE {id_column} BETWEEN {lo} AND {hi}"


def _column_diff_mask(a, b, threshold):
    """Type-aware 'values differ' mask for two aligned Series, treating
    (NaN, NaN) / (None, None) as equal. Numeric vs numeric -> numeric compare;
    datetime -> datetime compare; otherwise string compare."""
    from pandas.api.types import is_numeric_dtype, is_datetime64_any_dtype
    if is_numeric_dtype(a) and is_numeric_dtype(b):
        an, bn = pd.to_numeric(a, errors='coerce'), pd.to_numeric(b, errors='coerce')
        return (an != bn) & ~(an.isna() & bn.isna())
    if is_datetime64_any_dtype(a) or is_datetime64_any_dtype(b):
        an, bn = pd.to_datetime(a, errors='coerce'), pd.to_datetime(b, errors='coerce')
        return (an != bn) & ~(an.isna() & bn.isna())
    an, bn = a.astype('string'), b.astype('string')
    return (an != bn) & ~(an.isna() & bn.isna())


def compare_chunk_multi(first_df, second_df, value_columns, mode, threshold, database1, database2):
    """Compare one id-range chunk across (optionally) many columns.

    mode='missing' -> only missing-id detection (value_columns may be empty).
    mode='full'    -> missing-id detection + per-column value differences.

    Returns (missing_in_db1, missing_in_db2, differing_records) where each
    differing record is a long-format dict:
        {id, column, value_<db1>, value_<db2>}.
    """
    first_df.columns = ['id'] + list(value_columns)
    second_df.columns = ['id'] + list(value_columns)
    first_df['id'] = first_df['id'].astype(str)
    second_df['id'] = second_df['id'].astype(str)

    s1 = set(first_df['id'])
    s2 = set(second_df['id'])
    missing_in_db2 = list(s1 - s2)   # in db1 but not db2
    missing_in_db1 = list(s2 - s1)   # in db2 but not db1

    if mode == 'missing' or not value_columns:
        return missing_in_db1, missing_in_db2, []

    f = first_df.drop_duplicates('id', keep='last')
    s = second_df.drop_duplicates('id', keep='last')
    merged = pd.merge(f, s, on='id', how='inner', suffixes=('__s', '__t'))
    if merged.empty:
        return missing_in_db1, missing_in_db2, []

    val1 = f'value_{database1}'
    val2 = f'value_{database2}'
    records = []
    for col in value_columns:
        a = merged[f'{col}__s']
        b = merged[f'{col}__t']
        mask = _column_diff_mask(a, b, threshold)
        if mask.any():
            sub = merged.loc[mask, ['id', f'{col}__s', f'{col}__t']]
            part = pd.DataFrame({
                'id': sub['id'].values,
                'column': col,
                val1: sub[f'{col}__s'].values,
                val2: sub[f'{col}__t'].values,
            })
            records.extend(part.to_dict('records'))
    return missing_in_db1, missing_in_db2, records


def main(config):
    # Reuse credentials already loaded by config.py if present, otherwise
    # load each database's credential file from the creds/ directory.
    credentials = dict(config.get('credentials') or {})

    for db in ['mysql', 'clickhouse', 'postgres', 'oracle', 'aws', 'ali']:
        if db in credentials:
            continue
        try:
            with open(f'creds/{db}.json', 'r') as f:
                credentials[db] = yaml.safe_load(f)
        except FileNotFoundError:
            pass
    databases_to_check = config.get('databases', [])
    batch_size = config.get('batch_size', 50000)  # Increase default batch size for large datasets
    output_directory = config.get('output_directory', './output')
    os.makedirs(output_directory, exist_ok=True)
    data_type = config.get('data_type')
    threshold = config.get('threshold',1)
    
    output_summary_subdirectory = os.path.join(output_directory, "result")
    os.makedirs(output_summary_subdirectory, exist_ok=True)

    # Prepare composite ID expression dynamically
    composite_columns = config['composite_id_columns']
    
    # Handle single column case to avoid unnecessary composite formatting
    if len(composite_columns) == 1:
        single_col = composite_columns[0]
        id_expr_templates = {
            'aws': f"CAST(COALESCE(CAST({single_col} AS VARCHAR), '0') AS VARCHAR)",
            'ali': f"CAST(COALESCE(CAST({single_col} AS STRING), '0') AS STRING)",
            'postgres': f"CAST(COALESCE(CAST(\"{single_col}\" AS VARCHAR), '0') AS VARCHAR)",
            'oracle': f"CAST(COALESCE(CAST({single_col} AS VARCHAR2(255)), '0') AS VARCHAR2(255))",
            'clickhouse': f"CAST(COALESCE(CAST({single_col} AS String), '0') AS String)",
            'mysql': f"COALESCE(CAST({single_col} AS CHAR), '0')"
        }
    else:
        # Multiple columns - use composite logic
        id_expr_templates = {
            'aws': " || '_' || ".join([f"CAST(COALESCE(CAST({col} AS VARCHAR), '0') AS VARCHAR)" for col in composite_columns]),
            'ali': " || '_' || ".join([f"CAST(COALESCE(CAST({col} AS STRING), '0') AS STRING)" for col in composite_columns]),
            'postgres': " || '_' || ".join([f"CAST(COALESCE(CAST(\"{col}\" AS VARCHAR), '0') AS VARCHAR)" for col in composite_columns]),
            'oracle': " || '_' || ".join([f"CAST(COALESCE(CAST({col} AS VARCHAR2(255)), '0') AS VARCHAR2(255))" for col in composite_columns]),
            'clickhouse': " || '_' || ".join([f"CAST(COALESCE(CAST({col} AS String), '0') AS String)" for col in composite_columns]),
            'mysql': "CONCAT_WS('_', " + ", ".join([f"COALESCE(CAST({col} AS CHAR), '0')" for col in composite_columns]) + ")"
        }
    
    def construct_query(database, table_name, date_column=None):
        """
        Generate query for the given database and table based on its specific syntax.
        """
        # Check if manual queries are being used
        is_manual = config.get('is_using_manual_queries', 'no')
        print(f"DEBUG: is_using_manual_queries = {is_manual} (type: {type(is_manual)})")
        print(f"DEBUG: str(is_manual).lower() = {str(is_manual).lower()}")
        if str(is_manual).lower() in ['yes', 'true']:
            print(f"DEBUG: Using manual queries for {database}")
            if database == database1:
                manual_query = config['queries']['first_query'].strip()
                print(f"DEBUG: Manual query for {database1}: {manual_query}")
                # Add ORDER BY to manual query
                if 'ORDER BY' not in manual_query.upper():
                    manual_query = f"{manual_query} ORDER BY id"
                return manual_query
            elif database == database2:
                manual_query = config['queries']['second_query'].strip()
                print(f"DEBUG: Manual query for {database2}: {manual_query}")
                # Add ORDER BY to manual query
                if 'ORDER BY' not in manual_query.upper():
                    manual_query = f"{manual_query} ORDER BY id"
                return manual_query
            else:
                raise ValueError(f"Manual query not found for database: {database}")
        
        # Auto-generated queries (original logic)
        id_expr = id_expr_templates.get(database)
        if not id_expr:
            raise ValueError(f"Unsupported database type: {database}")

        # PostgreSQL
        if database == 'postgres':
            if 'start_date' in config and 'end_date' in config and date_column:
                return f"""
                    SELECT 
                        {id_expr} AS id,
                        "{config['check_column']}", 
                        "{date_column}" AS formatted_date
                    FROM {table_name}
                    WHERE "{date_column}" > timestamp '{config['start_date']} 00:00:00.000' 
                    AND "{date_column}" < timestamp '{config['end_date']} 00:00:00.000'
                    ORDER BY id
                """
            else:
                return f"""
                    SELECT 
                        {id_expr} AS id,
                        "{config['check_column']}"
                    FROM {table_name}
                    ORDER BY id
                """

        # Oracle
        elif database == 'oracle':
            if 'start_date' in config and 'end_date' in config and date_column:
                return f"""
                    SELECT 
                        {id_expr} AS id,
                        {config['check_column']}, 
                        TO_CHAR({date_column}, 'YYYY-MM-DD HH24:MI:SS') AS formatted_date
                    FROM {table_name}
                    WHERE {date_column} > TO_DATE('{config['start_date']} 00:00:00', 'YYYY-MM-DD HH24:MI:SS')
                    AND {date_column} < TO_DATE('{config['end_date']} 00:00:00', 'YYYY-MM-DD HH24:MI:SS')
                    ORDER BY id
                """
            else:
                return f"""
                    SELECT 
                        {id_expr} AS id,
                        {config['check_column']}
                    FROM {table_name}
                    ORDER BY id
                """

        # AWS 
        elif database == 'aws':
            if 'start_date' in config and 'end_date' in config and date_column:
                return f"""
                    SELECT 
                        {id_expr} AS id,
                        {config['check_column']}, 
                        {date_column} AS formatted_date
                    FROM {table_name}
                    WHERE {date_column} > '{config['start_date']} 00:00:00' 
                    AND {date_column} < '{config['end_date']} 00:00:00'
                    ORDER BY id
                """
            else:
                return f"""
                    SELECT 
                        {id_expr} AS id,
                        {config['check_column']}
                    FROM {table_name}
                    ORDER BY id
                """
        elif database == 'ali':
            if 'start_date' in config and 'end_date' in config and date_column:
                return f"""
                    SELECT 
                        {id_expr} AS id,
                        {config['check_column']}, 
                        {date_column} AS formatted_date
                    FROM {table_name}
                    WHERE {date_column} > '{config['start_date']} 00:00:00' 
                    AND {date_column} < '{config['end_date']} 00:00:00'
                    ORDER BY id
                """
            else:
                return f"""
                    SELECT 
                        {id_expr} AS id,
                        {config['check_column']}
                    FROM {table_name}
                    ORDER BY id
                """
        elif database == 'mysql':
            if 'start_date' in config and 'end_date' in config and date_column:
                return f"""
                    SELECT 
                        {id_expr} AS id,
                        {config['check_column']}, 
                        {date_column} AS formatted_date
                    FROM {table_name}
                    WHERE {date_column} > '{config['start_date']} 00:00:00' 
                    AND {date_column} < '{config['end_date']} 00:00:00'
                    ORDER BY id
                """
            else:
                return f"""
                    SELECT 
                        {id_expr} AS id,
                        {config['check_column']}
                    FROM {table_name}
                    ORDER BY id
                """
        elif database == 'clickhouse':
            if 'start_date' in config and 'end_date' in config and date_column:
                return f"""
                    SELECT 
                        {id_expr} AS id,
                        {config['check_column']}, 
                        {date_column} AS formatted_date
                    FROM {table_name}
                    WHERE {date_column} > '{config['start_date']} 00:00:00' 
                    AND {date_column} < '{config['end_date']} 00:00:00'
                    ORDER BY id
                """
            else:
                return f"""
                    SELECT 
                        {id_expr} AS id,
                        {config['check_column']}
                    FROM {table_name}
                    ORDER BY id
                """

        else:
            raise ValueError(f"Database type '{database}' is not supported in the query generator.")


    def fetch_data(database, query):
        """
        Generic fetch data function that dynamically calls the appropriate fetch method.
        """
        fetch_functions = {
            'aws': lambda: fetch_data_aws(
                query,
                credentials['aws']['aws_database'],
                credentials['aws']['output_location'],
                credentials['aws']['aws_region'],
                credentials['aws']['aws_access_key_id'],
                credentials['aws']['aws_secret_access_key'],
                batch_size
            ),
            'ali': lambda: fetch_data_alicloud(
                query,
                credentials['ali']['ali_access_id'],
                credentials['ali']['ali_access_key'],
                credentials['ali']['ali_project_name'],
                credentials['ali']['ali_endpoint'],
                batch_size
            ),
            'postgres': lambda: fetch_data_postgres(
                query,
                credentials['postgres']['hostname_postgres'],
                credentials['postgres']['port_postgres'],
                credentials['postgres']['database_postgres'],
                credentials['postgres']['username_postgres'],
                credentials['postgres']['password_postgres'],
                batch_size
            ),
            'oracle': lambda: fetch_data_oracle(
                query,
                credentials['oracle']['dsn_oracle'],
                credentials['oracle']['username_oracle'],
                credentials['oracle']['password_oracle'],
                batch_size
            ),
            'clickhouse': lambda: fetch_data_clickhouse(
                query,
                credentials['clickhouse']['host_clickhouse'],
                credentials['clickhouse']['port_clickhouse'],
                credentials['clickhouse']['database_clickhouse'],
                credentials['clickhouse']['username_clickhouse'],
                credentials['clickhouse']['password_clickhouse'],
                batch_size
            ),
            'mysql': lambda: fetch_data_mysql(
                query,
                credentials['mysql']['hostname_mysql'],
                credentials['mysql']['port_mysql'],
                credentials['mysql']['database_mysql'],
                credentials['mysql']['username_mysql'],
                credentials['mysql']['password_mysql'],
                batch_size
            )
        }
        
        fetch_function = fetch_functions.get(database.lower())
        if not fetch_function:
            raise ValueError(f"Unsupported database type: {database}")
        return fetch_function()
    
    # Process databases dynamically
    if len(databases_to_check) != 2:
        raise ValueError("Config must define exactly two databases to check.")
    
    database1, database2 = databases_to_check

    def _safe(value):
        return "".join(c if c.isalnum() else "_" for c in str(value))

    # ---- Chunked-by-id validation path (memory-safe, period-agnostic) ----
    # Processes the full tables in id ranges so memory stays bounded, while
    # missing-id detection remains correct across ALL periods (an id lands in
    # the same range on both sides regardless of its created_at).
    if str(config.get('chunk_by_id', 'no')).lower() in ('yes', 'true'):
        id_col = composite_columns[0]
        chunk_size = int(config.get('id_chunk_size', 2000000))
        use_final = str(config.get('clickhouse_final', 'yes')).lower() in ('yes', 'true')
        table1_name = config[f'{database1}_table_name']
        table2_name = config[f'{database2}_table_name']

        # Validation mode: 'missing' (ids only) or 'full' (ids + value diffs).
        mode = str(config.get('mode', 'full')).lower()
        if mode not in ('missing', 'full'):
            mode = 'full'

        # In full mode, compare every column common to both tables (minus the id
        # and ClickHouse/dlt meta columns) unless an explicit check_column is set.
        if mode == 'full':
            explicit = config.get('check_column')
            if explicit:
                value_columns = [explicit]
            else:
                value_columns = detect_value_columns(
                    fetch_data, database1, table1_name, database2, table2_name, id_col,
                    extra_excludes=config.get('exclude_columns'))
        else:
            value_columns = []

        scope = "missing-id only" if mode == 'missing' else f"{len(value_columns)} columns: {value_columns}"
        head0 = f"Validation mode: {mode} ({scope})"
        logging.info(head0); print(head0)

        # Determine the global id range across both databases.
        mm1 = fetch_data(database1, build_minmax_query(database1, id_col, table1_name, use_final))
        mm2 = fetch_data(database2, build_minmax_query(database2, id_col, table2_name, use_final))
        gmin = int(min(mm1.iloc[0, 0], mm2.iloc[0, 0]))
        gmax = int(max(mm1.iloc[0, 1], mm2.iloc[0, 1]))
        total_chunks = (gmax - gmin) // chunk_size + 1
        head = (f"Chunked validation: id range [{gmin}, {gmax}], "
                f"chunk_size={chunk_size}, total_chunks={total_chunks}")
        logging.info(head); print(head)

        all_missing_db1, all_missing_db2, all_diffs = [], [], []
        chunk_idx = 0
        for lo in range(gmin, gmax + 1, chunk_size):
            hi = lo + chunk_size - 1
            chunk_idx += 1
            q1 = build_range_query_multi(database1, id_col, value_columns, table1_name, lo, hi, use_final)
            q2 = build_range_query_multi(database2, id_col, value_columns, table2_name, lo, hi, use_final)
            with ThreadPoolExecutor() as ex:
                fu1 = ex.submit(fetch_data, database1, q1)
                fu2 = ex.submit(fetch_data, database2, q2)
                d1 = fu1.result()
                d2 = fu2.result()
            m1, m2, recs = compare_chunk_multi(d1, d2, value_columns, mode, threshold, database1, database2)
            all_missing_db1.extend(m1)
            all_missing_db2.extend(m2)
            all_diffs.extend(recs)
            msg = (f"[checkpoint] chunk {chunk_idx}/{total_chunks} id[{lo}-{hi}] "
                   f"rows {database1}={len(d1)} {database2}={len(d2)} | "
                   f"this chunk: missing_{database1}+={len(m1)} missing_{database2}+={len(m2)} diff+={len(recs)} | "
                   f"running totals: missing_{database1}={len(all_missing_db1)} "
                   f"missing_{database2}={len(all_missing_db2)} diff={len(all_diffs)}")
            logging.info(msg); print(msg)

        # Write outputs. The output name tag reflects the comparison scope.
        scope_tag = 'missing' if mode == 'missing' else (
            _safe(value_columns[0]) if len(value_columns) == 1 else 'allcolumns')
        output_filename_base = (
            f"output_{database1}_{_safe(table1_name)}_vs_{database2}_{_safe(table2_name)}_"
            f"{scope_tag}_{mode}_{timestamp}_result.csv"
        )
        output_csv_name = os.path.join(output_summary_subdirectory, output_filename_base)

        max_len = max(len(all_missing_db1), len(all_missing_db2), len(all_diffs), 0)
        def _pad(lst):
            return list(lst) + [None] * (max_len - len(lst))
        validation_df = pd.DataFrame({
            f'missing_in_{database1}': _pad(all_missing_db1),
            f'missing_in_{database2}': _pad(all_missing_db2),
            'differing_values': _pad(all_diffs),
        })
        validation_df.to_csv(output_csv_name, index=False)

        # Detail file: long format (id, column, value_<db1>, value_<db2>).
        detail_name = (f"{output_csv_name[:-4] if output_csv_name.endswith('.csv') else output_csv_name}"
                       f"_differing_values.csv")
        detail_cols = ['id', 'column', f'value_{database1}', f'value_{database2}']
        detail_df = pd.DataFrame(all_diffs) if all_diffs else pd.DataFrame(columns=detail_cols)
        detail_df.to_csv(detail_name, index=False)

        done = (f"✅ Chunked validation done [mode={mode}]. missing_in_{database1}={len(all_missing_db1)}, "
                f"missing_in_{database2}={len(all_missing_db2)}, differing_values={len(all_diffs)}")
        logging.info(done); print(done)
        logging.info(f"Validation results saved to {output_csv_name}.")
        print(f"Validation result saved to ... {output_csv_name}.")
        logging.info(f"Id Differing Values csv file save into {detail_name}")
        print(f"Id Differing Values csv file save into {detail_name}")
        return

    query1 = construct_query(database1, config[f'{database1}_table_name'], config.get(f'{database1}_database_date_column'))
    query2 = construct_query(database2, config[f'{database2}_table_name'], config.get(f'{database2}_database_date_column'))

    print(f"execute query1 for {database1}")
    print(f"execute query1 for {query1}")
    print(f"execute query1 for {database2}")
    print(f"execute query1 for {query2}")
    
    print("Starting parallel data fetching from both databases...")
    logging.info("Starting parallel data fetching from both databases...")
    
    with ThreadPoolExecutor() as executor:
        first_future = executor.submit(fetch_data, database1, query1)
        second_future = executor.submit(fetch_data, database2, query2)
        
        print(f"Waiting for {database1} data fetch to complete...")
        logging.info(f"Waiting for {database1} data fetch to complete...")
        first_df = first_future.result()
        print(f"{database1} data fetch completed successfully!")
        logging.info(f"{database1} data fetch completed successfully!")
        
        print(f"Waiting for {database2} data fetch to complete...")
        logging.info(f"Waiting for {database2} data fetch to complete...")
        second_df = second_future.result()
        print(f"{database2} data fetch completed successfully!")
        logging.info(f"{database2} data fetch completed successfully!")
    
    print("✅ Both database fetches completed. Starting validation process...")
    logging.info("✅ Both database fetches completed. Starting validation process...")
    
    # Sort and compare results
    first_df.columns.values[0] = first_df.columns.values[0].lower()
    second_df.columns.values[0] = second_df.columns.values[0].lower()
    first_df = first_df.sort_values(by=['id'], ascending=True)
    second_df = second_df.sort_values(by=['id'], ascending=True)
    
    # Build a descriptive, timestamped output name:
    # output_<db1>_<table1>_vs_<db2>_<table2>_<column>_<timestamp>_result.csv
    table1 = _safe(config.get(f'{database1}_table_name') or database1)
    table2 = _safe(config.get(f'{database2}_table_name') or database2)
    check_col = _safe(config['check_column'])
    output_filename_base = (
        f"output_{database1}_{table1}_vs_{database2}_{table2}_"
        f"{check_col}_{timestamp}_result.csv"
    )
    output_csv_name = os.path.join(output_summary_subdirectory, output_filename_base)
    print(f"Comparison result saved to: {output_csv_name}")
      

    if data_type.lower() == 'integer':
            validate_data_integer(first_df, second_df, config['check_column'], output_csv_name, 'id',database1, database2)
        
    elif data_type.lower() == 'string':
             validate_data_string(first_df, second_df, config['check_column'], output_csv_name, 'id', threshold, database1, database2)

    elif data_type.lower() == 'date':
             validate_data_date(first_df, second_df, config['check_column'], output_csv_name, 'id', database1, database2)
    else: 
        print("Error identify data type........")
        logging.info("Error identify data type......")


if __name__ == "__main__":
    import sys

    # Args: python running_validation.py [config.yaml] [--mode missing|full]
    config_path = "config.yaml"
    cli_mode = None
    _args = sys.argv[1:]
    _i = 0
    while _i < len(_args):
        a = _args[_i]
        if a == '--mode' and _i + 1 < len(_args):
            cli_mode = _args[_i + 1].lower(); _i += 2
        elif a.startswith('--mode='):
            cli_mode = a.split('=', 1)[1].lower(); _i += 1
        else:
            config_path = a; _i += 1

    with open(config_path, 'r') as f:
        cfg = yaml.safe_load(f)
    if cli_mode is not None:
        cfg['mode'] = cli_mode
    main(cfg)













        









        


    


