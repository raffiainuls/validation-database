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
    logging.info(f"password: {password}")
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
     logging.info(f"password: {password}")
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

def fetch_data_aws(query, database, output_location, region_name, aws_access_key_id, aws_secret_access_key):
    print("Try to connect AWS Athnea.......")
    logging.info("Try to connect AWS Athena....")
    logging.info(f"Database: {database}")
    logging.info(f"output_location: {output_location}")
    logging.info(f"region_name: {region_name}")
    logging.info(f"aws_access_key_id: {aws_access_key_id}")
    logging.info(f"aws_secret_access_key: {aws_secret_access_key}")
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

    response = client.stat_query_execution(
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

def fetch_data_alicloud(query, access_id, access_key, project_name, endpoint, batch_size):
    print("Try to connect Alibaba Max Compute....")
    logging.info("Try to connect Alibaba Max Compute.....")
    logging.info(f"AccessID: {access_id}")
    logging.info(f"AccessKey: {access_key}")
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

def validate_data_integer(first_df, second_df, check_column, output_filename, id_column, database1, database2):

    logging.info("Starting Validate Data")
    print("Starting Validate Data")
    logging.info("Column Check is Integer, Validate Data Integer Start .......")
    first_df[id_column] = first_df[id_column].astype(str)
    second_df[id_column] = second_df[id_column].astype(str)

    #validate missing IDs
    print("Processing Find Missing Ids")
    logging.info("Processing Find Missing Ids")
    missing_in_first_database = second_df[~second_df[id_column].isin(first_df[id_column])][id_column].tolist()
    missing_in_second_database = first_df[~first_df[id_column].isin(second_df[id_column])][id_column].tolist()
    print("Processing Validate Missing Ids Done.")
    logging.info("Processing Validate Missing Ids Done.")

    #validate differing values 
    print("Processing Differing Values ......")
    logging.info("Processing Differing Values.......")
    merged_df = pd.merge(
        first_df[[id_column, check_column]],
        second_df[[id_column, check_column]],
        on=id_column,
        suffixes=(f'_{database1}', f'_{database2}'),
        how= 'inner'
    )

    # handle cases 13 and 13.0 is same
    merged_df[check_column + f'_{database1}'] = merged_df[check_column + f'_{database1}'].apply(pd.to_numeric, errors='coerce')
    merged_df[check_column + f'_{database2}'] = merged_df[check_column + f'_{database2}'].apply(pd.to_numeric, errors='coerce')

    # filter differing values while ignoring tobe rows where both are Nan 
    differing_values = merged_df[
        (merged_df[f'{check_column}_{database1}'] != merged_df[f'{check_column}_{database2}']) &
        ~(merged_df[f'{check_column}_{database1}'].isna() & merged_df[f'{check_column}_{database2}'].isna())
    ]

    # log result 
    # logging.info(f"IDs Missing in {database1}: {missing_in_first_database}")
    # logging.info(f"IDs Missing in {database2}: {missing_in_second_database}")
    # logging.info(f"Differing values:\n{differing_values}")

    if not differing_values.empty:
        differing_values_list = differing_values.to_dict('records')
    else:
        differing_values_list =[]

    # menyesuaikan panjang with None or Nan
    max_len = max(len(missing_in_first_database), len(missing_in_second_database), len(differing_values_list))

    #ensure all list have same length 
    missing_in_first_database.extend([None] * (max_len - len(missing_in_first_database)))
    missing_in_second_database.extend([None] * (max_len - len(missing_in_second_database)))
    differing_values_list.extend([None] * (max_len - len(differing_values_list)))

    # Create DataFrame for validation results 

    validation_df = pd.DataFrame({
        f'missing_in_{database1}': missing_in_first_database,
        f'missing_in_{database2}': missing_in_second_database,
        'differing_values': differing_values_list
    })

    print("Processing Validate Data Done")
    logging.info("Processing Validate Data Done")

    print("saving result to csv file ........")

    # save result to csv
    validation_df.to_csv(output_filename, index=False)
    logging.info(f"Validation results saved to {output_filename}.")
    print(f"Validation result saved to ... {output_filename}.")
    

    outputfile_id_differing_values = f"{output_filename}_differing_values.csv"

    # Create a CSV for differing values (ID and check_column only)
    if not differing_values.empty:
        differing_values_csv = differing_values[[id_column, f'{check_column}_{database1}', f'{check_column}_{database2}']]
        differing_values_csv.to_csv(outputfile_id_differing_values, index=False)
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

        outputfile_id_differing_values = f"{output_filename}_differing_values.csv"

        validation_df.to_csv(output_filename, index=False)
        logging.info(f"Validation results saved to {output_filename}.")
        if not differing_values.empty:
            differing_values_csv = differing_values[[id_column, f'{check_column}_{database1}', f'{check_column}_{database2}']]
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

        outputfile_id_differing_values = f"{output_filename}_differing_values.csv"

            # Create a CSV for differing values (ID and check_column only)
        if not differing_values.empty:
            differing_values_csv = differing_values[[id_column, f'{check_column}_{database1}', f'{check_column}_{database2}']]
            differing_values_csv.to_csv(outputfile_id_differing_values, index=False)
        logging.info(f"Id Differing Values csv file save into {outputfile_id_differing_values}")
        print(f"Id Differing Values csv file save into {outputfile_id_differing_values}")

    
def main(config):
    # Load variables from config
    credentials = config['credentials']
    databases_to_check = config.get('databases', [])
    batch_size = config.get('batch_size', 1000)
    output_directory = config.get('output_directory', './output')
    os.makedirs(output_directory, exist_ok=True)
    data_type = config.get('data_type')
    threshold = config.get('threshold',1)
    
    output_summary_subdirectory = os.path.join(output_directory, "result")
    os.makedirs(output_summary_subdirectory, exist_ok=True)

    # Prepare composite ID expression dynamically
    composite_columns = config['composite_id_columns']
    id_expr_templates = {
        'aws': " || '_' || ".join([f"CAST(COALESCE(CAST({col} AS VARCHAR), '0') AS VARCHAR)" for col in composite_columns]),
        'ali': " || '_' || ".join([f"CAST(COALESCE(CAST({col} AS STRING), '0') AS STRING)" for col in composite_columns]),
        'postgres': " || '_' || ".join([f"CAST(COALESCE(CAST(\"{col}\" AS VARCHAR), '0') AS VARCHAR)" for col in composite_columns]),
        'oracle': " || '_' || ".join([f"CAST(COALESCE(CAST({col} AS VARCHAR2(255)), '0') AS VARCHAR2(255))" for col in composite_columns])
    }
    
    def construct_query(database, table_name, date_column=None):
        """
        Generate query for the given database and table based on its specific syntax.
        """
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
                """
            else:
                return f"""
                    SELECT 
                        {id_expr} AS id,
                        "{config['check_column']}"
                    FROM {table_name}
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
                """
            else:
                return f"""
                    SELECT 
                        {id_expr} AS id,
                        {config['check_column']}
                    FROM {table_name}
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
                """
            else:
                return f"""
                    SELECT 
                        {id_expr} AS id,
                        {config['check_column']}
                    FROM {table_name}
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
                """
            else:
                return f"""
                    SELECT 
                        {id_expr} AS id,
                        {config['check_column']}
                    FROM {table_name}
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
                credentials.get('aws_database'),
                credentials.get('output_location'),
                credentials.get('aws_region'),
                credentials.get('aws_access_key_id'),
                credentials.get('aws_secret_access_key'),
                batch_size
            ),
            'ali': lambda: fetch_data_alicloud(
                query,
                credentials.get('ali_access_id'),
                credentials.get('ali_access_key'),
                credentials.get('ali_project_name'),
                credentials.get('ali_endpoint'),
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
    query1 = construct_query(database1, config[f'{database1}_table_name'], config.get(f'{database1}_database_date_column'))
    query2 = construct_query(database2, config[f'{database2}_table_name'], config.get(f'{database2}_database_date_column'))

    print(f"execute query1 for {database1}")
    print(f"execute query1 for {query1}")
    print(f"execute query1 for {database2}")
    print(f"execute query1 for {query2}")
    
    with ThreadPoolExecutor() as executor:
        first_future = executor.submit(fetch_data, database1, query1)
        second_future = executor.submit(fetch_data, database2, query2)
        
        first_df = first_future.result()
        second_df = second_future.result()
    
    # Sort and compare results
    first_df.columns.values[0] = first_df.columns.values[0].lower()
    second_df.columns.values[0] = second_df.columns.values[0].lower()
    first_df = first_df.sort_values(by=['id'], ascending=True)
    second_df = second_df.sort_values(by=['id'], ascending=True)
    
    output_csv_name = os.path.join(output_summary_subdirectory, f"output_{database1}_{database2}_{config['check_column']}_result.csv")
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
     main()













        









        


    


