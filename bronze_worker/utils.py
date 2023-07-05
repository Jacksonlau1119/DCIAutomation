import argparse
from datetime import datetime, date
import os
import glob
import pandas as pd
import zipfile
import time
import json
from sqlalchemy import create_engine, MetaData, Table
import pyodbc
import sys
import bcpandas
import sqlalchemy

def pd_read_txt(file_path):
    # Read the file using pandas
    try:
        df = pd.read_csv(file_path, sep='|', encoding = 'unicode_escape', low_memory=False, on_bad_lines='skip', dtype=str)  
    except:
        df = pd.read_csv(file_path, sep='|', low_memory=False, dtype=str)
    return df

def create_folder(folder_path):
    try:
        os.makedirs(folder_path)
        print(f"Folder created: {folder_path}")
    except FileExistsError:
        print(f"Folder already exists: {folder_path}")
    except Exception as e:
        print(f"An error occurred while creating the folder: {e}")

def arguments():
    # Create an argument parser
    parser = argparse.ArgumentParser(description='Process the current date')

    # Add an argument for the current date
    parser.add_argument('--cur_date', type=str, default=str(date.today()), help='The current date (YYYY-MM-DD)')

    # Add an argument for the DCI directory
    parser.add_argument('--dci_dir', type=str, default='./dci/', help='The DCI directory')
    
    parser.add_argument('--json_path', type=str, default='', help='The DCI directory')
    
    parser.add_argument('--mp', type=int, default=4, help='Number of processes for processing')
    
    # Parse the command-line arguments
    args = parser.parse_args()

    return args

def create_server_conn(server='35.182.148.237',
                       database = 'DEV_DCI'):
    
    username = os.environ['AIRFLOW_VAR_USERNAME']
    password = os.environ['AIRFLOW_VAR_PWD']
    # Create the connection string
    connection_string = f"DRIVER=ODBC Driver 17 for SQL Server;SERVER={server};DATABASE={database};UID={username};PWD={password}"
    
    # Create a connection engine using SQLAlchemy
    conn_engine = create_engine(f'mssql+pyodbc:///?odbc_connect={connection_string}', fast_executemany=True)
    
    return conn_engine.connect()

def scan_json(json_path, mp):
    folder_list = []
    
    with open(json_path, 'r') as file:
        sorted_dict = json.load(file)
    
    zipfile_list = list(sorted_dict.keys())
    zipfile_list.reverse()
    for zipfile in zipfile_list:
        
        #Check whether a ZIP file has been unzipped
        folder_path = os.path.splitext(zipfile)[0]
        if not os.path.exists(folder_path):
            print(f'{zipfile} has not been extracted')
            os.makedirs(folder_path, exist_ok=True)
            
            # Unzip the ZIP file into the folder path
            with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
                zip_ref.extractall(folder_path)
        folder_list.append(folder_path)
        
    return folder_list


def create_creds(server='35.182.148.237',
                       database = 'DEV_DCI'):
    
    username = os.environ['AIRFLOW_VAR_USERNAME']
    password = os.environ['AIRFLOW_VAR_PWD']
    # Create the connection string
    
    return bcpandas.SqlCreds(server, database, username, password)

def upload_data(path, max_retries=3, retry_delay=3, drop=False):
    retry_count = 0
    success = False
    
    while retry_count < max_retries and not success:
        try:
            # Establish a connection to the SQL server
            conn = create_server_conn()
            creds = create_creds()
            name = os.path.basename(path)
            brand = name[:3]
            df_type = name.split('_')[1].replace('.txt','')
            table_name = f'{brand}_{df_type}_bronze'
            
            # Drop the table if 'drop' is True
            if drop:
                if pd.io.sql.table_exists(table_name, conn):
                    drop_query = f'''
                    DROP TABLE {table_name}
                    '''
                    conn.execute(drop_query)
                conn.close()
                success = True
                continue
                
             # Read the text file into a DataFrame
            df = pd_read_txt(path)

            # Add an 'upload_time' column with the current timestamp
            df['upload_time'] = datetime.now().date()
            columns_to_char = [x for x in df.columns if x != 'upload_time']
            # Set the dtype parameter for varchar columns
            dtype_sql = {column: sqlalchemy.types.VARCHAR() for column in columns_to_char}
            #remove '\.0'
            df[columns_to_char] =  df[columns_to_char].astype(str).replace('\.0', '', regex=True)
            # Check if the table already exists in the database
            if pd.io.sql.table_exists(table_name, conn):
                # Append data to the existing table
                #df.to_sql(table_name, conn, if_exists='append', index=False, method='multi')
                bcpandas.to_sql(df, table_name, creds, if_exists='append', index=False, print_output = False, dtype=dtype_sql)
            else:
                # Create a new table and upload data
                #df.to_sql(table_name, conn, if_exists='replace', index=False)
                bcpandas.to_sql(df, table_name, creds, if_exists='replace', index=False, print_output = False, dtype=dtype_sql)
                print(f"{table_name} created and data uploaded.")
            conn.close()
            success = True
        except Exception as e:
            print("An error occurred:", str(e))
            retry_count += 1
            if retry_count < max_retries:
                print("Retrying after", retry_delay, "seconds...")
                time.sleep(retry_delay)
            else:
                print("Maximum retries exceeded. Unable to process the file:", path)
        sys.stdout.flush()

def upload_data_in_folder(folder_path):
    # Upload data for each file in the folder
    for path in glob.glob(folder_path+'/*'):
        upload_data(path)



    
    