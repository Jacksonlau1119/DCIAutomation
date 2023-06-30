import argparse
from datetime import datetime, date
import os
import glob
import pandas as pd
import zipfile
import time
from ftplib import FTP
import re

def pd_read_txt(file_path):
    try:
        df = pd.read_csv(file_path, sep='|', encoding = 'unicode_escape', low_memory=False, on_bad_lines='skip')  # Read the file using pandas
    except:
        df = pd.read_csv(file_path, sep='|', low_memory=False)
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

    # Add an argument for the initial flag
    parser.add_argument('--initial', action='store_true', default=False, help='Flag for initial processing')
    
    # Add an argument for the unzip flag
    parser.add_argument('--unzip', action='store_false', default=True, help='Flag for multiprocessing')
    
    # Add an argument for the initial flag
    parser.add_argument('--ftp', action='store_true', default=False, help='Flag for initial processing')

    # Parse the command-line arguments
    args = parser.parse_args()

    return args

def find_latest_path(suffix, files):
    latest_path = ''
    for path in files:
        if suffix in path:
            if latest_path == '':
                latest_path = path
                continue
            else:
                latest_path_date = os.path.basename(latest_path)[3:11]
                latest_path_date = datetime.strptime(latest_path_date, '%Y%m%d').date()
                path_date = os.path.basename(path)[3:11]
                path_date = datetime.strptime(path_date, '%Y%m%d').date()
                if path_date>latest_path_date:
                    latest_path = path
    return latest_path

def files_to_unzip(cur_date, dci_dir, initial):
    files_to_unzip = []
    if os.path.isdir(dci_dir):
        print("DCI directory:", dci_dir)
    else:
        print("Invalid DCI directory:", dci_dir)
        return files_to_unzip
    
    cur_date = datetime.strptime(cur_date, '%Y-%m-%d').date()
    print("Current date:", cur_date)
    print("Initial flag:", initial)
    if initial:
        brand_list = []
        for path in glob.glob(os.path.join(dci_dir,'*.zip')):
            name = os.path.basename(path)
            brand = name[:3]
            if brand in brand_list:
                continue
            else:
                brand_list.append(brand)
        brand_date = {}
        for brand in brand_list:
            files_to_unzip.append(find_latest_path('ACESV4Text', glob.glob(os.path.join(dci_dir, f'{brand}*.zip'))))
            files_to_unzip.append(find_latest_path('PIES72Flat', glob.glob(os.path.join(dci_dir, f'{brand}*.zip'))))
        files_to_unzip = [path for path in files_to_unzip if path !='']
        num_brand = len(brand_list)
        num_aces_brand = len([file for file in files_to_unzip if 'ACESV4Text' in file])
        num_pies_brand = len([file for file in files_to_unzip if 'PIES72Flat' in file])
        print(
            f'Num of all brand: {num_brand}, ',
            f'Num of ACES brand: {num_aces_brand}, ',
            f'Num of PIES brand: {num_pies_brand}'
        )
    else:
        cur_date_str = cur_date.strftime('%Y%m%d')
        print(cur_date_str)
        for path in glob.glob(os.path.join(dci_dir, '*.zip')):
            if cur_date_str in path:
                if 'ACESV4Text' in path or 'PIES72Flat' in path:
                    files_to_unzip.append(path)
    return(sorted(files_to_unzip))

def ftp_regex(file_list, pattern):

    matching_files = []
    for file_name in file_list:
        if re.match(pattern, file_name):
            matching_files.append(file_name)

    return matching_files

def files_to_unzip_ftp(
    cur_date,
    dci_dir,
    initial,
    ftp_dci_dir='/dci',
    ip='35.182.148.237'):
    
    user = os.environ['FTP_USERNAME']
    passwd = os.environ['FTP_PWD']
    
    files_to_unzip = []
    
    # Connect to the FTP server
    ftp = FTP(ip)
    ftp.login(user=user, passwd=passwd)
    
    # List directories/files
    ftp.cwd(ftp_dci_dir)
    files = ftp.nlst()
    
    cur_date = datetime.strptime(cur_date, '%Y-%m-%d').date()
    print("Current date:", cur_date)
    print("Initial flag:", initial)
    if initial:
        brand_list = []
        for path in ftp_regex(files, r'.*\.zip$'):
            name = os.path.basename(path)
            brand = name[:3]
            if brand in brand_list:
                continue
            else:
                brand_list.append(brand)
        brand_date = {}
        for brand in brand_list:
            files_to_unzip.append(find_latest_path('ACESV4Text', ftp_regex(files, f'^{brand}.*\.zip$')))
            files_to_unzip.append(find_latest_path('PIES72Flat', ftp_regex(files, f'^{brand}.*\.zip$')))
        files_to_unzip = [path for path in files_to_unzip if path !='']
        num_brand = len(brand_list)
        num_aces_brand = len([file for file in files_to_unzip if 'ACESV4Text' in file])
        num_pies_brand = len([file for file in files_to_unzip if 'PIES72Flat' in file])
        print(
            f'Num of all brand: {num_brand}, ',
            f'Num of ACES brand: {num_aces_brand}, ',
            f'Num of PIES brand: {num_pies_brand}'
        )
    else:
        cur_date_str = cur_date.strftime('%Y%m%d')
        for path in ftp_regex(files, r'.*\.zip$'):
            if cur_date_str in path:
                if 'ACESV4Text' in path or 'PIES72Flat' in path:
                    files_to_unzip.append(path)
                    
    create_folder(dci_dir)
    for ftp_file in files_to_unzip:
        with open(os.path.join(dci_dir, ftp_file), 'wb') as file:
            ftp.retrbinary('RETR ' + ftp_file, file.write) 
    files_to_unzip = [os.path.join(dci_dir,file) for file in files_to_unzip]
    
    return sorted(files_to_unzip)

def estimate_unzip_and_reading_time(zip_file_path, unzip=False):
    unzip_time = 0
    reading_time = 0

    # Create a folder using the zip file path
    folder_path = os.path.splitext(zip_file_path)[0]
    os.makedirs(folder_path, exist_ok=True)

    # Estimate unzip time
    start_time = time.time()
    with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
        zip_ref.extractall(folder_path)
    end_time = time.time()
    unzip_time = end_time - start_time

    # Estimate reading time for all files in the folder
    start_time = time.time()
    for file_name in os.listdir(folder_path):
        file_path = os.path.join(folder_path, file_name)
        if os.path.isfile(file_path):
            df = pd_read_txt(file_path)
    end_time = time.time()
    reading_time = end_time - start_time

    if not unzip:
        # Remove the extracted folder
        for file_name in os.listdir(folder_path):
            file_path = os.path.join(folder_path, file_name)
            if os.path.isfile(file_path):
                os.remove(file_path)
        os.rmdir(folder_path)

    return unzip_time, reading_time
