# DCIAutomation

## Planner

### utils.py

- `pd_read_txt(file_path)`: This function reads a text file (CSV format) using the pandas library and returns a DataFrame. It handles different encoding options and skips bad lines if encountered.

- `create_folder(folder_path)`: This function creates a new folder at the specified `folder_path` if it doesn't already exist. It handles errors related to existing folders or any other exceptions that may occur during the folder creation process.

- `arguments()`: This function sets up an argument parser using the `argparse` module to parse command-line arguments. It defines and retrieves arguments for the current date, DCI directory, initial flag, unzip flag, and FTP flag.

- `find_latest_path(suffix, files)`: This function searches for the latest file path in a list of files that matches the given `suffix` pattern. It compares the dates embedded in the filenames and returns the path with the latest date.

- `files_to_unzip(cur_date, dci_dir, initial)`: This function identifies the files that need to be unzipped based on the current date, DCI directory, and initial flag. It considers either the initial processing scenario or the regular scenario, where files matching the current date are selected for unzipping.

- `ftp_regex(file_list, pattern)`: This function applies a regular expression pattern to filter a list of file names obtained from an FTP server. It returns a list of matching file names.

- `files_to_unzip_ftp(cur_date, dci_dir, initial, ftp_dci_dir='/dci', ip='35.182.148.237')`: This function retrieves the files to be unzipped from an FTP server based on the current date, DCI directory, initial flag, FTP directory path, and FTP server IP address. It uses the `ftp_regex` function to filter the files and downloads them to the local DCI directory.

- `estimate_unzip_and_reading_time(zip_file_path, unzip=False)`: This function estimates the time required to unzip a ZIP file and read the extracted files. It creates a folder based on the ZIP file path, extracts the files using `zipfile.ZipFile`, and measures the time taken for the unzip process. It then reads each file using the `pd_read_txt` function and calculates the time taken for reading. If the `unzip` flag is set to `False`, it removes the extracted files and the folder.

### main.py

- The script imports the necessary modules and functions, including the utility functions from `utils.py`.

- The `main()` function is defined, which serves as the entry point for the script.

- The script retrieves the command-line arguments using the `arguments()` function from `utils.py`. These arguments include the DCI directory, current date, initial flag, unzip flag, and FTP flag.

- Depending on the FTP flag, the script determines the list of files to be unzipped. If the FTP flag is `True`, the `files_to_unzip_ftp()` function from `utils.py` is called with the appropriate arguments. Otherwise, the `files_to_unzip()` function is used.

- For each ZIP file path in the list of files to be unzipped, the script estimates the unzip time and reading time using the `estimate_unzip_and_reading_time()` function from `utils.py`. The total time is calculated as the sum of the unzip time and reading time.

- The results are stored in a dictionary called `time_dict`, where the keys are the ZIP file paths, and the values are the total times.

- The `time_dict` dictionary is sorted in ascending order based on the total times, and the sorted dictionary is stored in the `sorted

_dict` variable.

- The estimated total run time is printed as the sum of all the values in the `sorted_dict` dictionary.

- The script creates a new folder within the DCI directory based on the current date, using the `create_folder()` function from `utils.py`.

- The sorted dictionary is saved as a JSON file named "files.json" within the newly created folder.

- The absolute path of the resulting JSON file is printed as the final output.

## Bronzer Worker

### utils.py

- `create_server_conn(server, database)`: This function creates a connection engine to a SQL server using the specified server and database credentials. It uses the `pyodbc` library and the `create_engine` function from SQLAlchemy.

- `scan_json(json_path, mp)`: This function scans a JSON file that contains a sorted dictionary of ZIP file paths and their corresponding total times. It checks if each ZIP file has been unzipped, and if not, it extracts the files into their respective folders. It returns a list of the folder paths.

- `create_creds(server, database)`: This function creates SQL credentials using the specified server and database credentials. It returns a `bcpandas.SqlCreds` object.

- `upload_data(path, max_retries, retry_delay, drop)`: This function uploads data from a text file to a SQL server table. It establishes a connection to the server, reads the text file into a DataFrame, adds an 'upload_time' column with the current date, and uploads the data to the table. It supports a maximum number of retries, a delay between retries, and the option to drop the table before uploading.

- `upload_data_in_folder(folder_path)`: This function uploads data for each file in a specified folder. It iterates over the files using `glob.glob` and calls the `upload_data` function for each file.

### main.py

- The `main()` function now initializes the arguments using the `arguments()` function from `utils.py` and sets the values for `dci_dir`, `cur_date`, `json_path`, and `mp` (number of processes) accordingly.

- If `json_path` is not provided as a command-line argument, it is constructed based on `dci_dir` and `cur_date`.

- The script verifies the validity of the `dci_dir` and `json_path`. It checks if `dci_dir` is a valid directory and if `json_path` is a valid file.

- The `scan_json()` function from `utils.py` is called with the `json_path` and `mp` arguments to retrieve the list of folders to process. This list is stored in the `folder_list` variable.

- If `mp` (number of processes) is greater than 1, multiprocessing is enabled. The script creates a multiprocessing pool with the specified number of processes and applies the `upload_data_in_folder()` function to each folder in `folder_list` using `pool.imap()`. The progress is tracked using the `tqdm` library, which displays a progress bar during multiprocessing. The multiprocessing pool is closed and joined after completion.

- If `mp` is 1 (or less), indicating single-process execution, the script iterates over the `folder_list` and calls the `upload_data_in_folder()` function for each folder. The progress is also tracked using the `tqdm` library.

- Upon completion of the multiprocessing or single-process execution, the script prints "Processing complete" to indicate that the process has finished.

## Other Files

- `daily_ftp.sh`: This shell script runs the planner and bronzer_worker scripts to upload today's data.

- `init_ftp.sh`: This shell script runs the planner and bronzer_worker scripts to upload all brands with the latest data.

- Workflow diagrams can be found [here](https://app.diagrams.net/#G1k1bPGJvw5_0EDjDJcDgfvx3ddAmMgiU_#%7B%22pageId%22%3A%22L1Wit_bGYVjArb1imkON%22%7D).
