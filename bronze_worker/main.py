from utils import * 
import os
from tqdm import tqdm
import multiprocessing
from functools import partial

def main():
    args = arguments()
    dci_dir = args.dci_dir
    cur_date = datetime.strptime(args.cur_date, '%Y-%m-%d').date()
    json_path = args.json_path
    mp = args.mp
    # If `json_path` is not provided, construct it based on `dci_dir` and `cur_date`
    if not json_path:
        json_path = os.path.join(dci_dir, os.path.join(cur_date.strftime('%Y%m%d')+'_plan', 'files.json'))
        
    print("Current date:", cur_date)
    
    # Check if `dci_dir` is a valid directory
    if os.path.isdir(dci_dir):
        print("DCI directory:", dci_dir)
    else:
        print("Invalid DCI directory:", dci_dir)
    
    # Check if `json_path` is a valid file
    if os.path.isfile(json_path):
        print(f"Start processing {json_path}")
    else:
        print(f"{json_path} is not a file or doesn't exist.")
    
    folder_list = scan_json(json_path, mp)
    
    if mp>1:
        num_processes = mp#multiprocessing.cpu_count()# Set the number of processes
        print(f"Start multiprocessing with {num_processes} processes")
        # Create a multiprocessing pool
        pool = multiprocessing.Pool(processes=num_processes)
        # Apply multiprocessing on the folder list
        list(tqdm(pool.imap(upload_data_in_folder, folder_list), total=len(folder_list), desc='Processing'))
        # Close the multiprocessing pool
        pool.close()
        pool.join()      
    else:
        for folder in tqdm(folder_list, desc='Processing'):
            upload_data_in_folder(folder)
    
    print("Processing complete.")  
    
if __name__ == "__main__":
    main()