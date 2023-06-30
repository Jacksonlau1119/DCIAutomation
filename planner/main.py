from utils import * 
import json
from datetime import datetime, date

def main():
    args = arguments()
    dci_dir = args.dci_dir
    cur_date = args.cur_date
    initial = args.initial
    unzip = args.unzip
    ftp = args.ftp
    time_dict = {}
    
    if ftp:
        files_to_unzip_list = files_to_unzip_ftp(cur_date, dci_dir, initial)
    else:
        files_to_unzip_list = files_to_unzip(cur_date, dci_dir, initial)
    
    for zip_file_path in files_to_unzip_list:
        unzip_time, reading_time = estimate_unzip_and_reading_time(zip_file_path, unzip=unzip)
        total_time = sum([unzip_time, reading_time])
        time_dict[zip_file_path] = total_time
        
    sorted_dict = dict(sorted(time_dict.items(), key=lambda x: x[1]))
    print("Estimate total run time:", sum(sorted_dict.values()))
    
    cur_date = datetime.strptime(cur_date, '%Y-%m-%d').date()
    create_folder(os.path.join(dci_dir, cur_date.strftime('%Y%m%d')+'_plan'))
    
    result_path = os.path.join(dci_dir, os.path.join(cur_date.strftime('%Y%m%d')+'_plan', 'files.json'))
    with open(result_path, "w") as fp:
        json.dump(sorted_dict , fp)
        
    print("Result:", result_path)

if __name__ == "__main__":
    main()