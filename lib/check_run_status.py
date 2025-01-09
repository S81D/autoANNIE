import os
from glob import glob

# Check run status of event building
# Author: Steven Doran

# output will be:
#                       [run number] [(run type)] [total raw part files] [total processed part files]
#  

##########################################################################################

run_back = 5000                # the script will only check runs this far back

run_type = ['3', '34', '39']   # specify run type you would like to check

runtype_flag = '39'            # mostly for beam runs --> 39 is different as it is PPS 1

min_part_size = 3              # only check for runs with atleast this many part files

##########################################################################################

# define color codes for text output
RESET = "\033[0m"
CYAN = "\033[96m"  
YELLOW = "\033[93m"  
RED = "\033[91m"  

rawdata_path = '/pnfs/annie/persistent/raw/raw/'
prodata_path = '/pnfs/annie/persistent/processed/processed_EBV2/'
SQL_path = '/pnfs/annie/scratch/users/doran/autoANNIE_v14/ANNIE_SQL_RUNS.txt'
    
# grab the run types from the SQL file
def read_SQL(SQL_file, run_back, run_type):
    
    run_data = {}

    # check if the SQL file exists
    if not os.path.isfile(SQL_file):
        print(f"\nERROR: The SQL file '{SQL_file}' does not exist. Please follow the README and generate an SQL txt file.\nExiting...\n")
        exit()

    # read the SQL file, get the run numbers and the run type
    with open(SQL_file, 'r') as file:
        lines = file.readlines()[2:] 
        for line in lines:
            columns = [col.strip() for col in line.split('|')]
            if len(columns) > 1:
                runnum = columns[1]
                runconfig = columns[5]   # run type

            if int(runnum) >= int(run_back) and runconfig in run_type:    
                    run_data[runnum] = int(runconfig) if runconfig.isdigit() else None

    return run_data


def check_run_data(sql, rawdata_path, prodata_path, runtype_flag):
    for run_number, run_type in sql.items():

        raw_run_folder = os.path.join(rawdata_path, run_number)
        processed_run_folder = os.path.join(prodata_path, f"R{run_number}")

        # we can highlight runs that are of the same run type but are different (like for beam, 39 is PPS 1)
        run_type_str = f"{CYAN}({run_type}){RESET}" if run_type == int(runtype_flag) else f"({run_type})"

        # check if RAWDATA folder exists
        if not os.path.isdir(raw_run_folder):
            print(f"{RED}{run_number} {run_type_str} RAWDATA not present{RESET}")
            continue

        # count part files in RAWDATA
        raw_files = glob(os.path.join(raw_run_folder, f"RAWDataR{run_number}S0p*"))
        num_raw_files = len(raw_files)

        # skip the very small runs
        if num_raw_files < min_part_size:
            continue

        # check if PROCESSED folder exists
        if not os.path.isdir(processed_run_folder):

            # custom for flagged runs that are not yet processed (cyan + yellow)
            run_type_str = f"{CYAN}({run_type}){YELLOW}" if run_type == int(runtype_flag) else f"({run_type})"
            
            print(f"{YELLOW}{run_number} {run_type_str} {num_raw_files} NOT PROCESSED{RESET}")
            continue


        # count part files in PROCESSED data
        processed_files = glob(os.path.join(processed_run_folder, f"ProcessedData*R{run_number}S0p*"))
        num_processed_files = len(processed_files)

        print(f"{run_number} {run_type_str} {num_raw_files} {num_processed_files}")
          
print('\n')
sql = read_SQL(SQL_path, run_back, run_type)
check_run_data(sql, rawdata_path, prodata_path, runtype_flag)
print('\n')

# done
