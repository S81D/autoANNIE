import os, sys

# script to give you a quick snapshot on the data processing status of a particular run

''' 
usage: python3 scripts/missing_scratch.py

output:

Number of raw files:  120
Number of processed files:  118
Number of missing files:  2
Missing processed files:  [46, 47]

Percentage processed:  98.33 %
'''

run = sys.argv[1]
raw_path = '/pnfs/annie/persistent/raw/raw/'
output_path = '/pnfs/annie/scratch/users/<user>/output/'  # adjust accordingly
run_type = 'beam'

# check output location for missing processed files after job submissions (in /scratch)
def missing_scratch(run_number, raw_path, output_path, run_type):
    
    raw_data_dir = raw_path + run_number + "/"
    processed_dir = output_path + run_number + "/"

    if run_type == 'beam' or run_type == 'beam_39':
        basename = "ProcessedData_PMTMRDLAPPD_R"
        indy = 34    # used to "hack" off the part file number below (in missing_files.append line)
    elif run_type == 'AmBe' or run_type == 'LED':
        basename = 'ProcessedData_PMT_R'
        indy = 26
    elif run_type == 'laser':
        basename = 'ProcessedData_PMTLAPPD_R'
        indy = 31
    elif run_type == 'cosmic':
        basename = 'ProcessedData_PMTMRD_R'
        indy = 29

    raw_files = [file for file in os.listdir(raw_data_dir) if file.startswith("RAWDataR" + run_number)]
    processed_files = [file for file in os.listdir(processed_dir) if file.startswith(basename + run_number) and not file.endswith(".data")]

    num_raw_files = len(raw_files)
    num_processed_files = len(processed_files)

    # Find the missing processed files
    missing_files = []
    for file in raw_files:
        expected_processed_file = basename + file[8:]  # Remove "RAWDataR" prefix
        if expected_processed_file not in processed_files:
            missing_files.append(int(expected_processed_file[indy:]))

    # Print the results
    print("\nNumber of raw files: ", num_raw_files)
    print("Number of processed files: ", num_processed_files)
    print("Number of missing files: ", num_raw_files-num_processed_files)
    print("Missing processed files: ", missing_files)
    print("\nPercentage processed: ", round((num_processed_files/num_raw_files)*100,2), '%')
    print('\n')

    return


missing_scratch(run, raw_path, output_path, run_type)

