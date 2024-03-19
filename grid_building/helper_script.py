import sys, os
import time

# --------------------------------------------------------------- #
# Some helpful functions to call for the automated event building #


# Produce trigoverlap files for event building
def trig_overlap(run):

    os.system('rm trig.list')
    os.system('echo "' + run + '" >> trig.list')
    time.sleep(1)

    # does the run have an overlap tar file?
    exists = os.path.isfile('/pnfs/annie/persistent/processed/trigoverlap/R' + run + '_TrigOverlap.tar.gz')
    if exists == False:
        print('No trigoverlap tar file found in /persistent for ' + run + ', producing trig overlap files now...\n')
        os.system('sh run_trig.sh')
    else:
        print('Trigoverlap file present in /persistent for ' + run + ', moving on...\n')
    
    time.sleep(3)

    return


# check output location for missing processed files after job submissions (in /scratch)
def missing_scratch(run_number):
    
    raw_data_dir = "/pnfs/annie/persistent/raw/raw/" + run_number + "/"
    processed_dir = "/pnfs/annie/scratch/users/doran/output/" + run_number + "/"

    raw_files = [file for file in os.listdir(raw_data_dir) if file.startswith("RAWDataR" + run_number)]
    processed_files = [file for file in os.listdir(processed_dir) if file.startswith("ProcessedRawData_TankAndMRDAndCTC_R" + run_number) and not file.endswith(".data")]

    num_raw_files = len(raw_files)
    num_processed_files = len(processed_files)

    reprocess = False
    if num_raw_files != num_processed_files:
        reprocess = True

    return reprocess


# Check if there any missing processed files in /persistent after the file transfer
def missing_after_transfer(run_number):

    raw_data_dir = "/pnfs/annie/persistent/raw/raw/" + run_number + "/"
    processed_dir = "/pnfs/annie/persistent/processed/processed_hits_new_charge/R" + run_number + "/"

    raw_files = [file for file in os.listdir(raw_data_dir) if file.startswith("RAWDataR" + run_number)]
    processed_files = [file for file in os.listdir(processed_dir) if file.startswith("ProcessedRawData_TankAndMRDAndCTC_R" + run_number) and not file.endswith(".data")]

    num_raw_files = len(raw_files)
    num_processed_files = len(processed_files)

    # Find the missing processed files
    missing_files = []
    for file in raw_files:
        expected_processed_file = "ProcessedRawData_TankAndMRDAndCTC_R" + file[8:]  # Remove "RAWDataR" prefix
        if expected_processed_file not in processed_files:
            missing_files.append(int(expected_processed_file[42:]))

    # Print the results
    print("\nNumber of raw files: ", num_raw_files)
    print("Number of processed files: ", num_processed_files)
    print("Number of missing files: ", num_raw_files-num_processed_files)
    print("Missing processed files: ", missing_files)
    print("\nPercentage processed: ", round((num_processed_files/num_raw_files)*100,2), '%')
    print('\n')

    return


# Query active jobs list for the user (for event building jobs)
def my_jobs(submitted_runs):

    os.system('rm current_jobs.txt')
    time.sleep(1)
    print('\nFetching active job list...')
    os.system('jobsub_q -G annie --user doran >> current_jobs.txt')
    time.sleep(15)

    with open('current_jobs.txt', 'r') as file:
        lines = file.readlines()[:-1]

    # sometimes the .txt file contains "Attempting to get token from..." or whatever - irgnore those lines (only at the top)
    for i, line in enumerate(lines):
        if line.startswith("JOBSUBJOBID"):
            lines = lines[i:]
            break
    
    # only consider jobs that are part of the automated event building (prefix: AUTO)
    # and only consider jobs that are currently running or idle
    filtered_lines = [line.strip().split() for line in lines[1:] if ('I' in line.strip().split()[-5] or 'R' in line.strip().split()[-5]) and 'AUTO_' in line.strip().split()[-1]]
    jobs = [line for line in filtered_lines if line[9].startswith(f"AUTO_")]
    active_jobs = len(jobs)
    print('\nThere are', active_jobs, 'jobs currently running\n')
    why_r_u_running = [row[9] for row in jobs]

    which_runs = list(set([int(job.split('_')[1]) for job in why_r_u_running]))

    print('Run number | Still running?')
    print('-------------------------------------')
    
    check = [False for i in range(len(submitted_runs))]
    for i in range(len(submitted_runs)):
        if int(submitted_runs[i]) in which_runs:
            print('  ', submitted_runs[i], '   |    yes')
        else:
            print('  ', submitted_runs[i], '   |    no')
            check[i] = True

    return active_jobs, which_runs, check


# Query BeamCluster active jobs list for the user
def my_jobs_BC(submitted_runs):

    os.system('rm current_jobs.txt')
    time.sleep(1)
    print('\nFetching active job list...')
    os.system('jobsub_q -G annie --user doran >> current_jobs.txt')
    time.sleep(15)

    with open('current_jobs.txt', 'r') as file:
        lines = file.readlines()[:-1]

    # sometimes the .txt file contains "Attempting to get token from..." or whatever - irgnore those lines (only at the top)
    for i, line in enumerate(lines):
        if line.startswith("JOBSUBJOBID"):
            lines = lines[i:]
            break
    
    filtered_lines = [line.strip().split() for line in lines[1:] if ('I' in line.strip().split()[-7] or 'R' in line.strip().split()[-7]) and 'BC_' in line.strip().split()[-3]]
    jobs = [line for line in filtered_lines if line[9].startswith(f"BC_")]
    active_jobs = len(jobs)
    print('\nThere are', active_jobs, 'jobs currently running\n')
    why_r_u_running = [row[9] for row in jobs]

    which_runs = list(set([int(job.split('_')[1]) for job in why_r_u_running]))

    print('**** BeamCluster jobs ****')
    print('Run number | Still running?')
    print('-------------------------------------')
    
    check = [False for i in range(len(submitted_runs))]
    for i in range(len(submitted_runs)):
        if int(submitted_runs[i]) in which_runs:
            print('  ', submitted_runs[i], '   |    yes')
        else:
            print('  ', submitted_runs[i], '   |    no')
            check[i] = True

    return active_jobs, which_runs, check


# Tell the script to wait some amount of time
def wait(length_of_time):

    print('\n\n------------------------------------------------')
    print('Waiting for jobs to complete... going to sleep...\n')
    for i in range(length_of_time):
       print('Zzzz ... ' + str(i) + '/' + str(length_of_time) + ' minutes until my alarm goes off ... Zzzz\n')
       time.sleep(60)

    print('\nI am awake!\n')

    return


# breakup run parts into seperate jobs if the number of processed files exceeds 500
def BC_breakup(run_number):

    processed_dir = "/pnfs/annie/persistent/processed/processed_hits_new_charge/R" + run_number + "/"

    # Get the list of processed files
    processed_files = [file for file in os.listdir(processed_dir) if file.startswith("ProcessedRawData_TankAndMRDAndCTC_R" + run_number) and not file.endswith(".data")]

    # Count the number of processed files
    num_processed_files = len(processed_files)

    if num_processed_files <= 500:
        return ['0'], [str(num_processed_files - 1)]
    else:
        start_indices = ['0']
        end_indices = []
        for i in range(500, num_processed_files, 500):
            start_indices.append(str(i))
            end_indices.append(str(i - 1))
        end_indices.append(str(num_processed_files - 1))

        return start_indices, end_indices      # lists of starting part file and ending part file
    

# check for BC root files in /scratch output
def check_root(run_number, required_count, which_one):

    if which_one == 'scratch':
        output_path = "/pnfs/annie/scratch/users/doran/output/beamcluster/auto/" + run_number + "/"
    elif which_one == 'persistent':
        output_path = "/pnfs/annie/persistent/processed/BeamClusterTrees/auto/R" + run_number + "/"

    if not os.path.exists(output_path):
        return False

    root_files = [file for file in os.listdir(output_path) if file.endswith(".root")]

    return len(root_files) == required_count   # true if there are enough root files present