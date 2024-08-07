import sys, os
import time

# --------------------------------------------------------------- #
# Some helpful functions to call for the automated event building #


# Ask user for runs you would like to include
def get_runs_from_user():
    runs = []
    print("Enter the runs you want to include. Type 'done' when you are finished:")
    while True:
        user_input = input("Enter run number: ")
        if user_input.lower() == 'done':
            break
        runs.append(user_input)
    return runs
    

# Produce trigoverlap files for event building
def trig_overlap(run, trig_path, app_path, scratch_path, singularity):

    os.system('rm trig.list')
    os.system('echo "' + run + '" >> trig.list')
    time.sleep(1)

    # does the run have an overlap tar file?
    exists = os.path.isfile(trig_path + 'R' + run + '_TrigOverlap.tar.gz')
    if exists == False:
        print('\nNo trigoverlap tar file found in /persistent for ' + run + ', producing trig overlap files now...\n')
        os.system('sh run_trig.sh ' + app_path + ' ' + scratch_path + ' ' + trig_path + ' ' + singularity)
    else:
        print('Trigoverlap file present in /persistent for ' + run + ', moving on...\n')
    
    time.sleep(1)

    return


# produce beamfetcher part files
def beamfetcher(run, step_size, raw_path, app_path, scratch_path, singularity, beamfetcher_path):

    os.system('rm beam.list')
    os.system('echo "' + run + '" >> beam.list')
    
    # does the run already have a beamfetcher file?
    exists = os.path.isfile(beamfetcher_path + 'beamfetcher_' + run + '.root')
    
    if exists == False:

        raw_dir = raw_path + run + "/"

        # temporary storage for created beamfetcher files
        os.system('mkdir -p ' + app_path + run)
        
        # Get the list of raw files and how many
        raw_files = [file for file in os.listdir(raw_dir) if file.startswith("RAWData")]
        num_raw_files = len(raw_files)

        if num_raw_files <= step_size:
            start_indices = ['0']; end_indices = [str(num_raw_files - 1)]
        else:
            start_indices = ['0']
            end_indices = []
            for i in range(step_size, num_raw_files, step_size):
                start_indices.append(str(i))
                end_indices.append(str(i - 1))
            end_indices.append(str(num_raw_files - 1))

        print('\n' + run + ' has ' + str(int(end_indices[-1])+1) + ' part files. Executing script now...')
        print('\nSteps to run:')
        print(start_indices, end_indices)      # lists of starting part file and ending part file
        time.sleep(1)

        for i in range(len(start_indices)):
            print('\nRun ' + run + '  parts ' + start_indices[i] + '-' + end_indices[i])
            print('***********************************************************\n')
            os.system('sh run_beamfetcher.sh ' + start_indices[i] + ' ' + end_indices[i] + ' ' + app_path + ' ' + scratch_path + ' ' + raw_path + ' ' + singularity)
            time.sleep(1)

            # verify the file executed and there wasn't a toolchain crash (it will be very small if it failed < 5KB)
            # TODO - should probably add an exception if its a single part file
            # For now, re-run once
            size = os.path.getsize(app_path + 'beamfetcher_tree.root')
            if size < 5:   # smaller than 5KB - for now, just re-run once
                print('\nbeamfetcher file just produced is less than 5 KB - there must have been a crash')
                print('\nrerunning....')
                os.system('sh run_beamfetcher.sh ' + start_indices[i] + ' ' + end_indices[i] + ' ' + app_path + ' ' + scratch_path + ' ' + raw_path + ' ' + singularity)
            else:
                print('\nFile looks good (over 5KB). Proceeding to the next one...')

            time.sleep(1)
            os.system('cp ' + app_path + 'beamfetcher_tree.root ' + app_path + run + '/beamfetcher_' + run + '_p' + start_indices[i] + '_' + end_indices[i] + '.root')
            time.sleep(1)
            os.system('rm ' + app_path + 'beamfetcher_tree.root')
            time.sleep(1)

        print('\nMerging beam files...\n')
        os.system('sh merge_it.sh ' + singularity + ' ' + app_path + ' ' + run + ' ' + 'beamfetcher')
        time.sleep(1)
        
        print('\nTransferring beam file...\n')
        os.system('cp beamfetcher_' + run + '.root ' + beamfetcher_path + '.')
        time.sleep(1)
        os.system('ls -lrth ' + beamfetcher_path + 'beamfetcher_' + run + '.root')
        os.system('rm -rf ' + app_path + run + '/')     # folder to hold the segmented beamfetcher root files
        os.system('rm beamfetcher_' + run + '.root')

    else:
        print('BeamFetcher file present in /persistent for ' + run + ', moving on...\n')
    
    time.sleep(1)

    return



# check output location for missing processed files after job submissions (in /scratch)
def missing_scratch(run_number, raw_path, output_path):
    
    raw_data_dir = raw_path + run_number + "/"
    processed_dir = output_path + run_number + "/"

    raw_files = [file for file in os.listdir(raw_data_dir) if file.startswith("RAWDataR" + run_number)]
    processed_files = [file for file in os.listdir(processed_dir) if file.startswith("ProcessedData_PMTMRDLAPPD_R" + run_number) and not file.endswith(".data")]

    num_raw_files = len(raw_files)
    num_processed_files = len(processed_files)

    reprocess = False
    if num_raw_files != num_processed_files:
        reprocess = True

    return reprocess


# Check if there any missing processed files in /persistent after the file transfer
def missing_after_transfer(run_number, raw_path, data_path):

    raw_data_dir = raw_path + run_number + "/"
    processed_dir = data_path + "R" + run_number + "/"

    raw_files = [file for file in os.listdir(raw_data_dir) if file.startswith("RAWDataR" + run_number)]
    processed_files = [file for file in os.listdir(processed_dir) if file.startswith("ProcessedData_PMTMRDLAPPD_R" + run_number) and not file.endswith(".data")]

    num_raw_files = len(raw_files)
    num_processed_files = len(processed_files)

    # Find the missing processed files
    missing_files = []
    for file in raw_files:
        expected_processed_file = "ProcessedData_PMTMRDLAPPD_R" + file[8:]  # Remove "RAWDataR" prefix
        if expected_processed_file not in processed_files:
            missing_files.append(int(expected_processed_file[34:]))

    # Print the results
    print("\nNumber of raw files: ", num_raw_files)
    print("Number of processed files: ", num_processed_files)
    print("Number of missing files: ", num_raw_files-num_processed_files)
    print("Missing processed files: ", missing_files)
    print("\nPercentage processed: ", round((num_processed_files/num_raw_files)*100,2), '%')
    print('\n')

    # write to run summary file
    with open('R' + run_number + '_summary.txt', 'a') as file:
        file.write('R' + run_number + ' transfer summary\n')
        file.write('\nNumber of raw files: ' + str(num_raw_files))
        file.write('\nNumber of processed files: ' + str(num_processed_files))
        file.write('\nNumber of missing files: ' + str(num_raw_files-num_processed_files))
        file.write('\nMissing processed files: ' + str(missing_files) + '\n')
        file.write('\nPercentage processed: ' + str(round((num_processed_files/num_raw_files)*100,2)) + '%')
        
    os.system('cp R' + run_number + '_summary.txt ' + processed_dir + '.')
    time.sleep(1)
    os.system('rm R' + run_number + '_summary.txt')

    return


# Query active jobs list for the user (for event building jobs)
def my_jobs(submitted_runs, user):

    os.system('rm current_jobs.txt')
    time.sleep(1)
    print('\nFetching active job list...')
    os.system('jobsub_q -G annie --user ' + user + ' >> current_jobs.txt')
    time.sleep(15)    # this is so long because it sometimes takes time to query job lists

    with open('current_jobs.txt', 'r') as file:
        lines = file.readlines()[:-1]

    # sometimes the .txt file contains "Attempting to get token from..." or whatever - irgnore those lines (only at the top)
    for i, line in enumerate(lines):
        if line.startswith("JOBSUBJOBID"):
            lines = lines[i:]
            break
    
    # only consider jobs that are currently running or idle
    filtered_lines = [line.strip().split() for line in lines[1:] if ('I' in line.strip().split()[-5] or 'R' in line.strip().split()[-5])]
    # only consider jobs form the list with the form: <run>_<p1>_<p2>    (isdigit() checks that the string consists of only digits and underscores)
    jobs = [line for line in filtered_lines if line[9].replace('_', '').isdigit()]
    active_jobs = len(jobs)
    print('\nThere are', active_jobs, 'jobs currently running\n')
    job_names = [row[9] for row in jobs]

    which_runs = list(set([int(job.split('_')[0]) for job in job_names]))

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
def my_jobs_BC(submitted_runs, user):

    os.system('rm current_jobs.txt')
    time.sleep(1)
    print('\nFetching active job list...')
    os.system('jobsub_q -G annie --user ' + user + ' >> current_jobs.txt')
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
def BC_breakup(run_number, data_path):

    processed_dir = data_path + "R" + run_number + "/"

    # Get the list of processed files
    processed_files = [file for file in os.listdir(processed_dir) if file.startswith("ProcessedData_PMTMRDLAPPD_R" + run_number) and not file.endswith(".data")]

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
    

# check for BC root files in /scratch
def check_root_scratch(run_number,required_count,output_path):

    file_path = output_path + run_number + '/'

    if not os.path.exists(file_path):
        return False

    root_files = [file for file in os.listdir(file_path) if file.endswith(".ntuple.root")]

    return len(root_files) == required_count   # true if there are enough root files present


# check for BC root file in /persistent
def check_root_pro(run_number,output_path):

    name_of_file = 'BeamCluster_' + run_number
    file_path = os.path.join(output_path, f"{name_of_file}.ntuple.root")

    return os.path.exists(file_path)
