import sys, os
import submit_jobs     # other py script for generating the job submission scripts

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

user = sys.argv[1]                                            # annie username
run = sys.argv[2]                                             # run number to be processed
missing = sys.argv[3]                                         # re-run of missing files
step_size = int(sys.argv[4])                                  # how many part files per job
DLS = sys.argv[5]                                             # daylight Savings? (for the MRD)
TA_tar_name = sys.argv[6]                                     # name of toolanalysis tar-ball
name_TA = sys.argv[7]                                         # name of the TA directory (within tar-ball)
input_path = sys.argv[8]                                      # path to your grid input location (submit_job_grid.sh, run_container_job.sh, grid_job.sh and necessary submission files)
output_path = sys.argv[9]                                     # grid output location
raw_path = sys.argv[10]                                       # path to rawdata
trig_path = sys.argv[11]                                      # path to trig overlap tar balls
beamfetcher_path = sys.argv[12]                               # path to beamfetcher files

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

# sort and find the highest numbered part file from that run (need for breaking up the jobs)
all_files = os.listdir(raw_path + run + '/')
all_files.sort(key=lambda file: int(file.split('p')[-1]))
last_file = all_files[-1]
final_part = int(last_file.split('p')[-1])

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# normal event building (initial submission, not a re-run of missing files)

if missing == 'n':

    first_part = int(0)
    last_part = final_part

    # We need to break the batch into seperate jobs. Unless the batch size is evenly divisible by
    # the step size, the last job will be smaller than the other ones.

    part_list = [[], []]     # [0] = first,  [1] = final
    for i in range(first_part, last_part + 1, step_size):
            
        if i != 0:
            part_list[0].append(i-1)
        else:
            part_list[0].append(i)
        if (i+step_size) > last_part:
            if last_part < final_part:
                part_list[1].append(last_part+1)
            else:
                part_list[1].append(final_part)
        else:
            part_list[1].append(i + step_size)


    # calculate disk space requirements
    import math
    disk_space = str(math.ceil(5 + .3*(step_size+2)))    # refined for beam runs based on job histories
    #disk_space = str(10)     # typically fine as a default for jobs with less than ~8 part files or so

    # Submit the entire batch through multiple jobs, based on the user input (above)

    first = False; final = False
    n_jobs = len(part_list[0])

    os.system('rm grid_job_' + run + '*.sh'); os.system('rm run_container_job_' + run + '*.sh'); os.system('rm submit_grid_job_' + run + '*.sh')
    for i in range(len(part_list[0])):     # loop over number of jobs

        if i == 0:               # first job
            first = True
            sh_name = 'submit_grid_job_' + run + '_first.sh'
        elif i == (n_jobs - 1):  # final job
            final = True
            sh_name = 'submit_grid_job_' + run + '_final.sh'
        else:
            sh_name = 'submit_grid_job_' + run + '.sh'
            os.system('rm grid_job_' + run + '.sh'); os.system('rm run_container_job_' + run + '.sh'); os.system('rm submit_grid_job_' + run + '.sh')

        if n_jobs == 1:    # handle weird cases of small runs with potentially only one job
            first = True; final = True
            sh_name = 'submit_grid_job_' + run + '.sh'
        
        # create the run_container_job and grid_job scripts
        submit_jobs.grid_job(run, user, TA_tar_name, name_TA, first, final)
        submit_jobs.run_container_job(run, name_TA, DLS, first, final)

        # We can then create the job_submit script that will send our job (with files) to the grid
        submit_jobs.submit_grid_job(run, part_list[0][i], part_list[1][i], input_path, output_path, TA_tar_name, disk_space, raw_path, trig_path, beamfetcher_path)

        # Lastly, we can execute the job submission script and send the job to the grid
        os.system('sh submit_grid_job_' + run + '.sh')
        print('\n# # # # # # # # # # # # # # # # # # # # #')
        print('Run ' + run + ' p' + str(part_list[0][i]) + '-' + str(part_list[1][i]) + ' sent')
        print('# # # # # # # # # # # # # # # # # # # # #\n')

        first = False; final = False



# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# re-submit jobs that failed

if missing == 'y':
    
    # same as running find_missing_files.py
    run_number = run

    raw_data_dir = raw_path + run_number + "/"
    processed_dir = output_path + run_number + "/"

    print('\nFinding missing files in ' + str(processed_dir))

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

    print("\nNumber of raw files: ", num_raw_files)
    print("Number of processed files: ", num_processed_files)
    print("Number of missing files: ", num_raw_files-num_processed_files)
    print("Missing processed files: ", missing_files)


    # go through the missing processed files to group them if there are consecutive part files
    def group_consecutive_elements(lst):
        result = []
        current_group = []
        for i in range(len(lst)):
            if i > 0 and lst[i] != lst[i - 1] + 1:
                result.append(current_group)
                current_group = []
            current_group.append(lst[i])
        if current_group:
            result.append(current_group)
        return result

    missing_list = group_consecutive_elements(missing_files)
    ml = 0
    for i in range(len(missing_list)):
        if len(missing_list[i]) > ml:
            ml = len(missing_list[i])
        
    print('\nMaximum part bunch size = ' + str(ml))


    auto_step_size = step_size        # pass different step size arg if running 'y' for missing

    for l in range(len(missing_list)):

        first_part = min(missing_list[l])
        last_part = max(missing_list[l])

        if len(missing_list[l]) >= auto_step_size:
            step_size = auto_step_size
        elif len(missing_list[l]) < auto_step_size:
            step_size = len(missing_list[l])

        part_list = [[], []]     # [0] = first,  [1] = final
        for i in range(first_part, last_part + 1, step_size):
                
            if i != 0:
                part_list[0].append(i-1)
            else:
                part_list[0].append(i)
            if (i+step_size) > last_part:
                if last_part < final_part:
                    part_list[1].append(last_part+1)
                else:
                    part_list[1].append(final_part)
            else:
                part_list[1].append(i + step_size)


        # calculate disk space requirements
        import math
        str(math.ceil(5 + .3*(step_size+2)))    # refined for beam runs based on job histories
        #disk_space = str(10)     # typically fine as a default - good for Laser single part submissions

        # Submit the entire batch through multiple jobs, based on the user input (above)

        for i in range(len(part_list[0])):     # loop over number of jobs
            
            # create the run_container_job and grid_job scripts
            os.system('rm grid_job_' + run + '.sh')
            submit_jobs.grid_job(run, user, input_path, TA_tar_name, name_TA)
            os.system('rm run_container_job_' + run + '.sh')
            submit_jobs.run_container_job(run, name_TA, DLS)

            # We can then create the job_submit script that will send our job (with files) to the grid
            os.system('rm submit_grid_job_' + run + '.sh')
            submit_jobs.submit_grid_job(run, part_list[0][i], part_list[1][i], input_path, output_path, TA_tar_name, disk_space, raw_path, trig_path, beamfetcher_path)

            # Lastly, we can execute the job submission script and send the job to the grid
            os.system('sh submit_grid_job_' + run + '.sh')
            print('\n# # # # # # # # # # # # # # # # # # # # #')
            print('Run ' + run + ' p' + str(part_list[0][i]) + '-' + str(part_list[1][i]) + ' sent')
            print('# # # # # # # # # # # # # # # # # # # # #\n')


print('\nJobs successfully submitted!\n')
