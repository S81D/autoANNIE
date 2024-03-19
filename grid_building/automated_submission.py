import sys, os
import submit_jobs     # other py script for generating the job submission scripts

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# Modify:

TA_tar_name = 'MyToolAnalysis_grid.tar.gz'                    # name of toolanalysis tar-ball
name_TA = 'DD_TA'                                             # name of the TA directory (within tar-ball)

user = 'doran'                                                # annie username

input_path = '/pnfs/annie/scratch/users/doran/grid_building/' # path to your grid input location (submit_job_grid.sh, run_container_job.sh, grid_job.sh and necessary submission files)
output_path = '/pnfs/annie/scratch/users/doran/output/'       # grid output location

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
run = sys.argv[1]                                             # argument 1: run number to be processed

# for now, treat all run types the same
#run_type = sys.argv[2]                                       # argument 2: run type (important for overlap + part file size)

# sort and find the highest numbered part file from that run (need for breaking up the jobs)
all_files = os.listdir('/pnfs/annie/persistent/raw/raw/' + run + '/')
all_files.sort(key=lambda file: int(file.split('p')[-1]))
last_file = all_files[-1]
final_part = int(last_file.split('p')[-1])

missing = sys.argv[2]                                         # argument 2: re-run of missing files

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# normal event building

if missing == 'n':
        
    process_all = True

    first_part = int(0)
    last_part = final_part

    step_size = int(sys.argv[3])     # (this may need to be modified eventually depending on whether its a source run)

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
    disk_space = str(math.ceil(7 + .3*step_size + .05*step_size))   # fine for beam files
    #disk_space = str(10)     # typically fine as a default - good for Laser single part submissions

    # Submit the entire batch through multiple jobs, based on the user input (above)

    for i in range(len(part_list[0])):     # loop over number of jobs
        
        # create the run_container_job and grid_job scripts
        os.system('rm ' + input_path + 'grid_job.sh')
        submit_jobs.grid_job(run, user, input_path, TA_tar_name, name_TA)
        os.system('rm run_container_job.sh')
        submit_jobs.run_container_job(run, name_TA)

        # We can then create the job_submit script that will send our job (with files) to the grid
        os.system('rm submit_grid_job.sh')
        submit_jobs.submit_grid_job(run, part_list[0][i], part_list[1][i], input_path, output_path, TA_tar_name, disk_space)

        # Lastly, we can execute the job submission script and send the job to the grid
        os.system('sh submit_grid_job.sh')
        print('\n# # # # # # # # # # # # # # # # # # # # #')
        print('Run ' + run + ' p' + str(part_list[0][i]) + '-' + str(part_list[1][i]) + ' sent')
        print('# # # # # # # # # # # # # # # # # # # # #\n')



# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# re-submit jobs that failed

if missing == 'y':
    
    # same as running find_missing_files.py
    run_number = run

    raw_data_dir = "/pnfs/annie/persistent/raw/raw/" + run_number + "/"
    processed_dir = "/pnfs/annie/scratch/users/doran/output/" + run_number + "/"

    print('\nFinding missing files in ' + str(processed_dir))

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


    auto_step_size = int(sys.argv[3])        # can be adjusted

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
        disk_space = str(math.ceil(7 + .3*step_size + .05*step_size))   # fine for beam files
        #disk_space = str(10)     # typically fine as a default - good for Laser single part submissions

        # Submit the entire batch through multiple jobs, based on the user input (above)

        for i in range(len(part_list[0])):     # loop over number of jobs
            
            # create the run_container_job and grid_job scripts
            os.system('rm ' + input_path + 'grid_job.sh')
            submit_jobs.grid_job(run, user, input_path, TA_tar_name, name_TA)
            os.system('rm run_container_job.sh')
            submit_jobs.run_container_job(run, name_TA)

            # We can then create the job_submit script that will send our job (with files) to the grid
            os.system('rm submit_grid_job.sh')
            submit_jobs.submit_grid_job(run, part_list[0][i], part_list[1][i], input_path, output_path, TA_tar_name, disk_space)

            # Lastly, we can execute the job submission script and send the job to the grid
            os.system('sh submit_grid_job.sh')
            print('\n# # # # # # # # # # # # # # # # # # # # #')
            print('Run ' + run + ' p' + str(part_list[0][i]) + '-' + str(part_list[1][i]) + ' sent')
            print('# # # # # # # # # # # # # # # # # # # # #\n')


print('\nJobs successfully submitted!\n')
