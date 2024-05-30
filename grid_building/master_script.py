# master script for automated event building

# Author: Steven Doran
# Date: March 2024

usage_verbose = """
#########################################################################################
# Usage: python3 master_script.py --run_ceiling --run_floor --step_size --resub_step_size

# run_ceiling = where to start from (set to 'current' to build all runs from present)
# run_floor = furthest back run in time the code will event build (input run number)
# step_size = number of part files per job for event building
# resub_step_size = number of part files per job for resubmitting event building jobs
# (last two are sensitive to run type)


# Grid job specifications:
# -- lifetime: 1hr  (for event building)    2hr (BC)
# -- memory: 2GB (event building)  1GB (BC)
# -- disk: varies depending on number of part files, but typically between 5 and 10GB (event building)   10GB for BC

#########################################################################################
"""

import sys, os
import time
import helper_script

print('\n')
print(usage_verbose, '\n')

run_ceiling = input('Run ceiling (earliest run):    ')
run_floor = int(input('Run floor (furthest back run):    '))
step_size = int(input('Job part file size:    '))
resub_step_size = int(input('Job re-submission part file size:    '))

# -------------------------------------------------------------
print('\n---------------------------------------------------')
print('        Automated EventBuilding initiated            ')
print('---------------------------------------------------\n')

print('The following arguments have been provided:\n')
print('  - Run ceiling (earliest run):  ' + str(run_ceiling))
print('  - Run floor (furthest back run):  ' + str(run_floor))
print('  - Job part file size:  ' + str(step_size))
print('  - Job re-submission part file size:  ' + str(resub_step_size))
time.sleep(5)
print('\n\nProceeding with event building...')
time.sleep(3)

# Part 1 - Initial job submission

print('\nChecking available runs...')

# - - - - - - - - - - - - - - - - - - -
# If your SSH connection breaks or some other error is encountered (or you just want to build the BeamCluster root files),
# You can uncomment sections of the code so it will skip the event building stage and only build runs you want
# - - - - - - - - - - - - - - - - - - -

#''' # uncomment to only submit BC jobs

# give us the available runs
all_raw = os.listdir('/pnfs/annie/persistent/raw/raw/')
all_raw.sort()
all_raw.reverse()

all_pro = os.listdir('/pnfs/annie/persistent/processed/processed_hits_new_charge/')
all_pro = [all_pro[i][1:] for i in range(len(all_pro))]
all_pro.sort()
all_pro.reverse()

# there may be some runs that failed to transfer, etc... and may be present on /scratch
# If that's the case, do not send them in the initial batch

runs_to_run = []; length_of_runs = []
time.sleep(1)
if str(run_ceiling) == 'current':
    starting_point = 1
else:
    starting_point = 0
for i in range(starting_point,len(all_raw)):      # if 'current', ignore latest run as it is still likely transferring from DAQ
    if str(run_ceiling) != 'current':
        if all_raw[i] not in all_pro and int(run_ceiling) >= int(all_raw[i]) >= run_floor:
            list_parts = os.listdir('/pnfs/annie/persistent/raw/raw/' + all_raw[i] + '/')
            list_parts.sort(key=lambda file: int(file.split('p')[-1]))
            last_file = list_parts[-1]
            final_part = int(last_file.split('p')[-1])
            if (final_part+1) >= 3:               # only process runs with at least 3 part files
                runs_to_run.append(all_raw[i])
                length_of_runs.append(final_part + 1)
    elif str(run_ceiling) == 'current':
        if all_raw[i] not in all_pro and int(all_raw[i]) >= run_floor:
            list_parts = os.listdir('/pnfs/annie/persistent/raw/raw/' + all_raw[i] + '/')
            list_parts.sort(key=lambda file: int(file.split('p')[-1]))
            last_file = list_parts[-1]
            final_part = int(last_file.split('p')[-1])
            if (final_part+1) >= 3:    
                runs_to_run.append(all_raw[i])
                length_of_runs.append(final_part + 1)

#''' # uncomment to only submit BC jobs
# runs_to_run = ['4764', '4763', '4762', '4761', '4760']

print('\nAvailable runs:\n', runs_to_run)
time.sleep(5)

print('\n\n---------------------------')
print('Moving onto PreProcess...\n')
time.sleep(3)

#''' # uncomment to only submit BC jobs

# Produce trig overlap files if they havent yet been created
for run in runs_to_run:
    helper_script.trig_overlap(run)


# Now that we have the trigoverlap files produced, we can send the initial grid jobs
print('\n-----------------------------------------------------------------')
print('Submitting initial set of jobs to the grid with step size = ' + str(step_size) + '...\n')
time.sleep(3)
for i in range(len(runs_to_run)):
    if length_of_runs[i] < step_size:
        small_step = length_of_runs[i]
    else:
        small_step = step_size

    # omit the runs that have some part files in /scratch
    output_path = "/pnfs/annie/scratch/users/doran/output/" + runs_to_run[i] + "/"
    exists_and_contains = os.path.exists(output_path) and any(file.startswith('Processed') and not file.endswith(".data") for file in os.listdir(output_path))
    if exists_and_contains == False:
        os.system('python3 automated_submission.py ' + runs_to_run[i] + ' n ' + str(small_step))   # no re-run
        time.sleep(3)
    else:
        print('\n' + runs_to_run[i] + ' processed files present in /scratch, not submitting this run in first batch...')

print('\nAll initial jobs submitted\n')
time.sleep(5)

# display active jobs
os.system('\njobsub_q -G annie --user doran\n')
time.sleep(1)


# -------------------------------------------------------------
# Part 2 - Resubmission

resubs = [0 for i in range(len(runs_to_run))]
complete = 0

#''' # uncomment to only submit BC jobs

BC_resubs = [0 for i in range(len(runs_to_run))]
complete_BC = 0

print('\n------------------------------------------------\n')

#''' # uncomment to only submit BC jobs

while complete != len(resubs):

    # check jobs
    active_jobs, which_runs, check = helper_script.my_jobs(runs_to_run)

    check_count = 0
    for i in range(len(check)):

        if check[i] == True and resubs[i] < 2:
            reprocess = helper_script.missing_scratch(runs_to_run[i])
            time.sleep(1)
            if reprocess == True:   # if there are missing files in scratch, re-submit
                # note that the step size for resubmissions is currently set to 4
                os.system('python3 automated_submission.py ' + runs_to_run[i] + ' y ' + str(resub_step_size))
                resubs[i] += 1
            else:                   # if there aren't any missing files, transfer
                if resubs[i] != -1:
                    print('\n\nRun ' + runs_to_run[i] + ' is done! It was resubmitted ' + str(resubs[i]) + ' time(s). Initiating transfer...\n')
                    os.system('sh copy_grid_output.sh ' + runs_to_run[i])
                    time.sleep(1)
                    helper_script.missing_after_transfer(runs_to_run[i])
                    complete += 1; resubs[i] = -1
                else:
                    print('\nRun ' + runs_to_run[i] + ' already transferred\n')
                    check_count += 1

        elif check[i] == True and resubs[i] == 2:    # no more jobs, but already re-submitted twice
            print('\nMax re-submissions reached for run ' + runs_to_run[i] + '! Initiating transfer...\n')
            os.system('sh copy_grid_output.sh ' + runs_to_run[i])
            time.sleep(1)
            helper_script.missing_after_transfer(runs_to_run[i])
            complete += 1; resubs[i] = -1

        
        else:   # still running
            check_count += 1     # how many jobs are still active


    # if all jobs are still active, wait and return to start (dont submit any BC jobs)
    if check_count == len(check):
        helper_script.wait(5)     # wait 5 minutes
    
#''' # uncomment to only submit BC jobs

# once the event building jobs are completed and have been transferred, move onto submitting BC jobs
print('\n------------------------------------------------\n')
print('Moving onto BeamCluster jobs...\n')
time.sleep(3)
os.chdir('BeamCluster')
print('We should be in the BeamCluster/ directory:\n')
os.system('pwd')
print('- - - - - - - - - - - - - - - - - - - - - - - - - - - - - -')
os.system('ls')
time.sleep(5)

while complete_BC != len(BC_resubs):

    BC_active_jobs, BC_which_runs, BC_check = helper_script.my_jobs_BC(runs_to_run)

    check_count_BC = 0
    for i in range(len(BC_check)):

        # breaks the part files into 500 part sections
        parts_i, parts_f = helper_script.BC_breakup(runs_to_run[i])
        n_jobs = len(parts_i)
        
        # initial submission
        if BC_check[i] == True and BC_resubs[i] == 0:
            
            # check if all root files are present
            present = helper_script.check_root(runs_to_run[i],n_jobs,'scratch')

            if present == False:
                print('\nSubmitting BeamCluster job for Run ' + runs_to_run[i] + '...\n')
                # produce processed file tar-ball
                os.system('sh tar_files.sh ' + runs_to_run[i])
                time.sleep(1)
                # just resubmit all of them
                for j in range(n_jobs):
                    # each individual job will use the tar-ball produced above
                    os.system('sh submit_grid_job.sh ' + runs_to_run[i] + ' ' + parts_i[j] + ' ' + parts_f[j])
                    time.sleep(1)
                BC_resubs[i] += 1
            else:
                print('\nall .root file(s) found in /scratch output for Run ' + runs_to_run[i] + ', skipping job submission\n')
                BC_resubs[i] = -1

        # no active jobs but re-submitted (get 1 resubmission)
        elif BC_check[i] == True and BC_resubs[i] == 1:
            
            present = helper_script.check_root(runs_to_run[i],n_jobs,'scratch')

            if present == False:
                print('\nRe-submitting BeamCluster job for Run ' + runs_to_run[i] + '...\n')
                time.sleep(1)
                # again, just resubmit all of them
                for j in range(n_jobs):
                    os.system('sh submit_grid_job.sh ' + runs_to_run[i] + ' ' + parts_i[j] + ' ' + parts_f[j])
                    time.sleep(1)
                BC_resubs[i] += 1
            else:
                print('\nBC job complete for Run ' + runs_to_run[i] + '\n')
                BC_resubs[i] = -1

        elif BC_check[i] == True and BC_resubs[i] == 2:    # no more jobs, but already re-submitted twice
            print('\nMax re-submissions reached for run ' + runs_to_run[i] + ' for BC jobs! Will not transfer to /persistent\n')
            complete_BC += 1

        
        # actually complete, transfer
        elif BC_resubs[i] == -1:
            present = helper_script.check_root(runs_to_run[i],n_jobs,'persistent')
            if present == False:
                os.system('sh BC_copy.sh ' + runs_to_run[i])
                check_count_BC += 1
                complete_BC += 1
            else:
                print('\nRun ' + runs_to_run[i] + ' already transferred\n')
                check_count_BC += 1


        else:   # still running
            check_count_BC += 1     # how many jobs are still active


    if check_count_BC == len(BC_check):
        helper_script.wait(5)     # wait 5 minutes


# BC files are transferred here: /pnfs/annie/persistent/processed/BeamClusterTrees/auto

print('\nNo jobs left! All runs', runs_to_run, 'completed!')
print('\nCleaning up...\n')   # remove leftover tar files containing Processed files for the BC jobs
os.system('rm Processed*.tar.gz')
time.sleep(1)
print('\nExiting...\n')
