# autoANNIE

Scripts to Event build on the grid.

-----------------------

### Usage:

1. After cloning a copy of this repo to your user directory in ```/pnfs/annie/scratch/users/<doran>/```, make the following changes to ToolAnalysis before tar-balling:
   - Ensure ```CreateMyList.sh``` is present in the ```PreProcessTrigOverlap``` toolchain config directory (used for generating the list file)
   - Remove ```my_files.txt``` from both the ```LAPPD_EB``` and ```EventBuilderV2``` toolchains.
2. Tar your ToolAnalysis directory via: ```tar -czvf <tarball_name>.tar.gz -C /<path_to_user_directory> <ToolAnalysis_folder>```
3. Copy this tar-ball to your scratch user directory (```/pnfs/annie/scratch/users/<doran>/```).
4. Edit ```master_script.py``` to reflect your username, the bind mounted folders you are using when entering the singularity container, and other paths.
4. Run the the master script: ```python3 master_script.py```
   - Provide the necessary user inputs

-----------------------

An event building guide using ToolAnalysis can be found here: https://cdcvs.fnal.gov/redmine/projects/annie_experiment/wiki/Event_Building_with_ToolAnalysis

A guide for grid submissions can be found here: https://cdcvs.fnal.gov/redmine/projects/annie_experiment/wiki/General_guideline_for_running_ANNIE_Singularity_Containers_on_Grid

-----------------------

To check on your jobs, use: ```jobsub_q -G annie --user <<username>>```

To cancel job submissions, use: ```jobsub_rm -G annie <<username>>```
