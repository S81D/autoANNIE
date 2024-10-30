# autoANNIE

Scripts to Event build and create ANNIEEvent root files on the grid.

-----------------------

### Usage:

1. After cloning a copy of this repo to your user directory in ```/pnfs/annie/scratch/users/<doran>/```, make sure you have the latest copy of ToolAnalysis built in ```/exp/annie/app/users/<user>/```. It is recommended to keep this directory present and up to date exclusively for these grid jobs.
2. For the event building, this directory will be used to run the ```PreProcessTrigOverlap``` and ```BeamFetcherV2``` toolchains to create the necessary files prior to submitting grid jobs.
3. Both the ```BeamClusterAnalysis``` and ```EventBuilding``` features of this script will submit the same tar-ball of ToolAnalysis. Make the following changes to ToolAnalysis before using/tar-balling:
   - (TODO: change this so it pulls from ```/persistent```) For the `BeamClusterAnalysis` toolchain, copy the necessary LAPPD pedestal files and place them in your ToolAnalysis directory. Ensure the ```Configs``` file for the ```LAPPDLoadStore``` tool correctly points to these files. The current pedestal file path that works for grid submissions is: ```./Pedestals/LAPPD645839/*.txt```.
4. Tar your ToolAnalysis directory via: ```tar -czvf <tarball_name>.tar.gz -C /<path_to_user_directory> <ToolAnalysis_folder>```
5. Copy this tar-ball to your scratch user directory (```/pnfs/annie/scratch/users/<user>/```).
6. Edit ```master_script.py``` to reflect your username, the bind mounted folders you are using when entering the singularity container, and other paths.
7. Copy + paste the updated runs information into `ANNIE_SQL_RUNS.txt` from the ANNIE SQL webpage. This is important as the scripts will know which DLS parameters to use. 
8. Run the the master script: ```python3 master_script.py``` and specify which mode you want to use: (1) for EventBuilder, (2) for BeamClusterAnalysis jobs.
   - Provide the necessary user inputs

-----------------------

An event building guide using ToolAnalysis can be found here: https://cdcvs.fnal.gov/redmine/projects/annie_experiment/wiki/Event_Building_with_ToolAnalysis

A guide for grid submissions can be found here: https://cdcvs.fnal.gov/redmine/projects/annie_experiment/wiki/General_guideline_for_running_ANNIE_Singularity_Containers_on_Grid

-----------------------

To check on your jobs, use: ```jobsub_q -G annie --user <<username>>```

To cancel job submissions, use: ```jobsub_rm -G annie <<username>>```
