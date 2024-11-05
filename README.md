# autoANNIE

Scripts to Event build and create ANNIEEvent root files on the grid.

-----------------------

### Usage:

1. After cloning a copy of this repo to your user directory in ```/pnfs/annie/scratch/users/<username>/```, make sure you have the latest copy of ToolAnalysis built in ```/exp/annie/app/users/<username>/```. It is recommended to keep this directory present and up to date and use it exclusively for these grid jobs.
2. The MRD subsystem records data in CDT (and is therefore daylight savings (DLS) dependent). To ensure we enable the correct DLS configuration in the `MRDDataDecoder` tool for the event building toolchain, we must first fetch and generate an SQL txt file from the DAQ's database. Create this txt file locally (based on the following instructions from Marvin: https://cdcvs.fnal.gov/redmine/projects/annie_experiment/wiki/DAQ's_DB_in_the_gpvm): 
   - From your local computer, open a terminal and establish an SSH tunneling:
     - `ssh -K -L 5433:192.168.163.21:5432 annie@annie-gw01.fnal.gov`
   - Run the sql command in another local terminal to get the DB and save it to a .txt file:
     - `echo "Select * from run order by id desc" | psql annie -h localhost -p 5433 -d rundb > ANNIE_SQL_RUNS.txt`
   - Copy it from your local computer to your scratch area:
     - ```scp ANNIE_SQL_RUNS.txt <username>@anniegpvm02.fnal.gov:/pnfs/annie/scratch/users/<username>/<repo_name>/.```
3. Tar your ToolAnalysis directory via: ```tar -czvf <tarball_name>.tar.gz -C /<path_to_user_directory> <ToolAnalysis_folder>```.
4. Copy this tar-ball to your scratch user directory (```/pnfs/annie/scratch/users/<username>/<repo_name>/```).
5. Edit ```master_script.py``` to reflect your username, the bind mounted folders you are using when entering the singularity container, the name of the ANNIE SQL txt file you generated, and other paths.
6. Run the the master script: ```python3 master_script.py``` and specify which mode you want to use: (1) for EventBuilder, (2) for BeamClusterAnalysis jobs, and provide the necessary user inputs when prompted.

-----------------------

### Additional information

- For the event building, the local copy of ToolAnalysis in ```/exp/annie/app/users/<username>/``` will be used to run the ```PreProcessTrigOverlap``` and ```BeamFetcherV2``` toolchains to create the necessary files prior to submitting grid jobs. This is why it is recommended to have an "event building" ToolAnalysis folder present in your `/exp/annie/app/` area.

- No additional modifications of the ToolAnalysis directory is needed prior to tar-balling. The scripts will handle DLS, input filename modifications, LAPPD pedestal files, etc... (assuming you are using the latest event building version).

- Both the ```BeamClusterAnalysis``` and ```EventBuilding``` features of this script will submit the same tar-ball of ToolAnalysis.

- An event building guide using ToolAnalysis can be found here: https://cdcvs.fnal.gov/redmine/projects/annie_experiment/wiki/Event_Building_with_ToolAnalysis

- A guide for grid submissions can be found here: https://cdcvs.fnal.gov/redmine/projects/annie_experiment/wiki/General_guideline_for_running_ANNIE_Singularity_Containers_on_Grid

- Running event building jobs now works on the FNAL-only nodes. More testing is needed to work out additional bugs. For now (October 2024), it is recommended to continue to run the jobs OFFSITE.

-----------------------

To check on your jobs, use: ```jobsub_q -G annie --user <<username>>```

To cancel job submissions, use: ```jobsub_rm -G annie <<username>>```
