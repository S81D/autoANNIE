# autoANNIE

Scripts to automatically submit and re-submit jobs to the grid, used to event build and process data into ntuples for analysis. Jobs will submit a tar-ball of ToolAnalysis (https://github.com/ANNIEsoft/ToolAnalysis) to execute the ```EventBuilder``` toolchain and deposit the job output to ```/pnfs/annie/scratch/users/<user>/```. Once part files have been processed, the script will automatically move the data to ```/pnfs/annie/persistent``` and submit jobs to compile that data into easy-to-analyze ntuples by using the ```BeamClusterAnalysis``` toolchain. These ntuples are also moved to ```/pnfs/annie/persistent``` upon completion.

An event building guide using ToolAnalysis can be found here: https://cdcvs.fnal.gov/redmine/projects/annie_experiment/wiki/Event_Building_with_ToolAnalysis

A guide for grid submissions can be found here: https://cdcvs.fnal.gov/redmine/projects/annie_experiment/wiki/General_guideline_for_running_ANNIE_Singularity_Containers_on_Grid

-----------------------

To check on your jobs, use: ```jobsub_q -G annie --user <<username>>```

To cancel job submissions, use: ```jobsub_rm -G annie <<username>>```
