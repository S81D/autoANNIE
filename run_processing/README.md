.csv file contains copied SQL entries from the database. Get a global look at how many processed runs have been completed, what fraction of total part files have been processed, and which runs still need to be processed.

All runs are included in the .csv files for the PhaseII data taking period (since the 2021 DAQ restart). Replace run type in the python script ```run_processing.py``` to check other run types. Currently, the script is set up to check for beam runs.

Included in the .csv file is a column designating whether the run occured during DLS (1) or not (0); runs with -9999 occured during the DLS transition period. This is relevant for evet building with the MRD + Veto, as that detector subsystem records timestamps in local DLS-dependent central time. 

### Usage: 
1. ```<enter_your_singularity_container>```
2. ```python3 run_processing.py```

Entering the singularity container is required as the script loads the ```pandas``` library.
