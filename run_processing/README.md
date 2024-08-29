.csv files contain copied SQL entries from the database. Get a global look at how many processed runs have been completed, what fraction of total part files have been processed, and which runs still need to be processed.

All runs are included in the .csv files for the 2022, 2023, and 2024 beam year periods. Replace run type in the python script ```run_processing.py``` to check other run types. Currently, the script is set up to check for beam runs.

Note that for 2023-2024, the beam trigger word is either 34 or 39, while in 2022 it was 3. You will need to change this in the run_processing script to reflect this.

### Usage: 
1. ```<enter_your_singularity_container>```
2. ```python3 run_processing.py```

Entering the singularity container is required as the script loads the ```pandas``` library.
