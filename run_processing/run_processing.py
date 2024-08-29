import os
import numpy as np
import pandas as pd

print('\nLoading information... (this may take a minute)\n')

df = pd.read_csv('ANNIE_SQL_RUNS.csv', header = 0, dtype = str, skiprows = 1)

# beam runs: 39/34 (new) or 3 (old DAQ)
beam_runs = []
for i in range(len(df)):
    if df.T[i][1] == '39' or df.T[i][1] == '34' or df.T[i][1] == '3':
        beam_runs.append(int(df.T[i][0]))


raw_runs = []; raw_parts = []; no_raw_data = [] # no raw_data runs
for i in range(len(beam_runs)):
    
    raw_data_dir = "/pnfs/annie/persistent/raw/raw/" + str(beam_runs[i]) + "/"

    try:
        raw_files = [file for file in os.listdir(raw_data_dir) if file.startswith("RAWDataR" + str(beam_runs[i]))]
        raw_parts.append(len(raw_files))
        raw_runs.append(beam_runs[i])
        
    except:
        no_raw_data.append(beam_runs[i])
        

# List which runs are fully processed or over >90%, which are present but below 50%, and which ones are absent

pro_parts = []; counter = [0, 0, 0, 0, 0]   # 100%, >90%, >50%, <50%, 0%
missing_runs = []; len_missing_runs = []
for i in range(len(raw_runs)):
    
    processed_dir = "/pnfs/annie/persistent/processed/processed_EBV2/R" + str(raw_runs[i]) + "/"
    
    try:
        processed_files = [file for file in os.listdir(processed_dir) if file.startswith("ProcessedData_PMTMRDLAPPD_R" + str(raw_runs[i])) and not file.endswith(".data")]
        p = len(processed_files)
        if p/raw_parts[i] == 1:
            counter[0] += 1 
        elif p/raw_parts[i] > 0.9:
            counter[1] += 1
        elif p/raw_parts[i] > 0.5:
            counter[2] += 1
        else:
            counter[3] += 1
        pro_parts.append(len(processed_files))
        
    except:
        pro_parts.append(0)
        counter[4] += 1
        missing_runs.append(raw_runs[i])
        len_missing_runs.append(raw_parts[i])

print('Total runs: ' + str(len(raw_runs)) + ' | Total part files: ' + str(sum(raw_parts)))        
print('Total part files processed: ' + str(sum(pro_parts)))
print('Percentage of runs fully processed: ' + str(round(100*(counter[0]/len(raw_runs)),2)) + '%')
print('Percentage of runs processed at least > 90%: ' + str(round(100*((counter[0]+counter[1])/len(raw_runs)),2)) + '%')
print('Percentage of runs that are at least half processed (> 50%): ' + str(round(100*(((counter[0]+counter[1])+counter[2])/len(raw_runs)),2)) + '%')
print('Percentage of runs at least partially processed (> 0%): ' + str(round(100*((counter[0]+counter[1]+counter[2]+counter[3])/len(raw_runs)),2)) + '%')
print('Percentage of runs not processed at all: ' + str(round(100*(counter[4]/len(raw_runs)),2)) + '%')
print('\nTotal percentage of part files processed: ' + str(round(100*(sum(pro_parts)/sum(raw_parts)),2)) + '%')
print('\n')

print('\n------------------------------------')
print('Runs that are not processed/missing:\n')
for i in range(len(missing_runs)):
    print(missing_runs[i], '(', len_missing_runs[i], ')')

print('\n------------------------------------\n')
print('done\n')
