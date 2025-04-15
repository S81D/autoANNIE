#!/usr/bin/env python3

# copy multiple runs to persistent/ outside of the event building main scripts
# you need to populate a runs.list file (RUN_LIST) with the desired runs to copy

''' 
usage: python3 scripts/copy_runs.py
'''

import os
import subprocess
import time

# configurations
RUN_LIST = "runs.list"    # list of runs to copy
COPY_SCRIPT = "lib/copy_grid_output.sh"
OUTPUT_PATH = "/pnfs/annie/scratch/users/<user>/output/"      # adjust accordingly
PROCESSED_PATH = "/pnfs/annie/persistent/processed/processingData_EBV2/processed_EBV2/"
LAPPD_PROCESSED_PATH = "/pnfs/annie/persistent/processed/processingData_EBV2/LAPPD_EB_output/"
RAW_PATH = "/pnfs/annie/persistent/raw/raw/"
RUN_TYPE = "beam"            # ProcessedData format will be different for each run type

def missing_after_transfer(run_number):
    raw_data_dir = os.path.join(RAW_PATH, run_number)
    processed_dir = os.path.join(PROCESSED_PATH, f"R{run_number}")

    if RUN_TYPE in ['beam', 'beam_39']:
        basename = "ProcessedData_PMTMRDLAPPD_R"
        indy = 34
    elif RUN_TYPE in ['AmBe', 'LED']:
        basename = "ProcessedData_PMT_R"
        indy = 26
    elif RUN_TYPE == 'laser':
        basename = "ProcessedData_PMTLAPPD_R"
        indy = 31
    elif RUN_TYPE == 'cosmic':
        basename = "ProcessedData_PMTMRD_R"
        indy = 29
    else:
        print(f"Unknown run type: {RUN_TYPE}")
        return

    raw_files = [f for f in os.listdir(raw_data_dir) if f.startswith(f"RAWDataR{run_number}")]
    processed_files = [f for f in os.listdir(processed_dir)
                       if f.startswith(basename + run_number) and not f.endswith(".data")]

    num_raw = len(raw_files)
    num_proc = len(processed_files)

    missing = []
    for f in raw_files:
        expected = basename + f[8:]
        if expected not in processed_files:
            missing.append(int(expected[indy:]))

    print(f"\nRUN {run_number} Summary")
    print(f"Raw: {num_raw} | Processed: {num_proc} | Missing: {len(missing)}")
    print(f"Missing files: {missing}")
    print(f"Percentage processed: {round(num_proc/num_raw*100, 2)}%\n")

    # write summary file
    summary_file = f"R{run_number}_summary.txt"
    with open(summary_file, 'w') as f:
        f.write(f"R{run_number} transfer summary\n")
        f.write(f"\nNumber of raw files: {num_raw}")
        f.write(f"\nNumber of processed files: {num_proc}")
        f.write(f"\nNumber of missing files: {len(missing)}")
        f.write(f"\nMissing processed files: {missing}\n")
        f.write(f"\nPercentage processed: {round(num_proc/num_raw*100, 2)}%\n")

    # copy and clean up
    os.system(f"cp {summary_file} {processed_dir}/.")
    time.sleep(1)
    os.remove(summary_file)

def process_run(run_number):
    print(f"\nTransferring run {run_number}...")

    subprocess.run([
        "bash", COPY_SCRIPT,
        run_number,
        PROCESSED_PATH,
        OUTPUT_PATH,
        LAPPD_PROCESSED_PATH,
        RUN_TYPE
    ])

    missing_after_transfer(run_number)

def main():
    if not os.path.exists(RUN_LIST):
        print(f"Run list '{RUN_LIST}' not found!")
        return

    with open(RUN_LIST, 'r') as f:
        for line in f:
            run = line.strip()
            if run:
                process_run(run)

    print("\nAll runs processed.\n")

if __name__ == "__main__":
    main()
