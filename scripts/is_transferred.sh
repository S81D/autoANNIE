#!/bin/bash

# check entirity of run has likely transferred from the DAQ
# a sign all part files are transferred is that typically the final part file will be smaller than the avg 
# (as the data taking was stopped during that part file)

# usage: sh is_transferred.sh

echo ""
if [ -z "$1" ]; then
    echo "Usage: $0 <run_number>"
    exit 1
fi

RUN_NUMBER=$1
DIRECTORY="/pnfs/annie/persistent/raw/raw/$RUN_NUMBER"
RAWFILE="RAWDataR${RUN_NUMBER}S0p*"

# # # # # # # # # # # # # # # # # # # # # # # # # # # # #

# First check if the directory exists
if [ ! -d "$DIRECTORY" ]; then
    echo "Run $RUN_NUMBER does not yet exist in persistent/"
    exit 1
fi

cd "$DIRECTORY" || exit 1

# List the last 3 part files based on their numeric order
echo "Last 3 part files:"
#ls -lh $PATTERN | sort -V | tail -3
ls -lhv $RAWFILE | tail -3

# Calculate the average file size (excluding the last part file)
ALL_FILES=($(ls -1 $RAWFILE | sort -V))
TOTAL_FILES=${#ALL_FILES[@]}

# If there is only 1 part file (or 0), let the user know
if [ $TOTAL_FILES -lt 2 ]; then
    echo "Only one part file for run $RUN_NUMBER!"
    exit 1
fi

# Exclude the last file
EXCLUDING_LAST=${ALL_FILES[@]::($TOTAL_FILES-1)}

TOTAL_SIZE=0
for FILE in ${EXCLUDING_LAST[@]}; do
    SIZE=$(stat --format="%s" "$FILE")
    TOTAL_SIZE=$((TOTAL_SIZE + SIZE))
done

AVERAGE_SIZE=$((TOTAL_SIZE / (TOTAL_FILES - 1)))

# Get the size of the final part file
LAST_FILE=${ALL_FILES[-1]}
LAST_FILE_SIZE=$(stat --format="%s" "$LAST_FILE")

# Calculate the percentage size of the last file compared to the average
PERCENTAGE=$(awk "BEGIN {printf \"%.2f\", ($LAST_FILE_SIZE / $AVERAGE_SIZE) * 100}")

echo ""
echo "Final part file % of average: $PERCENTAGE%"
echo ""
