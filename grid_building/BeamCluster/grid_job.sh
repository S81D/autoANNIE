#!/bin/bash 
# From James Minock 

cat <<EOF
condor   dir: $CONDOR_DIR_INPUT 
process   id: $PROCESS 
output   dir: $CONDOR_DIR_OUTPUT 
EOF

HOSTNAME=$(hostname -f) 
GRIDUSER="doran"            

# Argument passed through job submission 
FIRST_ARG=$1
PART_NAME=$(echo "$FIRST_ARG" | grep -oE '[0-9]+')
PI=$2
PF=$3

# Create a dummy log file in the output directory
DUMMY_OUTPUT_FILE=${CONDOR_DIR_OUTPUT}/${JOBSUBJOBID}_dummy_output 
touch ${DUMMY_OUTPUT_FILE} 

# Copy datafiles from $CONDOR_INPUT onto worker node (/srv)
${JSB_TMP}/ifdh.sh cp -D $CONDOR_DIR_INPUT/ProcessedRawData_R${PART_NAME}.tar.gz . 
${JSB_TMP}/ifdh.sh cp -D $CONDOR_DIR_INPUT/BC.tar.gz . 

# un-tar TA
tar -xzf BC.tar.gz
tar -xzf ProcessedRawData_R${PART_NAME}.tar.gz
rm BC.tar.gz
rm ProcessedRawData_R${PART_NAME}.tar.gz

# Remove Processed files outside bounds of the job
echo "Removing Processed files outside job limits..." >> ${DUMMY_OUTPUT_FILE}
for file in /srv/Processed*; do
    part_number=$(echo "$file" | sed 's/.*S0p\([0-9]*\)$/\1/')
    if [ "$part_number" -lt "$PI" ] || [ "$part_number" -gt "$PF" ]; then
        rm "$file"
    fi
done
ls -v /srv/Processed* >> ${DUMMY_OUTPUT_FILE}
echo "Loop complete" >> ${DUMMY_OUTPUT_FILE}
echo "" >> ${DUMMY_OUTPUT_FILE}

ls -v /srv/Processed* >> my_inputs.txt

echo "" >> ${DUMMY_OUTPUT_FILE}
echo "Processed files present:" >> ${DUMMY_OUTPUT_FILE}
ls -v /srv/Processed* >> ${DUMMY_OUTPUT_FILE}
echo "" >> ${DUMMY_OUTPUT_FILE}

# Setup singularity container 
singularity exec -B/srv:/srv /cvmfs/singularity.opensciencegrid.org/anniesoft/toolanalysis\:latest/ $CONDOR_DIR_INPUT/run_container_job.sh $PART_NAME $PI $PF

# ------ The script run_container_job.sh will now run within singularity ------ #

# cleanup and move files to $CONDOR_OUTPUT after leaving singularity environment
echo "Moving the output files to CONDOR OUTPUT..." >> ${DUMMY_OUTPUT_FILE} 
${JSB_TMP}/ifdh.sh cp -D /srv/logfile* $CONDOR_DIR_OUTPUT         # log files
${JSB_TMP}/ifdh.sh cp -D /srv/*.ntuple.root $CONDOR_DIR_OUTPUT    # Modify: any .root files etc.. that are produced from your toolchain

echo "" >> ${DUMMY_OUTPUT_FILE} 
echo "Input:" >> ${DUMMY_OUTPUT_FILE} 
ls $CONDOR_DIR_INPUT >> ${DUMMY_OUTPUT_FILE} 
echo "" >> ${DUMMY_OUTPUT_FILE} 
echo "Output:" >> ${DUMMY_OUTPUT_FILE} 
ls $CONDOR_DIR_OUTPUT >> ${DUMMY_OUTPUT_FILE} 

echo "" >> ${DUMMY_OUTPUT_FILE} 
echo "Cleaning up..." >> ${DUMMY_OUTPUT_FILE} 
echo "srv directory:" >> ${DUMMY_OUTPUT_FILE} 
ls -v /srv >> ${DUMMY_OUTPUT_FILE} 

# make sure to clean up the files left on the worker node
rm /srv/Processed* 
rm /srv/my_inputs.txt
rm -rf /srv/TA_James_Genie_PR 

### END ###
