import sys

# this file is called by auto_submit_job.py to produce the necessary grid submission scripts


# this script is for actually submitting the job to the FermiGrid
def submit_grid_job(run, p_start, p_end, input_path, output_path, TA_tar_name, disk_space):
    
    file = open('submit_grid_job.sh', "w")

    file.write('export INPUT_PATH=' + input_path +  '\n')
    file.write('export RAWDATA_PATH=/pnfs/annie/persistent/raw/raw/' + run + '/ \n')
    file.write('\n')
    #file.write('QUEUE=medium \n')
    file.write('\n')
    file.write('OUTPUT_FOLDER=' + output_path + run + '\n')
    file.write('mkdir -p $OUTPUT_FOLDER \n')
    file.write('\n')
    file.write('jobsub_submit --memory=4000MB --expected-lifetime=2h -G annie --disk=' + disk_space + 'GB --resource-provides=usage_model=OFFSITE --site=Colorado,BNL,Caltech,Nebraska,SU-OG,UCSD,NotreDame,MIT,Michigan,MWT2,UChicago,Hyak_CE ')

    for i in range(int(p_start), int(p_end)+1):
        file.write('-f ${RAWDATA_PATH}/RAWDataR' + run + 'S0p' + str(i) + ' ')

    #file.write('-f ${INPUT_PATH}/' + run + '_beamdb ')
    file.write('-f ${INPUT_PATH}/run_container_job.sh ')
    file.write('-f ${INPUT_PATH}/' + TA_tar_name + ' ')
    file.write('-f /pnfs/annie/persistent/processed/trigoverlap/R' + run + '_TrigOverlap.tar.gz ')
    file.write('-d OUTPUT $OUTPUT_FOLDER ')
    file.write('file://${INPUT_PATH}/grid_job.sh AUTO_' + run + '_' + str(p_start) + '_' + str(p_end) + '\n')
    file.close()

    return


# this next script is what is ran once it is in the grid
def grid_job(run, user, input_path, TA_tar_name, name_TA):

    file = open(input_path + 'grid_job.sh', "w")

    file.write('#!/bin/bash \n')
    file.write('# From James Minock \n')
    file.write('\n')

    file.write('cat <<EOF\n')
    file.write('condor   dir: $CONDOR_DIR_INPUT \n')
    file.write('process   id: $PROCESS \n')
    file.write('output   dir: $CONDOR_DIR_OUTPUT \n')
    file.write('EOF\n')
    file.write('\n')

    file.write('HOSTNAME=$(hostname -f) \n')
    file.write('GRIDUSER="' + user + '" \n')
    file.write('\n')

    file.write('# Argument passed through job submission \n')
    file.write('PART_NAME=$1 \n')
    file.write('\n')

    file.write('# Create a dummy file in the output directory \n')
    file.write('DUMMY_OUTPUT_FILE=${CONDOR_DIR_OUTPUT}/${JOBSUBJOBID}_dummy_output \n')
    file.write('touch ${DUMMY_OUTPUT_FILE} \n')
    file.write('\n')

    file.write('# Copy datafiles \n')
    file.write('${JSB_TMP}/ifdh.sh cp -D $CONDOR_DIR_INPUT/RAWData* . \n')
    file.write('${JSB_TMP}/ifdh.sh cp -D $CONDOR_DIR_INPUT/' + TA_tar_name + ' . \n')
    #file.write('${JSB_TMP}/ifdh.sh cp -D $CONDOR_DIR_INPUT/' + run + '_beamdb . \n') 
    file.write('${JSB_TMP}/ifdh.sh cp -D $CONDOR_DIR_INPUT/R' + run + '_TrigOverlap.tar.gz . \n')
    file.write('tar -xzf ' + TA_tar_name + '\n')
    file.write('tar -xzf R' + run + '_TrigOverlap.tar.gz \n')
    file.write('rm ' + TA_tar_name + '\n')
    file.write('rm R' + run + '_TrigOverlap.tar.gz\n')
    
    file.write('\n')

    file.write('ls -v /srv/RAWData* >> my_files.txt \n')
    file.write('echo "" >> ${DUMMY_OUTPUT_FILE} \n')
    file.write('echo "Trig overlap files present:" >> ${DUMMY_OUTPUT_FILE} \n')
    file.write('ls -v /srv/Trig* >> ${DUMMY_OUTPUT_FILE} \n')
    file.write('echo "" >> ${DUMMY_OUTPUT_FILE} \n')
    file.write('\n')

    file.write('# Setup singularity container \n')
    file.write('singularity exec -B/srv:/srv /cvmfs/singularity.opensciencegrid.org/anniesoft/toolanalysis\:latest/ $CONDOR_DIR_INPUT/run_container_job.sh $PART_NAME \n')
    file.write('\n')

    file.write('echo "Moving the output files to CONDOR OUTPUT..." >> ${DUMMY_OUTPUT_FILE} \n')
    file.write('\n')

    file.write('${JSB_TMP}/ifdh.sh cp -D /srv/logfile* $CONDOR_DIR_OUTPUT \n')
    file.write('${JSB_TMP}/ifdh.sh cp -D /srv/Orphan* $CONDOR_DIR_OUTPUT \n')
    file.write('${JSB_TMP}/ifdh.sh cp -D /srv/Processed* $CONDOR_DIR_OUTPUT \n')
    file.write('\n')

    file.write('echo "" >> ${DUMMY_OUTPUT_FILE} \n')
    file.write('echo "Input:" >> ${DUMMY_OUTPUT_FILE} \n')
    file.write('ls $CONDOR_DIR_INPUT >> ${DUMMY_OUTPUT_FILE} \n')
    file.write('echo "" >> ${DUMMY_OUTPUT_FILE} \n')
    file.write('echo "Output:" >> ${DUMMY_OUTPUT_FILE} \n')
    file.write('ls $CONDOR_DIR_OUTPUT >> ${DUMMY_OUTPUT_FILE} \n')
    file.write('\n')
    file.write('echo "" >> ${DUMMY_OUTPUT_FILE} \n')
    file.write('echo "Cleaning up..." >> ${DUMMY_OUTPUT_FILE} \n')
    file.write('echo "srv directory:" >> ${DUMMY_OUTPUT_FILE} \n')
    file.write('ls -v /srv >> ${DUMMY_OUTPUT_FILE} \n')
    file.write('rm /srv/RAWData* \n')
    file.write('rm /srv/my_files.txt \n')
    file.write('rm /srv/' + run + '_beamdb \n')    # typically this won't be here - but some of the tar folders for the overlap files contain a beamdb file as well
    file.write('rm -rf ' + name_TA + '/ \n')
    file.write('rm /srv/Trig* \n')
    file.write('rm /srv/Orphan* /srv/Processed* \n')
    file.write('rm /srv/grid_job.sh /srv/*.txt \n')
    file.write('echo "" >> ${DUMMY_OUTPUT_FILE} \n')
    file.write('echo "Any remaining contents?" >> ${DUMMY_OUTPUT_FILE} \n')
    file.write('ls -v /srv >> ${DUMMY_OUTPUT_FILE} \n')
    file.write('echo "" >> ${DUMMY_OUTPUT_FILE} \n')
    
    file.write('### END ###')

    file.close()

    return


# third script is the script that actually execuates the ToolChains once in the singularity environment
def run_container_job(run, name_TA):

    file = open('run_container_job.sh', "w")

    file.write('#!/bin/bash \n')
    file.write('# James Minock \n')
    file.write('\n')

    file.write('PART_NAME=$1 \n')
    file.write('\n')

    file.write('# sanity checks \n')
    file.write('touch /srv/logfile_${PART_NAME}.txt \n')
    file.write('pwd >> /srv/logfile_${PART_NAME}.txt \n')
    file.write('ls -v >>/srv/logfile_${PART_NAME}.txt \n')
    file.write('echo "" >>/srv/logfile_${PART_NAME}.txt \n')
    file.write('\n')


    # modify if needed, depending on the TC you are running

    file.write('# copy files \n')

    file.write('echo "contents of my_files:" >> /srv/logfile_${PART_NAME}.txt \n')
    file.write('cat /srv/my_files.txt >> /srv/logfile_${PART_NAME}.txt \n')
    file.write('pwd >> /srv/logfile_${PART_NAME}.txt \n')
    file.write('echo "" >>/srv/logfile_${PART_NAME}.txt \n')

    file.write('\cp /srv/my_files.txt /srv/' + name_TA + '/configfiles/EventBuilder/ \n')
    file.write('echo "contents of my_files in the toolchain:" >> /srv/logfile_${PART_NAME}.txt \n')
    file.write('cat /srv/' + name_TA + '/configfiles/EventBuilder/my_files.txt >> /srv/logfile_${PART_NAME}.txt \n')
    file.write('echo "" >>/srv/logfile_${PART_NAME}.txt \n')
    #file.write('\cp /srv/' + run + '_beamdb /srv/' + name_TA + '/ \n')
    file.write('\cp /srv/Trig* /srv/' + name_TA + '/ \n')
    file.write('echo "ToolAnalysis directory contents:" >> /srv/logfile_${PART_NAME}.txt \n')
    file.write('ls -v /srv/' + name_TA + '/ >>/srv/logfile_${PART_NAME}.txt \n')
    file.write('echo "" >>/srv/logfile_${PART_NAME}.txt \n')
    
    
    file.write('\n')

    file.write('# enter ToolAnalysis directory \n')
    file.write('cd ' + name_TA + '/ \n')
    file.write('echo "Are we now in the ToolAnalysis directory?" >> /srv/logfile_${PART_NAME}.txt \n')
    file.write('pwd >> /srv/logfile_${PART_NAME}.txt \n')
    file.write('echo "" >>/srv/logfile_${PART_NAME}.txt \n')
    file.write('echo "Toolchain folder contents:" >> /srv/logfile_${PART_NAME}.txt \n')
    file.write('ls configfiles/EventBuilder/ >> /srv/logfile_${PART_NAME}.txt \n')
    file.write('echo "" >>/srv/logfile_${PART_NAME}.txt \n')

    file.write('\n')

    file.write('# Daylight Savings? (MRDDataDecoder config file) \n')
    file.write('echo "MRDDataDecoderConfig" >> /srv/logfile_${PART_NAME}.txt \n')
    file.write('cat configfiles/EventBuilder/MRDDataDecoderConfig >> /srv/logfile_${PART_NAME}.txt \n')
    file.write('echo "" >>/srv/logfile_${PART_NAME}.txt \n')
    
    file.write('\n')

    file.write('# set up paths and libraries \n')
    file.write('source Setup.sh \n')
    file.write('\n')

    file.write('# Run the toolchain \n')
    file.write('./Analyse configfiles/EventBuilder/ToolChainConfig  >> /srv/logfile_EventBuilder_${PART_NAME}.txt \n')      # execute Event Building TC
    file.write('\n')
    
    file.write('pwd >> /srv/logfile_${PART_NAME}.txt \n')
    file.write('echo "" >>/srv/logfile_${PART_NAME}.txt \n')
    file.write('echo "Contents of TA after toolchain:" >> /srv/logfile_${PART_NAME}.txt \n')
    file.write('ls -lrth >> /srv/logfile_${PART_NAME}.txt \n')
    file.write('echo "" >>/srv/logfile_${PART_NAME}.txt \n')
    file.write('\n')
    file.write('echo "Copying output files... ending script..." >> /srv/logfile_${PART_NAME}.txt \n')

    file.write('# copy any produced files \n')
    file.write('cp Orphan* /srv/ \n')
    file.write('cp Processed* /srv/ \n')
    file.write('\n')
    file.write('# make sure any output files you want to keep are put in /srv or any subdirectory of /srv \n')
    file.write('\n')
    file.write('### END ###')

    file.close()

    return


### done ###




    














