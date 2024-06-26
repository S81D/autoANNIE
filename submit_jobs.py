import sys

# this file produces the necessary grid submission scripts


# this script is for actually submitting the job to the FermiGrid
def submit_grid_job(run, p_start, p_end, input_path, output_path, TA_tar_name, disk_space, raw_path, trig_path, beamfetcher_path):
    
    file = open('submit_grid_job_' + run + '.sh', "w")

    file.write('export INPUT_PATH=' + input_path +  '\n')
    file.write('export RAWDATA_PATH=' + raw_path + run + '/ \n')
    file.write('\n\n')
    file.write('OUTPUT_FOLDER=' + output_path + run + '\n')
    file.write('mkdir -p $OUTPUT_FOLDER \n')
    file.write('\n')

    # Default (offsite resources) - FNAL-only nodes give errors with current TA
    file.write('jobsub_submit --memory=2000MB --expected-lifetime=2h -G annie --disk=' + disk_space + 'GB --resource-provides=usage_model=OFFSITE --site=Colorado,BNL,Caltech,Nebraska,SU-OG,UCSD,NotreDame,MIT,Michigan,MWT2,UChicago,Hyak_CE ')
    
    for i in range(int(p_start), int(p_end)+1):
        file.write('-f ${RAWDATA_PATH}/RAWDataR' + run + 'S0p' + str(i) + ' ')

    file.write('-f ${INPUT_PATH}/run_container_job_' + run + '.sh ')
    file.write('-f ${INPUT_PATH}/' + TA_tar_name + ' ')
    file.write('-f ' + trig_path + 'R' + run + '_TrigOverlap.tar.gz ')
    file.write('-f ' + beamfetcher_path + 'beamfetcher_' + run + '.root ')
    file.write('-d OUTPUT $OUTPUT_FOLDER ')
    file.write('file://${INPUT_PATH}/grid_job_' + run + '.sh ' + run + '_' + str(p_start) + '_' + str(p_end) + '\n')
    file.close()

    return


# this next script executes on the grid node
def grid_job(run, user, input_path, TA_tar_name, name_TA):

    file = open(input_path + 'grid_job_' + run + '.sh', "w")

    file.write('#!/bin/bash \n')
    file.write('# Steven Doran \n')
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
    file.write('echo "This dummy file belongs to job ${PART_NAME}" >> ${DUMMY_OUTPUT_FILE} \n')
    file.write('echo "" >> ${DUMMY_OUTPUT_FILE} \n')
    file.write('\n')

    file.write('# Copy datafiles \n')
    file.write('${JSB_TMP}/ifdh.sh cp -D $CONDOR_DIR_INPUT/RAWData* . \n')
    file.write('${JSB_TMP}/ifdh.sh cp -D $CONDOR_DIR_INPUT/' + TA_tar_name + ' . \n')
    file.write('${JSB_TMP}/ifdh.sh cp -D $CONDOR_DIR_INPUT/beamfetcher_' + run + '.root . \n') 
    file.write('${JSB_TMP}/ifdh.sh cp -D $CONDOR_DIR_INPUT/R' + run + '_TrigOverlap.tar.gz . \n')
    file.write('tar -xzf ' + TA_tar_name + '\n')
    file.write('tar -xzf R' + run + '_TrigOverlap.tar.gz \n')
    file.write('rm ' + TA_tar_name + '\n')
    file.write('rm R' + run + '_TrigOverlap.tar.gz\n')
    
    file.write('\n')

    file.write('ls -v /srv/RAWData* >> my_files.txt \n')
    file.write('echo "Trig overlap files present:" >> ${DUMMY_OUTPUT_FILE} \n')
    file.write('ls -v /srv/Trig* >> ${DUMMY_OUTPUT_FILE} \n')
    file.write('echo "" >> ${DUMMY_OUTPUT_FILE} \n')
    file.write('echo "BeamFetcherV2 file present:" >> ${DUMMY_OUTPUT_FILE} \n')
    file.write('ls -v /srv/beamfetcher*.root >> ${DUMMY_OUTPUT_FILE} \n')
    file.write('echo "" >> ${DUMMY_OUTPUT_FILE} \n')
    file.write('\n')

    # ensure singularity is properly bind-mounted
    file.write('\n')
    file.write('echo "Make sure singularity is bind mounting correctly (ls /cvmfs/singularity)" >> ${DUMMY_OUTPUT_FILE} \n')
    file.write('ls /cvmfs/singularity.opensciencegrid.org >> ${DUMMY_OUTPUT_FILE} \n')
    file.write('\n')

    file.write('# Setup singularity container \n')
    file.write('singularity exec -B/srv:/srv /cvmfs/singularity.opensciencegrid.org/anniesoft/toolanalysis\:latest/ $CONDOR_DIR_INPUT/run_container_job_' + run + '.sh $PART_NAME \n')
    file.write('\n')

    file.write('echo "Moving the output files to CONDOR OUTPUT..." >> ${DUMMY_OUTPUT_FILE} \n')
    file.write('\n')

    file.write('${JSB_TMP}/ifdh.sh cp -D /srv/logfile* $CONDOR_DIR_OUTPUT \n')
    file.write('${JSB_TMP}/ifdh.sh cp -D /srv/Processed* $CONDOR_DIR_OUTPUT \n')
    file.write('${JSB_TMP}/ifdh.sh cp -D /srv/LAPPDTree*.root $CONDOR_DIR_OUTPUT \n')
    file.write('${JSB_TMP}/ifdh.sh cp -D /srv/offsetFitResult*.root $CONDOR_DIR_OUTPUT \n')
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
    file.write('rm /srv/*_beamdb \n')          # typically this won't be here - but some of the tar folders for the overlap files contain a beamdb file as well (from old version of the tool)
    file.write('rm /srv/beamfetcher_*.root \n')
    file.write('rm -rf ' + name_TA + ' \n')
    file.write('rm /srv/Trig* \n')
    file.write('rm /srv/Processed* \n')
    file.write('rm /srv/LAPPDTree*.root \n')
    file.write('rm /srv/offsetFitResult*.root \n')
    file.write('rm /srv/grid_job*.sh /srv/*.txt \n')
    file.write('echo "" >> ${DUMMY_OUTPUT_FILE} \n')
    file.write('echo "Any remaining contents?" >> ${DUMMY_OUTPUT_FILE} \n')
    file.write('ls -v /srv >> ${DUMMY_OUTPUT_FILE} \n')
    file.write('echo "" >> ${DUMMY_OUTPUT_FILE} \n')
    
    file.write('### END ###')

    file.close()

    return


# third script actually executes the ToolChains once in the singularity environment
def run_container_job(run, name_TA, DLS):

    file = open('run_container_job_' + run + '.sh', "w")

    file.write('#!/bin/bash \n')
    file.write('# Steven Doran \n')
    file.write('\n')

    file.write('PART_NAME=$1 \n')
    file.write('\n')

    file.write('# sanity checks \n')
    file.write('touch /srv/logfile_${PART_NAME}.txt \n')
    file.write('pwd >> /srv/logfile_${PART_NAME}.txt \n')
    file.write('ls -v >>/srv/logfile_${PART_NAME}.txt \n')
    file.write('echo "" >>/srv/logfile_${PART_NAME}.txt \n')
    file.write('\n')

    file.write('# copy files \n')

    file.write('echo "contents of my_files:" >> /srv/logfile_${PART_NAME}.txt \n')
    file.write('cat /srv/my_files.txt >> /srv/logfile_${PART_NAME}.txt \n')
    file.write('pwd >> /srv/logfile_${PART_NAME}.txt \n')
    file.write('echo "" >>/srv/logfile_${PART_NAME}.txt \n')

    file.write('\cp /srv/my_files.txt /srv/' + name_TA + '/configfiles/LAPPD_EB/ \n')
    file.write('\cp /srv/my_files.txt /srv/' + name_TA + '/configfiles/EventBuilderV2/ \n')
    file.write('echo "contents of my_files in the LAPPD_EB toolchain:" >> /srv/logfile_${PART_NAME}.txt \n')
    file.write('cat /srv/' + name_TA + '/configfiles/LAPPD_EB/my_files.txt >> /srv/logfile_${PART_NAME}.txt \n')
    file.write('echo "" >>/srv/logfile_${PART_NAME}.txt \n')
    file.write('echo "contents of my_files in the EventBuilder toolchain:" >> /srv/logfile_${PART_NAME}.txt \n')
    file.write('cat /srv/' + name_TA + '/configfiles/EventBuilderV2/my_files.txt >> /srv/logfile_${PART_NAME}.txt \n')
    file.write('echo "" >>/srv/logfile_${PART_NAME}.txt \n')
    file.write('\cp /srv/beamfetcher_' + run + '.root /srv/' + name_TA + '/ \n')
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
    file.write('echo "LAPPD_EB Toolchain folder contents:" >> /srv/logfile_${PART_NAME}.txt \n')
    file.write('ls configfiles/LAPPD_EB/ >> /srv/logfile_${PART_NAME}.txt \n')
    file.write('echo "" >>/srv/logfile_${PART_NAME}.txt \n')
    file.write('echo "EventBuilder Toolchain folder contents:" >> /srv/logfile_${PART_NAME}.txt \n')
    file.write('ls configfiles/EventBuilderV2/ >> /srv/logfile_${PART_NAME}.txt \n')
    file.write('echo "" >>/srv/logfile_${PART_NAME}.txt \n')

    file.write('\n')

    # Create DaylightSavings Config for MRD
    file.write('# create DLS config for MRD \n')
    file.write('echo "verbosity 0" >> configfiles/EventBuilderV2/MRDDataDecoderConfig \n')
    file.write('echo "DaylightSavingsSpring ' + DLS + '" >> configfiles/EventBuilderV2/MRDDataDecoderConfig \n')
    file.write('\n')
    file.write('# Daylight Savings? (MRDDataDecoder config file) \n')
    file.write('echo "MRDDataDecoderConfig (DLS ' + DLS + ' was selected)" >> /srv/logfile_${PART_NAME}.txt \n')
    file.write('cat configfiles/EventBuilderV2/MRDDataDecoderConfig >> /srv/logfile_${PART_NAME}.txt \n')
    file.write('echo "" >>/srv/logfile_${PART_NAME}.txt \n')
    
    file.write('\n')

    # Change EBSaver beam file path
    file.write('# append EBSaver with the correct beam file name \n')
    file.write('echo "OG EBSaver (incorrect BeamFetcherV2 file):" >> /srv/logfile_${PART_NAME}.txt \n')
    file.write('cat configfiles/EventBuilderV2/EBSaverConfig >> /srv/logfile_${PART_NAME}.txt \n')
    file.write('echo "" >> /srv/logfile_${PART_NAME}.txt \n')
    file.write('echo "EBSaver after, with the correct BeamFetcherV2 file:" >> /srv/logfile_${PART_NAME}.txt \n')
    file.write("sed -i '$ s/.*/beamInfoFileName beamfetcher_" + run + ".root/' configfiles/EventBuilderV2/EBSaverConfig \n")
    file.write('cat configfiles/EventBuilderV2/EBSaverConfig >> /srv/logfile_${PART_NAME}.txt \n')
    file.write('echo "" >> /srv/logfile_${PART_NAME}.txt \n')

    file.write('\n')

    file.write('# set up paths and libraries \n')
    file.write('source Setup.sh \n')
    file.write('\n')

    # this toolchain produces an LAPPDTree.root file
    file.write('# Get LAPPD timing information (LAPPD_EB toolchain) \n')
    # 2>&1 redirects the stderr to the same place as stdout, which is the logfile
    file.write('./Analyse configfiles/LAPPD_EB/ToolChainConfig  >> /srv/logfile_LAPPD_${PART_NAME}.txt 2>&1 \n')      # execute lappd timing info toolchain
    file.write('\n')

    # obtain offsets
    file.write("""root -l -q 'offsetFit_MultipleLAPPD.cpp("LAPPDTree.root", 14, 1, 10, 0)' >> /srv/logfile_LAPPD_${PART_NAME}.txt 2>&1 \n""")
    file.write('\n')

    file.write('# Run the Event Building toolchain \n')
    file.write('./Analyse configfiles/EventBuilderV2/ToolChainConfig  >> /srv/logfile_EventBuilder_${PART_NAME}.txt 2>&1 \n')      # execute Event Building TC
    file.write('\n')
    
    file.write('pwd >> /srv/logfile_${PART_NAME}.txt \n')
    file.write('echo "" >>/srv/logfile_${PART_NAME}.txt \n')
    file.write('echo "Contents of TA after toolchain:" >> /srv/logfile_${PART_NAME}.txt \n')
    file.write('ls -lrth >> /srv/logfile_${PART_NAME}.txt \n')
    file.write('echo "" >>/srv/logfile_${PART_NAME}.txt \n')
    file.write('\n')
    file.write('echo "Copying output files... ending script..." >> /srv/logfile_${PART_NAME}.txt \n')

    file.write('# copy any produced files \n')
    file.write('cp Processed* /srv/ \n')
    file.write('cp LAPPDTree.root /srv/LAPPDTree_${PART_NAME}.root \n')
    file.write('cp offsetFitResult.root /srv/offsetFitResult_${PART_NAME}.root \n')
    file.write('\n')
    file.write('# make sure any output files you want to keep are put in /srv or any subdirectory of /srv \n')
    file.write('\n')
    file.write('### END ###')

    file.close()

    return


### done ###




    














