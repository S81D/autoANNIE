#!/bin/bash 
# James Minock 

FIRST_ARG=$1
PART_NAME=$(echo "$FIRST_ARG" | grep -oE '[0-9]+')
PI=$2
PF=$3

# logfile
touch /srv/logfile_${PART_NAME}_${PI}_${PF}.txt 
pwd >> /srv/logfile_${PART_NAME}_${PI}_${PF}.txt
ls -v >>/srv/logfile_${PART_NAME}_${PI}_${PF}.txt
echo "" >>/srv/logfile_${PART_NAME}_${PI}_${PF}.txt

# place the input file containing the necessary data files in the toolchain
echo "my_inputs.txt paths:" >> /srv/logfile_${PART_NAME}_${PI}_${PF}.txt
cat /srv/my_inputs.txt >> /srv/logfile_${PART_NAME}_${PI}_${PF}.txt 
\cp /srv/my_inputs.txt /srv/TA_James_Genie_PR/configfiles/BeamClusterAnalysis/ 
echo "" >> /srv/logfile_${PART_NAME}_${PI}_${PF}.txt

# enter ToolAnalysis directory 
cd TA_James_Genie_PR/ 

# adjust TreeMaker to have the correct name
cd configfiles/BeamClusterAnalysis
python3 config.py ${PART_NAME} ${PI} ${PF}
echo "PhaseIITreeMaker config:" >> /srv/logfile_${PART_NAME}_${PI}_${PF}.txt
cat PhaseIITreeMakerConfig >> /srv/logfile_${PART_NAME}_${PI}_${PF}.txt
echo "" >> /srv/logfile_${PART_NAME}_${PI}_${PF}.txt

cd ../../

# set up paths and libraries 
source Setup.sh 

# Run the toolchain, and output verbose to log file 
./Analyse configfiles/BeamClusterAnalysis/ToolChainConfig >> /srv/logfile_BC_${PART_NAME}_${PI}_${PF}.txt 

# log files
echo"" >> /srv/logfile_${PART_NAME}_${PI}_${PF}.txt
echo "ToolAnalysis directory contents:" >> /srv/logfile_${PART_NAME}_${PI}_${PF}.txt
ls -lrth >> /srv/logfile_${PART_NAME}_${PI}_${PF}.txt
echo "" >> /srv/logfile_${PART_NAME}_${PI}_${PF}.txt
echo "ToolChain files:" >> /srv/logfile_${PART_NAME}_${PI}_${PF}.txt
ls configfiles/BeamClusterAnalysis/ >> /srv/logfile_${PART_NAME}_${PI}_${PF}.txt 
echo "" >> /srv/logfile_${PART_NAME}_${PI}_${PF}.txt

# copy any produced files to /srv for extraction
cp *.ntuple.root /srv/ 

# make sure any output files you want to keep are put in /srv or any subdirectory of /srv 

### END ###
