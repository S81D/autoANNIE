RUN=$1
PI=$2
PF=$3

export INPUT_PATH=/pnfs/annie/scratch/users/doran/grid_building/BeamCluster/                  
export DATA_PATH=/pnfs/annie/persistent/processed/processed_hits_new_charge/R${RUN}

echo ""
echo "submitting job..."
echo ""

QUEUE=medium 

OUTPUT_FOLDER=/pnfs/annie/scratch/users/doran/output/beamcluster/auto/${RUN}
mkdir -p $OUTPUT_FOLDER                                                 

# wrapper script to submit your grid job
jobsub_submit --memory=1000MB --expected-lifetime=2h -G annie --disk=10GB --resource-provides=usage_model=OFFSITE --site=Colorado,BNL,Caltech,Nebraska,SU-OG,UCSD,NotreDame,MIT,Michigan,MWT2,UChicago,Hyak_CE -f ${INPUT_PATH}/ProcessedRawData_R${RUN}.tar.gz -f ${INPUT_PATH}/run_container_job.sh -f ${INPUT_PATH}/BC.tar.gz -d OUTPUT $OUTPUT_FOLDER file://${INPUT_PATH}/grid_job.sh BC_${RUN} ${PI} ${PF}

echo "job name is: BC_${RUN} ${PI} ${PF}"
echo ""

## done ##
