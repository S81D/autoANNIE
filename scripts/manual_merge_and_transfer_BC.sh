#!/bin/bash

# if some pesky BeamCluster jobs are holding and you want to initiate the merge + transfer, run this script!
# Usage: sh manual_merge_and_transfer_BC.sh <run>

if [ "$#" -ne 1 ]; then
    echo "Usage: sh manual_merge_and_transfer_BC.sh <run>"
    exit 1
fi

RUN=$1

# edit accordingly

USER=<USER>

SINGULARITY_COMMAND="-B/pnfs:/pnfs,/exp/annie/app/users/${USER}/temp_directory:/tmp,/exp/annie/data:/exp/annie/data,/exp/annie/app:/exp/annie/app"
BC_PATH=/pnfs/annie/persistent/processed/processingData_EBV2/BeamClusterTrees/
SCRATCH_PATH=/pnfs/annie/scratch/users/${USER}/autoANNIE/
LAPPD_BC_PATH=/pnfs/annie/persistent/processed/processingData_EBV2/BeamClusterTrees/LAPPDBeamClusterTrees/
BC_SCRATCH_OUTPUT_PATH=/pnfs/annie/scratch/users/${USER}/output/beamcluster/
LAPPD_FILTER_PATH=/pnfs/annie/persistent/processed/processingData_EBV2/processed_EBV2_LAPPDFiltered/
MRD_FILTER_PATH=/pnfs/annie/persistent/processed/processingData_EBV2/processed_EBV2_MRDFiltered/


# - - - - - - - - - - - - - - - - - - - - - - - - - - 
# first merge the BC files

# BC                    --> singularity arg is passed differently to preserve the string structure in quotes for proper bind-mounting when we enter the container
sh lib/merge_it.sh ${SINGULARITY_COMMAND} "$BC_SCRATCH_OUTPUT_PATH" "$RUN" BC
sleep 1

# LAPPD
sh lib/merge_it.sh ${SINGULARITY_COMMAND} "$BC_SCRATCH_OUTPUT_PATH" "$RUN" LAPPD
sleep 1


# - - - - - - - - - - - - - - - - - - - - - - - - - - 
# copy merged and filtered files (../BeamCluster/BC_copy.sh) path assumes you're working out of the scripts/ subfolder

# BC
sh BeamCluster/BC_copy.sh ${RUN} ${BC_PATH} ${SCRATCH_PATH} BC ${LAPPD_BC_PATH} ${BC_SCRATCH_OUTPUT_PATH} ${LAPPD_FILTER_PATH} ${MRD_FILTER_PATH}
sleep 1

# LAPPD BC
sh BeamCluster/BC_copy.sh ${RUN} ${BC_PATH} ${SCRATCH_PATH} LAPPD ${LAPPD_BC_PATH} ${BC_SCRATCH_OUTPUT_PATH} ${LAPPD_FILTER_PATH} ${MRD_FILTER_PATH}
sleep 1

# LAPPD_filter
sh BeamCluster/BC_copy.sh ${RUN} ${BC_PATH} ${SCRATCH_PATH} LAPPD_filter ${LAPPD_BC_PATH} ${BC_SCRATCH_OUTPUT_PATH} ${LAPPD_FILTER_PATH} ${MRD_FILTER_PATH}
sleep 1

# MRD_filter
sh BeamCluster/BC_copy.sh ${RUN} ${BC_PATH} ${SCRATCH_PATH} MRD_filter ${LAPPD_BC_PATH} ${BC_SCRATCH_OUTPUT_PATH} ${LAPPD_FILTER_PATH} ${MRD_FILTER_PATH}
sleep 1

echo ""
echo "done"
echo ""
