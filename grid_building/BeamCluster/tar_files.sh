RUN=$1

export INPUT_PATH=/pnfs/annie/scratch/users/doran/grid_building/BeamCluster/                  
export DATA_PATH=/pnfs/annie/persistent/processed/processed_hits_new_charge/R${RUN}

echo ""
echo "tar-ing processed files..."
echo ""

rm -rf ProcessedRawData_R${RUN}.tar.gz
cp /pnfs/annie/persistent/processed/processed_hits_new_charge/R${RUN}/Processed* .
tar -czvf ProcessedRawData_R${RUN}.tar.gz -C ${INPUT_PATH} ProcessedRawData_Tank*

echo ""
echo "tar-ing complete..."
echo ""

rm ProcessedRawData_Tank*
