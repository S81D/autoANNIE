RUN=$1
INPUT_PATH=$2BeamCluster/
DATA_PATH=$3R${RUN}/

echo ""
echo "tar-ing processed files..."
echo ""

rm -rf ProcessedRawData_R${RUN}.tar.gz
cp ${DATA_PATH}Processed* BeamCluster/.

cd BeamCluster

tar -czvf ProcessedRawData_R${RUN}.tar.gz -C ${INPUT_PATH} ProcessedData_PMTMRDLAPPD*

echo ""
echo "tar-ing complete..."
echo ""

cd -

rm BeamCluster/ProcessedData_PMTMRDLAPPD*
