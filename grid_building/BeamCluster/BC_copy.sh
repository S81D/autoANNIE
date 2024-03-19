#!/bin/bash

run=$1
processed_path=/pnfs/annie/persistent/processed/BeamClusterTrees/auto
output_path=/pnfs/annie/scratch/users/doran/output/beamcluster/auto/ 

mkdir -p $processed_path/R$run
chmod 777 $processed_path/R$run

echo ""
echo "Copying BC root file..."
echo ""

source /cvmfs/fermilab.opensciencegrid.org/products/common/etc/setup
setup ifdhc v2_5_4

ifdh cp $output_path/$run/*.root $processed_path/R$run/
echo ""
ls -lrth $processed_path/R$run/
echo ""
