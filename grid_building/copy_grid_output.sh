#!/bin/bash

run=$1
processed_path=/pnfs/annie/persistent/processed/processed_hits_new_charge/
output_path=/pnfs/annie/scratch/users/doran/output/  

mkdir -p $processed_path/R$run
chmod 777 $processed_path/R$run

echo ""
echo "Copying Processed Files..."
echo ""

source /cvmfs/fermilab.opensciencegrid.org/products/common/etc/setup
setup ifdhc v2_5_4

ifdh cp $output_path/$run/Processed* $processed_path/R$run/
echo ""
echo "Copying Orphan Files..."
echo ""
ifdh cp $output_path/$run/Orphan* $processed_path/R$run/

rm $processed_path/R$run/*.data
