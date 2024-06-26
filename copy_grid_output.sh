#!/bin/bash

run=$1
data_path=$2
output_path=$3
lappd_EB_path=$4


# Copy processed files

mkdir -p $data_path/R$run
chmod 777 $data_path/R$run

echo ""
echo "Copying Processed Files..."
echo ""

source /cvmfs/fermilab.opensciencegrid.org/products/common/etc/setup
setup ifdhc v2_5_4

ifdh cp $output_path/$run/Processed* $data_path/R$run/


# Copy LAPPD-related EB files (LAPPDTree + offsetFit result)

mkdir -p $lappd_EB_path/LAPPDTree/R$run
chmod 777 $lappd_EB_path/LAPPDTree/R$run

mkdir -p $lappd_EB_path/offsetFit/R$run
chmod 777 $lappd_EB_path/offsetFit/R$run

echo ""
echo "Copying LAPPD Files..."
echo ""

ifdh cp $output_path/$run/LAPPDTree* $lappd_EB_path/LAPPDTree/R$run/
ifdh cp $output_path/$run/offsetFitResult* $lappd_EB_path/offsetFit/R$run/
