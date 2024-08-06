#!/bin/bash

run=$1
processed_path=$2
output_path=$3

echo ""
echo "Copying BC root file..."
echo ""

source /cvmfs/fermilab.opensciencegrid.org/products/common/etc/setup
setup ifdhc v2_5_4

ifdh cp $output_path/BeamCluster/BeamCluster_$run.root $processed_path/
echo ""
ls -lrth $processed_path/BeamCluster_$run.root
echo ""
