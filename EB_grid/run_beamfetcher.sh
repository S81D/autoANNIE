#!/bin/bash

p_start=$1
p_end=$2
APP=$3
SCRATCH=$4
RAW=$5
BIND=$6

while read run; do

    echo ""
    echo "Running BeamFetcherV2 toolchain on run: $run over [${p_start},${p_end}]"
    echo ""

    cd ${APP}
    cd configfiles/BeamFetcherV2/

    # Create my_files.txt with the appropriate part files included

    NUMFILES=$(ls -1q ${RAW}${run}/RAWDataR${run}* | wc -l)
    echo "NUMBER OF FILES IN ${RAW}${run}: ${NUMFILES}"
    rm my_files.txt
    for p in $(seq ${p_start} ${p_end})
    do
        echo "${RAW}${run}/RAWDataR${run}S0p${p}" >> my_files.txt
    done

    cd ../../

    rm beamfetcher_tree.root

    # enter the singularity environment
    singularity shell ${BIND} /cvmfs/singularity.opensciencegrid.org/anniesoft/toolanalysis\:latest/ << EOF

    source Setup.sh

    ./Analyse ./configfiles/BeamFetcherV2/ToolChainConfig

    exit

EOF

    cd ${SCRATCH}

done < "beam.list"
