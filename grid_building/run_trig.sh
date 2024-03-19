#!/bin/bash

APP=/exp/annie/app/users/doran/DD_TA
SCRATCH=/pnfs/annie/scratch/users/doran/grid_building/

while read run; do

    echo ""
    echo "Running PreProcessTrigOverlap on run: $run"

    cd ${APP}
    cd configfiles/PreProcessTrigOverlap/

    sh CreateMyList.sh $run

    cd ../../

    # enter the singularity environment
    singularity shell -B/pnfs:/pnfs,/exp/annie/app/users/doran/temp_directory:/tmp,/exp/annie/data:/exp/annie/data,/exp/annie/app:/exp/annie/app /cvmfs/singularity.opensciencegrid.org/anniesoft/toolanalysis\:latest/ << EOF

    source Setup.sh

    ./Analyse ./configfiles/PreProcessTrigOverlap/ToolChainConfig
    
    exit

EOF
    
    tar -czvf R${run}_TrigOverlap.tar.gz -C ${APP} TrigOverlap_R${run}S0p*
    
    cp R${run}_TrigOverlap.tar.gz /pnfs/annie/persistent/processed/trigoverlap/.

    rm TrigOverlap_R${run}S0p*

    cd ${SCRATCH}


done < "trig.list"
