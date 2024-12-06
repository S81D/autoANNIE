#!/bin/bash

BIND=$1
FILE_PATH=$2
RUN=$3
WHICH=$4

if [ "$WHICH" == "beamfetcher" ]; then
    FILE_NAME="beamfetcher"
    TREE_NAME="BeamTree"
elif [ "$WHICH" == "BC" ]; then
    FILE_NAME="BeamCluster"
    TREE_NAME="Event"
elif [ "$WHICH" == "LAPPD" ]; then
    FILE_NAME="LAPPDBeamCluster"
    TREE_NAME="Event"
fi

singularity shell ${BIND} /cvmfs/singularity.opensciencegrid.org/anniesoft/toolanalysis\:latest/ << EOF

    root -l -q 'lib/mergeBeamTrees.C("$FILE_PATH", $RUN, "$FILE_NAME", "$TREE_NAME")'

    .q

    exit

EOF


if [ "$WHICH" == "BC" ]; then
    mv BeamCluster_$RUN.root BeamCluster/
    echo ""
    ls -lrth BeamCluster/BeamCluster*
    echo ""
elif [ "$WHICH" == "LAPPD" ]; then
    mv LAPPDBeamCluster_$RUN.root BeamCluster/
    echo ""
    ls -lrth BeamCluster/LAPPDBeamCluster*
    echo ""
fi

