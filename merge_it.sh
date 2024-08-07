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
fi

singularity shell ${BIND} /cvmfs/singularity.opensciencegrid.org/anniesoft/toolanalysis\:latest/ << EOF

    root -l -q 'mergeBeamTrees.C("$FILE_PATH", $RUN, "$FILE_NAME", "$TREE_NAME")'

    .q

    exit

EOF

