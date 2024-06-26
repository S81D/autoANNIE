#!/bin/bash

BIND=$1
APP_PATH=$2
RUN=$3

singularity shell ${BIND} /cvmfs/singularity.opensciencegrid.org/anniesoft/toolanalysis\:latest/ << EOF

    root -l -q 'mergeBeamTrees.C("$APP_PATH", $RUN)'

    .q

    exit

EOF

