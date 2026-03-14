#!/bin/bash

# Generate catalogs from pfsStar files

# Source the configuration file which sets the environment variables
# for the catalog generation
CONFIGFILE=$1
source $CONFIGFILE

export DATADIR="/scratch/aszalay1/dobos/pfs"
export OBSLOGDIR="/home/dobos/project/Subaru-PFS/spt_ssp_observation"

export GAPIPE_RERUNDIR="$RERUN"
export GAPIPE_RERUN="$RERUN"
export GAPIPE_GARUN="$GARUN"

# Read and parse the obs logs to get the list of visits for the given OBSDATE
export OBSLOGS="$OBSLOGDIR/runs/$OBSDATE/obslog/*.csv"
export VISITS="$(cat $OBSLOGS | grep SSP_GA | cut -d ',' -f 1)"

echo "The following visits are available in the obs log:"
echo $VISITS

# Run the catalog generation command with the specified parameters
gapipe-catalog \
    --rerundir $RERUN \
    --visit $VISITS \
    --obs-log $OBSLOGS \
    --catid $CATID \
    --catname $CATNAME \
    --objid $OBJID \
    --debug