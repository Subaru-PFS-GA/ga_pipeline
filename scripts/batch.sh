#!/bin/bash

# Configure the pipeline for a given field

# NOTE: dSph data from different runs cannot currently be combined into a single catalog!

# Data directories
export DATADIR="/scratch/aszalay1/dobos/pfs"
export OBSLOGDIR="/home/dobos/project/Subaru-PFS/spt_ssp_observation"

# Configure gapipe for a given run and field
declare -a PROPOSAL
declare -a RERUN
declare -a OBSLOGS
declare -a VISITS
declare -a CATID
declare -a OBJID

CONFIGFILE=$1
source $CONFIGFILE

echo "Number of configuration entries: ${#PROPOSAL[@]}"

# Get the list of unique obs logs from the configuration file to use for catalog generation
UNIQUE_OBSLOGS="$(for log in "${OBSLOGS[*]}"; do echo "$log"; done | sort -u | tr '\n' ' ')"
# echo $UNIQUE_OBSLOGS

# Get the list of unique visits
UNIQUE_VISITS="$(for visit in ${VISITS[*]}; do echo $visit; done | sort -u | tr '\n' ' ')"
# echo $UNIQUE_VISITS

# Get the list of unique CATIDs
UNIQUE_CATIDS="$(for catid in ${CATID[*]}; do echo $catid; done | sort -u | tr '\n' ' ')"
# echo $UNIQUE_CATIDS

# Iterate over the configuration entries and run the gapipe-config script for
# each catalog entry.
for i in "${!PROPOSAL[@]}"; do
    echo "Configuring gapipe for entry $i:"
    echo "  GARUN: ${GARUN}"
    echo "  PROPOSAL: ${PROPOSAL[$i]}"
    echo "  RERUN: ${RERUN[$i]}"
    echo "  VISITS: ${VISITS[$i]}"
    echo "  CATID: ${CATID[$i]}"
    echo "  OBJID: ${OBJID[$i]}"

    # These environment variables are used by the gapipe-configure
    # end gapipe-run to find the relevant data
    export BUTLER_CONFIGDIR="$DATADIR/ssp/${PROPOSAL[$i]}/2d"
    export BUTLER_COLLECTIONS="${RERUN[$i]}"
    export GAPIPE_RERUNDIR="${RERUN[$i]}"
    export GAPIPE_RERUN="${RERUN[$i]}"
    export GAPIPE_GARUNDIR="${GARUN}"

#     echo "The following collections are available in the butler repo:"
#     python <<EOF
# from lsst.daf.butler import Butler
# butler = Butler('$DATADIR/ssp/${PROPOSAL[$i]}/2d', writeable=False)
# print(butler.registry.queryCollections())
# EOF

    # Generate the configuration files for a given field
    # gapipe-configure \
    #     --config ./configs/gapipe/${RERUN[$i]}/single.py \
    #     --yes \
    #     --visit ${VISITS[$i]} \
    #     --obs-logs ${OBSLOGS[$i]} \
    #     --catid ${CATID[$i]} \
    #     --objid ${OBJID[$i]}

    # Schedule the pipeline run for the stars of the given field
    gapipe-run \
        --yes \
        --visit ${VISITS[$i]} \
        --catid ${CATID[$i]} \
        --objid ${OBJID[$i]} \
        --batch slurm --partition cpu --cpus 4 --mem 12G

done

# # Generate the catalog for the given field for each catId
# for catid in ${UNIQUE_CATIDS[*]}; do
#     echo "Generating catalog for catID $catid"
#     gapipe-catalog \
#         --obs-log ${UNIQUE_OBSLOGS[*]} \
#         --visit ${UNIQUE_VISITS[*]} \
#         --catid $catid
# done