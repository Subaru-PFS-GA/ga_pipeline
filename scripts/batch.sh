#!/bin/bash

# Configure the pipeline for a given field

# NOTE: dSph data from different runs cannot currently be combined into a single catalog!

# Data directories
export DATADIR="/scratch/aszalay1/dobos/pfs"
export OBSLOGDIR="/home/dobos/project/Subaru-PFS/spt_ssp_observation"

# Configure gapipe for a given run and field
declare -a GARUN
declare -a PROPOSAL
declare -a RERUN
declare -a PIPECONFIG
declare -a OBSLOGS
declare -a TARGETLISTS
declare -a ASSIGNMENTS
declare -a VISITS
declare -a CATID
declare -a OBJID

# Batch config from a shell script
BATCHCONFIG=$1
source $BATCHCONFIG

# Pipeline config name. Path is generated from RERUN and PIPECONFIG.
PIPECONFIG=$2

EXTRAPARAMS="--debug"
# EXTRAPARAMS="--top 10 --debug"
# EXTRAPARAMS=""

BATCHPARAMS="--batch slurm --partition cpu --cpus 4 --mem 12G"

function unique_array() {
    local array=("$@")
    local unique_array=()
    local item
    for item in "${array[@]}"; do
        if [[ ! " ${unique_array[*]} " =~ " ${item} " ]]; then
            unique_array+=("$item")
        fi
    done
    echo "${unique_array[@]}"
}

echo "Number of configuration entries: ${#PROPOSAL[@]}"

# Iterate over the configuration entries and run the gapipe-config script for
# each catalog entry.
for i in "${!PROPOSAL[@]}"; do

    # Skip the first few entries
    if [ $i -lt 1 ]; then
        continue
    fi

    echo "Configuring gapipe for entry $i:"
    echo "  GARUN: ${GARUN[$i]}"
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
    export GAPIPE_GARUNDIR="${GARUN[$i]}_${PIPECONFIG}"

#     echo "The following collections are available in the butler repo:"
#     python <<EOF
# from lsst.daf.butler import Butler
# butler = Butler('$DATADIR/ssp/${PROPOSAL[$i]}/2d', writeable=False)
# print(butler.registry.queryCollections())
# EOF

    # Get the list of unique values for each of the configuration parameters
    UNIQUE_OBSLOGS=$(unique_array "${OBSLOGS[$i]}")
    UNIQUE_ASSIGNMENTS=$(unique_array "${ASSIGNMENTS[$i]}")
    UNIQUE_VISITS=$(unique_array "${VISITS[$i]}")
    UNIQUE_CATIDS=$(unique_array "${CATID[$i]}")

    # OBJID[$i]="0x0000000200003a7e"

    # # Generate the configuration files for a given field
    gapipe-configure \
        --config ./configs/gapipe/${RERUN[$i]}/${PIPECONFIG}.py \
        --yes ${EXTRAPARAMS} \
        --visit ${VISITS[$i]} \
        --obs-logs ${OBSLOGS[$i]} \
        --target-lists ${TARGETLISTS[$i]} \
        --catid ${CATID[$i]} \
        --objid ${OBJID[$i]}

    # Schedule the pipeline run for the stars of the given field
    # gapipe-run \
    #     --yes ${EXTRAPARAMS} \
    #     --visit ${VISITS[$i]} \
    #     --catid ${CATID[$i]} \
    #     --objid ${OBJID[$i]} \
    #     $BATCHPARAMS

    # # Generate the catalog for the given field for each catId
    # for catid in ${UNIQUE_CATIDS[*]}; do
    #     echo "Generating catalog for catID $catid"
    #     gapipe-catalog \
    #         --obs-log ${UNIQUE_OBSLOGS[*]} \
    #         --assignments ${UNIQUE_ASSIGNMENTS[*]} \
    #         --visit ${UNIQUE_VISITS[*]} \
    #         --include-missing-objects \
    #         --catid $catid ${EXTRAPARAMS}
    # done

    # exit 0

done