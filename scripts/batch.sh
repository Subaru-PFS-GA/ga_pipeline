#!/bin/bash

# Configure and execute the pipeline for a given field in batch mode
#
# Usage:
#
#   ./scripts/batch.sh <VERB> <PARAM_RUN> <PARAM_GARUN> <PARAM_CONFIG>
#
# Example:
#
#   ./scripts/batch.sh configure u_price_dobos-20260327_20260327T185911Z dSph_dra_2025-06-pointings b
#
#   This command will load the common config from
#       ./configs/gapipe/u_price_dobos-20260327_20260327T185911Z/common.sh,
#   then load the field-specific config from
#       ./configs/gapipe/u_price_dobos-20260327_20260327T185911Z/garuns/dSph_dra_2025-06-pointings.sh
#   and then run the gapipe-configure command with the configuration template
#       ./configs/gapipe/u_price_dobos-20260327_20260327T185911Z/b.py
#
# Arguments:
#
#   <VERB>: The action to perform. Must be one of:
#       * extract: Extract the data from the PIPE2D run pfsCalibrated files
#       * configure: Generate the configuration files for the GAPIPE run
#       * run: Run the pipeline for the GAPIPE run
#       * submit: Submit the pipeline run to the batch system (if configured)
#       * catalog: Generate the catalog for the GAPIPE run
#   <PARAM_RUN>: The name of the PIPE2D run to process the data from. This should
#       match the directory name in ./configs/gapipe/<PARAM_RUN>/common.sh
#   <PARAM_GARUN>: The name of the GAPIPE run to create. This should match the
#       file name in ./configs/gapipe/<PARAM_RUN>/garuns/<PARAM_GARUN>.sh
#   <PARAM_CONFIG>: The configuration template to use for the gapipe-configure command.
#       This should match the file name in ./configs/gapipe/<PARAM_RUN>/<PARAM_CONFIG>.py
#       Not used with the "extract" verb.

### Developer notes:

# NOTE: data from different PIPE2D runs cannot currently be combined into a single catalog!
# TODO: allow for specifying more than one pipeline configs

### End of developer notes

### Start of user config section -- costumize script behavior here

# Send any extra parameters for gapipe commands
EXTRAPARAMS=""
# EXTRAPARAMS="--dry-run --log-level DEBUG"
# EXTRAPARAMS="--dry-run --debug"
# EXTRAPARAMS="--debug"

# Skip processing entries
SKIP_BEFORE=
SKIP_AFTER=

# Run in slurm, only applies to the gapipe-run command, only used with the "submit" verb
BATCHPARAMS="--batch slurm --partition cpu --cpus 4 --mem 12G"

### End of user config section

declare -a PROPOSAL
declare -a RUN
declare -a RUNDIR
declare -a GARUN
declare -a GARUNDIR
declare -a CONFIGRUN
declare -a CONFIGRUNDIR
declare -a OBSLOGS
declare -a TARGETLISTS
declare -a ASSIGNMENTS
declare -a VISITS
declare -a CATID
declare -a OBJID

function unique_array() {
    local array=("$@")
    local unique_array=()
    local item
    for item in "${array[@]}"; do
        if [[ ! " ${unique_array[*]} " =~ " ${item} " ]]; then
            unique_array+=("$item")
        fi
    done
    echo ${unique_array[@]}
}

function run_cmd() {
    cmd="$1"
    echo "About to run command:"
    echo "$cmd"
    
    # read -p "Proceed with command? [Y/n] " -n 1 -r
    # echo
    # if [[ ! $REPLY =~ ^[Yy]?$ ]]; then
    #     echo "Aborting."
    #     exit 1
    # fi
    
    eval "$cmd"
}

function load_script_config() {
    # Load main config file
    echo "Loading configuration for run $PARAM_RUN."
    source ./configs/gapipe/$PARAM_RUN/common.sh
}

function load_gapipe_config() {
    # Load the GAPIPE config file
    echo "Loading configuration for ${PARAM_GARUN}."
    source ./configs/gapipe/${PARAM_RUN}/garuns/$PARAM_GARUN.sh

    echo "Number of configuration entries: ${#GARUN[@]}"
}

function run_extract() {
    # Find the obs logs and the relevant entries
    echo "Finding visits in the obslog with prefix $OBSPREFIX"
    OBSLOGS="$GAPIPE_OBSLOGDIR/runs/${OBSDATE}/obslog/*.csv"
    VISITS=($(cat ${OBSLOGS[0]} | grep "$OBSPREFIX" | cut -d ',' -f 1))
    echo "Found ${#VISITS[@]} visits in the obs logs for prefix $OBSPREFIX."

    echo "Data directory: $GAPIPE_DATADIR/$GAPIPE_RUNDIR"
    if [[ $GAPIPE_USE_BUTLER -eq 1 ]]; then
        echo "Using butler."
        echo "Butler directory: $BUTLER_CONFIGDIR"
        echo "Butler collections: $BUTLER_COLLECTIONS"
    else
        echo "Not using butler."
    fi

    cmd=$(cat <<EOF
gapipe-repo extract-product PfsCalibrated,PfsSingle \
    --visit ${VISITS[@]} \
    ${EXTRAPARAMS}
EOF
    )

    run_cmd "$cmd"
}

function run_configure() {
    i=$1

    # Generate the configuration files for a given GAPIPE run
    cmd=$(cat <<EOF
gapipe-configure \
    --config "./configs/gapipe/${RUN[$i]}/common.py" "./configs/gapipe/${RUN[$i]}/${PARAM_CONFIG}.py" \
    --yes ${EXTRAPARAMS} \
    --obs-logs ${UNIQUE_OBSLOGS} \
    --target-lists ${UNIQUE_TARGETLISTS} \
    --configrundir "${CONFIGRUNDIR}" \
    --configrun "${CONFIGRUN}" \
    --run "${RUN[$i]}" \
    --rundir "${RUNDIR[$i]}" \
    --garun "${GARUN[$i]}" \
    --garundir "${GARUNDIR[$i]}" \
    --visit ${UNIQUE_VISITS} \
    --catid ${UNIQUE_CATIDS} \
    --objid ${OBJID[$i]}
EOF
    )
    run_cmd "$cmd"
}

function run_run() {
    i=$1

    # Run the pipeline for the given field
        cmd=$(cat <<EOF
gapipe-run \
    --yes ${EXTRAPARAMS} ${BATCHPARAMS} \
    --configrundir "${CONFIGRUNDIR}" \
    --configrun "${CONFIGRUN}" \
    --run "${RUN[$i]}" \
    --rundir "${RUNDIR[$i]}" \
    --garun "${GARUN[$i]}" \
    --garundir "${GARUNDIR[$i]}" \
    --visit ${UNIQUE_VISITS} \
    --catid ${UNIQUE_CATIDS} \
    --objid ${OBJID[$i]}
EOF
    )
    run_cmd "$cmd"
}

function run_catalog() {
    j=$1

    # Generate the catalog for the given field for each catId
    for catid in ${UNIQUE_CATIDS[*]}; do
        echo "Generating catalog for catID $catid"
        gapipe-catalog \
            --config "./configs/gapipe/${RUN[$j]}/common.py" "./configs/gapipe/${RUN[$j]}/${PARAM_CONFIG}.py" \
            --obs-log ${UNIQUE_OBSLOGS[*]} \
            --target-lists ${UNIQUE_TARGETLISTS[*]} \
            --assignments ${UNIQUE_ASSIGNMENTS[*]} \
            --configrundir "${CONFIGRUNDIR}" \
            --configrun "${CONFIGRUN}" \
            --run "${RUN[$j]}" \
            --rundir "${RUNDIR[$j]}" \
            --garun "${GARUN[$j]}" \
            --garundir "${GARUNDIR[$j]}" \
            --visit ${UNIQUE_VISITS[*]} \
            --include-missing-objects \
            --catid $catid ${EXTRAPARAMS}
    done
}

function main_loop() {

    echo "Starting main loop over ${#GARUN[@]} configuration entries."

    # Iterate over the configuration entries and run the gapipe-config script for
    # each catalog entry.
    for i in "${!GARUN[@]}"; do

        # # Skip entries outside the specified range
        if [ -n "$SKIP_BEFORE" ] && [ $i -lt $SKIP_BEFORE ]; then
            echo "Skipping entry $i before $SKIP_BEFORE"
            continue
        fi

        if [ -n "$SKIP_AFTER" ] && [ $i -ge $SKIP_AFTER ]; then
            echo "Skipping entry $i after $SKIP_AFTER"
            continue
        fi

        # Limit to a single object for testing
        # OBJID[$i]="0x0000000200003a7e"

        # Get the list of unique values for each of the configuration parameters
        UNIQUE_OBSLOGS=$(unique_array "${OBSLOGS[$i]}")
        UNIQUE_TARGETLISTS=$(unique_array "${TARGETLISTS[$i]}")
        UNIQUE_ASSIGNMENTS=$(unique_array "${ASSIGNMENTS[$i]}")
        UNIQUE_VISITS=$(unique_array "${VISITS[$i]}")
        UNIQUE_CATIDS=$(unique_array "${CATID[$i]}")

        echo "Running GAPIPE for entry $i:"
        echo "  Input directory: $GAPIPE_DATADIR/$RUNDIR"
        echo "  Work directory: $GAPIPE_WORKDIR/$GAPIPE_RUNDIR"
        echo "  Output directory: $GAPIPE_OUTDIR/$GAPIPE_RUNDIR"
        echo "  PROPOSAL: ${PROPOSAL[$i]}"
        echo "  RUN: ${RUN[$i]}"
        echo "  RUNDIR: ${RUNDIR[$i]}"
        echo "  CONFIGRUN: $CONFIGRUN"
        echo "  CONFIGRUNDIR: $CONFIGRUNDIR"
        echo "  GARUN: ${GARUN[$i]}"
        echo "  GARUNDIR: ${GARUNDIR[$i]}"
        echo "  VISITS: ${UNIQUE_VISITS}"
        echo "  CATID: ${UNIQUE_CATIDS}"
        echo "  OBJID: ${OBJID[$i]}"

        if [[ $GAPIPE_USE_BUTLER -eq 1 ]]; then
            # TODO: how to get BUTLER_CONFIGDIR from the config shell file?
            export BUTLER_COLLECTIONS="${RUN[$i]}"
            export BUTLER_CONFIGDIR="$DATADIR/data/repo/ssp/${PROPOSAL[$i]}/2d"

            echo "Using butler."
            echo "Butler directory: $BUTLER_CONFIGDIR"
            echo "Butler collections: $BUTLER_COLLECTIONS"

    #     echo "The following collections are available in the butler repo:"
    #     python <<EOF
    # from lsst.daf.butler import Butler
    # butler = Butler('$DATADIR/ssp/${PROPOSAL[$i]}/2d', writeable=False)
    # print(butler.registry.queryCollections())
    # EOF

        else
            echo "Not using butler for entry $i."
        fi

        case $PARAM_VERB in
            "configure")
                run_configure $i
                ;;
            "run"|"submit")
                run_run $i
                ;;
            "catalog")
                run_catalog $i
                ;;
            *)
                echo "Invalid verb: $PARAM_VERB. Must be one of: extract, configure, run, submit, catalog."
                exit 1
                ;;
        esac

    done
}

set -e

# Process command line arguments

PARAM_VERB="$1"           # extract, configure, run, submit, catalog
PARAM_RUN="$2"            # PIPE2D run
PARAM_GARUN="$3"          # GAPIPE run
PARAM_CONFIG="$4"         # Pipeline config name. Path is generated from RUN and PIPECONFIG.

# Set variables that are used in the config files based on the command line arguments

GAPIPE_RUN="${PARAM_RUN}"
GAPIPE_CONFIG="${PARAM_CONFIG}"

case $PARAM_VERB in
    "extract")
        load_script_config
        run_extract
        ;;
    "configure")
        load_script_config
        load_gapipe_config
        main_loop
        ;;
    "run")
        load_script_config
        load_gapipe_config
        BATCHPARAMS=""
        main_loop
        ;;
    "submit")
        load_script_config
        load_gapipe_config
        main_loop
        ;;
    "catalog")
        load_script_config
        load_gapipe_config
        main_loop
        ;;
    *)
        echo "Invalid verb: $PARAM_VERB. Must be one of: extract, configure, run, submit, catalog."
        exit 1
        ;;
esac

set +e