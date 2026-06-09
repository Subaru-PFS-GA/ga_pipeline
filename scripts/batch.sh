#!/bin/bash

# Configure and execute the pipeline for a given field in batch mode
#
# Usage:
#
#   ./scripts/batch.sh <VERB> <PARAM_RUN> <PARAM_GARUN> <PARAM_CONFIG>
#
# Example:
#
#   ./scripts/batch.sh download S25A_April2026 dSph_dra_2025-03 b_mr
#   ./scripts/batch.sh extract S25A_April2026 dSph_dra_2025-03 b_mr
#   ./scripts/batch.sh configure S25A_April2026 dSph_dra_2025-03 b_mr
#   ./scripts/batch.sh submit S25A_April2026 dSph_dra_2025-03 b_mr
#   ./scripts/batch.sh catalog S25A_April2026 dSph_dra_2025-03 b_mr
#
#   This command will load the common config from
#       ./configs/gapipe/S25A_April2026/common.sh,
#   then load the field-specific config from
#       ./configs/gapipe/S25A_April2026/garuns/dSph_dra_2025-03.sh
#   and then run the gapipe-configure command with the configuration template
#       ./configs/gapipe/S25A_April2026/b_mr.py
#
# Arguments:
#
#   <VERB>: The action to perform. Must be one of:
#       * download: Download the pfsConfig and pfsCalibrated files for the visits
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
#       This should match the file name in ./configs/gapipe/<PARAM_CONFIG>.py
#       Not used with the "extract" verb.

### Developer notes:

# NOTE: data from different PIPE2D runs cannot currently be combined into a single catalog!
# TODO: allow for specifying more than one pipeline configs

### End of developer notes

### Start of user config section -- costumize script behavior here

# Send any extra parameters for gapipe commands
# EXTRAPARAMS=""
# EXTRAPARAMS="--dry-run --log-level DEBUG --top 10"
# EXTRAPARAMS="--dry-run --log-level DEBUG"
# EXTRAPARAMS="--dry-run --debug"
# EXTRAPARAMS="--debug"
# EXTRAPARAMS="--log-level DEBUG"
# EXTRAPARAMS="--top 10"


# Skip processing entries
SKIP_BEFORE=1
SKIP_AFTER=

# Run in slurm, only applies to the gapipe-run command, only used with the "submit" verb
BATCH_PARTITION="cpu"
BATCH_PARAMS="--batch slurm --partition ${BATCH_PARTITION} --cpus 4 --mem 12G"

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
    printf '%s\n' "${unique_array[@]}" | sort | tr '\n' ' '
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
    # Combine all visits from all configuration entries into a single array
    ALL_VISITS=()
    for i in "${!VISITS[@]}"; do
        ALL_VISITS+=(${VISITS[$i]})
    done

    echo "Extracting data for ${#ALL_VISITS[@]} visits."

    UNIQUE_VISITS=($(unique_array "${ALL_VISITS[@]}"))
    echo "Found ${#UNIQUE_VISITS[@]} unique visits in the obs logs."

    # TODO: limit extract to certain catIDs or objIDs
    # TODO: limit extract to certain visits

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
    --visit ${UNIQUE_VISITS[*]} \
    --yes ${EXTRAPARAMS} ${BATCH_PARAMS}
EOF
    )

    run_cmd "$cmd"
}

function run_download() {
    # Download the data from the science platform
    # Note, that it requires a working Butler setup to query the location of the
    # files and butler.yaml and gen3.sqlite have to be downloaded manually
    # Also note, that the download command works with the butler only because
    # there is no way to figure out the file paths without it.

    i=$1

    UNIQUE_VISITS=($(unique_array "${VISITS[$i]}"))
    echo "Found ${#UNIQUE_VISITS[@]} visits in the obs logs."
    echo "${UNIQUE_VISITS[@]}"

    # Query the database for the pfsConfig files

    cmd=$(cat <<EOF
gapipe-repo find-product PfsConfig \
    --visit ${UNIQUE_VISITS[@]} \
    --butler \
    --format path \
    | sed "s|${GAPIPE_DATADIR}/||g" \
    > "${GARUN[$i]}_pfsConfig.txt"
EOF
    )
    run_cmd "$cmd"

    # Download the pfsConfig files using wget

    cmd=$(cat <<EOF
wget \
    -i "${GARUN[$i]}_pfsConfig.txt" \
    --header="Authorization: Bearer ${PFSSP_TOKEN}" \
    --base "https://hscpfs.mtk.nao.ac.jp/fileaccess/pfs/programs/${PROPOSAL[$i]}/2d/" \
    -P ${GAPIPE_DATADIR} \
    --no-host-directories \
    --cut-dirs=5 -x -c
EOF
    )
    run_cmd "$cmd"

    # Query the database for the pfsCalibrated files

    cmd=$(cat <<EOF
gapipe-repo find-product PfsCalibrated \
    --visit ${UNIQUE_VISITS[@]} \
    --catid ${CATID[$i]} \
    --objid ${OBJID[$i]} \
    --butler \
    --format path \
    | sed "s|${GAPIPE_DATADIR}/||g" \
    > "${GARUN[$i]}_${CATID[$i]}_pfsCalibrated.txt"
EOF
    )
    run_cmd "$cmd"

    # Generate the sbatch script for downloading the pfsCalibrated files in parallel

    cat > "${GARUN[$i]}_${CATID[$i]}_pfsCalibrated.sh" <<EOF
#!/bin/bash
#SBATCH --job-name=gapipe_download
#SBATCH --output=logs/%x-%A_%a.out
#SBATCH --time=02:00:00
#SBATCH --partition=${BATCH_PARTITION}
#SBATCH --cpus-per-task=2
#SBATCH --mem=4G

# run it as sbatch --array=0-7 ${GARUN[$i]}_${CATID[$i]}_pfsCalibrated.sh

FILELIST="${GARUN[$i]}_${CATID[$i]}_pfsCalibrated.txt"
TASK_ID="\$SLURM_ARRAY_TASK_ID"
NUM_TASKS="\$SLURM_ARRAY_TASK_COUNT"

echo "Task \$TASK_ID of \$NUM_TASKS starting"

# Read file list with line numbers
lineno=0
while read -r url; do
    lineno=\$((lineno + 1))

    # Check if this line belongs to this task
    mod=\$(( (lineno - 1) % NUM_TASKS ))
    if [[ \$mod -eq \$TASK_ID ]]; then
        echo "Task \$TASK_ID downloading line \$lineno: \$url"
        wget --header="Authorization: Bearer ${PFSSP_TOKEN}" \\
            "https://hscpfs.mtk.nao.ac.jp/fileaccess/pfs/programs/${PROPOSAL[$i]}/2d/\$url" \\
            -P "${GAPIPE_DATADIR}/" \\
            --no-host-directories \\
            --cut-dirs=5 -x -c
    fi

done < "\${FILELIST}"
EOF

    echo "${GARUN[$i]}_${CATID[$i]}_pfsCalibrated.sh" " has been generated."

    # Submit the batch job array for downloading the pfsCalibrated files

    cmd=$(cat <<EOF
sbatch --array=0-7 ${GARUN[$i]}_${CATID[$i]}_pfsCalibrated.sh
EOF
    )
    # run_cmd "$cmd"
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
    --visit ${UNIQUE_VISITS[@]} \
    --catid ${UNIQUE_CATIDS[@]} \
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
    --yes ${EXTRAPARAMS} ${BATCH_PARAMS} \
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
            echo "Using butler."
            echo "  BUTLER_CONFIGDIR: $BUTLER_CONFIGDIR"
            echo "  BUTLER_COLLECTIONS: $BUTLER_COLLECTIONS"

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
            "download")
                run_download $i
                ;;
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
    "download")
        load_script_config
        load_gapipe_config
        main_loop
        ;;
    "extract")
        load_script_config
        load_gapipe_config
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
        BATCH_PARAMS=""
        main_loop
        ;;
    "submit")
        # make sure we're not already inside a slurm job
        # if [[ -n "$SLURM_JOB_ID" ]]; then
        #     echo "Error: Cannot run with verb 'submit' inside a slurm job. Please run with verb 'run' instead."
        #     exit 1
        # fi

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
