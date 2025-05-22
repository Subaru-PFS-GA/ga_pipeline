#!/bin/bash

# This script is used to install the PFS GA Pipeline software stack.

# Defaults
GAPIPE_DEBUG=0                                  # 1 for debugging
GAPIPE_LOGLEVEL=1                               # log level
GAPIPE_FORCE=0                                  # Force installation
GAPIPE_DIR="$(realpath "${HOME}")/gapipe"       # Installation directory
GAPIPE_CONDA_DIR="./conda"                      # Conda installation directory
GAPIPE_CONDA_ENV="gapipe"                       # Conda environment name
GAPIPE_CONDA_ENV_FILE="./setup/gapipe.yaml"     # Conda environment file
GAPIPE_MODE="SOURCE"                            # Install from source instead of package

# Constants
DATAMODEL_GITHUB="Subaru-PFS/datamodel"
DATAMODEL_GIT_TAG="tickets/DAMD-162"
PFSSPEC_GITHUB="Subaru-PFS-GA/ga_pfsspec_all"
PFSSPEC_GIT_TAG="master"
GAPIPE_GITHUB="Subaru-PFS-GA/ga_pipeline"
GAPIPE_GIT_TAG="master"
CHEMFIT_GITHUB="Subaru-PFS-GA/ga_chemfit"
CHEMFIT_GIT_TAG="main"

function print_header() {
    echo "=============================="
    echo "  PFS GA Pipeline Installer   "
    echo "=============================="
}

# Parse command-line arguments
function parse_args() {
    PARAMS=""

    while (( "$#" )); do
        case "$1" in
        --debug)
            GAPIPE_DEBUG=1
            GAPIPE_LOGLEVEL=2
            shift
            ;;
        -f|--force)
            GAPIPE_FORCE=1
            shift
            ;;
        -d|--dir)
            GAPIPE_DIR="$2"
            shift 2
            ;;
        --conda-dir)
            GAPIPE_CONDA_DIR="$2"
            shift 2
            ;;
        -e|--env|--conda-env)
            GAPIPE_CONDA_ENV="$2"
            shift 2
            ;;
        --source)
            GAPIPE_MODE="SOURCE"
            shift
            ;;
        --) # end argument parsing
            shift
            break
            ;;
        *) # preserve all other arguments
            PARAMS="$PARAMS $1"
            shift
            ;;
        esac
    done
}

function print_usage() {
    echo "Usage: $0 [options]"
    echo "Options:"
    echo "  --debug               Enable debug mode"
    echo "  -d, --dir <dir>       Installation directory (default: $GAPIPE_DIR)"
    echo "  --conda-dir <dir>     Conda installation directory (default: $GAPIPE_CONDA_DIR)"
    echo "  -e, --env <env>       Conda environment name (default: $GAPIPE_CONDA_ENV)"
    echo "  --source              Install from source instead of package"
}

function print_summary() {
    conda_dir=$(get_conda_dir)
    echo "Installation completed successfully."
    echo "Please run the following command to activate the conda environment:"
    echo "source ${conda_dir}/bin/activate ${GAPIPE_CONDA_ENV}"
}

function log_message() {
    local level="$1"
    local message="$2"
    
    # If log level is less than or equal to the current log level, print the message
    if [[ $GAPIPE_LOGLEVEL -lt $level ]]; then
        return
    fi

    case "$level" in
        INFO)
            echo -e "\033[1;34m[INFO]\033[0m $message"
            ;;
        WARNING)
            echo -e "\033[1;33m[WARNING]\033[0m $message"
            ;;
        ERROR)
            echo -e "\033[1;31m[ERROR]\033[0m $message"
            ;;
        *)
            echo "$message"
            ;;
    esac
}

function log_info() {
    log_message "INFO" "$1"
}
function log_warning() {
    log_message "WARNING" "$1"
}
function log_error() {
    log_message "ERROR" "$1"
}

function print_args() {
    log_info "GAPIPE_DEBUG: $GAPIPE_DEBUG"
    log_info "GAPIPE_FORCE: $GAPIPE_FORCE"
    log_info "GAPIPE_LOGLEVEL: $GAPIPE_LOGLEVEL"
    log_info "GAPIPE_DIR: $GAPIPE_DIR"
    log_info "GAPIPE_CONDA_DIR: $GAPIPE_CONDA_DIR"
    log_info "GAPIPE_CONDA_ENV: $GAPIPE_CONDA_ENV"
    log_info "GAPIPE_CONDA_ENV_FILE: $GAPIPE_CONDA_ENV_FILE"
    log_info "GAPIPE_SOURCE: $GAPIPE_SOURCE"
}

function join_path() {
    if [[ "$2" = /* ]]; then
        # If the second path is absolute, return it as is
        echo "$2"
    else
        echo "${1%/}/$2"  # Otherwise, join paths while avoiding duplicate slashes
    fi
}

function is_gapipe_dir() {
    # Check if the installation directory exists

    if [[ -d "${GAPIPE_DIR}" ]]; then
        return 0
    else
        return 1
    fi
}

function get_conda_dir() {
    # Return the conda directory

    conda_dir=$(join_path "${GAPIPE_DIR}" "${GAPIPE_CONDA_DIR}")
    conda_dir=$(realpath "${conda_dir}")
    echo "${conda_dir}"
}

function is_conda_dir() {
    # Check if the conda directory exists
    
    conda_dir=$(get_conda_dir)
    
    if [[ -d "${conda_dir}" ]]; then
        return 0
    else
        return 1
    fi
}

function install_conda() {
    # Install Miniconda silently

    conda_dir=$(get_conda_dir)

    log_info "Conda installation not found. Installing Miniconda into ${conda_dir}."

    log_info "Downloading Miniconda3 installer."
    wget -q https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh -O "${GAPIPE_DIR}/miniconda.sh"
    
    log_info "Executing Miniconda3 installer."
    bash "${GAPIPE_DIR}/miniconda.sh" -b -p "${conda_dir}"

    log_info "Removing Miniconda3 installer."
    rm "${GAPIPE_DIR}/miniconda.sh"
}

function is_conda_env() {
    # Check if the conda environment exists
    # Assume that conda is already activated

    log_info "Checking if conda environment ${GAPIPE_CONDA_ENV} exists."
    conda_dir=$(get_conda_dir)

    # TODO: is this correct?
    #       does conda print the name of the env only when it
    #       is installed under the particular anaconda install?
    conda_envs=$(conda env list | \
                 grep -v '^#' | \
                 awk '{print($1)}' | \
                 grep -E "^${GAPIPE_CONDA_ENV}$")

    if [[ -n "${conda_envs}" ]]; then
        return 0
    else
        return 1
    fi
}

function install_conda_env() {
    # Install the conda environment from a file
    # Assume that conda is already activated

    env_file=$(realpath "${GAPIPE_CONDA_ENV_FILE}")

    conda env create \
        --name "${GAPIPE_CONDA_ENV}" \
        --file "${env_file}" \
        --verbose --yes
}

function ensure_github_ssh() {
    # Try to ssh to github.com with the default settings and check if the connection is successful
    ssh git@github.com 2>&1 | grep -q "successfully authenticated" && {
        log_info "SSH key is available for GitHub access."
        return 0
    } || {
        log_error "SSH key is not available for GitHub access. Please set up your SSH key."
        exit 1
    }
}

function clone_github_repo() {
    # Clone the GitHub repository
    # $1: repository name
    # $2: target directory
    # $3: use SSH (1 for yes, 0 for https)

    repo_name="$1"
    target_dir="$2"
    use_ssh="$3"

    if [[ "$use_ssh" -eq 1 ]]; then
        log_info "Cloning ${repo_name}"
        git clone "git@github.com:${repo_name}.git" \
            "${target_dir}" \
            --recursive
    else
        log_info "Cloning ${repo_name}"
        git clone "https://github.com/${repo_name}.git" \
            "${target_dir}" \
            --recursive
    fi
}

function checkout_git_tag() {
    # Check out the specified git tag
    # $1: git tag

    git_tag="$1"
    repo_name=$(basename "$(pwd)")

    log_info "Checking out tag ${git_tag} for ${repo_name}"
    git checkout "${git_tag}"
}

function install_datamodel_source() {
    log_info "Installing and configuring datamodel."

    clone_github_repo "${DATAMODEL_GITHUB}" datamodel 1
    pushd "datamodel" > /dev/null
    checkout_git_tag "${DATAMODEL_GIT_TAG}"

    popd > /dev/null
    log_info "Finished installing datamodel."
}

function install_pfsspec_source() {
    log_info "Installing and configuring pfsspec."

    clone_github_repo "${PFSSPEC_GITHUB}" ga_pfsspec_all 1
    pushd "ga_pfsspec_all" > /dev/null    
    checkout_git_tag "${PFSSPEC_GIT_TAG}"

    conda_dir=$(get_conda_dir)

    # Generate the default pfsspec environment
    cat > ./configs/envs/default.sh <<EOF
export PFSSPEC_CONDAPATH="${conda_dir}"
export PFSSPEC_CONDAENV="${GAPIPE_CONDA_ENV}"
export PFSSPEC_ROOT="${GAPIPE_DIR}/src/ga_pfsspec_all"
export PFSSPEC_DATA="${GAPIPE_DIR}/data"

# export PFSSPEC_PFS_DATADIR="/datascope/subaru/data/commissioning/gen3"
# export PFSSPEC_PFS_RERUNDIR="run20/20250228a"
# export PFSSPEC_PFS_RERUN="u_kiyoyabe_processing_run20_20250228a"
# export PFSSPEC_PFS_DESIGNDIR="/datascope/subaru/data/commissioning/gen3"
# export PFSSPEC_PFS_CONFIGDIR="/datascope/subaru/data/commissioning/gen3"

# export PYTHONPATH="${GAPIPE_DIR}/src/ga_pfsspec_all:/home/dobos/project/ga_datamodel/python:/home/dobos/project/Subaru-PFS/datamodel/python:/home/dobos/project/Subaru-PFS/pfs_utils/python:../numdifftools/src:../pysynphot:../SciScript-Python/py3:../rvspecfit/py"
EOF

    # Check out each submodule to the HEAD of the current branch
    bash ./bin/checkout

    # Initialize the pfsspec environment in a new shell in order to generate the symlinks
    bash -c "source ./bin/init"

    popd > /dev/null
    log_info "Finished installing pfsspec."
}

function install_chemfit_source() {
    log_info "Installing and configuring chemfit."

    clone_github_repo "${CHEMFIT_GITHUB}" ga_chemfit 1
    pushd "ga_chemfit" > /dev/null
    checkout_git_tag "${CHEMFIT_GIT_TAG}"  

    popd > /dev/null
    log_info "Finished installing chemfit."
}

function install_gapipe_source() {
    log_info "Installing and configuring gapipe."

    clone_github_repo "${GAPIPE_GITHUB}" ga_pipeline 1
    pushd "ga_pipeline" > /dev/null
    checkout_git_tag "${GAPIPE_GIT_TAG}"

    conda_dir=$(get_conda_dir)

    # Generate the default gapipe environment
    cat > ./configs/envs/default <<EOF
export GAPIPE_CONDAPATH="${conda_dir}"
export GAPIPE_CONDAENV="${GAPIPE_CONDA_ENV}"
export GAPIPE_ROOT="${GAPIPE_DIR}/src/ga_pipeline"

export PFSSPEC_ROOT="${GAPIPE_DIR}/src/ga_pfsspec_all"
# export PFSSPEC_DATA="/datascope/subaru/data/pfsspec"

# export GAPIPE_DATADIR="/datascope/subaru/data/commissioning/gen3"
# export GAPIPE_WORKDIR="/datascope/subaru/user/dobos/gapipe/work"
# export GAPIPE_OUTDIR="/datascope/subaru/user/dobos/gapipe/out"
# export GAPIPE_RERUNDIR="run20/20250228a"
# export GAPIPE_RERUN="u_kiyoyabe_processing_run20_20250228a"

# export PFSSPEC_PFS_DATADIR="/datascope/subaru/data/commissioning/gen3"
# export PFSSPEC_PFS_RERUNDIR="run20/20250228a"
# export PFSSPEC_PFS_RERUN="u_kiyoyabe_processing_run20_20250228a"
# export PFSSPEC_PFS_DESIGNDIR="/datascope/subaru/data/commissioning/gen3"
# export PFSSPEC_PFS_CONFIGDIR="/datascope/subaru/data/commissioning/gen3"

# export PYTHONPATH="/home/dobos/project/Subaru-PFS/datamodel/python:/home/dobos/project/Subaru-PFS/pfs_utils/python:/home/dobos/project/numdifftools/src:/home/dobos/project/pysynphot:"
EOF

    # Initialize the gapipe environment in a new shell in order to generate the symlinks
    bash -c "source ./bin/init"

    popd > /dev/null
    log_info "Finished installing gapipe."
}

####

set -e

print_header
parse_args "$@"
print_args

# Check if the installation directory exists and create if not
if is_gapipe_dir; then
    if [[ $GAPIPE_FORCE -eq 1 ]]; then
        log_warning "Installation directory ${GAPIPE_DIR} already exists, but --force option is set. Proceeding with installation."
    else
        log_error "Installation directory ${GAPIPE_DIR} already exists. Use --force to overwrite."
        exit 1
    fi
else
    log_info "Creating installation directory ${GAPIPE_DIR}."
    mkdir -p "${GAPIPE_DIR}"
fi

# Check if conda is installed and if not, donwload and execute installer silently
if is_conda_dir; then
    log_info "Conda already installed, skipping task."
else
    log_info "Conda not found, installing conda."
    install_conda
fi

# Activate the base conda environment
log_info "Activating conda environment base."
conda_dir=$(get_conda_dir)
source "${conda_dir}/bin/activate" base

# Check if the target environment exists
if is_conda_env; then
    log_info "Conda environment ${GAPIPE_CONDA_ENV} already exists, skipping task."
    # TODO: only install packages
else
    log_info "Conda environment ${GAPIPE_CONDA_ENV} does not exist, creating it."
    install_conda_env
fi

# Activate the target environment
log_info "Activating conda environment ${GAPIPE_CONDA_ENV}."
conda activate "${GAPIPE_CONDA_ENV}"

# Depending on the mode, install the software stack
if [[ "$GAPIPE_MODE" == "SOURCE" ]]; then
    log_info "Installing from source."

    # Make sure ssh to github is available
    ensure_github_ssh

    mkdir -p "${GAPIPE_DIR}/src"
    pushd "${GAPIPE_DIR}/src" > /dev/null

    # Clone and configure the repositories
    install_datamodel_source
    install_pfsspec_source
    install_chemfit_source
    install_gapipe_source

    popd > /dev/null

    # Configure the conda environment
    log_info "Configuring conda environment ${GAPIPE_CONDA_ENV}."

    cat > "${CONDA_PREFIX}/etc/conda/activate.d/gapipe.sh" <<EOF
export PFSSPEC_ROOT="${GAPIPE_DIR}/src/ga_pfsspec_all"
export GAPIPE_ROOT="${GAPIPE_DIR}/src/ga_pipeline"

export PYTHONPATH="${GAPIPE_DIR}/src/datamodel/python:${GAPIPE_DIR}/src/ga_pfsspec_all/python:${GAPIPE_DIR}/src/ga_pipeline/python:$PYTHONPATH"

echo PFS GA Pipeline environment activated.
EOF

    log_info "Finished configuring conda environment ${GAPIPE_CONDA_ENV}."

else
    log_info "Installing from package."
    log_error "Package installation not implemented yet."
    exit 1
fi

# Create the remainder of the directory structure
mkdir -p "${GAPIPE_DIR}/data"

# TODO: Check if the installation was successful

# Print the installation summary
print_summary