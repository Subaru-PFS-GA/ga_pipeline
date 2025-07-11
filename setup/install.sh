#!/bin/bash

# This script is used to install the PFS GA Pipeline software stack.
# Example usage:
#   ./setup/install.sh --debug -d /path/to/install/dir --conda

# TODO: allow installing gapipe as package (conda and/or eups)
# TODO: write generated files into the log
# TODO: update git repos when re-running installer
# TODO: add --quiet switch to suppress conda progress

# Defaults
GAPIPE_DEBUG=0                                  # 1 for debugging
GAPIPE_LOGLEVEL=1                               # log level
GAPIPE_FORCE=0                                  # Force installation
GAPIPE_DIR="$(realpath "${HOME}")/gapipe"       # Installation directory
GAPIPE_PACKAGE="SOURCE"                         # Install from source instead of package
GAPIPE_CONDA_DIR="./stack/conda"                # Conda installation directory, relative to GAPIPE_DIR
GAPIPE_CONDA_ENV="gapipe"                       # Conda environment name
GAPIPE_CONDA_ENV_FILE="gapipe.yaml"             # Conda environment file
GAPIPE_LSST=1                                   # 1 for installing on the LSST stack
LSST_VERSION="w.2025.23"                        # LSST version to install, if GAPIPE_LSST is set to 1
LSST_DIR="./stack"                              # LSST installation directory, relative to GAPIPE_DIR
LSST_CONDA_DIR="./conda"                        # LSST conda installation directory, relative to LSST_DIR
LSST_CONDA_ENV=""                               # LSST conda environment name, if GAPIPE_LSST is set to 1
LSST_CONDA_ENV_FILE="lsst.yaml"                 # Conda environment file
PFS_PIPE2D_VERSION="w.2025.23"                  # PFS PIPE2D version to install, if GAPIPE_LSST is set to 1
PFS_EUPS_PKGROOT="https://hscpfs.mtk.nao.ac.jp/pfs-drp-2d/Linux64"

# Observation data locations
PFS_DATADIR="/datascope/subaru/data/edr3/run21"
PFS_RERUNDIR="u/kiyoyabe/processing/run21"
PFS_RERUN="u_kiyoyabe_processing_run21_20250415a"
PFS_DESIGNDIR="PFS/raw/pfsDesign"
PFS_CONFIGDIR="PFS/raw/pfsConfig"
LSST_BUTLER_CONFIGDIR="${PFS_DATADIR}"
LSST_BUTLER_COLLECTIONS="${PFS_CONFIGDIR}:${PFS_RERUNDIR}"

# Library github URLs and tags. For PFSSPEC, install all submodules with the same tag.
# datamodel is installed as source because the eups package does not contain the most
# up-to-date version of GA extensions yet.
DATAMODEL_GITHUB="Subaru-PFS/datamodel"
DATAMODEL_GIT_TAG="tickets/DAMD-162"
PFSSPEC_GITHUB="Subaru-PFS-GA/ga_pfsspec_all"
PFSSPEC_GIT_TAG="master"
GAPIPE_GITHUB="Subaru-PFS-GA/ga_pipeline"
GAPIPE_GIT_TAG="master"
CHEMFIT_GITHUB="Subaru-PFS-GA/ga_chemfit"
CHEMFIT_GIT_TAG="main"

function print_header() {
    echo "==================================="
    echo "   PFS GA Pipeline Installer       "
    echo "   (c) 2019-2025 the PFS GA team   "
    echo "==================================="
}

# Parse command-line arguments
function parse_args() {
    PARAMS=""

    while (( "$#" )); do
        case "$1" in
        --debug)
            GAPIPE_DEBUG=1
            GAPIPE_LOGLEVEL=4
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
            LSST_CONDA_DIR="$2"
            shift 2
            ;;
        -e|--env|--conda-env)
            GAPIPE_CONDA_ENV="$2"
            LSST_CONDA_ENV="$2"
            shift 2
            ;;
        --lsst)
            GAPIPE_LSST=1
            shift
            ;;
        --no-lsst)
            GAPIPE_LSST=0
            shift
            ;;
        --source)
            GAPIPE_PACKAGE="SOURCE"
            shift
            ;;
        --conda)
            GAPIPE_PACKAGE="CONDA"
            shift
            ;;
        --eups)
            GAPIPE_PACKAGE="EUPS"
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
    echo "  --conda               Install as a conda package (not implemented yet)"
    echo "  --eups                Install as an EUPS package (not implemented yet)"
    echo "  --lsst                Install on the LSST stack (default: enabled)"
    echo "  --no-lsst             Install on its own conda stack (default: disabled)"
}

function print_summary() {
    conda_dir=$(get_conda_dir)
    echo "Installation completed successfully."
    echo "Please run the following command to activate the GAPIPE environment:"
    echo "cd ${GAPIPE_DIR}/src/ga_pipeline && source ./bin/init"
}

function log_message() {
    local level="$1"
    local message="$2"
    
    # If log level is less than or equal to the current log level, print the message
    if [[ $GAPIPE_LOGLEVEL -lt $level ]]; then
        return
    fi

    case "$level" in
        4)
            echo -e "\033[1;34m[DEBG]\033[0m $message"
            ;;
        3)
            echo -e "\033[1;34m[INFO]\033[0m $message"
            ;;
        2)
            echo -e "\033[1;33m[WARN]\033[0m $message"
            ;;
        1)
            echo -e "\033[1;31m[ERRR]\033[0m $message"
            ;;
        *)
            echo "$message"
            ;;
    esac
}

function log_debug() {
    log_message "4" "$1"
}

function log_info() {
    log_message "3" "$1"
}
function log_warning() {
    log_message "2" "$1"
}
function log_error() {
    log_message "1" "$1"
}

function print_args() {
    log_info "GAPIPE_DEBUG: $GAPIPE_DEBUG"
    log_info "GAPIPE_FORCE: $GAPIPE_FORCE"
    log_info "GAPIPE_LOGLEVEL: $GAPIPE_LOGLEVEL"
    log_info "GAPIPE_DIR: $GAPIPE_DIR"
    log_info "GAPIPE_CONDA_DIR: $GAPIPE_CONDA_DIR"
    log_info "GAPIPE_CONDA_ENV: $GAPIPE_CONDA_ENV"
    log_info "GAPIPE_CONDA_ENV_FILE: $GAPIPE_CONDA_ENV_FILE"
    log_info "GAPIPE_PACKAGE: $GAPIPE_PACKAGE"
    log_info "GAPIPE_LSST: $GAPIPE_LSST"
}

function join_path() {
    if [[ "$2" = /* ]]; then
        # If the second path is absolute, return it as is
        echo "$2"
    else
        echo "${1%/}/$2"  # Otherwise, join paths while avoiding duplicate slashes
    fi
}

function run_cmd() {
    # Run a command and log its output

    cmd=$1

    # Log the command into the installation command file
    log_debug "Running command: ${cmd}"
    echo "${cmd}" >> "${INSTALL_LOG_FILE}"
    
    # Run the command
    eval "${cmd}"
}

function download_file() {
    # Download a file from a URL
    # $1: URL
    # $2: target file name

    url="$1"
    target_file="$2"

    log_info "Downloading file from ${url} to $(realpath ${target_file})."
    run_cmd "curl -sOL \"${url}\" -o \"${target_file}\""
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
        url="git@github.com:${repo_name}.git"
    else
        url="https://github.com/${repo_name}.git"
    fi

    log_info "Cloning repository ${repo_name} from ${url} into ${target_dir}."
    run_cmd "git clone --quiet --recursive \"${url}\" \"${target_dir}\""
}

function checkout_git_tag() {
    # Check out the specified git tag
    # Assume the git repo is the current working directory

    # $1: git tag

    git_tag="$1"
    repo_name=$(basename "$(pwd)")

    log_info "Checking out tag ${git_tag} for ${repo_name}"
    run_cmd "git checkout --quiet \"${git_tag}\""
}

function get_conda_dir() {
    # Return the conda directory

    conda_dir=$(join_path "${GAPIPE_DIR}" "${GAPIPE_CONDA_DIR}")
    if [[ -d "${conda_dir}" ]]; then
        conda_dir=$(realpath "${conda_dir}")
    fi
    echo "${conda_dir}"
}

function install_conda() {
    # Install Miniconda silently

    conda_dir="$1"

    log_info "Conda installation not found. Installing Miniconda into ${conda_dir}."

    log_info "Downloading Miniconda3 installer."
    wget -q https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh -O "./miniconda.sh"
    
    log_info "Executing Miniconda3 installer."
    bash "./miniconda.sh" -b -p "${conda_dir}"

    log_info "Removing Miniconda3 installer."
    rm "./miniconda.sh"
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
    # Install the conda environment from a yaml file
    # Assume that conda is already activated

    env_name="$1"
    env_file="$2"

    run_cmd "conda env create --yes --quiet --name \""${env_name}"\" --file \""${env_file}"\""
}

function update_conda_env() {
    # Update a conda environment based on a yaml file
    # Assume that conda is already activated
    #

    env_name="$1"
    env_file="$2"

    run_cmd "conda env update --quiet --name \""${env_name}"\" --file \""${env_file}"\""
}

function get_lsst_dir() {
    # Return the directory of the LSST stack

    lsst_dir=$(join_path "${GAPIPE_DIR}" "${LSST_DIR}")
    lsst_dir=$(realpath "${lsst_dir}")
    echo "${lsst_dir}"
}

function get_lsst_conda_dir {
    # Return the directory of the LSST conda environment

    lsst_conda_dir=$(join_path "$(get_lsst_dir)" "${LSST_CONDA_DIR}")
    if [[ -d "${lsst_conda_dir}" ]]; then
        lsst_conda_dir=$(realpath "${lsst_conda_dir}")
    fi
    echo "${lsst_conda_dir}"
}

function get_lsst_version() {
    echo "${LSST_VERSION}" | sed 's/\./_/g'
}

function get_lsst_conda_env() {
    # If the LSST env name is not set, figure out the LSST stack version from the
    # git tag and return the default name of the conda environment. This is adopted
    # from the official LSST stack installation script.

    lsst_version="$(get_lsst_version)"

    if [[ -z "${LSST_CONDA_ENV}" ]]; then
        lsst_version=$(curl -ksS \
            "https://eups.lsst.codes/stack/src/tags/${lsst_version}.list" \
            | grep '^#CONDA_ENV=' | cut -d= -f2)
        LSST_CONDA_ENV="lsst-scipipe-${lsst_version}"
    fi

    echo "${LSST_CONDA_ENV}"
}

function reset_eups() {
    run_cmd "unset EUPS_DIR EUPS_PATH EUPS_PKGROOT EUPS_SHELL SETUP_EUPS"
    run_cmd "unset CONDA_DEFAULT_ENV CONDA_EXE CONDA_PREFIX CONDA_PROMPT_MODIFIER CONDA_PYTHON_EXE CONDA_SHLVL"
}

function run_shebangtron() {
    # Fix shebang lines in the installed scripts
    run_cmd "curl -sSL \"https://raw.githubusercontent.com/lsst/shebangtron/main/shebangtron\" | python"
}

function install_lsst_eups_packages() {
    # Install the LSST packages using EUPS
    
    tag="$(echo "$1" | sed 's/\./_/g')"
    pkgs="$2"
    from_source=false

    # Installing the LSST packages
    if [[ "${from_source}" == true ]]; then
        log_error "Installing EUPS packages from source is not implemented yet."
        exit 2
    fi
      
    # Install the packages using EUPS by tag
    for package in ${pkgs}; do
        eups list | grep "${package}" | grep -q "$tag" && {
            log_info "EUPS package ${package} is already installed. Skipping."
        } || {
            log_info "Installing EUPS package $package $tag"
            run_cmd "eups distrib install "$package" -t "${tag}" --no-server-tags"
        }
    done

    run_shebangtron
}

function install_lsst() {
    # Install the LSST stack

    # This function closely replicates the PFS drp install script, which in turn calls
    # the LSST install script.
    
    lsst_dir="$1"
    lsst_conda_dir="$2"
    lsst_env="$3"
    lsst_version="$4"
    
    log_info "LSST stack directory is set to ${lsst_dir}."
    log_info "LSST conda environment is set to ${lsst_env}."

    # Create installation directories
    run_cmd "mkdir -p \"${lsst_dir}\""
    run_cmd "mkdir -p \"${lsst_dir}/bin\""
    run_cmd "pushd \"${lsst_dir}\" > /dev/null"

    # Download the installer script
    log_info "Downloading LSST installation script."
    download_file "https://ls.st/lsstinstall" ./lsstinstall
    run_cmd "chmod u+x lsstinstall"

    # Patch the installer script to ignore server certificate errors and set
    # quiet mode for conda
    log_info "Patching LSST installation script."
    run_cmd "patch -p1 lsstinstall < \"${SCRIPT_DIR}/lsstinstall.patch\" > /dev/null"
    
    # Run the install script
    log_info "Running LSST installation script. This will take a while."

    # Set the SCONSFLAGS to use more threads when compiling from source
    export SCONSFLAGS="-j $NUMTHREADS"

    reset_eups
    run_cmd "./lsstinstall -T \"${lsst_version}\" -p \"${lsst_conda_dir}\" -e \"${lsst_env}\""

    run_cmd "popd > /dev/null"

    if [[ $? -ne 0 ]]; then
        log_error "LSST installation failed."
        exit 1
    else
        log_info "LSST installation completed successfully."
    fi
}

function activate_lsst_conda_env() {
    # Activate the LSST conda environment and set up the EUPS environment

    conda_dir="$1"
    conda_env="$2"

    log_info "Activating LSST conda environment ${conda_env} with EUPS."
    reset_eups
    run_cmd "source \"${conda_dir}/bin/activate\" \"${conda_env}\""
    run_cmd "export EUPS_PKGROOT=\"$PFS_EUPS_PKGROOT|$(cat "$EUPS_PATH/pkgroot")\""
}

function deactivate_lsst_conda_env() {
    # Deactivate the LSST conda environment and reset the EUPS environment

    log_info "Deactivating LSST conda environment."
    run_cmd "conda deactivate"
    reset_eups
}

function install_pfs_eups_packages() {
    # Install the LSST packages using EUPS
    
    version="$1"
    pkgs="$2"
    from_source=false

    # Installing the LSST packages
    if [[ "${from_source}" == true ]]; then
        log_error "Installing EUPS packages from source is not implemented yet."
        exit 2
    fi      

    # Install the packages using EUPS by version number
    # Here version is the LSST version, e.g. w.2025.23, including dots and not underscores
    for package in ${pkgs}; do
        eups list | grep "${package}" | grep -q "$version" && {
            log_info "EUPS package ${package} is already installed. Skipping."
        } || {
            log_info "Installing EUPS package $package $version"
            run_cmd "eups distrib install $package $version"
        }
    done

    run_shebangtron
}

function install_datamodel_source() {
    # Clone and check out the datamodel repository
    # Assume already in the src directory

    log_info "Installing and configuring datamodel from source."

    # If the datamodel directory doesn't exist, clone the repository
    if [[ ! -d "datamodel" ]]; then
        clone_github_repo "${DATAMODEL_GITHUB}" datamodel 1
    else
        log_info "Datamodel repository already exists. Skipping cloning."
    fi
    
    run_cmd "pushd datamodel > /dev/null"
    checkout_git_tag "${DATAMODEL_GIT_TAG}"

    log_info "Finished installing datamodel."

    run_cmd "popd > /dev/null"
}

function install_pfsspec_conda() {
    echo "Installing pfsspec as a conda package is not implemented yet." >/dev/stderr
    exit -2
}

function install_pfsspec_eups() {
    echo "Installing pfsspec as an EUPS package is not implemented yet." >/dev/stderr
    exit -2
}


function generate_pfsspec_env_file() {
    # Generate the gapipe environment config file

    conda_dir=$(get_conda_dir)

    cat > ./configs/envs/default.sh <<EOF
export PYTHONPATH=""

export PFSSPEC_LSST="${GAPIPE_LSST}"
export PFSSPEC_CONDAPATH="${conda_dir}"
export PFSSPEC_CONDAENV="${GAPIPE_CONDA_ENV}"
export PFSSPEC_ROOT="${GAPIPE_DIR}/src/ga_pfsspec_all"
export PFSSPEC_DATA="${GAPIPE_DIR}/data"

# Register the dependencies that are installed from source
export PFSSPEC_MODULES=\\
"datamodel:${GAPIPE_DIR}/src/datamodel:python"
EOF
}

function install_pfsspec_source() {
    # Clone and check out the ga_pfsspec_all repository and generate
    # the environment configuration file to initilize PFSSPEC
    # Assume we are already in the src directory

    log_info "Installing and configuring pfsspec from source."

    if [[ ! -d "ga_pfsspec_all" ]]; then
        clone_github_repo "${PFSSPEC_GITHUB}" ga_pfsspec_all 1
    else
        log_info "pfsspec repository already exists. Skipping cloning."
    fi

    run_cmd "pushd ga_pfsspec_all > /dev/null"
    checkout_git_tag "${PFSSPEC_GIT_TAG}"

    # Generate the default pfsspec environment
    generate_pfsspec_env_file

    # Check out each submodule to the HEAD of the current branch
    log_info "Checking out submodules for pfsspec."
    run_cmd "bash ./bin/checkout"

    # Initialize the pfsspec environment in a new shell in order to generate the symlinks
    log_info "Initializing the pfsspec environment."

    # Source the init script into a new shell in order to set up the
    # source code symlinks etc.
    run_cmd "bash -c \"source ./bin/init\""
    
    log_info "Finished installing pfsspec."

    run_cmd "popd > /dev/null"
}

function install_chemfit_conda() {
    echo "Installing chemfit as a conda package is not implemented yet." >/dev/stderr
    exit -2
}

function install_chemfit_eups() {
    echo "Installing chemfit as an EUPS package is not implemented yet." >/dev/stderr
    exit -2
}

function install_chemfit_source() {
    # Clone and check out the chemfit repository
    # Assume already in the src directory

    log_info "Installing and configuring chemfit."

    if [[ ! -d "ga_chemfit" ]]; then
        clone_github_repo "${CHEMFIT_GITHUB}" ga_chemfit 1
    else
        log_info "chemfit repository already exists. Skipping cloning."
    fi

    run_cmd "pushd ga_chemfit > /dev/null"
    checkout_git_tag "${CHEMFIT_GIT_TAG}"  
    log_info "Finished installing chemfit."
    
    run_cmd "popd > /dev/null"
}

function install_gapipe_conda() {
    echo "Installing gapipe as a conda package is not implemented yet." >/dev/stderr
    exit -2
}

function install_gapipe_eups() {
    echo "Installing gapipe as an EUPS package is not implemented yet." >/dev/stderr
    exit -2
}

function generate_gapipe_env_file() {
    # Generate the gapipe environment config file

    conda_dir=$(get_conda_dir)

    cat > ./configs/envs/default <<EOF
export PYTHONPATH=""

export GAPIPE_LSST="${GAPIPE_LSST}"
export GAPIPE_CONDAPATH="${conda_dir}"
export GAPIPE_CONDAENV="${GAPIPE_CONDA_ENV}"

export GAPIPE_ROOT="${GAPIPE_DIR}/src/ga_pipeline"
export GAPIPE_DATADIR="${GAPIPE_DIR}/data"
export GAPIPE_WORKDIR="${GAPIPE_DIR}/work"
export GAPIPE_OUTDIR="${GAPIPE_DIR}/out"

export PFSSPEC_ROOT="${GAPIPE_DIR}/src/ga_pfsspec_all"
export PFSSPEC_DATA="${GAPIPE_DIR}/data/pfsspec"

# Observation file locations when running on the LSST stack
export BUTLER_CONFIGDIR="${LSST_BUTLER_CONFIGDIR}"
export BUTLER_COLLECTIONS="${LSST_BUTLER_COLLECTIONS}"
export GAPIPE_RERUNDIR="${PFS_RERUNDIR}"
export GAPIPE_RERUN="${PFS_RERUN}"

# Observation file locations when running on a standard conda stack
export PFSSPEC_PFS_DATADIR="${PFS_DATADIR}"
export PFSSPEC_PFS_RERUNDIR="${PFS_RERUNDIR}"
export PFSSPEC_PFS_RERUN="${PFS_RERUN}"
export PFSSPEC_PFS_DESIGNDIR="${PFS_DATADIR}/${PFS_DESIGNDIR}"
export PFSSPEC_PFS_CONFIGDIR="${PFS_DATADIR}/${PFS_CONFIGDIR}"

# Register the dependencies that are installed from source
# Format: <module_name>:<path_to_source>:<rel_path_to_module>
export GAPIPE_MODULES=\\
"datamodel:${GAPIPE_DIR}/src/datamodel:python
ga_pfsspec:${GAPIPE_DIR}/src/ga_pfsspec_all:python"
EOF
}

function install_gapipe_source() {
    # Install the GAPIPE source code and generate the default environment file
    # Assume already in the src directory

    log_info "Installing and configuring gapipe."

    if [[ ! -d "ga_pipeline" ]]; then
        clone_github_repo "${GAPIPE_GITHUB}" ga_pipeline 1
    else
        log_info "ga_pipeline repository already exists. Skipping cloning."
    fi

    
    run_cmd "pushd ga_pipeline > /dev/null"
    checkout_git_tag "${GAPIPE_GIT_TAG}"

    # Generate the default gapipe environment
    generate_gapipe_env_file

    # Initialize the gapipe environment in a new shell in order to generate the symlinks
    run_cmd "bash -c \"source ./bin/init\""

    run_cmd "popd > /dev/null"
    log_info "Finished installing gapipe."
}

function init_gapipe_source() {
    # Generate the initialization script for the GAPIPE source installation

    run_cmd "cp \"${SCRIPT_DIR}/etc/conda/activate.d/activate-gapipe.sh\" \\
       \"${CONDA_PREFIX}/etc/conda/activate.d/\""

    run_cmd "cp \"${SCRIPT_DIR}/etc/conda/deactivate.d/deactivate-gapipe.sh\" \\
       \"${CONDA_PREFIX}/etc/conda/deactivate.d/\""

}

################################################################################

set -e

print_header
parse_args "$@"
print_args

# Get the path of the currently executing script
if [[ -n $_Dbg_script_file ]]; then
    # If running under a debugger, use the script file from the debug context
    SCRIPT_DIR="$(dirname "$(realpath "$_Dbg_script_file")")"
else
    # Otherwise, use the script file from the current context
    SCRIPT_DIR="$(dirname "$(realpath "$0")")"
fi
log_info "Install script directory is ${SCRIPT_DIR}"

# Some other setting
NUMTHREADS=$(($(nproc) / 2))
if [[ $NUMTHREADS -lt 1 ]]; then
    NUMTHREADS=1
fi
export OMP_NUM_THREADS=1
export PYTHONHTTPSVERIFY=0                      # Disable SSL verification for Python HTTPS requests

# Check if github access is configured, if required
if [[ "$GAPIPE_PACKAGE" == "SOURCE" ]]; then
    # Make sure ssh to github is available
    ensure_github_ssh
fi

# Check if the installation directory exists and create if not
if [[ -d "${GAPIPE_DIR}" ]]; then
    if [[ $GAPIPE_FORCE -eq 1 ]]; then
        log_warning "Installation directory ${GAPIPE_DIR} already exists, but --force option is set. Proceeding with installation."
    else
        log_error "Installation directory ${GAPIPE_DIR} already exists. Use --force to overwrite."
        exit 1
    fi
else
    log_info "Creating gapipe directory ${GAPIPE_DIR}."
    mkdir -p "${GAPIPE_DIR}"
fi

# Initialize Log file for the installation commands
INSTALL_LOG_FILE="$(join_path "${GAPIPE_DIR}" "install.log")"
echo "# Logging started at $(date)" >> "${INSTALL_LOG_FILE}"

run_cmd "pushd \"$(realpath "${GAPIPE_DIR}")\" > /dev/null"

# Create subdirectories for the installation
run_cmd "mkdir -p ./bin"
run_cmd "mkdir -p ./data"

# Make sure no other python installations interfere with the installation
run_cmd "unset PYTHONPATH"
run_cmd "unset CONDA_DEFAULT_ENV CONDA_EXE CONDA_PREFIX CONDA_PROMPT_MODIFIER CONDA_PYTHON_EXE CONDA_SHLVL"

# If installing on the LSST stack, run the LSST installation, otherwise
# run the Anaconda installation
if [[ $GAPIPE_LSST -eq 1 ]]; then
    # Install the LSST stack

    # Check if the LSST stack is already installed
    lsst_dir=$(get_lsst_dir)
    lsst_conda_dir=$(get_lsst_conda_dir)
    lsst_conda_env=$(get_lsst_conda_env)
    lsst_version=$(get_lsst_version)

    log_info "LSST installation directory is set to ${lsst_dir}, conda directory is set to ${lsst_conda_dir}, and conda environment is set to ${lsst_conda_env}."
    
    if [[ -d "${lsst_dir}" ]]; then
        log_info "LSST stack already installed, skipping task."
    else
        log_info "LSST stack not found, installing LSST stack version ${lsst_version}."
        install_lsst "${lsst_dir}" "${lsst_conda_dir}" "${lsst_conda_env}" "${lsst_version}"
    fi

    # Override the conda directory and environment name
    GAPIPE_CONDA_DIR="$(get_lsst_conda_dir)"
    GAPIPE_CONDA_ENV="$(get_lsst_conda_env)"

    # Activate the newly installed conda environment
    activate_lsst_conda_env "${GAPIPE_CONDA_DIR}" "${GAPIPE_CONDA_ENV}"

    # Install additional conda packages required by gapipe
    log_info "Installing additional conda packages for gapipe."
    conda_env_file="$(realpath $(join_path "$SCRIPT_DIR" "${LSST_CONDA_ENV_FILE}"))"
    update_conda_env "${GAPIPE_CONDA_ENV}" "${conda_env_file}"

    # Install the LSST packages using EUPS
    install_lsst_eups_packages "${LSST_VERSION}" \
        "cp_pipe ctrl_bps ctrl_bps_parsl display_ds9 display_matplotlib display_astrowidgets"

    # Install PFS PIPE2D using EUPS
    install_pfs_eups_packages "${PFS_PIPE2D_VERSION}" \
        "pfs_pipe2d"

    deactivate_lsst_conda_env
else
    # Install a standard conda environment

    # Check if conda is installed and if not, donwload and execute installer silently
    conda_dir=$(get_conda_dir)
    if [[ -d "$conda_dir" ]]; then
        log_info "Conda already installed, skipping task."
    else
        log_info "Conda not found, installing conda."
        install_conda "${conda_dir}"
    fi

    # Temporarily activate the base conda environment
    log_info "Activating conda environment base."
    run_cmd "source \"${conda_dir}/bin/activate\" base"

    # Check if the target environment exists
    if is_conda_env; then
        log_info "Conda environment ${GAPIPE_CONDA_ENV} already exists, skipping task."
        # TODO: only install packages
    else
        log_info "Conda environment ${GAPIPE_CONDA_ENV} does not exist, creating it."
        update_conda_env ${GAPIPE_CONDA_ENV} $(realpath $(join_path "$SCRIPT_DIR" "${GAPIPE_CONDA_ENV_FILE}"))
    fi

    run_cmd "conda deactivate"
fi

# Activate the target environment
log_info "Activating conda environment ${GAPIPE_CONDA_ENV}."
run_cmd "source \"${GAPIPE_CONDA_DIR}/bin/activate\" \"${GAPIPE_CONDA_ENV}\""

# Depending on the package mode, install the GAPIPE software stack
if [[ "$GAPIPE_PACKAGE" == "SOURCE" ]]; then
    log_info "Installing GAPIPE from source."

    run_cmd "mkdir -p \"${GAPIPE_DIR}/src\""
    run_cmd "pushd \"${GAPIPE_DIR}/src\" > /dev/null"

    # Clone and configure the repositories
    install_datamodel_source
    install_pfsspec_source
    install_chemfit_source
    install_gapipe_source

    run_cmd "popd > /dev/null"

    # Configure the conda environment
    log_info "Generating activation scripts for conda environment ${GAPIPE_CONDA_ENV}."
    init_gapipe_source
else
    log_info "Installing from package."
    log_error "Package installation not implemented yet."
    exit 1
fi

# Create the remainder of the directory structure
run_cmd "mkdir -p \"${GAPIPE_DIR}/data\""
run_cmd "mkdir -p \"${GAPIPE_DIR}/data/pfsspec\""
run_cmd "mkdir -p \"${GAPIPE_DIR}/work\""
run_cmd "mkdir -p \"${GAPIPE_DIR}/out\""

# TODO: Check if the installation was successful

run_cmd "popd > /dev/null"

# Print the installation summary
print_summary