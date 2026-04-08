#!/bin/bash

# This script is used to install the PFS GA Pipeline software stack.
#
# Usage example:
#
#   ./setup/install.sh --debug -d /path/to/install/dir --source
#
# To successfully run this script, you have to have ssh access to github.com with an ssh
# key served by an ssh agent to avoid entering passwords multiple times during the installation.
# Please refer to https://docs.github.com/en/authentication/connecting-to-github-with-ssh

# TODO: allow installing gapipe as package (conda and/or eups)
# TODO: write generated files into the log
# TODO: update git repos when re-running installer
# TODO: add --quiet switch to suppress conda progress

# Defaults
GAPIPE_DEBUG=0                                  # 1 for debugging
GAPIPE_UPGRADE=0                                # 1 for upgrading an existing installation
GAPIPE_LOGLEVEL=3                               # Console log level
GAPIPE_FORCE=0                                  # Force installation
GAPIPE_ROOT="$(realpath "${HOME}")/pfs/gapipe"  # Installation directory, can be overridden by command-line argument
GAPIPE_TAG="master"                             # Git tag/branch to install
GAPIPE_PACKAGE="SOURCE"                         # Install from source instead of package
GAPIPE_CONDA_DIR="./stack/conda"                # Conda installation directory, relative to GAPIPE_ROOT
GAPIPE_CONDA_ENV="gapipe"                       # Conda environment name, when not using LSST
GAPIPE_CONDA_ENV_FILE="gapipe.yaml"             # Conda environment file to install additional dependencies.
GAPIPE_LSST=1                                   # 1 for installing on the LSST stack
LSST_VERSION="w.2026.14"                        # LSST version to install, if GAPIPE_LSST is set to 1
LSST_DIR="./stack"                              # LSST installation directory, relative to GAPIPE_ROOT
LSST_CONDA_DIR="./conda"                        # LSST conda installation directory, relative to LSST_DIR
LSST_CONDA_ENV=""                               # LSST conda environment name, if GAPIPE_LSST is set to 1
LSST_CONDA_ENV_FILE="lsst.yaml"                 # Conda environment file to install additional dependencies.
LSST_PIP_REQ_FILE='lsst_requirements.txt'       # Pip requirements file for additional dependencies to install in the LSST conda environment
PFS_PIPE2D_VERSION="w.2026.14"                  # PFS PIPE2D version to install, if GAPIPE_LSST is set to 1
PFS_EUPS_PKGROOT="https://hscpfs.mtk.nao.ac.jp/pfs-drp-2d/Linux64"

# Observation data locations
# Set these variables before running the installer. This will be the default
# values that are written to your environment configuration script.
GAPIPE_DATAROOT="$(realpath "${HOME}")/pfs/data/programs"
GAPIPE_OBSLOGDIR="$(realpath "${HOME}")/pfs/Subaru-PFS/spt_ssp_observation"
GAPIPE_TARGETINGDIR="$(realpath "${HOME}")/pfs/targeting"

# Configure a single data reduction run as an example
GAPIPE_DATADIR="$GAPIPE_DATAROOT/S25A-OT02/2d"
GAPIPE_RUN="S25A_November2025"
GAPIPE_RUNDIR="S25A_November2025"
GAPIPE_CONFIGRUN="S25A_November2025"
GAPIPE_CONFIGRUNDIR="S25A_November2025"
GAPIPE_GARUNDIR="S25A_November2025"
GAPIPE_GARUN="S25A_November2025"

BUTLER_CONFIGDIR="${GAPIPE_DATADIR}"
BUTLER_COLLECTIONS="${BUTLER_CONFIGDIR}:${GAPIPE_RUNDIR}"

# Library github URLs and tags. For PFSSPEC, install all submodules with the same tag.
# datamodel is installed as source because the eups package does not contain the most
# up-to-date version of GA extensions yet.
DATAMODEL_GITHUB="Subaru-PFS/datamodel"
DATAMODEL_GIT_TAG="tickets/DAMD-162"
GACOMMON_GITHUB="Subaru-PFS-GA/ga_common"
GACOMMON_GIT_TAG="$GAPIPE_TAG"
PFSSPEC_GITHUB="Subaru-PFS-GA/ga_pfsspec_all"
PFSSPEC_GIT_TAG="$GAPIPE_TAG"
GAPIPE_GITHUB="Subaru-PFS-GA/ga_pipeline"
GAPIPE_GIT_TAG="$GAPIPE_TAG"
CHEMFIT_GITHUB="Subaru-PFS-GA/ga_chemfit"
CHEMFIT_GIT_TAG="$GAPIPE_TAG"

function print_header() {
    echo "=================================================="
    echo "   PFS GA Pipeline Installer                      "
    echo "   (c) 2019-2026 Laszlo Dobos & the PFS GA team   "
    echo "=================================================="
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
        --upgrade)
            GAPIPE_UPGRADE=1
            shift
            ;;
        -f|--force)
            GAPIPE_FORCE=1
            shift
            ;;
        -d|--dir)
            GAPIPE_ROOT="$(realpath "$2")"
            shift 2
            ;;
        -t|--tag)
            GAPIPE_TAG="$2"
            GACOMMON_GIT_TAG="$GAPIPE_TAG"
            PFSSPEC_GIT_TAG="$GAPIPE_TAG"
            GAPIPE_GIT_TAG="$GAPIPE_TAG"
            CHEMFIT_GIT_TAG="$GAPIPE_TAG"
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
    echo "  -d, --dir <dir>       Installation directory (default: $GAPIPE_ROOT)"
    echo "  -t, --tag <tag>       Git tag/branch to install (default: $GAPIPE_TAG)"
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
    echo "Please run the following command to activate the GAPIPE environment:"
    echo "cd ${GAPIPE_ROOT}/src/ga_pipeline && source ./bin/init"
}

function init_logging() {
    # Initialize log file for the installation commands
    INSTALL_LOG_DIR="$(join_path "${GAPIPE_ROOT}" "logs")"
    INSTALL_COMMANDS_LOG="$(join_path "${INSTALL_LOG_DIR}" "commands.log")"
    INSTALL_MESSAGES_LOG="$(join_path "${INSTALL_LOG_DIR}" "install.log")"

    mkdir -p "${INSTALL_LOG_DIR}"
    echo "# Logging started at $(date)" >> "${INSTALL_COMMANDS_LOG}"
    echo "# Logging started at $(date)" >> "${INSTALL_MESSAGES_LOG}"
}

function log_message() {
    local level="$1"
    local message="$2"
    
    # If log level is less than or equal to the current log level, print the message
    if [[ $GAPIPE_LOGLEVEL -lt $level ]]; then
        return
    fi

    dt="$(date '+%Y-%m-%d %H:%M:%S')"

    if [[ -n $INSTALL_MESSAGES_LOG ]]; then
        case "$level" in
            4) echo "$dt [DEBG] $message" >> "$INSTALL_MESSAGES_LOG" ;;
            3) echo "$dt [INFO] $message" >> "$INSTALL_MESSAGES_LOG" ;;
            2) echo "$dt [WARN] $message" >> "$INSTALL_MESSAGES_LOG" ;;
            1) echo "$dt [ERRR] $message" >> "$INSTALL_MESSAGES_LOG" ;;
            *) echo "$message" >> "$INSTALL_MESSAGES_LOG" ;;
        esac
    fi

    case "$level" in
        4)
            echo -e "$dt \033[1;34m[DEBG]\033[0m $message"
            ;;
        3)
            echo -e "$dt \033[1;34m[INFO]\033[0m $message"
            ;;
        2)
            echo -e "$dt \033[1;33m[WARN]\033[0m $message"
            ;;
        1)
            echo -e "$dt \033[1;31m[ERRR]\033[0m $message"
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
    # Loop over all bash variables and print them if their name starts with GAPIPE_
    log_info "Installation parameters:"
    for var in $(compgen -v | grep '^GAPIPE_'); do
        value="${!var}"
        if [[ -n "$value" ]]; then
            log_info "  $var: $value"
        else
            log_info "  $var: (not set)"
        fi
    done
}

function join_path() {
    local result=""
    for part in "$@"; do
        # If part is empty, skip
        [[ -z "$part" ]] && continue
        # If part is absolute, reset result
        if [[ "$part" = /* ]]; then
            result="$part"
        else
            if [[ -z "$result" ]]; then
                result="$part"
            else
                result="${result%/}/$part"
            fi
        fi
    done
    echo "$result"
}

function run_cmd() {
    # Run a command and log its output

    cmd=$1
    log_file="$2"

    # Log the command into the installation command file
    log_debug "Running command: ${cmd}"
    if [[ -n $INSTALL_COMMANDS_LOG ]]; then
        echo "# Running command at $(date)" >> "${INSTALL_COMMANDS_LOG}"
        echo "${cmd}" >> "${INSTALL_COMMANDS_LOG}"
    fi
    
    # If a log file is specified, redirect output into it
    if [[ -n $log_file ]]; then
        log_file=$(join_path "${INSTALL_LOG_DIR}" "${log_file}")
        echo "\$ cd $PWD" >> "$log_file"
        echo "\$ ${cmd}" >> "$log_file"

        cmd=""${cmd}" 2>&1 | tee -a \"$log_file\""
    fi
    
    # Temporarily disable exit on error to capture the exit status
    set +e
    eval "${cmd}"
    local status=$?
    set -e

    # Log and error message if the status in non-zero
    # if [[ $status -ne 0 ]]; then
    #     log_error "Command failed with exit status $status: ${cmd}"
    # fi

    return $status
}

function download_file() {
    # Download a file from a URL
    # $1: URL
    # $2: target file name

    url="$1"
    target_file="$2"

    log_info "Downloading file from ${url} to $(realpath ${target_file})."
    run_cmd \
        "curl -sOL \"${url}\" -o \"${target_file}\"" \
        "download.log"
}

function ensure_command() {
    # Check if a command is available, and exit with an error message if not

    cmd="$1"

    if ! command -v "${cmd}" &> /dev/null; then
        log_error "Command ${cmd} is not available. Please install it and try again."
        exit 1
    fi
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

function git_clone() {
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
    run_cmd \
        "git clone --recursive \"${url}\" \"${target_dir}\"" \
        "git_clone_$(basename "$repo_name").log"

    run_cmd "pushd \"${target_dir}\" > /dev/null"
    run_cmd "git config pull.ff only"
    run_cmd "popd > /dev/null"
}

function git_checkout() {
    # Check out the specified git tag
    # Assume the git repo is the current working directory

    # $1: git tag

    git_tag="$1"
    repo_name=$(basename "$(pwd)")

    log_info "Checking out tag ${git_tag} for ${repo_name}"
    run_cmd \
        "git checkout \"${git_tag}\"" \
        "git_checkout_$(basename "$repo_name").log"
}

function git_fetch() {
    # Fetch the latest changes from the remote repository
    # Assume the git repo is the current working directory

    repo_name=$(basename "$(pwd)")

    log_info "Fetching latest changes for ${repo_name}"
    run_cmd \
        "git fetch" \
        "git_fetch_$(basename "$repo_name").log"
}

function git_pull() {
    # Pull the latest changes from the remote repository
    # Assume the git repo is the current working directory

    repo_name=$(basename "$(pwd)")

    log_info "Pulling latest changes for ${repo_name}"
    run_cmd "git config pull.ff only"
    run_cmd \
        "git pull" \
        "git_pull_$(basename "$repo_name").log"
}

function git_reset() {
    # Reset the current branch to the latest commit from origin
    # Assume the git repo is the current working directory

    branch_name="$1"
    repo_name=$(basename "$(pwd)")

    log_info "Reseting repo ${repo_name} to latest commit from origin/${branch_name}"
    run_cmd \
        "git reset --hard \"origin/${branch_name}\"" \
        "git_reset_$(basename "$repo_name").log"
}

function is_git_tag() {
    # Check if the specified git tag exists in the current repository
    # Assume the git repo is the current working directory

    git_tag="$1"

    run_cmd "git show-ref --tags --quiet --verify \"refs/tags/${git_tag}\""
    return $?
}

function is_git_branch() {
    # Check if the specified git branch exists in the current repository
    # Assume the git repo is the current working directory

    git_branch="$1"

    run_cmd "git show-ref --heads --quiet --verify \"refs/heads/${git_branch}\""
    return $?
}

function git_checkout_or_update() {
    # If the specified git tag is already checked out, do nothing.
    # If already on the branch, pull the latest changes.
    # If on a different tag or commit, check out the specified tag.
    # If on a different branch, switch to the specified tag.

    git_tag="$1"
    repo_name=$(basename "$(pwd)")

    git_fetch

    if is_git_tag "${git_tag}"; then
        git_checkout "${git_tag}"
    elif is_git_branch "${git_tag}"; then
        # To handle the case when the branches have diverged (due to rebase, etc.)
        # we make sure that the branch is reset to the lastest commit from origin
        git_checkout "${git_tag}"
        git_reset "${git_tag}"
    else
        log_error "Git tag or branch ${git_tag} does not exist in repository ${repo_name}."
        exit 1
    fi
}

function get_conda_dir() {
    # Return the conda directory

    conda_dir=$(join_path "${GAPIPE_ROOT}" "${GAPIPE_CONDA_DIR}")
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
    download_file \
        "https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh" \
        "./miniconda.sh"
    
    log_info "Executing Miniconda3 installer."
    run_cmd \
        "./miniconda.sh" -b -p "${conda_dir}" \
        "miniconda_install.log"

    log_info "Removing Miniconda3 installer."
    run_cmd "./miniconda.sh"
}

function update_conda() {
    echo "Updating conda is not implemented yet." >/dev/stderr
    exit 2
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

    run_cmd \
        "conda env create --yes --quiet --name \""${env_name}"\" --file \""${env_file}"\"" \
        "conda_create_${env_name}.log"
}

function update_conda_env() {
    # Update a conda environment based on a yaml file
    # Assume that conda is already activated
    #

    env_name="$1"
    env_file="$2"

    log_info "Updating conda environment ${env_name} from ${env_file}."

    run_cmd \
        "conda env update --quiet --name \""${env_name}"\" --file \""${env_file}"\"" \
        "conda_update_${env_name}.log"
}

function install_pip_requirements() {
    # Install a pip requirements file into the current conda environment
    # Assume that conda is already activated

    req_file="$1"

    run_cmd \
        "pip install --quiet --no-color --no-input -r \"${req_file}\"" \
        "pip_install_$(basename "${req_file}" | sed 's/\.[^.]*$//').log"
}

function get_lsst_dir() {
    # Return the directory of the LSST stack

    lsst_dir=$(join_path "${GAPIPE_ROOT}" "${LSST_DIR}")
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
        
        if [[ -z "$lsst_version" ]]; then
            log_error "Failed to retrieve LSST conda environment name. Check if the version is valid."
            exit 1
        fi
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
        eups list 2> /dev/null | grep "${package}" | grep -q "$tag" && {
            log_info "EUPS package ${package} is already installed. Skipping."
        } || {
            log_info "Installing EUPS package $package $tag"
            run_cmd \
                "eups distrib install "$package" -t "${tag}" --no-server-tags" \
                "eups_install.log"
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
    run_cmd \
        "./lsstinstall -T \"${lsst_version}\" -p \"${lsst_conda_dir}\" -e \"${lsst_env}\"" \
        "lsstinstall.log"

    run_cmd "popd > /dev/null"

    if [[ $? -ne 0 ]]; then
        log_error "LSST installation failed."
        exit 1
    else
        log_info "LSST installation completed successfully."
    fi
}

function upgrade_lsst() {
    log_info "Upgrading the LSST stack is not implemented yet."
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
        eups list 2> /dev/null | grep "${package}" | grep -q "$version" && {
            log_info "EUPS package ${package} is already installed. Skipping."
        } || {
            log_info "Installing EUPS package $package $version"
            run_cmd \
                "eups distrib install $package $version" \
                "eups_install.log"
        }
    done

    run_shebangtron
}

function install_module_source()
{
    # Clone and check out a git repository
    # Assume already in the src directory

    module="$1"
    repo="$2"
    tag="$3"

    log_info "Installing and configuring ${module} from source."

    # If the datamodel directory doesn't exist, clone the repository
    if [[ ! -d "${module}" ]]; then
        git_clone "${repo}" ${module} 1
            
        run_cmd "pushd ${module} > /dev/null"
        git_checkout "${tag}"

        run_cmd "popd > /dev/null"
        log_info "Finished installing ${module}."
    elif [[ -d "${module}" && $GAPIPE_UPGRADE -eq 1 ]]; then
        log_info "Module ${module} already exists, but --upgrade option is set. Proceeding with upgrade."

        run_cmd "pushd ${module} > /dev/null"
        git_checkout_or_update "${tag}"

        run_cmd "popd > /dev/null"
        log_info "Finished upgrading module ${module}."
    else
        log_info "Module ${module} already exists. Skipping cloning."
    fi
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
export PFSSPEC_ROOT="${GAPIPE_ROOT}/src/ga_pfsspec_all"
export PFSSPEC_DATA="${GAPIPE_ROOT}/data/pfsspec"

# Register the dependencies that are installed from source
export PFSSPEC_MODULES=\\
"datamodel:${GAPIPE_ROOT}/src/datamodel:python"
EOF
}

function install_pfsspec_source() {
    # Clone and check out the ga_pfsspec_all repository and generate
    # the environment configuration file to initilize PFSSPEC
    # Assume we are already in the src directory

    log_info "Installing and configuring pfsspec from source."

    if [[ ! -d "ga_pfsspec_all" ]]; then
        git_clone "${PFSSPEC_GITHUB}" ga_pfsspec_all 1

        run_cmd "pushd ga_pfsspec_all > /dev/null"
        git_checkout "${PFSSPEC_GIT_TAG}"

        # Generate the default pfsspec environment
        generate_pfsspec_env_file

        # Check out each submodule to the HEAD of the current branch
        log_info "Checking out submodules for pfsspec."
        run_cmd "bash ./bin/checkout ${PFSSPEC_GIT_TAG}" "checkout_pfsspec.log"

        # Initialize the pfsspec environment in a new shell in order to generate the symlinks
        log_info "Initializing the pfsspec environment."

        # Source the init script into a new shell in order to set up the
        # source code symlinks etc.
        run_cmd "bash -c \"source ./bin/init\"" "init_pfsspec.log"

        run_cmd "popd > /dev/null"
        log_info "Finished installing pfsspec."
    elif [[ -d "ga_pfsspec_all" && $GAPIPE_UPGRADE -eq 1 ]]; then
        log_info "pfsspec repository already exists, but --upgrade option is set. Proceeding with upgrade."

        run_cmd "pushd ga_pfsspec_all > /dev/null"
        git_checkout_or_update "${PFSSPEC_GIT_TAG}"

        # Check out each submodule to the HEAD of the current branch
        log_info "Updating submodules for pfsspec."
        run_cmd "bash ./bin/pull" "pull_pfsspec.log"

        run_cmd "popd > /dev/null"
        log_info "Finished upgrading pfsspec."
    else
        log_info "pfsspec repository already exists. Skipping cloning."
    fi
}

function install_chemfit_conda() {
    echo "Installing chemfit as a conda package is not implemented yet." >/dev/stderr
    exit -2
}

function install_chemfit_eups() {
    echo "Installing chemfit as an EUPS package is not implemented yet." >/dev/stderr
    exit -2
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
#!/bin/bash

# This shell script is generated by the gapipe installer.
# It is sourced by the init script to set up the environment variables for gapipe.
# You can modify this file to change the default values of the environment variables.

# Directories of the GAPIPE installation
export GAPIPE_ROOT="${GAPIPE_ROOT}"
export GAPIPE_CONDAPATH="${conda_dir}"
export GAPIPE_CONDAENV="${GAPIPE_CONDA_ENV}"

# Use these settings to enable Butler for data lookup
export GAPIPE_LSST="${GAPIPE_LSST}"
export GAPIPE_USE_BUTLER="${GAPIPE_LSST}"

# Uncomment this if you don't want to use Butler
# export GAPIPE_USE_BUTLER="0"

# Uncomment these if you don't want to use the LSST stack
# export GAPIPE_LSST="0"
# export GAPIPE_CONDAPATH="~/miniconda"
# export GAPIPE_CONDAENV="gapipe"

# Data root directory, this can be used in the config files
# to make them portable across different machines with different
# directory structures.
export GAPIPE_DATAROOT="$GAPIPE_DATAROOT"

# Data directories for temporary data (work) and final output (out)
# You can change these to point to scratch or other fast storage when
# running on a cluster.
export GAPIPE_WORKDIR="$GAPIPE_ROOT/work"
export GAPIPE_OUTDIR="$GAPIPE_ROOT/out"

# Full path to the observation logs
# For the SSP, use the git repo https://github.com/Subaru-PFS/spt_ssp_observation
export GAPIPE_OBSLOGDIR="$GAPIPE_OBSLOGDIR"

# To include the targeting data in the final catalog, such as fluxes and magnitudes,
# you need to specify the location of the targeting data. This is only relevant
# for field prepared with the GA version of Netflow.
export GAPIPE_TARGETINGDIR="$GAPIPE_TARGETINGDIR"

# The remaining configuration variables are for a specific GA pipeline run
# You tipically want to override these before executing the pipeline
# When using the batch script, use the common.sh config file to define these
# instead of setting them here in the global environment
export GAPIPE_DATADIR="${GAPIPE_DATADIR}"
export GAPIPE_RUNDIR="$GAPIPE_RUNDIR"
export GAPIPE_RUN="$GAPIPE_RUN"
export GAPIPE_CONFIGRUNDIR="$GAPIPE_CONFIGRUNDIR"
export GAPIPE_CONFIGRUN="$GAPIPE_CONFIGRUN"
export GAPIPE_GARUNDIR="$GAPIPE_GARUNDIR"
export GAPIPE_GARUN="$GAPIPE_GARUN"
export BUTLER_CONFIGDIR="${BUTLER_CONFIGDIR}"
export BUTLER_COLLECTIONS="${BUTLER_COLLECTIONS}"

# --------------------------------------------------------
# The remaining configuration variables are for developers

# pfsspec library settings

export PFSSPEC_ROOT="${GAPIPE_ROOT}/src/ga_pfsspec_all"
export PFSSPEC_DATA="${GAPIPE_ROOT}/data/pfsspec"

# Define dependencies that are not installed as modules (conda or eups)
# but should be added to PYTHONPATH. Also define dependencies that are to be
# loaded from "source".
# The format is <module_name>:<path_to_source>:<rel_path_to_module> where
# * <module_name> is the name of the module to be imported in python
# * <path_to_source> is the path to the root of the source code of the module
# * <rel_path_to_module> is the relative path to the module from the root of the source code,
#   i.e. the path to be added to PYTHONPATH, typically 'python' or 'src/python'
# Specify multiple modules separated by new lines.
export GAPIPE_MODULES="datamodel:$GAPIPE_ROOT/src/datamodel:python
ga_common:$GAPIPE_ROOT/src/ga_common:python
ga_pfsspec:$GAPIPE_ROOT/src/ga_pfsspec_all:python
gapipe:$GAPIPE_ROOT/src/ga_pipeline:python
gapipe-test:${GAPIPE_ROOT}/src/ga_pipeline:tests"

# Define the debug port for remote debugging with vscode.
export GAPIPE_DEBUGPORT=""

# Add extra modules to PYTHONPATH
export PYTHONPATH=""
EOF
}

function install_gapipe_source() {
    # Install the GAPIPE source code and generate the default environment file
    # Assume already in the src directory

    log_info "Installing and configuring gapipe."

    if [[ ! -d "ga_pipeline" ]]; then
        git_clone "${GAPIPE_GITHUB}" ga_pipeline 1

        run_cmd "pushd ga_pipeline > /dev/null"
        git_checkout "${GAPIPE_GIT_TAG}"

        # Generate the default gapipe environment
        generate_gapipe_env_file

        # Initialize the gapipe environment in a new shell in order to generate the symlinks
        run_cmd "bash -c \"source ./bin/init\"" "init_gapipe.log"

        run_cmd "popd > /dev/null"
        log_info "Finished installing gapipe."
    elif [[ -d "ga_pipeline" && $GAPIPE_UPGRADE -eq 1 ]]; then
        log_info "gapipe repository already exists, but --upgrade option is set. Proceeding with upgrade."

        run_cmd "pushd ga_pfsspec_all > /dev/null"
        git_checkout_or_update "${PFSSPEC_GIT_TAG}"

        run_cmd "popd > /dev/null"
        log_info "Finished upgrading gapipe."
    else
        log_info "ga_pipeline repository already exists. Skipping cloning."
    fi
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

# Check if the installation directory exists and create if not
if [[ -d "${GAPIPE_ROOT}" ]]; then
    if [[ $GAPIPE_UPGRADE -eq 0 && $GAPIPE_FORCE -eq 1 ]]; then
        log_warning "Installation directory ${GAPIPE_ROOT} already exists, but --force option is set. Proceeding with installation."
    elif [[ $GAPIPE_UPGRADE -eq 1 ]]; then
        log_info "Installation directory ${GAPIPE_ROOT} already exists, proceeding with upgrade."
    else
        log_error "Installation directory ${GAPIPE_ROOT} already exists. Use --force to overwrite."
        exit 1
    fi
else
    if [[ $GAPIPE_UPGRADE -eq 1 ]]; then
        log_error "Installation directory ${GAPIPE_ROOT} does not exist, cannot upgrade."
        exit 1
    else
        log_info "Creating gapipe directory ${GAPIPE_ROOT}."
        run_cmd "mkdir -p \"${GAPIPE_ROOT}\""
    fi
fi

init_logging
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

# Make sure no other python installations interfere with the installation
run_cmd "unset PYTHONPATH"
run_cmd "unset CONDA_DEFAULT_ENV CONDA_EXE CONDA_PREFIX CONDA_PROMPT_MODIFIER CONDA_PYTHON_EXE CONDA_SHLVL"

# Some other setting
NUMTHREADS=$(($(nproc) / 2))
if [[ $NUMTHREADS -lt 1 ]]; then
    NUMTHREADS=1
fi
export OMP_NUM_THREADS=1
export PYTHONHTTPSVERIFY=0                      # Disable SSL verification for Python HTTPS requests

# Ensure some fundamental unix commands are available
ensure_command "curl"
ensure_command "git"
ensure_command "patch"
ensure_command "tar"

# Check if github access is configured, if required
if [[ "$GAPIPE_PACKAGE" == "SOURCE" ]]; then
    # Make sure ssh to github is available
    ensure_github_ssh
fi

# Create subdirectories for the installation
run_cmd "pushd \"$(realpath "${GAPIPE_ROOT}")\" > /dev/null"
run_cmd "mkdir -p ./bin"
run_cmd "mkdir -p ./data"

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
    
    if [[ -d "${lsst_dir}" && $GAPIPE_UPGRADE -eq 0 ]]; then
        log_info "LSST stack already installed, skipping task."
    elif [[ -d "${lsst_dir}" && $GAPIPE_UPGRADE -eq 1 ]]; then
        log_info "LSST stack already installed, but --upgrade option is set. Proceeding with upgrade."
        upgrade_lsst "${lsst_dir}" "${lsst_conda_dir}" "${lsst_conda_env}" "${lsst_version}"
    else
        log_info "LSST stack not found, installing LSST stack version ${lsst_version}."
        install_lsst "${lsst_dir}" "${lsst_conda_dir}" "${lsst_conda_env}" "${lsst_version}"
    fi

    # Override the conda directory and environment name
    GAPIPE_CONDA_DIR="$(get_lsst_conda_dir)"
    GAPIPE_CONDA_ENV="$(get_lsst_conda_env)"

    # Activate the newly installed conda environment
    activate_lsst_conda_env "${GAPIPE_CONDA_DIR}" "${GAPIPE_CONDA_ENV}"

    # TODO: installing the pip package will try to activate the environment
    #       but it fails because ${EUPS_DIR} has the value of
    #           /srv/local/tmp/dobos/gapipe/stack/conda/eups
    #       instead of
    #           /srv/local/tmp/dobos/gapipe/stack/conda/envs/lsst-scipipe-12.1.0/eups

    # Install additional conda packages required by gapipe
    log_info "Installing additional conda packages for gapipe."
    conda_env_file="$(realpath $(join_path "$SCRIPT_DIR" "${LSST_CONDA_ENV_FILE}"))"
    update_conda_env "${GAPIPE_CONDA_ENV}" "${conda_env_file}"

    # NOTE: activation of the LSST conda env breaks if trying to pip-install
    #       packages with 'conda env update'. Install instead directly with pip
    #       which will print a lot of errors but it's OK

    pip_req_file="$(realpath $(join_path "$SCRIPT_DIR" "${LSST_PIP_REQ_FILE}"))"
    install_pip_requirements "${pip_req_file}"

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
    if [[ -d "$conda_dir" && $GAPIPE_UPGRADE -eq 0 ]]; then
        log_info "Conda already installed, skipping task."
    elif [[ -d "$conda_dir" && $GAPIPE_UPGRADE -eq 1 ]]; then
        log_info "Conda already installed, but --upgrade option is set. Proceeding with upgrade."
        update_conda "${conda_dir}"
    else
        log_info "Conda not found, installing conda."
        install_conda "${conda_dir}"
    fi

    # Temporarily activate the base conda environment
    log_info "Activating conda environment base."
    run_cmd "source \"${conda_dir}/bin/activate\" base"

    # Check if the target environment exists
    if [[ is_conda_env == 1 && $GAPIPE_UPGRADE -eq 0 ]]; then
        log_info "Conda environment ${GAPIPE_CONDA_ENV} already exists, skipping task."
        # TODO: only install packages
    elif [[ is_conda_env == 1 && $GAPIPE_UPGRADE -eq 1 ]]; then
        log_info "Conda environment ${GAPIPE_CONDA_ENV} already exists, but --upgrade option is set. Proceeding with upgrade."
        update_conda_env ${GAPIPE_CONDA_ENV} $(realpath $(join_path "$SCRIPT_DIR" "${GAPIPE_CONDA_ENV_FILE}"))
    else
        log_info "Conda environment ${GAPIPE_CONDA_ENV} does not exist, creating it."
        install_conda_env ${GAPIPE_CONDA_ENV} $(realpath $(join_path "$SCRIPT_DIR" "${GAPIPE_CONDA_ENV_FILE}"))
    fi

    run_cmd "conda deactivate"
fi

# Activate the target environment
log_info "Activating conda environment ${GAPIPE_CONDA_ENV}."
run_cmd "source \"${GAPIPE_CONDA_DIR}/bin/activate\" \"${GAPIPE_CONDA_ENV}\""

# Depending on the package mode, install the GAPIPE software stack
if [[ "$GAPIPE_PACKAGE" == "SOURCE" ]]; then
    log_info "Installing GAPIPE from source."

    run_cmd "mkdir -p \"${GAPIPE_ROOT}/src\""
    run_cmd "pushd \"${GAPIPE_ROOT}/src\" > /dev/null"

    # Clone and configure the repositories
    install_module_source "datamodel" "${DATAMODEL_GITHUB}" "${DATAMODEL_GIT_TAG}"
    install_module_source "ga_common" "${GACOMMON_GITHUB}" "${GACOMMON_GIT_TAG}"
    install_module_source "ga_chemfit" "${CHEMFIT_GITHUB}" "${CHEMFIT_GIT_TAG}"

    install_pfsspec_source

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
run_cmd "mkdir -p \"${GAPIPE_ROOT}/data\""
run_cmd "mkdir -p \"${GAPIPE_ROOT}/data/pfsspec\""
run_cmd "mkdir -p \"${GAPIPE_ROOT}/work\""
run_cmd "mkdir -p \"${GAPIPE_ROOT}/out\""

# TODO: Check if the installation was successful

run_cmd "popd > /dev/null"

if [[ $GAPIPE_UPGRADE -eq 1 ]]; then
    log_info "GAPIPE upgrade completed successfully."
else
    log_info "GAPIPE installation completed successfully."
fi

# Print the installation summary
print_summary