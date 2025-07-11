#!/bin/bash

# This file is necessary to set up the environment, including EUPS packages for debugging
# GAPIPE in vscode. It hooks into the standard conda activation process to set up the
# environment when running code via `conda run -n <env> <command>` and sets up the
# necessary EUPS packages.

function eups_setup_safe() {
    # Attempt to set up an eups distr

    pkg="$1"

    eups list "${pkg}" 2>/dev/null | grep . >/dev/null && {
        setup "${pkg}"
    } || {
        echo "EUPS package ${pkg} is not installed."
    }
}

function eups_unsetup_safe() {
    # Attempt to unsetup an eups distribution
    pkg="$1"

    eups list "${pkg}" 2>/dev/null | grep . >/dev/null && {
        unsetup "${pkg}"
    } || {
        echo "EUPS package ${pkg} is not installed."
    }
}

# If the EUPS_PATH variable is not set, make sure the eups environment is activated
# first
if [[ -z "${EUPS_PATH}" ]]; then
    if [[ -f "${CONDA_PREFIX}/etc/conda/activate.d/eups_activate.sh" ]]; then
        source "${CONDA_PREFIX}/etc/conda/activate.d/eups_activate.sh"
    else
        unset EUPS_PATH
    fi
fi

# If eups is available, set up the eups environment
if [[ -n "${EUPS_PATH}" ]]; then

    export EUPS_PKGROOT="https://hscpfs.mtk.nao.ac.jp/pfs-drp-2d/Linux64|$(cat $EUPS_PATH/pkgroot)"

    eups_setup_safe cp_pipe
    eups_setup_safe pfs_pipe2d
    
    # TODO: Because the GA datamodel is not part of the EUPS package yet,
    #       we unsetup it and use the library from source
    eups_unsetup_safe datamodel

    # These are necessary to run Butler after datamodel is unsetup'd
    eups_setup_safe sphgeom
    eups_setup_safe utils

    # EUPS resets the pythonpath so in order to keep anything that was previously
    # set, we need to look at CONDA_EUPS_BACKUP_PYTHONPATH and construct a new
    # PYTHONPATH that includes the original directories _before_ any packages 
    # registered by EUPS. This is necessary to allow overriding the package search
    # order in development mode when the local source code has precedence over what's
    # installed by EUPS.
    PP=""
    for dir in $(echo "$CONDA_EUPS_BACKUP_PYTHONPATH" | tr ':' '\n'); do
        if [[ "$dir" != *"$EUPS_PKGROOT"* ]]; then
            PP="$PP:$dir"
        fi
    done

    # If PP is not empty, set it as the new PYTHONPATH
    if [[ -n "$PP" ]]; then
        PYTHONPATH="$PP:$PYTHONPATH"
    fi

    export PYTHONPATH
fi

# Configure the conda environment prompt
export GAPIPE_PS1OLD="$PS1"
export PS1="\\u@\\h:\\w\\n\[\e[0;31m\]$CONDA_PROMPT_MODIFIER\[\e[0m\]\\$ "

echo Activated conda environment for PFS GAPIPE