#!/bin/bash

# This file is necessary for debugging GAPIPE in vscode since it uses the standard
# conda activation methods to set up the environment.

# If the EUPS_PATH variable is not set, make sure the eups environment is activated
# first
if [[ -z "${EUPS_PATH}" ]]; then
    if [[ -f "${CONDA_PREFIX}/etc/conda/activate.d/eups_activate.sh" ]]; then
        source "${CONDA_PREFIX}/etc/conda/activate.d/eups_activate.sh"
    else
        echo "EUPS_PATH is not set and eups_activate.sh is not found in the conda environment."
        exit 1
    fi
fi

export GAPIPE_PS1OLD="$PS1"
export PS1="\\u@\\h:\\w\\n\[\e[0;31m\]$CONDA_PROMPT_MODIFIER\[\e[0m\]\\$ "
export EUPS_PKGROOT="https://hscpfs.mtk.nao.ac.jp/pfs-drp-2d/Linux64|$(cat $EUPS_PATH/pkgroot)"

function eups_setup_safe() {
    # Attempt to set up an eups distr

    pkg="$1"

    eups list 2>&1 | grep -q "${pkg}" && {
        setup "${pkg}"
    } || {
        echo "EUPS package ${pkg} is not installed."
    }
}

eups_setup_safe cp_pipe
eups_setup_safe pfs_pipe2d


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

echo Activated conda environment for PFS GAPIPE