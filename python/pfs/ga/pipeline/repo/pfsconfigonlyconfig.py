import os
import re
from types import SimpleNamespace

from pfs.datamodel import *
from pfs.ga.pfsspec.survey.pfs import PfsGen3FileSystemConfig as PfsGen3FileSystemConfigBase

# Use this repo config for pfsConfig files only

PfsConfigOnlyConfig = SimpleNamespace(
    root = '$datadir',
    variables = {
        ** PfsGen3FileSystemConfigBase.variables,
        'datadir': '$GAPIPE_DATADIR',
        'rundir': '$GAPIPE_RUNDIR',
        'configrundir': '$GAPIPE_CONFIGRUNDIR'
    },
    products = {
        PfsConfig: PfsGen3FileSystemConfigBase.products[PfsConfig],
    }
)