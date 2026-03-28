import os
import re
from types import SimpleNamespace

from pfs.datamodel import *
from pfs.ga.pfsspec.survey.repo import IntFilter, HexFilter, DateFilter, StringFilter
from pfs.ga.pfsspec.survey.pfs import PfsGen3FileSystemConfig as PfsGen3FileSystemConfigBase

# Extend the basic PfsFileSystemConfig with the GAPipelineConfig. This is only used
# when Butler is not available.

PfsGen3FileSystemConfig = SimpleNamespace(
    root = '$datadir',
    variables = {
        ** PfsGen3FileSystemConfigBase.variables,
        'datadir': '$GAPIPE_DATADIR',
        'rundir': '$GAPIPE_RUNDIR',
    },
    products = {
        **PfsGen3FileSystemConfigBase.products,
    }
)