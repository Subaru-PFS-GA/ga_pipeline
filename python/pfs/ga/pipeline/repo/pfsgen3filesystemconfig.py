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

        'workdir': '$GAPIPE_WORKDIR',
        'outdir': '$GAPIPE_OUTDIR',
        'datadir': '$GAPIPE_DATADIR',
        'rerundir': '$GAPIPE_RERUNDIR',
        'rerun': '$GAPIPE_RERUN',
    },
    products = {
        **PfsGen3FileSystemConfigBase.products,
    }
)