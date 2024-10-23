import os
import re
from types import SimpleNamespace

from pfs.datamodel import *
from pfs.ga.pfsspec.survey.repo import IntFilter, HexFilter, DateFilter, StringFilter
from pfs.ga.pfsspec.survey.pfs import PfsFileSystemConfig as PfsFileSystemConfigBase

from ..config import GA1DPipelineConfig

# Extend the basic PfsFileSystemConfig with the GA1DPipelineConfig

PfsFileSystemConfig = SimpleNamespace(
    root = '$datadir',
    variables = {
        'workdir': '$GAPIPE_WORKDIR',
        'datadir': '$GAPIPE_DATADIR',
        'rerundir': '$GAPIPE_RERUNDIR',
    },
    products = {
        **PfsFileSystemConfigBase.products,

        GA1DPipelineConfig: SimpleNamespace(
            params = SimpleNamespace(
                catId = IntFilter(name='catId', format='{:05d}'),
                tract = IntFilter(name='tract', format='{:05d}'),
                patch = StringFilter(name='patch'),
                objId = HexFilter(name='objId', format='{:016x}'),
                nVisit = IntFilter(name='nVisit', format='{:03d}'),
                pfsVisitHash = HexFilter(name='pfsVisitHash', format='{:016x}'),
            ),
            params_regex = [
                re.compile(r'pfsGAObject-(?P<catId>\d{5})-(?P<tract>\d{5})-(?P<patch>.*)-(?P<objId>[0-9a-f]{16})-(?P<nVisit>\d{3})-0x(?P<pfsVisitHash>[0-9a-f]{16})\.(yaml)$'),
            ],
            dir_format = '$workdir/rerun/$rerundir/pfsGAObject/{catId}/{tract}/{patch}/pfsGAObject-{catId}-{tract}-{patch}-{objId}-{nVisit}-0x{pfsVisitHash}',
            filename_format = 'pfsGAObject-{catId}-{tract}-{patch}-{objId}-{nVisit}-0x{pfsVisitHash}.yaml',
            load = lambda identity, filename, dir:
                GA1DPipelineConfig.from_file(path=os.path.join(dir, filename)),
        ),
    }
)