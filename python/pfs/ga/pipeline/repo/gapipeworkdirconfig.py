import os
import re
from types import SimpleNamespace

from pfs.ga.pfsspec.survey.pfs.datamodel import *
from pfs.ga.pfsspec.survey.repo import IntFilter, HexFilter, DateFilter, StringFilter

from ..gapipe.config import GAPipelineConfig

# Extend the basic PfsFileSystemConfig with the GAPipelineConfig
# These file naming conventions are specific for the GAPipeline

def load_GAPipelineConfig(identity, filename, dir):
    return GAPipelineConfig.from_file(path=os.path.join(dir, filename))

def save_GAPipelineConfig(data, identity, filename, dir):
    data.save(os.path.join(dir, filename))

GAPipeWorkdirConfig = SimpleNamespace(
    variables = {
        'datadir': '$GAPIPE_WORKDIR',
        'rundir': '$GAPIPE_RUNDIR',
    },
    root = '$datadir',
    products = {
        # GAPipelineConfig files are stored in the datadir. These are yaml files
        # names the same as the PfsStar files, but with a .yaml extension. We have
        # no corresponding class in the datamodel, but we can load them using the GAPipelineConfig class.
        GAPipelineConfig: SimpleNamespace(
            name = 'GAPipelineConfig',
            params = SimpleNamespace(
                run = StringFilter(name='run'),
                catId = IntFilter(name='catId', format='{:05d}'),
                objId = HexFilter(name='objId', format='{:016x}'),
                nVisit = IntFilter(name='nVisit', format='{:03d}'),
                pfsVisitHash = HexFilter(name='pfsVisitHash', format='{:016x}'),
            ),
            params_regex = [
                re.compile(r'pfsStar-(?P<catId>\d{5})-(?P<objId>[0-9a-f]{16})-(?P<nVisit>\d{3})-0x(?P<pfsVisitHash>[0-9a-f]{16})_(?P<run>[^.]+)\.(yaml)$'),
            ],
            dir_format = '${datadir}/${rundir}/pfsStar/{catId}/{objId}-{nVisit}-0x{pfsVisitHash}',
            filename_format = 'pfsStar_PFS_{catId}-{objId}-{nVisit}-0x{pfsVisitHash}_{run_}.yaml',
            load = load_GAPipelineConfig,
            save = save_GAPipelineConfig,
        ),
    }
)