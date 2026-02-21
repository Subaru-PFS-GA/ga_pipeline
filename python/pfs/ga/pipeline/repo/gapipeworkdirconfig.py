import os
import re
from types import SimpleNamespace

from pfs.ga.pfsspec.survey.pfs.datamodel import *
from pfs.ga.pfsspec.survey.repo import IntFilter, HexFilter, DateFilter, StringFilter
from pfs.ga.pfsspec.survey.pfs import PfsGen3FileSystemConfig
from pfs.ga.pfsspec.survey.pfs.pfsgen3filesystemconfig import load_PfsSingle, save_PfsSingle

from ..gapipe.config import GAPipelineConfig

# Extend the basic PfsFileSystemConfig with the GAPipelineConfig
# These file naming conventions are specific for the GAPipeline

GAPipeWorkdirConfig = SimpleNamespace(
    root = '$datadir',
    variables = {
        'workdir': '$GAPIPE_WORKDIR',
        'outdir': '$GAPIPE_OUTDIR',
        'datadir': '$GAPIPE_DATADIR',
        'rerundir': '$GAPIPE_RERUNDIR',
        'garundir': '$GAPIPE_GARUNDIR',
    },
    products = {
        # PfsSingle files are extracted from the pfsCalibrated files
        PfsSingle: SimpleNamespace(
            name = 'pfsSingle',
            params = SimpleNamespace(
                catId = IntFilter(name='catId', format='{:05d}'),
                objId = HexFilter(name='objId', format='{:016x}'),
                visit = IntFilter(name='visit', format='{:06d}'),
            ),
            params_regex = [
                re.compile(r'pfsSingle-(?P<catId>\d{5})-(?P<objId>[0-9a-f]{16})-(?P<visit>\d{6})\.(fits)$'),
            ],
            dir_format = '$workdir/$rerundir/pfsSingle/{catId}/{objId}',
            filename_format = 'pfsSingle-{catId}-{objId}-{visit}.fits',
            identity = lambda data:
                SimpleNamespace(catId=data.target.catId, tract=data.target.tract, patch=data.target.patch, objId=data.target.objId, visit=data.observations.visit[0]),
            load = load_PfsSingle,
            save = save_PfsSingle,
        ),

        # GAPipelineConfig files are stored in the workdir. These are yaml files
        # names the same as the PfsStar files, but with a .yaml extension. We have
        # no corresponding class in the datamodel, but we can load them using the GAPipelineConfig class.
        GAPipelineConfig: SimpleNamespace(
            name = 'GAPipelineConfig',
            params = SimpleNamespace(
                catId = IntFilter(name='catId', format='{:05d}'),
                objId = HexFilter(name='objId', format='{:016x}'),
                nVisit = IntFilter(name='nVisit', format='{:03d}'),
                pfsVisitHash = HexFilter(name='pfsVisitHash', format='{:016x}'),
            ),
            params_regex = [
                re.compile(r'pfsStar-(?P<catId>\d{5})-(?P<objId>[0-9a-f]{16})-(?P<nVisit>\d{3})-0x(?P<pfsVisitHash>[0-9a-f]{16})\.(yaml)$'),
            ],
            dir_format = '$workdir/$garundir/pfsStar/{catId}/{objId}-{nVisit}-0x{pfsVisitHash}',
            filename_format = 'pfsStar-{catId}-{objId}-{nVisit}-0x{pfsVisitHash}.yaml',
            load = lambda identity, filename, dir:
                GAPipelineConfig.from_file(path=os.path.join(dir, filename)),
            save = lambda config, identity, filename, dir:
                config.save(filename),
        ),

        PfsStar: SimpleNamespace(
            name = PfsGen3FileSystemConfig.products[PfsStar].name,
            params = PfsGen3FileSystemConfig.products[PfsStar].params,
            identity = PfsGen3FileSystemConfig.products[PfsStar].identity,
            load = PfsGen3FileSystemConfig.products[PfsStar].load,
            save = PfsGen3FileSystemConfig.products[PfsStar].save,
            params_regex = [
                re.compile(r'pfsStar-(?P<catId>\d{5})-(?P<objId>[0-9a-f]{16})-(?P<nVisit>\d{3})-0x(?P<pfsVisitHash>[0-9a-f]{16})\.(fits|fits\.gz)$'),
            ],
            dir_format = '$outdir/$garundir/pfsStar/{catId}/{objId}-{nVisit}-0x{pfsVisitHash}',
            filename_format = 'pfsStar-{catId}-{objId}-{nVisit}-0x{pfsVisitHash}.fits',
        ),

        PfsStarCatalog: SimpleNamespace(
            name = PfsGen3FileSystemConfig.products[PfsStarCatalog].name,
            params = PfsGen3FileSystemConfig.products[PfsStarCatalog].params,
            identity = PfsGen3FileSystemConfig.products[PfsStarCatalog].identity,
            load = PfsGen3FileSystemConfig.products[PfsStarCatalog].load,
            save = PfsGen3FileSystemConfig.products[PfsStarCatalog].save,
            params_regex = [
                re.compile(r'pfsStarCatalog-(?P<catId>\d{5})-(?P<nVisit>\d{3})-0x(?P<pfsVisitHash>[0-9a-f]{16})\.(fits|fits\.gz)$'),
            ],
            dir_format = '$outdir/$garundir/pfsStarCatalog/{catId}/{nVisit}-0x{pfsVisitHash}',
            filename_format = 'pfsStarCatalog-{catId}-{nVisit}-0x{pfsVisitHash}.fits',
        ),
    }
)