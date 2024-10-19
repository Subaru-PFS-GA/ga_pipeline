from types import SimpleNamespace

from pfs.datamodel import *

from .intfilter import IntFilter
from .hexfilter import HexFilter
from .datefilter import DateFilter
from .stringfilter import StringFilter

config = SimpleNamespace(
    variables = {
        'root': '$GAPIPE_DATADIR',
        'rerun': '$GAPIPE_RERUNDIR',
    },
    products = {
        PfsDesign: SimpleNamespace(
            params = SimpleNamespace(
                pfsDesignId = HexFilter(name='pfsDesignId', format='{:016x}')
            ),
            params_regex = [
                r'pfsDesign-0x(?P<pfsDesignId>[0-9a-fA-F]{16})\.(?:fits|fits\.gz)$',
            ],
            dir_format = 'pfsDesign',
            filename_format = 'pfsDesign-0x{pfsDesignId}.fits',
            load = lambda identity, filename, dir:
                PfsDesign.read(pfsDesignId=identity.pfsDesignId, dirName=dir),
        ),
        PfsConfig: SimpleNamespace(
            params = SimpleNamespace(
                pfsDesignId = HexFilter(name='pfsDesignId', format='{:016x}'),
                visit = IntFilter(name='visit', format='{:06d}'),
                date = DateFilter(name='date', format='{:%Y-%m-%d}'),
            ),
            params_regex = [
                r'(?P<date>\d{4}-\d{2}-\d{2})/pfsConfig-0x(?P<pfsDesignId>[0-9a-fA-F]{16})-(?P<visit>\d{6})\.(fits|fits\.gz)$',
                r'pfsConfig-0x(?P<pfsDesignId>[0-9a-fA-F]{16})-(?P<visit>\d{6})\.(fits|fits\.gz)$'
            ],
            dir_format = 'pfsConfig/{date}/',
            filename_format = 'pfsConfig-0x{pfsDesignId}-{visit}.fits',
            load = lambda identity, filename, dir: 
                PfsConfig.read(pfsDesignId=identity.pfsDesignId, visit=identity.visit, dirName=dir),
        ),
        PfsSingle: SimpleNamespace(
            params = SimpleNamespace(
                catId = IntFilter(name='catId', format='{:05d}'),
                tract = IntFilter(name='tract', format='{:05d}'),
                patch = StringFilter(name='patch'),
                objId = HexFilter(name='objId', format='{:016x}'),
                visit = IntFilter(name='visit', format='{:06d}'),
            ),
            params_regex = [
                r'pfsSingle-(?P<catId>\d{5})-(?P<tract>\d{5})-(?P<patch>.*)-(?P<objId>[0-9a-f]{16})-(?P<visit>\d{6})\.(fits|fits\.gz)$',
            ],
            dir_format = 'rerun/$rerun/pfsSingle/{catId}/{tract}/{patch}',
            filename_format = 'pfsSingle-{catId}-{tract}-{patch}-{objId}-{visit}.fits',
            load = lambda identity, filename, dir:
                PfsSingle.read(identity.__dict__, dirName=dir),
        ),
        PfsObject: SimpleNamespace(
            params = SimpleNamespace(
                catId = IntFilter(name='catId', format='{:05d}'),
                tract = IntFilter(name='tract', format='{:05d}'),
                patch = StringFilter(name='patch'),
                objId = HexFilter(name='objId', format='{:016x}'),
                nVisit = IntFilter(name='nVisit', format='{:03d}'),
                pfsVisitHash = HexFilter(name='pfsVisitHash', format='{:016x}'),
            ),
            params_regex = [
                r'pfsObject-(?P<catId>\d{5})-(?P<tract>\d{5})-(?P<patch>.*)-(?P<objId>[0-9a-f]{16})-(?P<nVisit>\d{3})-0x(?P<pfsVisitHash>[0-9a-f]{16})\.(fits|fits\.gz)$',
            ],
            dir_format = 'rerun/$rerun/pfsObject/{catId}/{tract}/{patch}',
            filename_format = 'pfsObject-{catId}-{tract}-{patch}-{objId}-{nVisit}-0x{pfsVisitHash}.fits',
            load = lambda identity, filename, dir:
                PfsObject.read(identity.__dict__, dirName=dir),
        ),
    }
)