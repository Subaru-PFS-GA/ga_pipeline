import os
from pfs.ga.pfsspec.core import Trace

GAPIPE_ROOT = os.environ['GAPIPE_ROOT']

config = dict(
    tempfit = dict(
        fit_arms = [ 'b', 'm' ],
        model_grid_path = f'{GAPIPE_ROOT}/data/pfsspec/models/stellar/grid/phoenix/phoenix_HiRes/spectra.h5',
        model_grid_resolution = {
            'b': 500000,
            'm': 500000,
            'r': 500000,
        },
    ),
    coadd = dict(
        coadd_arms = [ 'b', 'm' ],
    )
)
