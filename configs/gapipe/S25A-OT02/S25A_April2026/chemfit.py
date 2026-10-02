import os
from pfs.ga.pfsspec.core import Trace
from pfs.ga.pfsspec.core import Physics

GAPIPE_ROOT = os.environ['GAPIPE_ROOT']
ARMS = [ 'b', 'm' ]

config = dict(
    run_chemfit = True,
    chemfit = dict(
        fit_arms = ARMS,
        settings = dict(
            griddir = None,
            filter_dir = None,
            mag_systems = None,
            default_mag_system = None,
            default_reddening = None,
            masks = {
                # In this rest frame, we use the "defective" mask that removes parts of the spectrum where the models do not
                # match the spectra of Arcturus and the Sun well
                'rest': [[100, 4000], [4006, 4012], [4065, 4075], [4093, 4110], [4140, 4165], [4170, 4180], [4205, 4220],
                        [4285, 4300], [4335, 4345], [4375, 4387], [4700, 4715], [4775, 4790], [4855, 4865], [5055, 5065],
                        [5145, 5160], [5203, 5213], [5885, 5900], [6355, 6365], [6555, 6570], [7175, 7195], [7890, 7900],
                        [8320, 8330], [8490, 8505], [8530, 8555], [8650, 8672]],

                # In the lab frame, we use the "aggressive" telluric mask that attempts to completely remove all regions affected
                # by telluric absoprption
                'lab': [[6270, 6330], [6860, 6970], [7150, 7400], [7590, 7715], [8100, 8380], [8915, 9910], [10730, 12300], [12450, 12900]],
            },
            arms = {
                'b': {
                    'FWHM': 2.07,
                    'wl': None,
                },
                'r': {
                    'FWHM': 2.63,
                    'wl': None,
                },
                'm': {
                    'FWHM': 1.368,
                    'wl': None,
                },
                'n': {
                    'FWHM': 2.4,
                    'wl': None,
                }
            },
            grid_filename = None,
            scratch = f"{GAPIPE_ROOT}/tmp/chemfit_scratch",
        ),
    )
)
