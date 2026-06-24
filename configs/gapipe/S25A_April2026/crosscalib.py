import os
from pfs.ga.pfsspec.core import Trace

GAPIPE_ROOT = os.environ['GAPIPE_ROOT']
ARMS = [ 'b', 'm' ]

config = dict(
    trace_args = dict(
        plot_coadd_spec = {
            # Additional plots of RVFit results
            'pfsGA-coadd-best-700nm-{id}': dict(
                wlim=[700, 800],
                plot_flux=True, plot_model=True,
                normalize_cont=True),
            'pfsGA-coadd-best-800nm-{id}': dict(
                wlim=[800, 900],
                plot_flux=True, plot_model=True,
                normalize_cont=True),
        },
    ),    
    tempfit = dict(
        fit_photometry = False,        
        psf_path = f'{GAPIPE_ROOT}/data/instruments/pfs/psf/import/{{arm}}.real/gauss.h5',
        mask_flags = [
            'BAD',
            # 'BAD_FIBERTRACE',
            # 'BAD_FLAT',
            # 'BAD_FLUXCAL',
            # 'BAD_SKY',
            'CR',
            # 'DETECTED',
            # 'DETECTED_NEGATIVE',
            # 'EDGE',
            # 'FIBERTRACE',
            # 'INTRP',
            # 'IPC',
            # 'NO_DATA',
            # 'REFLINE',
            'SAT',
            # 'SUSPECT',
            'UNMASKEDNAN'
        ],
        required_products = [ 'PfsConfig', 'PfsArm' ],

        # RV fitting wavelength ranges
        wave_include = [
            [7000, 7150],
            [7325, 7580],
            [7700, 8100],
            [8480, 8900],
        ],

        # In case of unfluxed spectra, exclude strong absorption bands
        # wave_exclude = [[7100.0, 7350.0],
        #                 [7555.0, 7750.0],
        #                 [8100.0, 8400.0]],

        correction_model = 'fluxcorr',
        correction_model_args = dict(
            flux_corr_per_arm = True,
            flux_corr_per_exp = False,

            flux_corr_degree = 10,
        ),

        extinction_model = None,

        tempfit_args = dict(
            amplitude_per_arm = True,
            amplitude_per_exp = True,

            M_H = [ -1.5, 0.0 ],
            M_H_dist = [ "normal", -0.0, 1.0 ],
            T_eff = [ 5500, 7000 ],
            T_eff_dist = [ "normal", 5000, 350 ],
            
            log_g = [ 0.0, 5.0 ],
            log_g_dist = [ "normal", 4.5, 2.5 ],

            rv = [ -250, 250 ],
            rv_step = 20,

            ebv = 0,
            # ebv_dist = [ "normal", 0.01, 0.01 ],

            # Roman's grid
            a_M = 0.0,
            C = 0.0,
            
            # a_M = [ -1.2, 0.8 ],
            # a_M_dist = [ "normal", 0.0, 0.5 ],
            # C = [ -0.2, 0.2 ],

            # PHOENIX grid
            # a_M = 0.0,
        ),
    ),
)
