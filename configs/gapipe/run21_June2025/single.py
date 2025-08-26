from pfs.ga.pfsspec.core import Trace

config = dict(
    workdir = '/datascope/subaru/user/dobos/gapipe/work',
    outdir = '/datascope/subaru/user/dobos/gapipe/out',
    ignore_missing_files = True,
    rvfit = dict(
        fit_arms = [ 'b', 'r', 'm' ],
        require_all_arms = False,
        
        model_grid_path = '/datascope/subaru/data/pfsspec/models/stellar/grid/phoenix/phoenix_HiRes/spectra.h5',
        # model_grid_path = {
        #     'b': '/datascope/subaru/data/pfsspec/models/stellar/grid/roman/gridie/spectra.h5',
        #     'r': '/datascope/subaru/data/pfsspec/models/stellar/grid/roman/grid7/spectra.h5',
        #     'm': '/datascope/subaru/data/pfsspec/models/stellar/grid/roman/grid7/spectra.h5',
        # },
        
        psf_path = '/datascope/subaru/data/pfsspec/subaru/pfs/psf/import/{arm}.real/gauss.h5',
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
        required_products = [ 'PfsConfig', 'PfsSingle' ],

        # RV fitting wavelength ranges
        wave_include = None,

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

        # correction_model = 'contnorm',
        # correction_model_args = dict(
        #     cont_norm = True,
        #     cont_per_arm = True,
        #     cont_per_exp = True,

        #     # Continuum finder wavelength ranges
        #     cont_wave_include = [[3500., 5164.322],
        #                          [5170.322, 5892.924],
        #                          [5898.924, 8488.023],
        #                          [8508.023, 8525.091],
        #                          [8561.091, 8645.141],
        #                          [8679.141, 9100.]],
        #     cont_wave_exclude = [],
        # ),

        rvfit_args = dict(
            amplitude_per_arm = True,
            amplitude_per_exp = True,

            # a_M = 0.5,    # Roman's grid
            a_M = 0.0,      # PHOENIX grid
        ),
        trace_args = dict(
            plot_level = Trace.PLOT_LEVEL_INFO,
            plot_fit_spec = {
                # Additional plots of RVFit results
                'pfsGA-tempfit-best-full-{id}': dict(
                    plot_flux=True, plot_model=True,
                    normalize_cont=True),
                'pfsGA-tempfit-best-400nm-{id}': dict(
                    wlim=[385, 405],
                    plot_flux=True, plot_model=True,
                    normalize_cont=True),
                'pfsGA-tempfit-best-Ca-{id}': dict(
                    wlim=[849, 870],
                    plot_flux=True, plot_model=True,
                    normalize_cont=True),
            }
        )
    ),
    coadd = dict(
        coadd_arms = [ 'b', 'm', 'r' ]
    )
)
