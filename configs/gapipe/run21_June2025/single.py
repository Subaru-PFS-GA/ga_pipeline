from pfs.ga.pfsspec.core import Trace

config = dict(
    workdir = '/datascope/subaru/user/dobos/gapipe/work',
    outdir = '/datascope/subaru/user/dobos/gapipe/out',
    ignore_missing_files = True,
    trace_args = dict(
        plot_level = Trace.PLOT_LEVEL_INFO,
        plot_tempfit_spec = {
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
        },
        plot_coadd_spec = {
            # Additional plots of Coadd results
            'pfsGA-coadd-best-full-{id}': dict(
                plot_flux=True, plot_model=True,
                normalize_cont=True),
            'pfsGA-coadd-best-400nm-{id}': dict(
                wlim=[385, 405],
                plot_flux=True, plot_model=True,
                normalize_cont=True),
            'pfsGA-coadd-best-Ca-{id}': dict(
                wlim=[849, 870],
                plot_flux=True, plot_model=True,
                normalize_cont=True),
        }
    ),
    tempfit = dict(
        fit_arms = [ 'b', 'r', 'm' ],
        require_all_arms = False,
        
        # model_grid_path = '/datascope/subaru/data/pfsspec/models/stellar/grid/phoenix/phoenix_HiRes/spectra.h5',
        model_grid_path = '/datascope/subaru/data/pfsspec/models/stellar/grid/gk2025/gk2025_binned_compressed/spectra.h5',
        model_grid_mmap = False,
        model_grid_preload = False,
        # model_grid_path = {
        #     'b': '/datascope/subaru/data/pfsspec/models/stellar/grid/roman/gridie/spectra.h5',
        #     'r': '/datascope/subaru/data/pfsspec/models/stellar/grid/roman/grid7/spectra.h5',
        #     'm': '/datascope/subaru/data/pfsspec/models/stellar/grid/roman/grid7/spectra.h5',
        # },

        fit_photometry = True,
        photometry = {
            'g_ps1': dict(
                instrument = 'ps1',
                filter_name = 'g_ps1',
                filter_path = '/datascope/subaru/data/instruments/ps1/filters/PAN-STARRS_PS1.g.dat',
            ),
            'r_ps1': dict(
                instrument = 'ps1',
                filter_name = 'r_ps1',
                filter_path = '/datascope/subaru/data/instruments/ps1/filters/PAN-STARRS_PS1.r.dat',
            ),
            'i_ps1': dict(
                instrument = 'ps1',
                filter_name = 'i_ps1',
                filter_path = '/datascope/subaru/data/instruments/ps1/filters/PAN-STARRS_PS1.i.dat',
            ),
            'g_hsc': dict(
                instrument = 'hsc',
                filter_name = 'g_hsc',
                filter_path = '/datascope/subaru/data/instruments/hsc/filters/HSC-g.txt',
            ),
            'i_hsc': dict(
                instrument = 'hsc',
                filter_name = ['i_hsc', 'i_old_hsc'],
                filter_path = '/datascope/subaru/data/instruments/hsc/filters/HSC-i.txt',
            ),
            'i2_hsc': dict(
                instrument = 'hsc',
                filter_name = ['i2_hsc'],
                filter_path = '/datascope/subaru/data/instruments/hsc/filters/HSC-i2.txt',
            ),
        },
        
        psf_path = '/datascope/subaru/data/instruments/pfs/psf/import/{arm}.real/gauss.h5',
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

        extinction_model = 'default',
        extinction_model_args = dict(
            ext_type = 'ccm89',
            R_V = 3.1,
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

        tempfit_args = dict(
            amplitude_per_arm = True,
            amplitude_per_exp = True,

            M_H = [ -5.0, 0.5 ],
            M_H_dist = [ "normal", -1.5, 0.5 ],
            T_eff = [ 3200, 6500 ],
            T_eff_dist = [ "normal", 5500, 500 ],
            log_g = [ 1.5, 5.5 ],
            log_g_dist = [ "normal", 3.5, 1.0 ],

            ebv = [0.0, 0.05],

            # Roman's grid
            # a_M = 0.0,
            C = 0.0,
            
            a_M = [ -1.2, 0.8 ],
            a_M_dist = [ "normal", 0.0, 0.5 ],
            # C = [ -0.2, 0.2 ],

            # PHOENIX grid
            # a_M = 0.0,
        ),
        trace_args = dict(
            plot_level = Trace.PLOT_LEVEL_INFO,
        )
    ),
    coadd = dict(
        coadd_arms = [ 'b', 'm', 'r' ],
        trace_args = dict(
            plot_level = Trace.PLOT_LEVEL_INFO,
        ),
    )
)
