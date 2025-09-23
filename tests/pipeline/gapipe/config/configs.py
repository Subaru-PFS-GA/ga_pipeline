TEST_CONFIG_RUN21_JUNE2025_10092 = dict(
    rerundir = 'rerun/run17',
    target = dict(
        identity = dict(
            catId = 10092,
            tract = 1,
            patch = '1,1',
            objId = 25769842441,
            nVisit = 2,
            pfsVisitHash = 6954995582404002557,
        ),
        observations = dict(
            visit = [ 123064, 123066, ],
            arm = [ 'bmn', 'bmn' ],
            spectrograph = [ 1, 1 ],
            pfsDesignId = [ 8643551219043481049, 8643551219043481049 ],
            fiberId = [ 116, 116 ],
            fiberStatus = [ 1, 1 ],
            # pfiNominal = [ [ -81.89517211914062, -76.48998260498047 ], [ -81.89517211914062, -76.48998260498047 ] ],
            # pfiCenter = [ [ -81.89949035644531, -76.49072265625 ], [ -81.89949035644531, -76.49072265625 ] ],
            obsTime = [ '2024-05-29T00:00:00', '2024-05-29T00:15:00' ],
            expTime = [ 1800, 1800 ],
        )
    ),
    tempfit = dict(
        model_grid_path = '/datascope/subaru/data/pfsspec/models/stellar/grid/phoenix/phoenix_HiRes/spectra.h5',
        psf_path = '/datascope/subaru/data/pfsspec/subaru/pfs/psf/import/{arm}.real/gauss.h5',    
        fit_arms = [ 'b', 'm', 'n' ],
        required_products = [ 'PfsSingle' ],
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
        correction_model = 'fluxcorr',
        correction_model_args = dict(
            flux_corr_per_arm = True,
            flux_corr_per_exp = False,
            flux_corr_degree = 10,
        ),
        tempfit_args = dict(
            amplitude_per_arm = True,
            amplitude_per_exp = True
        ),
        trace_args = dict(
            plot_fit_spec = {
                'pfsGA-tempfit-best-400nm-{id}': dict(
                    wlim=[385, 405],
                    plot_spectrum=True, plot_model=True,
                    plot_flux_err=True, plot_residual=False),
                'pfsGA-tempfit-best-Ca-{id}': dict(
                    wlim=[849, 870],
                    plot_spectrum=True, plot_model=True,
                    plot_flux_err=True, plot_residual=False),
            }
        ),
    ),
    coadd = dict(
        coadd_arms = [ 'b', 'm']
    )
)