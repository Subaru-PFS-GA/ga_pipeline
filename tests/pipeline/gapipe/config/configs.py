TEST_CONFIG_RUN17_10015 = dict(
    rerundir = 'rerun/run17',
    target = dict(
        identity = dict(
            catId = 10015,
            tract = 1,
            patch = '1,1',
            objId = 3,
            nVisit = 2,
            pfsVisitHash = 0x40137cf8387c4130,
        ),
        observations = dict(
            visit = [ 111009, 111010, ],
            arm = [ 'bmn', 'bmn' ],
            spectrograph = [ 3, 3 ],
            pfsDesignId = [ 0x6d832ca291636984, 0x6d832ca291636984 ],
            fiberId = [ 1777, 1777 ],
            fiberStatus = [ 1, 1 ],
            pfiNominal = [ [ -81.89517211914062, -76.48998260498047 ], [ -81.89517211914062, -76.48998260498047 ] ],
            pfiCenter = [ [ -81.89949035644531, -76.49072265625 ], [ -81.89949035644531, -76.49072265625 ] ],
            obsTime = [ '2024-05-29T00:00:00', '2024-05-29T00:15:00' ],
            expTime = [ 1800, 1800 ],
        )
    ),
    rvfit = dict(
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
        rvfit_args = dict(
            flux_corr_per_arm = True,
            flux_corr_per_exp = False,
            amplitude_per_arm = True,
            amplitude_per_exp = True,
        ),
        trace_args = dict(
            plot_fit_spec = {
                'pfsGA-tempfit-best-400nm-{id}': dict(
                    wlim=[385, 405],
                    plot_spectrum=True, plot_processed_template=True,
                    plot_flux_err=True, plot_residual=False),
                'pfsGA-tempfit-best-Ca-{id}': dict(
                    wlim=[849, 870],
                    plot_spectrum=True, plot_processed_template=True,
                    plot_flux_err=True, plot_residual=False),
            }
        ),
    ),
    coadd = dict(
        coadd_arms = [ 'b', 'm']
    )
)