TEST_CONFIG_EDR2_90006 = dict(
    rerundir = 'edr2-20231203',
    object = dict(
        catId = 90006,
        tract = 1,
        patch = '1,1',
        objId = 36072,
        visits = {
            97821: dict(
                date = '2023-07-24',
                pfsDesignId = 0x66f04f565a40eca5,
                fiberId = 640
            ),
            98408: dict(
                date = '2023-07-26',
                pfsDesignId = 0x25d33a63c51002d5,
                fiberId = 640
            )
        },
    ),
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
    rvfit = dict(
        fit_arms = [ 'b', 'm', 'n' ],
        rvfit_args = dict(
            flux_corr_per_arm = True,
            flux_corr_per_exp = False,
            amplitude_per_arm = True,
            amplitude_per_exp = True,
        ),
        trace_args = dict(
            plot_fit_spec = {
                'rvfit_best_400nm': dict(
                    wlim=[385, 405],
                    plot_spectra=True, plot_processed_templates=True,
                    plot_flux_err=True, plot_residuals=False),
                'rvfit_best_ca': dict(
                    wlim=[849, 870],
                    plot_spectra=True, plot_processed_templates=True,
                    plot_flux_err=True, plot_residuals=False),
            }
        ),
    ),
    coadd = dict(
        coadd_arms = [ 'b', 'm']
    )
)