from types import SimpleNamespace

from .repoconfig import RepoConfig
from .gatargetconfig import GATargetConfig
from .tempfitconfig import TempFitConfig
from .chemfitconfig import ChemfitConfig
from .coaddconfig import CoaddConfig

class GAPipelineConfig(RepoConfig):
    """
    Galactic Archeology Spectrum Processing Pipeline configuration.
    """

    def __init__(self,
                 target: GATargetConfig = GATargetConfig(),
                 tempfit: TempFitConfig = TempFitConfig(),
                 coadd: CoaddConfig = CoaddConfig(),
                 chemfit: ChemfitConfig = ChemfitConfig()):

        super().__init__()

        # GA target object configuration
        self.target = target

        # TODO: need to get the photometry and priors from somewhere

        # GA pipeline configuration

        # Arm definitions with defaults

        # These values are safe only when the M arm is used
        # self.arms = { 
        #     'b': dict(
        #         wave = [ 3500, 6600 ],
        #         pix_per_res = 3,
        #     ),
        #     'r': dict(
        #         wave = [ 6600, 9300 ],
        #         pix_per_res = 3,
        #     ),
        #     'm': dict(
        #         wave = [ 6900, 9000 ],
        #         pix_per_res = 4,
        #     ),
        #     'n': dict(
        #         wave = [ 9300, 12600 ],
        #         pix_per_res = 3,
        #     ),
        # }
        
        # These values ignore the overlap between arms when the R arm is used
        # TODO: convert these into ArmConfig classes?
        self.arms = { 
            'b': dict(
                wave = [ 3500, 6200 ],
                snr = dict(
                    type = 'quantile',
                    args = dict(
                        q = 0.95,
                        binning = 3,
                    ),
                    mask_flags = [
                        'BAD',
                        'BAD_FIBERTRACE',
                        'BAD_FLAT',
                        'BAD_FLUXCAL',
                        'BAD_SKY',
                        'CR',
                        'DETECTED',
                        'DETECTED_NEGATIVE',
                        'EDGE',
                        'FIBERTRACE',
                        'INTRP',
                        'IPC',
                        'NO_DATA',
                        'REFLINE',
                        'SAT',
                        'SUSPECT',
                        'UNMASKEDNAN'
                    ]
                ),
            ),
            'r': dict(
                wave = [ 6600, 9300 ],
                snr = dict(
                    type = 'quantile',
                    args = dict(
                        q = 0.95,
                        binning = 3,
                    ),
                    mask_flags = [
                        'BAD',
                        'BAD_FIBERTRACE',
                        'BAD_FLAT',
                        'BAD_FLUXCAL',
                        'BAD_SKY',
                        'CR',
                        'DETECTED',
                        'DETECTED_NEGATIVE',
                        'EDGE',
                        'FIBERTRACE',
                        'INTRP',
                        'IPC',
                        'NO_DATA',
                        'REFLINE',
                        'SAT',
                        'SUSPECT',
                        'UNMASKEDNAN'
                    ]
                )
            ),
            'm': dict(
                wave = [ 7050, 8850 ],
                snr = dict(
                    type = 'quantile',
                    args = dict(
                        q = 0.95,
                        binning = 4,
                    ),
                    mask_flags = [
                        'BAD',
                        'BAD_FIBERTRACE',
                        'BAD_FLAT',
                        'BAD_FLUXCAL',
                        'BAD_SKY',
                        'CR',
                        'DETECTED',
                        'DETECTED_NEGATIVE',
                        'EDGE',
                        'FIBERTRACE',
                        'INTRP',
                        'IPC',
                        'NO_DATA',
                        'REFLINE',
                        'SAT',
                        'SUSPECT',
                        'UNMASKEDNAN'
                    ]
                )
            ),
            'n': dict(
                wave = [ 9900, 12600 ],
                snr = dict(
                    type = 'quantile',
                    args = dict(
                        q = 0.95,
                        binning = 3,
                    ),
                    mask_flags = [
                        'BAD',
                        'BAD_FIBERTRACE',
                        'BAD_FLAT',
                        'BAD_FLUXCAL',
                        'BAD_SKY',
                        'CR',
                        'DETECTED',
                        'DETECTED_NEGATIVE',
                        'EDGE',
                        'FIBERTRACE',
                        'INTRP',
                        'IPC',
                        'NO_DATA',
                        'REFLINE',
                        'SAT',
                        'SUSPECT',
                        'UNMASKEDNAN'
                    ]
                )
            ),
        }

        # TODO
        self.ref_mag = 'hsc_g'                  # Reference magnitude

        # TODO
        # Type of velocity corrections, 'barycentric' or 'heliocentric' or 'none'
        self.v_corr = 'barycentric'
        
        self.tempfit = tempfit
        self.run_tempfit = True

        self.coadd = coadd
        self.run_coadd = True

        self.chemfit = chemfit
        self.run_chemfit = False

        self.trace_args = {
            'plot_exposures': None,
            'plot_exposures_spec': {
                'pfsGA-exposures-full-{id}': dict(
                    plot_flux=True,
                    plot_flux_err=True,
                    plot_mask=True,
                    print_snr=True,
                    normalize_cont=True),
            },
            'plot_tempfit': None,
            'plot_tempfit_spec': {
                # Default plots of TempFit results
                'pfsGA-tempfit-best-full-{id}': dict(
                    plot_flux=True, plot_model=True,
                    normalize_cont=True),
                'pfsGA-tempfit-residuals-{id}': dict(
                    plot_flux=False, plot_model=False,
                    plot_residual=True,
                    normalize_cont=True),
            },
            'plot_coadd': None,
            'plot_coadd_spec': {
                # Default plots of Coadd results
                'pfsGA-coadd-best-full-{id}': dict(
                    plot_flux=True, plot_model=True,
                    normalize_cont=True),
                'pfsGA-coadd-residuals-{id}': dict(
                    plot_flux=False, plot_model=False,
                    plot_residual=True,
                    normalize_cont=True),
            },
            **self.trace_args
        }

    def enumerate_visits(self):
        """
        Enumerate the visits in the configs and return an identity for each.
        """

        for i, visit in enumerate(sorted(self.target.observations.visit)):
            identity = SimpleNamespace(**self.target.get_identity(visit))
            yield i, visit, identity