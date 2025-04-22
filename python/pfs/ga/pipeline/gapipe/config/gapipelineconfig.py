from types import SimpleNamespace

from ...common import PipelineConfig
from .gatargetconfig import GATargetConfig
from .rvfitconfig import RVFitConfig
from .chemfitconfig import ChemfitConfig
from .coaddconfig import CoaddConfig

class GAPipelineConfig(PipelineConfig):
    """
    Galactic Archeology Spectrum Processing Pipeline configuration.
    """

    def __init__(self,
                 rerun: str = None,
                 target: GATargetConfig = GATargetConfig(),
                 rvfit: RVFitConfig = RVFitConfig(),
                 coadd: CoaddConfig = CoaddConfig(),
                 chemfit: ChemfitConfig = ChemfitConfig()):
        
        self.workdir = self._get_env('GAPIPE_WORKDIR')        # Working directory
        self.datadir = self._get_env('GAPIPE_DATADIR')        # PFS survey data directory root
        self.rerundir = self._get_env('GAPIPE_RERUNDIR')      # Path to rerun data, absolute or relative to `datadir`
        self.outdir = self._get_env('GAPIPE_OUTDIR')          # Pipeline output directory
        self.rerun = self._get_env('GAPIPE_RERUN')            # Rerun name, used in file names

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
        
        self.rvfit = rvfit
        self.run_rvfit = True

        self.coadd = coadd
        self.run_coadd = True

        self.chemfit = chemfit
        self.run_chemfit = False
        
        super().__init__()

    def enumerate_visits(self):
        """
        Enumerate the visits in the configs and return an identity for each.
        """

        for i, visit in enumerate(sorted(self.target.observations.visit)):
            identity = SimpleNamespace(**self.target.get_identity(visit))
            yield i, visit, identity