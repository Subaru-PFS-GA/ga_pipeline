from .pipelineconfig import PipelineConfig
from .gatargetconfig import GATargetConfig
from .rvfitconfig import RVFitConfig
from .chemfitconfig import ChemfitConfig
from .coaddconfig import CoaddConfig

class GA1DPipelineConfig(PipelineConfig):
    """
    Galactic Archeology Spectrum Processing Pipeline configuration.
    """

    # TODO: Maybe split it into ObjectConfig, RVFitConfig, ChemFitConfig

    def __init__(self):
        # Path to rerun data, absolute or relative to `datadir`
        self.rerundir = self._get_env('GAPIPE_RERUNDIR')

        # GA target object configuration
        self.target = GATargetConfig()

        # GA pipeline configuration

        # Arm definitions with defaults
        self.arms = { 
            'b': dict(
                wave = [ 3800, 6500 ],
                pix_per_res = 3,
            ),
            'r': dict(
                wave = [ 6300, 9700 ],
                pix_per_res = 3,
            ),
            'm': dict(
                wave = [ 7100, 8850 ],
                pix_per_res = 4,
            ),
            'n': dict(
                wave = [ 9400, 12600 ],
                pix_per_res = 3,
            ),
        }  

        self.ref_mag = 'hsc_g'                  # Reference magnitude

        # Type of velocity corrections, 'barycentric' or 'heliocentric' or 'none'
        self.v_corr = 'barycentric'

        # Flags to treat as masked pixel, combined with logical or.
        self.mask_flags = [
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
        
        self.rvfit = RVFitConfig()
        self.run_rvfit = True

        self.coadd = CoaddConfig()
        self.run_coadd = True

        self.chemfit = ChemfitConfig()
        self.run_chemfit = False
        
        super().__init__()

    def _load_impl(self, config=None, ignore_collisions=False):
        self._load_config_from_dict(config=config,
                                    type_map={ 
                                        'object': GATargetConfig,
                                        'rvfit': RVFitConfig,
                                        'coadd': CoaddConfig,
                                        'chemfit': ChemfitConfig,
                                    },
                                    ignore_collisions=ignore_collisions)
        