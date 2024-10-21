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

    def __init__(self,
                 target: GATargetConfig = GATargetConfig(),
                 rvfit: RVFitConfig = RVFitConfig(),
                 coadd: CoaddConfig = CoaddConfig(),
                 chemfit: ChemfitConfig = ChemfitConfig()):
        
        self.workdir = self._get_env('GAPIPE_WORKDIR')        # Working directory
        self.datadir = self._get_env('GAPIPE_DATADIR')        # PFS survey data directory root
        self.rerundir = self._get_env('GAPIPE_RERUNDIR')      # Path to rerun data, absolute or relative to `datadir`
        self.outdir = None

        # GA target object configuration
        self.target = target

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
        
        self.rvfit = rvfit
        self.run_rvfit = True

        self.coadd = coadd
        self.run_coadd = True

        self.chemfit = chemfit
        self.run_chemfit = False
        
        super().__init__()

        