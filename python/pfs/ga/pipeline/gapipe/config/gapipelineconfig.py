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
                 target: GATargetConfig = GATargetConfig(),
                 rvfit: RVFitConfig = RVFitConfig(),
                 coadd: CoaddConfig = CoaddConfig(),
                 chemfit: ChemfitConfig = ChemfitConfig()):
        
        self.workdir = self._get_env('GAPIPE_WORKDIR')        # Working directory
        self.datadir = self._get_env('GAPIPE_DATADIR')        # PFS survey data directory root
        self.rerundir = self._get_env('GAPIPE_RERUNDIR')      # Path to rerun data, absolute or relative to `datadir`
        self.outdir = self._get_env('GAPIPE_OUTDIR')          # Pipeline output directory

        # GA target object configuration
        self.target = target

        # TODO: need to get the photometry and priors from somewhere

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