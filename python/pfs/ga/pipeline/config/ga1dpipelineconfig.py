from .pipelineconfig import PipelineConfig
from .gaobjectconfig import GAObjectConfig
from .rvfitconfig import RVFitConfig
from .chemfitconfig import ChemfitConfig
from .stackingconfig import StackingConfig

class GA1DPipelineConfig(PipelineConfig):
    """
    Galactic Archeology Spectrum Processing Pipeline configuration.
    """

    # TODO: Maybe split it into ObjectConfig, RVFitConfig, ChemFitConfig

    def __init__(self, config=None):
        # Path to rerun data, absolute or relative to `datadir`
        self.rerundir = None

        # GA target object configuration
        self.object = GAObjectConfig()

        # GA pipeline configuration
        self.load_pfsConfig = True          # Load and use info from pfsConfig files
        
        self.rvfit = RVFitConfig()
        self.run_rvfit = True

        self.chemfit = ChemfitConfig()
        self.run_chemfit = False

        self.stacking = StackingConfig()
        self.run_stacking = False
        
        super().__init__(config=config)

    def _load_impl(self, config=None):
        self._load_config_from_dict(config=config,
                                    type_map={ 
                                        'object': GAObjectConfig,
                                        'rvfit': RVFitConfig,
                                        'chemfit': ChemfitConfig,
                                        'stacking': StackingConfig
                                    })