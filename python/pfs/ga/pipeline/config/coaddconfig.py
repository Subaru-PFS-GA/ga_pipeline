from .config import Config

class CoaddConfig(Config):
    def __init__(self, config=None):

        self.stacker_args = {
            'binning': 'lin',
            'binsize': 0.5,         # Angstroms
        }

        self.trace_args = {
            
        }

        super().__init__(config=config)