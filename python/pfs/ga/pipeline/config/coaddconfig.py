from .config import Config

class CoaddConfig(Config):
    """
    Configuration class for the coadd step of the pipeline.
    
    Parameters
    ----------
    coadd_arms: list of str
        List of arm names to calculate the coadded spectra for.
    stacker_args: dict
        Arguments for the stacker algorithm. See pfs.ga.pipeline.stack.Stacker for details.
    trace_args: dict
        Trace parameters for the coadd step.
    """
    def __init__(self):

        self.coadd_arms = [ 'b', 'm', 'n' ]
        self.no_data_flag = 'NO_DATA'

        self.stacker_args = {
            'binning': 'lin',
            'binsize': 0.5,                     # Angstroms
        }

        self.trace_args = {
            
        }

        super().__init__()