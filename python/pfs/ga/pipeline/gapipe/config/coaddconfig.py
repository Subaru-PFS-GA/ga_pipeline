from ...common.config import Config

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

        # Additional flags to be defined on coadded spectrum.
        self.extra_mask_flags = {
            20: 'NO_CONTINUUM'
        }
        
        # Flags to exclude from coadding
        self.mask_flags_exclude = [ 'NO_DATA', 'NO_CONTINUUM', 'BAD', 'SAT', 'CR', 'INTRP', 'BAD_FLAT', 'SUSPECT' ]
        
        # Flags to set on coadded spectrum
        self.mask_flag_no_data = 'NO_DATA'              # No data in pixel for any of the exposures
        self.mask_flag_no_continuum = 'NO_CONTINUUM'    # No continuum model is available in pixel

        self.stacker_args = {
            'binning': 'lin',
            'binsize': 0.5,                             # Angstroms
            'normalize_cont': True,
            'apply_flux_corr': True,
        }

        self.trace_args = {
            'plot_level': 1,
            'plot_input': None,
            'plot_stack': None,
            'plot_merged': None,
        }

        super().__init__()