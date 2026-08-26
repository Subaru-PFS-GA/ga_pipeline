import os

from pfs.ga.common.config import Config

try:
    import chemfit
    from chemfit.util import load_settings
except ImportError:
    chemfit = None
    load_settings = None

class ChemfitConfig(Config):
    """
    Configuration class for the chemfit step of the pipeline.
    """

    def __init__(self):

        # ChemFit global parameters
        # These override some of the default values in the original chemfit config
        # But we define them here to match the TempFit configuration

        # Fit the coadded spectrum, or the individual exposures
        self.fit_coadd = True

        # List of arms to attempt to fit
        self.fit_arms = [ 'b', 'm' ]

        # Fit the continuum-normalized spectrum
        self.normalize_continuum = False

        # Require all arms to run fit
        self.require_all_arms = True

        # Template grid path, str or dict, use {arm} for wildcard
        self.model_grid_path = None
        
        # Model grid resolution, float or dict, use {arm} for wildcard
        self.model_grid_resolution = None

        # Extra arguments to model grid, such as parameter limits
        self.model_grid_args = None
        
        # Memory map model grid files (only on supported file systems)
        # Only works with uncompressed HDF5 files, falls back to lazy-loading if mmap fails
        self.model_grid_mmap = True
        
        # Preload model grids into memory (requires large memory, not worth it)
        self.model_grid_preload = False
        
        # Only fit if there's enough non-masked pixels, otherwise skip
        self.min_unmasked_pixels = 3000  

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

        # Additional chemfit settings
        # These are merged into the default settings defined in
        # ./settings/roaming and ./settings/local
        if chemfit is not None:
            script_dir = os.path.abspath(os.path.join(os.path.dirname(chemfit.__file__), '../..'))
            self.settings = load_settings(script_dir, 'default', 'gridfit', 'localfit', 'PFS')
        else:
            self.settings = {}

        super().__init__()