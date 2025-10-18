from typing import Dict, List

from pfs.ga.common.config import Config

from .photometryconfig import PhotometryConfig

class TempFitConfig(Config):
    """
    Configuration class for the RV fit step of the pipeline

    Parameters
    ----------
    fit_arms: list of str
        List of arm names to fit the RV for.
    require_all_arms: bool
        Require all arms to run the fitting. If False, a subset of the arms can be used.
    model_grid_path: str
        Path to the model grid files. Use {arm} for wildcard.
    model_grid_args: dict
        Extra arguments to pass to the model grid. Use this to limit the parameter space.
    model_grid_mmap: bool
        Memory map the model grid files. Only works on supported file systems.
    model_grid_preload: bool
        Preload the model grid into memory. Requires large memory.
    psf_path: str
        Path to the line spread function files. Use {arm} for wildcard.
    min_unmasked_pixels: int
        Minimum number of unmasked pixels required to run the fitting.
    correction_model: str
        Correction model to use. Either 'fluxcorr' or 'contnorm'.
    extinction_model: str
        Extinction model to use. 'ccm89', 'fm07' etc.
    """

    def __init__(
            self,
            photometry: Dict[str, PhotometryConfig] = None,
        ):

        # TempFIT global parameters
        
        # List of arms to attempt to fit
        self.fit_arms = [ 'b', 'm', 'n' ]

        # Require all arms to run fit
        self.require_all_arms = True              
        
        # Map log likelihood over the model grid, expensive but useful for debugging
        self.map_log_L = False

        # Template grid path, str or dict, use {arm} for wildcard
        self.model_grid_path = None               
        
        # Extra arguments to model grid, such as parameter limits
        self.model_grid_args = None               
        
        # Memory map model grid files (only on supported file systems)
        # Only works with uncompressed HDF5 files, falls back to lazy-loading if mmap fails
        self.model_grid_mmap = True               
        
        # Preload model grids into memory (requires large memory, not worth it)
        self.model_grid_preload = False

        # Line spread function path, use {arm} for wildcard
        self.psf_path = None
        
        # Observed magnitudes to use in fluxed template fitting
        self.photometry = photometry
        
        # Fit photometry in addition to spectra
        self.fit_photometry = True

        # Broadband filter throughput cut-off to use when calculating synthetic magnitudes
        self.filter_cutoff = 1e-3

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

        self.required_products = [ ]
        self.correction_model = None
        self.correction_model_args = {}

        self.wave_include = None
        self.wave_exclude = None

        # Correction model to use, either 'fluxcorr' or 'contnorm'. Use 'fluxcorr' for
        # flux correcting the fluxed stellar templates and 'contnorm' to continuum-normalize the
        # observations when fitting with normalized templates.
        # self.required_products = [ 'PfsSingle' ]
        # self.correction_model = 'fluxcorr'
        # self.correction_model_args = {
        #     'flux_corr': True,
        #     'flux_corr_deg': 10,
        #     'flux_corr_per_arm': True,
        #     'flux_corr_per_exp': True,
        # }

        # TODO: Example configuration with `contnomr`, which currently has no arguments
        # self.required_products = [ 'PfsConfig', 'PfsMerged' ]
        # self.correction_model = 'contnorm'
        # self.correction_model_args = {
        #     'cont_per_arm': True,
        #     'cont_per_exp': True,
        # }

        self.extinction_model = 'default'
        self.extinction_model_args = {
            'R_V': 3.1,
        }

        # Arguments to pass to ModelGridTempFit
        # This is where we can set the parameter priors, etc.
        self.tempfit_args = {
            'amplitude_per_arm': True,
            'amplitude_per_exp': True,

            "M_H": [ -2.5, 0.0 ],
            "M_H_dist": [ "normal", -1.5, 0.5 ],
            "T_eff": [ 4000, 6000 ],
            "T_eff_dist": [ "normal", 5500, 50 ],
            "log_g": [ 1.5, 5.5 ],
            "log_g_dist": [ "normal", 3.5, 1.0 ],
            "a_M": 0.0,

            "ebv": [0.03, 0.06],
            "ebv_step": 0.01,
            
            "rv": [-400, 400],
            "rv_step": 20,

            "resampler": "interp",              # Resampler to use for resampling model grid
        }

        # TempFit trace args - these control plotting, etc.
        self.trace_args = { }

        super().__init__()
