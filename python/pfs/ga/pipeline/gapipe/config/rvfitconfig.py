from ...common.config import Config

class RVFitConfig(Config):
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
    """

    def __init__(self):

        # RVFIT global parameters
        
        self.fit_arms = [ 'b', 'm', 'n' ]
        self.require_all_arms = True              # Require all arms to run fit
        self.map_log_L = False                    # Map log likelihood over the model grid
        self.model_grid_path = None               # Template grid path, str or dict, use {arm} for wildcard
        self.model_grid_args = None               # Extra arguments to model grid
        self.model_grid_mmap = True               # Memory map model grid files (only on supported file systems)
        self.model_grid_preload = False           # Preload model grid into memory (requires large memory)
        self.psf_path = None                      # Line spread function path, use {arm} for wildcard

        self.min_unmasked_pixels = 3000           # Only fit if there's enough non-masked pixels

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

        # Arguments to pass to ModelGridTempFit
        # This is where we can set the parameter priors, etc.
        self.rvfit_args = {
            'amplitude_per_arm': True,
            'amplitude_per_exp': True,

            "M_H": [ -2.5, 0.0 ],
            "M_H_dist": [ "normal", -1.5, 0.5 ],
            "T_eff": [ 4000, 6000 ],
            "T_eff_dist": [ "normal", 5500, 50 ],
            "log_g": [ 1.5, 5.5 ],
            "log_g_dist": [ "normal", 3.5, 1.0 ],
            "a_M": 0.0,
            
            "rv": [-400, 400],
            "rv_step": 20,

            "resampler": "interp",              # Resampler to use for resampling model grid
        }

        # RVFIT trace args - these control plotting, etc.
        self.trace_args = {
            'plot_fit_spec': {
                # Default plots of RVFit results
                'pfsGA-tempfit-best-full-{id}': dict(
                    plot_flux=True, plot_model=True,
                    normalize_cont=True),
                'pfsGA-tempfit-residuals-{id}': dict(
                    plot_residual=True,
                    normalize_cont=True),
            },
        }

        super().__init__()
