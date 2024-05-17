from .config import Config

class RVFitConfig(Config):
    def __init__(self, config=None):

        # RVFIT global parameters
        
        self.fit_arms = [ 'b', 'm', 'n' ]
        self.require_all_arms = True              # Require all arms to run fit
        self.model_grid_path = None               # Template grid path, use {arm} for wildcard
        self.model_grid_args = None               # Extra arguments to model grid
        self.model_grid_mmap = True               # Memory map model grid files (only on supported file systems)
        self.model_grid_preload = False           # Preload model grid into memory (requires large memory)
        self.psf_path = None                      # Line spread function path, use {arm} for wildcard
        self.wave_resampler = "interp"
        self.min_unmasked_pixels = 3000           # Only fit if there's enough non-masked pixels

        # RVFIT parameter priors - arguments to pass to ModelGridRVFit
        self.rvfit_args = {
            "M_H": [ -2.5, 0.0 ],
            "M_H_dist": [ "normal", -1.5, 0.5 ],
            "T_eff": [ 4000, 6000 ],
            "T_eff_dist": [ "normal", 5500, 50 ],
            "log_g": [ 1.5, 5.5 ],
            "log_g_dist": [ "normal", 3.5, 1.0 ],
            "a_M": 0.0,
            "rv": [-300, 300],

            "flux_corr": True,
            "flux_corr_deg": 10,
        }

        # RVFIT trace args - these control plotting, etc.
        # TODO: change these to False by default
        self.trace_args = {
            'plot_priors': True,
            'plot_rv_guess': True,
            'plot_rv_fit': True,
            'plot_input_spec': True,
            'plot_fit_spec': {
                # Default plots of RVFit results
                'pfsGA-RVFit-best-{id}': dict(
                    plot_spectrum=True, plot_processed_template=True,
                    plot_flux_err=True, plot_residuals=False),
                'pfsGA-RVFit-residuals-{id}': dict(
                    plot_spectrum=False, plot_processed_template=False,
                    plot_flux_err=False, plot_residuals=True),
            },
            'plot_spec_flux_err': True,
            'plot_spec_mask': True,
            'plot_spec_cont': True,
            'plot_params_priors': True,
            'plot_params_cov': True
        }

        super().__init__(config=config)