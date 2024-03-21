from .config import Config

class RVFitConfig(Config):
    def __init__(self, config=None):

        # RVFIT global parameters
        
        self.ref_mag = 'hsc_g'
        self.fit_arms = [ 'b', 'm' ]
        self.require_all_arms = True              # Require all arms to run fit
        self.model_grid_path = None               # Template grid path, use {arm} for wildcard
        self.model_grid_args = None               # Extra arguments to model grid
        self.model_grid_mmap = True               # Memory map model grid files (only on supported file systems)
        self.model_grid_preload = False           # Preload model grid into memory (requires large memory)
        self.psf_path = None                      # Line spread function path, use {arm} for wildcard
        self.wave_resampler = "interp"
        self.min_unmasked_pixels = 3000           # Only fit if there's enough non-masked pixels

        # Arm definitions with defaults
        self.arms = { 
            'b': dict(
                wave = [ 3800, 6500 ],
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

        # RVFIT parameter priors - arguments to pass to ModelGridRVFit
        self.rvfit_args = {
            "M_H": [ -2.5, -0.5 ],
            "M_H_dist": [ "normal", -1.5, 0.5 ],
            "T_eff": [ 5250, 5750 ],
            "T_eff_dist": [ "normal", 5500, 50 ],
            "log_g": [ 1.5, 5.5 ],
            "log_g_dist": [ "normal", 3.5, 1.0 ],
            "a_M": 0.0,
            "rv": [-300, 300],

            "flux_corr": True,
            "flux_corr_deg": 10,
        }

        super().__init__(config=config)