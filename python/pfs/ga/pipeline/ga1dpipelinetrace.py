import numpy as np

from pfs.ga.pfsspec.core.plotting import SpectrumPlot, styles
from pfs.ga.pfsspec.core.util.args import *
from pfs.ga.pfsspec.core import Trace
from .pipelinetrace import PipelineTrace

class GA1DPipelineTrace(PipelineTrace):
    def __init__(self, figdir='.', logdir='.',
                 plot_inline=False, 
                 plot_level=Trace.PLOT_LEVEL_NONE, 
                 log_level=Trace.LOG_LEVEL_NONE):
        
        super().__init__(figdir=figdir, logdir=logdir,
                         plot_inline=plot_inline, 
                         plot_level=plot_level,
                         log_level=log_level)
        
        self.plot_flux_correction = True

        self.reset()

    def reset(self):
        pass    

    def add_args(self, config, parser):
        super().add_args(config, parser)
    
    def init_from_args(self, script, config, args):
        super().init_from_args(script, config, args)

        self.plot_flux_correction = get_arg('plot_flux_correction', self.plot_flux_correction, args)

    def on_rvfit_load_spectra(self, spectra):
        """Fired when the individual exposures are read from the pfsSingle files."""

        pass

    def on_coadd_get_templates(self, spectra, templates):
        """Fired when the templates at the best fit parameters are loaded."""
        
        pass

    def on_coadd_eval_flux_corr(self, spectra, templates, flux_corr, spec_norm, temp_norm):
        """Fired when the flux correction is evaluated."""

        if self.plot_flux_correction or self.plot_level >= Trace.PLOT_LEVEL_DEBUG:
            f = self.get_diagram_page('pfsGA-Coadd-fluxcorr-{id}', 1, 1, 1, diagram_size=(6.5, 3.5))

            p = SpectrumPlot()
            ax = f.add_diagram((0, 0, 0), p)

            p.plot_mask = False
            p.plot_flux_err = False
            p.plot_cont = False

            for arm, specs in spectra.items():
                for i, spec in enumerate(specs):
                    # s = styles.lightgray_line(**styles.thin_line())
                    p.plot_spectrum(spec, flux_corr=flux_corr[arm][i], auto_limits=True)

            f.match_limits()

            self.flush_figures()

        