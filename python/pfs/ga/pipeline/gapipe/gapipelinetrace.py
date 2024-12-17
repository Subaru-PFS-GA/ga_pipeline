import numpy as np

from pfs.ga.pfsspec.core.plotting import SpectrumPlot, styles
from pfs.ga.pfsspec.core.util.args import *
from pfs.ga.pfsspec.core import Trace, SpectrumTrace

from ..common import PipelineTrace

class GAPipelineTrace(PipelineTrace, SpectrumTrace):
    def __init__(self,
                 figdir='.',
                 logdir='.',
                 plot_inline=False, 
                 plot_level=Trace.PLOT_LEVEL_NONE, 
                 log_level=Trace.LOG_LEVEL_NONE,
                 id=None):
        
        self.__id = id                          # Identity represented as string
        
        self.plot_exposures = True
        self.plot_flux_correction = True

        super().__init__(figdir=figdir, logdir=logdir,
                         plot_inline=plot_inline, 
                         plot_level=plot_level,
                         log_level=log_level)

    def reset(self):
        super().reset()

    def update(self, figdir=None, logdir=None, id=None):
        super().update(figdir=figdir, logdir=logdir)
        
        self.__id = id if id is not None else self.__id

    #region Properties

    def __get_id(self):
        return self.__id
    
    def __set_id(self, value):
        self.__id = value

    id = property(__get_id, __set_id)

    #endregion

    def add_args(self, parser):
        super().add_args(parser)
    
    def init_from_args(self, script, args):
        SpectrumTrace.init_from_args(self, script, None, args)

        self.plot_flux_correction = get_arg('plot_flux_correction', self.plot_flux_correction, args)

    def on_load(self, spectra):
        """Fired when the individual exposures are read from the pfsSingle files."""

        if self.plot_exposures:
            self._plot_spectra('pfsGA-exposures-{id}',
                               spectra,
                               plot_flux=True, plot_flux_err=True, plot_mask=True,
                               title='Input spectra - {id}',
                               nrows=2, ncols=1, diagram_size=(6.5, 3.5))
            
            self.flush_figures()

    def on_coadd_get_templates(self, spectra, templates):
        """Fired when the templates at the best fit parameters are loaded."""
        
        pass

    def on_coadd_eval_correction(self, spectra, templates, corrections, correction_masks, spec_norm, temp_norm):
        """Fired when the flux correction is evaluated."""

        if self.plot_flux_correction or self.plot_level >= Trace.PLOT_LEVEL_DEBUG:
            f = self.get_diagram_page('pfsGA-Coadd-fluxcorr-{id}', 1, 1, 1, diagram_size=(6.5, 3.5))

            p = SpectrumPlot()
            ax = f.add_diagram((0, 0, 0), p)

            p.plot_mask = False
            p.plot_flux_err = False
            p.plot_cont = False

            # TODO: update this to allow for continumm fitting and flux correction

            for arm, specs in spectra.items():
                for i, spec in enumerate(specs):
                    if spec is not None:
                        s = styles.lightgray_line(**styles.thin_line())
                        p.plot_spectrum(spec, auto_limits=True)

            f.match_limits()

            self.flush_figures()

        