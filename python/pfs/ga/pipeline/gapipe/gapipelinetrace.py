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
                 plot_level=Trace.PLOT_LEVEL_INFO, 
                 log_level=Trace.LOG_LEVEL_INFO,
                 id=None):
        
        self.__id = id                          # Identity represented as string
        
        self.plot_exposures = None              # Plot the input spectra, each exposure separately
        self.plot_exposures_spec = {}
        self.plot_tempfit = None                  # Plot the best fit template and residuals after TempFit, each exposure separately
        self.plot_tempfit_spec = {}
        self.plot_coadd = None                  # Plot the best fit template and residuals after Coadd
        self.plot_coadd_spec = {}

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
        PipelineTrace.init_from_args(self, script, None, args)
        SpectrumTrace.init_from_args(self, script, None, args)

        self.plot_exposures = get_arg('plot_exposures', self.plot_exposures, args)
        self.plot_exposures_spec = get_arg('plot_exposures_spec', self.plot_exposures_spec, args)
        self.plot_tempfit = get_arg('plot_tempfit', self.plot_tempfit, args)
        self.plot_tempfit_spec = get_arg('plot_tempfit_spec', self.plot_tempfit_spec, args)
        self.plot_coadd = get_arg('plot_coadd', self.plot_coadd, args)
        self.plot_coadd_spec = get_arg('plot_coadd_spec', self.plot_coadd_spec, args)

    def on_load(self, spectra):
        """Fired when the individual exposures are read from the pfsSingle files."""

        if self.plot_exposures is None and self.plot_level >= Trace.PLOT_LEVEL_DEBUG \
            or self.plot_exposures:

            for key, config in self.plot_exposures_spec.items():
                self._plot_spectra(key, spectra, **config)
                self.flush_figures()
            
    def on_tempfit_finish_fit(self, spectra):
        # Plot rv_fit and rv_guess and the likelihood function
        if self.plot_tempfit is None and self.plot_level >= Trace.PLOT_LEVEL_DEBUG \
            or self.plot_tempfit:

            # Plot the final results based on the configuration settings
            for key, config in self.plot_tempfit_spec.items():
                self._plot_spectra(key, spectra, **config)
                self.flush_figures()

    def on_coadd_finish_coadd(self, spectra):
        """Fired when the coadding of spectra is finished."""

        # Plot rv_fit and rv_guess and the likelihood function
        if self.plot_coadd is None and self.plot_level >= Trace.PLOT_LEVEL_INFO \
            or self.plot_coadd:

            # Plot the final results based on the configuration settings
            for key, config in self.plot_coadd_spec.items():
                self._plot_spectra(key, spectra, **config)
                self.flush_figures()