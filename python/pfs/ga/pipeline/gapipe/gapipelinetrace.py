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
        
        self.plot_exposures = None

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

        self.plot_exposures = get_arg('plot_exposures', self.plot_exposures, args)

    def on_load(self, spectra):
        """Fired when the individual exposures are read from the pfsSingle files."""

        if self.plot_exposures is None and self.plot_level >= Trace.PLOT_LEVEL_DEBUG \
            or self.plot_exposures:

            self._plot_spectra('pfsGA-exposures-{id}',
                               spectra,
                               plot_flux=True, plot_flux_err=True, plot_mask=True,
                               print_snr=True,
                               title='Input spectra - {id}',
                               nrows=2, ncols=1, diagram_size=(6.5, 3.5))
            
            self.flush_figures()
