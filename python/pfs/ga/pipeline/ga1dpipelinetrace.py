import numpy as np

from pfs.ga.pfsspec.core.plotting import SpectrumPlot
from pfs.ga.pfsspec.stellar.rvfit import RVFitTrace
from .pipelinetrace import PipelineTrace

class GA1DPipelineTrace(PipelineTrace):
    

    def on_rvfit_load_spectra(self, spectra):
        """Fired when the individual exposures are read from the pfsSingle files."""

        pass

        