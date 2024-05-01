from pfs.ga.pfsspec.core import Trace

class PipelineTrace(Trace):
    """Base class for pipeline tracing"""

    def __get_figdir(self):
        return self.outdir
    
    def __set_figdir(self, value):
        self.outdir = value

    figdir = property(__get_figdir, __set_figdir)