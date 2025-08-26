from types import SimpleNamespace

class GAPipelineState(SimpleNamespace):
    def __init__(
        self,
        coadd_arms = None,
        coadd_stacker = None,
        coadd_merger = None,
        coadd_spectra = None,
        coadd_results = None,
        **kwargs
    ):
        super().__init__(**kwargs)
        
        self.coadd_arms = coadd_arms
        self.coadd_stacker = coadd_stacker
        self.coadd_merger = coadd_merger
        self.coadd_spectra = coadd_spectra
        self.coadd_results = coadd_results

    def copy(self):
        return GAPipelineState(**self.__dict__)
