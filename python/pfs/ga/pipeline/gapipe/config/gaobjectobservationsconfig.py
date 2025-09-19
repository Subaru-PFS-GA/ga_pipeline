from pfs.ga.common.config import Config

class GAObjectObservationsConfig(Config):
    """
    Galactic Archeology pipeline object observations configuration.
    """

    def __init__(self,
                visit = None,
                arms = None,
                spectrograph = None,
                pfsDesignId = None,
                fiberId = None,
                fiberStatus = None,
                obstime = None,
                exptime = None,
                seeing = None):

        self.visit = visit
        self.arms = arms
        self.spectrograph = spectrograph
        self.pfsDesignId = pfsDesignId
        self.fiberId = fiberId
        self.fiberStatus = fiberStatus
        self.obstime = obstime
        self.exptime = exptime
        self.seeing = seeing

        super().__init__()