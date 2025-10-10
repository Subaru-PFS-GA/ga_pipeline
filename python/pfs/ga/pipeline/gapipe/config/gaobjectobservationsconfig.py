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
                pfiNominal = None,
                pfiCenter = None,
                obsTime = None,
                expTime = None,
                seeing = None):

        self.visit = visit
        self.arms = arms
        self.spectrograph = spectrograph
        self.pfsDesignId = pfsDesignId
        self.fiberId = fiberId
        self.fiberStatus = fiberStatus
        self.pfiNominal = pfiNominal
        self.pfiCenter = pfiCenter
        self.obsTime = obsTime
        self.expTime = expTime
        self.seeing = seeing

        super().__init__()