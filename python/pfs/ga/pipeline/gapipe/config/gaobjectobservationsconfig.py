from ...common.config import Config

class GAObjectObservationsConfig(Config):
    """
    Galactic Archeology pipeline object observations configuration.
    """

    def __init__(self,
                visit = None,
                arm = None,
                spectrograph = None,
                pfsDesignId = None,
                fiberId = None,
                fiberStatus = None,
                pfiNominal = None,
                pfiCenter = None,
                obsTime = None,
                expTime = None):

        self.visit = visit
        self.arm = arm
        self.spectrograph = spectrograph
        self.pfsDesignId = pfsDesignId
        self.fiberId = fiberId
        self.fiberStatus = fiberStatus
        self.pfiNominal = pfiNominal
        self.pfiCenter = pfiCenter
        self.obsTime = obsTime
        self.expTime = expTime

        super().__init__()