from .config import Config

class VisitConfig(Config):
    """
    Galactic Archeology pipeline target visit configuration.

    Parameters
    ----------
    date: str
        Date of the observation.
    pfsDesignId: int
        PFS design identifier.
    fiberId: int
        Fiber identifier.
    """

    def __init__(self):
        self.date = None
        self.pfsDesignId = None
        self.fiberId = None

        super().__init__()