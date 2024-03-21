from .config import Config

class VisitConfig(Config):
    """
    Galactic Archeology pipeline target visit configuration.
    """
    def __init__(self, config=None):
        self.date = None
        self.pfsDesignId = None
        self.fiberId = None

        super().__init__(config=config)