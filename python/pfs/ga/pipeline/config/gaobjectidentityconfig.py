from .config import Config

class GAObjectIdentityConfig(Config):
    """
    Galactic Archeology pipeline object identity configuration.
    """

    def __init__(self,
                 catId = None,
                 tract = None,
                 patch = None,
                 objId = None,
                 nVisit = None,
                 pfsVisitHash = None):
        
        self.catId = catId
        self.tract = tract
        self.patch = patch
        self.objId = objId
        self.nVisit = nVisit
        self.pfsVisitHash = pfsVisitHash

        super().__init__()