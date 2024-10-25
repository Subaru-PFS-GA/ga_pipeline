from ...common.config import Config

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

    def __str__(self):
        if self.nVisit is not None and self.pfsVisitHash is not None:
            return f'{self.catId:05d}-{self.tract:05d}-{self.patch}-{self.objId:016x}-{self.nVisit:03d}-0x{self.pfsVisitHash:016x}'