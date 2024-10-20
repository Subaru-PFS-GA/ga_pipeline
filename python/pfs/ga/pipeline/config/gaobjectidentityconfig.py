from .config import Config

class GAObjectIdentityConfig(Config):
    """
    Galactic Archeology pipeline object identity configuration.
    """

    def __init__(self,
                 catId = None,
                 tract = None,
                 patch = None,
                 objId = None):
        
        self.catId = catId
        self.tract = tract
        self.patch = patch
        self.objId = objId

        super().__init__()