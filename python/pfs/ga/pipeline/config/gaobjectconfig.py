from .config import Config
from .visitconfig import VisitConfig

class GAObjectConfig(Config):
    """
    Galactic Archeology pipeline target object configuration.
    """

    def __init__(self, config=None):
        self.objId = None
        self.catId = None
        self.tract = None
        self.patch = None
        self.visits = None

        super().__init__(config=config)

    def _load_impl(self, config):
        self._load_config_from_dict(config=config,
                                    type_map={ 'visits': VisitConfig })
