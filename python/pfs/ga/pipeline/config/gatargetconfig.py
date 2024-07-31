from pfs.datamodel.utils import calculatePfsVisitHash, wraparoundNVisit

from .config import Config
from .visitconfig import VisitConfig

class GATargetConfig(Config):
    """
    Galactic Archeology pipeline target object configuration.
    """

    def __init__(self):
        self.objId = None
        self.catId = None
        self.tract = None
        self.patch = None
        self.visits = None

        super().__init__()

    def _load_impl(self, config, ignore_collisions=False):
        self._load_config_from_dict(config=config,
                                    type_map={ 'visits': VisitConfig },
                                    ignore_collisions=ignore_collisions)

    def get_identity(self, visit=None):
        """Return an identity dictionary similar to PFS datamodel."""

        identity = {}
        identity['catId'] = self.catId
        identity['tract'] = self.tract
        identity['patch'] = self.patch
        identity['objId'] = self.objId

        if visit is not None:
            identity['visit'] = visit
        else:
            identity['nVisit'] = wraparoundNVisit(len(self.visits))
            identity['pfsVisitHash'] = calculatePfsVisitHash(self.visits.keys())

        return identity