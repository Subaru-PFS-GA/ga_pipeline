from pfs.datamodel.utils import calculatePfsVisitHash, wraparoundNVisit

from .config import Config
from .gaobjectidentityconfig import GAObjectIdentityConfig
from .gaobjectobservationsconfig import GAObjectObservationsConfig

class GATargetConfig(Config):
    """
    Galactic Archeology pipeline target object configuration.
    """

    def __init__(self,
                 proposalId = None,
                 targetType = None,
                 identity = None,
                 observations = None):

        self.proposalId = proposalId
        self.targetType = targetType
        self.identity = identity
        self.observations = observations

        super().__init__()

    def _load_impl(self, config, ignore_collisions=False):
        self._load_config_from_dict(config=config,
                                    type_map={ 
                                        'identity': GAObjectIdentityConfig,
                                        'observations': GAObjectObservationsConfig
                                    },
                                    ignore_collisions=ignore_collisions)

    def get_identity(self, visit=None):
        """Return an identity dictionary similar to PFS datamodel."""

        # TODO Is it used anywhere? Delete if not.
        raise NotImplementedError()

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