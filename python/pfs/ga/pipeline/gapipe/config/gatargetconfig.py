from collections.abc import Iterable

from pfs.datamodel.utils import calculatePfsVisitHash, wraparoundNVisit

from ...common.config import Config
from .gaobjectidentityconfig import GAObjectIdentityConfig
from .gaobjectobservationsconfig import GAObjectObservationsConfig

class GATargetConfig(Config):
    """
    Galactic Archeology pipeline target object configuration.
    """

    def __init__(self,
                 proposalId = None,
                 targetType = None,
                 identity: GAObjectIdentityConfig = GAObjectIdentityConfig(),
                 observations: GAObjectObservationsConfig = GAObjectObservationsConfig()):

        self.proposalId = proposalId
        self.targetType = targetType
        self.identity = identity
        self.observations = observations

        super().__init__()

    def get_identity(self, visit=None):
        """Return an identity dictionary similar to PFS datamodel."""

        identity = {}
        identity['catId'] = self.identity.catId
        identity['tract'] = self.identity.tract
        identity['patch'] = self.identity.patch
        identity['objId'] = self.identity.objId

        if visit is not None and not isinstance(visit, Iterable):
            identity['visit'] = visit
        elif visit is not None:
            identity['nVisit'] = wraparoundNVisit(len(self.observation.visit))
            identity['pfsVisitHash'] = calculatePfsVisitHash(self.observation.visit)
        else:
            identity['nVisit'] = wraparoundNVisit(len(visit))
            identity['pfsVisitHash'] = calculatePfsVisitHash(visit)

        return identity