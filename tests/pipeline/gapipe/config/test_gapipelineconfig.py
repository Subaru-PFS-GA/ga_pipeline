import os
from unittest import TestCase

from pfs.ga.pipeline.gapipe.config import *
from .configs import *

class TestPipeline(TestCase):
    def test_init(self):
        config = GAPipelineConfig()

    def test_save(self):
        config = GAPipelineConfig()
        config.save('./tmp/test/pfsGAConfig.yaml')
        config.save('./tmp/test/pfsGAConfig.json')

    def test_load(self):
        config = GAPipelineConfig()
        config.load('./data/test/pfsGAObject.yaml', ignore_collisions=True)

        self.assertIsInstance(config.target, GATargetConfig)
        self.assertIsInstance(config.target.identity, GAObjectIdentityConfig)
        self.assertIsInstance(config.target.observations, GAObjectObservationsConfig)
        self.assertIsInstance(config.rvfit, RVFitConfig)
        self.assertIsInstance(config.coadd, CoaddConfig)
        self.assertIsInstance(config.chemfit, ChemfitConfig)