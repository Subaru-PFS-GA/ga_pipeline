import os
from unittest import TestCase

from pfs.ga.pipeline.gapipe.config import *
from .configs import *

class TestGAPipelineConfig(TestCase):
    def test_init(self):
        config = GAPipelineConfig()

    def test_save(self):
        config = GAPipelineConfig()
        config.save('./tmp/test/pfsStar.yaml')
        config.save('./tmp/test/pfsStar.json')

    def test_load(self):
        config = GAPipelineConfig()
        config.load('./data/test/pfsStar.yaml', ignore_collisions=True)

        self.assertIsInstance(config.target, GATargetConfig)
        self.assertIsInstance(config.target.identity, GAObjectIdentityConfig)
        self.assertIsInstance(config.target.observations, GAObjectObservationsConfig)
        self.assertIsInstance(config.tempfit, TempFitConfig)
        self.assertIsInstance(config.coadd, CoaddConfig)
        self.assertIsInstance(config.chemfit, ChemfitConfig)