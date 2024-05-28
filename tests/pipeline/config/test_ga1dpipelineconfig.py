import os
from unittest import TestCase

from pfs.ga.pipeline.config import GA1DPipelineConfig
from .configs import *

class TestPipeline(TestCase):
    def test_init(self):
        config = GA1DPipelineConfig()
        config = GA1DPipelineConfig(config=TEST_CONFIG_EDR2_90006)
