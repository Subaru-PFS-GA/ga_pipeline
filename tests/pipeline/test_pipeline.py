from unittest import TestCase

from pfs.ga.pipeline import Config, Pipeline

class TestPipeline(TestCase):
    def test_verify(self):
        config = Config()
        pipeline = Pipeline(config)
        pipeline.verify()