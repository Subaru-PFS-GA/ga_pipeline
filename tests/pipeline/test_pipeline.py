import os
from unittest import TestCase

from pfs.ga.pipeline import Config, Pipeline

class TestPipeline(TestCase):
    def get_test_config(self):
        config = Config()
        config.workdir = os.path.expandvars('${PFS_GA_TEMP}')
        config.logdir = os.path.expandvars('${PFS_GA_TEMP}/log')
        config.datadir = os.path.expandvars('${PFS_GA_DATA_RERUN}')
        config.outdir = os.path.expandvars('${PFS_GA_TEMP}')
        config.figdir = os.path.expandvars('${PFS_GA_TEMP}')
        config.modelGridPath = os.path.expandvars('${PFS_GA_SYNTH_GRID}/phoenix/phoenix_HiRes_FGK/spectra.h5')

        config.objId = 1
        config.visit = [
            98765,
            98766,
            98767
        ]
        config.catId = {
            98765: 90003,
            98766: 90003,
            98767: 90003,
        }
        config.tract = {
            98765: 1,
            98766: 1,
            98767: 1,
        }
        config.patch = {
            98765: '1,1',
            98766: '1,1',
            98767: '1,1',
        }
        config.designId = {
            98765: 7884270544754596914,
            98766: 7884270544754596914,
            98767: 7884270544754596914,
        }
        config.date = {
            98765: '2024-01-03',
            98766: '2024-01-03',
            98767: '2024-01-03',
        }
        config.fiber = {
            98765: '2024-01-03',
            98766: '2024-01-03',
            98767: '2024-01-03',
        }

        return config
    
    def get_test_pipeline(self, config):
        pipeline = Pipeline(config)
        return pipeline

    def test_validate(self):
        config = self.get_test_config()
        pipeline = self.get_test_pipeline(config)
        pipeline.validate()

    def test_start_stop_logging(self):
        config = self.get_test_config()
        pipeline = self.get_test_pipeline(config)
        pipeline._Pipeline__start_logging()
        pipeline._Pipeline__stop_logging()

        self.assertTrue(os.path.isfile(pipeline._Pipeline__logfile))
        os.remove(pipeline._Pipeline__logfile)

    def test_set_load(self):
        config = self.get_test_config()
        pipeline = self.get_test_pipeline(config)
        pipeline._Pipeline__start_logging()
        
        pipeline._Pipeline__step_load()
        
        pipeline._Pipeline__stop_logging()

        self.assertEqual(3, len(pipeline._Pipeline__pfsConfig))
        self.assertEqual(3, len(pipeline._Pipeline__pfsSingle))