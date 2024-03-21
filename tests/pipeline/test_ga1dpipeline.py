import os
from unittest import TestCase

from pfs.ga.pipeline import GA1DPipeline
from pfs.ga.pipeline.config import GA1DPipelineConfig
from pipeline.config.configs import *

class TestPipeline(TestCase):
    def get_test_config(self):
        config = GA1DPipelineConfig(config=TEST_CONFIG_EDR2_90006)

        config.workdir = os.path.expandvars('${PFS_GA_TEMP}')
        config.datadir = os.path.expandvars('${PFS_GA_DATA}')
        config.rerundir = os.path.expandvars('${PFS_GA_RERUN}')
        config.logdir = os.path.expandvars('${PFS_GA_TEMP}/log')
        config.figdir = os.path.expandvars('${PFS_GA_TEMP}')
        config.outdir = os.path.expandvars('${PFS_GA_TEMP}')

        config.rvfit.model_grid_path = os.path.expandvars('${PFS_GA_SYNTH_GRID}')
        config.rvfit.psf_path = os.path.expandvars('${PFS_GA_ARM_PSF}')
           
        return config
    
    def get_test_pipeline(self, config):
        pipeline = GA1DPipeline(config)
        return pipeline

    def test_validate_config(self):
        config = self.get_test_config()
        pipeline = self.get_test_pipeline(config)
        pipeline.validate_config()

    def test_start_stop_logging(self):
        config = self.get_test_config()
        pipeline = self.get_test_pipeline(config)

        pipeline._Pipeline__start_logging()
        pipeline._Pipeline__stop_logging()

        self.assertTrue(os.path.isfile(pipeline._Pipeline__logfile))
        os.remove(pipeline._Pipeline__logfile)

    def test_start_stop_tracing(self):
        config = self.get_test_config()
        pipeline = self.get_test_pipeline(config)

        pipeline._Pipeline__start_tracing()
        pipeline._Pipeline__stop_tracing()

    def test_step_load(self):
        config = self.get_test_config()
        pipeline = self.get_test_pipeline(config)
        pipeline._Pipeline__start_logging()
        
        pipeline._GA1DPipeline__step_load()
        
        pipeline._Pipeline__stop_logging()

        self.assertEqual(2, len(pipeline._GA1DPipeline__pfsConfig))
        self.assertEqual(2, len(pipeline._GA1DPipeline__pfsSingle))

    def test_step_validate(self):
        config = self.get_test_config()
        pipeline = self.get_test_pipeline(config)
        pipeline._Pipeline__start_logging()
        
        pipeline._GA1DPipeline__step_load()
        pipeline._GA1DPipeline__step_validate()
        
        pipeline._Pipeline__stop_logging()

        self.assertEqual(2, len(pipeline._GA1DPipeline__pfsConfig))
        self.assertEqual(2, len(pipeline._GA1DPipeline__pfsSingle))

    def test_step_rvfit(self):
        config = self.get_test_config()
        pipeline = self.get_test_pipeline(config)
        pipeline._Pipeline__start_logging()
        
        pipeline._GA1DPipeline__step_load()
        pipeline._GA1DPipeline__step_validate()
        pipeline._GA1DPipeline__step_rvfit()
        
        pipeline._Pipeline__stop_logging()

        self.assertEqual(2, len(pipeline._GA1DPipeline__pfsConfig))
        self.assertEqual(2, len(pipeline._GA1DPipeline__pfsSingle))