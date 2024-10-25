import os
from unittest import TestCase

from pfs.ga.pipeline.gapipe import GAPipeline, GAPipelineTrace
from pfs.ga.pipeline.gapipe.config import GAPipelineConfig
from tests.pipeline.gapipe.config.configs import *

class TestGAPipeline(TestCase):
    def get_test_config(self):
        config = GAPipelineConfig()
        config.load(TEST_CONFIG_EDR2_90006)

        workdir = os.path.expandvars(os.path.join('${GAPIPE_WORKDIR}', f'{config.target.objId:016x}'))

        config.workdir = workdir
        config.logdir = os.path.join(workdir, 'log')
        config.figdir = os.path.join(workdir, 'fig')
        config.outdir = workdir

        config.rvfit.model_grid_path = os.path.expandvars('${GAPIPE_SYNTH_GRID}')
        config.rvfit.psf_path = os.path.expandvars('${GAPIPE_ARM_PSF}')
           
        return config
    
    def create_test_pipeline(self, config):
        trace = GAPipelineTrace(config.figdir)
        pipeline = GAPipeline(config, trace)
        return pipeline

    def test_validate_config(self):
        config = self.get_test_config()
        pipeline = self.create_test_pipeline(config)
        pipeline.validate_config()

    def test_start_stop_logging(self):
        config = self.get_test_config()
        pipeline = self.create_test_pipeline(config)

        pipeline._Pipeline__start_logging()
        pipeline._Pipeline__stop_logging()

        self.assertTrue(os.path.isfile(pipeline._Pipeline__logfile))
        os.remove(pipeline._Pipeline__logfile)

    def test_start_stop_tracing(self):
        config = self.get_test_config()
        pipeline = self.create_test_pipeline(config)

        pipeline._Pipeline__start_tracing()
        pipeline._Pipeline__stop_tracing()

    def test_step_load(self):
        config = self.get_test_config()
        pipeline = self.create_test_pipeline(config)
        pipeline._Pipeline__start_logging()
        
        pipeline._GA1DPipeline__step_load()
        
        pipeline._Pipeline__stop_logging()

        self.assertEqual(2, len(pipeline._GA1DPipeline__pfsSingle))

    def test_step_validate(self):
        config = self.get_test_config()
        pipeline = self.create_test_pipeline(config)
        pipeline._Pipeline__start_logging()
        
        pipeline._GA1DPipeline__step_load()
        pipeline._GA1DPipeline__step_validate()
        
        pipeline._Pipeline__stop_logging()

        self.assertEqual(2, len(pipeline._GA1DPipeline__pfsSingle))

    def test_step_vcorr(self):
        config = self.get_test_config()
        pipeline = self.create_test_pipeline(config)
        pipeline._Pipeline__start_logging()
        
        pipeline._GA1DPipeline__step_load()
        pipeline._GA1DPipeline__step_validate()
        pipeline._GA1DPipeline__step_vcorr()
        
        pipeline._Pipeline__stop_logging()

        self.assertEqual(2, len(pipeline._GA1DPipeline__pfsSingle))

    def test_step_rvfit(self):
        config = self.get_test_config()
        pipeline = self.create_test_pipeline(config)
        pipeline._Pipeline__start_logging()
        
        pipeline._GA1DPipeline__step_load()
        pipeline._GA1DPipeline__step_validate()
        pipeline._GA1DPipeline__step_vcorr()
        pipeline._GA1DPipeline__step_rvfit()
        
        pipeline._Pipeline__stop_logging()

        self.assertEqual(2, len(pipeline._GA1DPipeline__pfsSingle))

    def test_step_coadd(self):
        config = self.get_test_config()
        pipeline = self.create_test_pipeline(config)
        pipeline._Pipeline__start_logging()
        
        pipeline._GA1DPipeline__step_load()
        pipeline._GA1DPipeline__step_validate()
        pipeline._GA1DPipeline__step_vcorr()
        pipeline._GA1DPipeline__step_rvfit()
        pipeline._GA1DPipeline__step_coadd()
        
        pipeline._Pipeline__stop_logging()

        self.assertEqual(2, len(pipeline._GA1DPipeline__pfsSingle))

    def test_step_rvfit_save(self):
        config = self.get_test_config()
        pipeline = self.create_test_pipeline(config)
        pipeline._Pipeline__start_logging()
        
        pipeline._GA1DPipeline__step_load()
        pipeline._GA1DPipeline__step_validate()
        pipeline._GA1DPipeline__step_rvfit()
        pipeline._GA1DPipeline__step_coadd()
        pipeline._GA1DPipeline__step_save()
        
        pipeline._Pipeline__stop_logging()

        self.assertEqual(2, len(pipeline._GA1DPipeline__pfsSingle))