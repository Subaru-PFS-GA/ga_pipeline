import os
from unittest import TestCase

from pfs.datamodel import *
from pfs.ga.pfsspec.survey.repo import FileSystemRepo as PfsFileSystemRepo
from pfs.ga.pipeline.repo import PfsFileSystemConfig
from pfs.ga.pipeline.gapipe import GAPipeline, GAPipelineTrace
from pfs.ga.pipeline.gapipe.config import GAPipelineConfig
from pfs.ga.pipeline.gapipe.steps import *
from tests.pipeline.gapipe.config.configs import *

class TestGAPipeline(TestCase):
    def get_test_config(self):
        config = GAPipelineConfig()
        config.load(TEST_CONFIG_RUN17_10015, ignore_collisions=True)

        workdir = os.path.expandvars(os.path.join('./tmp/test/work', f'{config.target.identity.objId:016x}'))

        config.workdir = workdir
        config.logdir = os.path.join(workdir, 'log')
        config.figdir = os.path.join(workdir, 'fig')
        config.outdir = workdir
           
        return config
    
    def get_test_repo(self, config):
        return PfsFileSystemRepo(PfsFileSystemConfig)
        
    def create_test_pipeline(self, config, repo):
        trace = GAPipelineTrace(config.figdir)
        pipeline = GAPipeline(config=config, trace=trace, repo=repo)
        return pipeline

    def test_validate_config(self):
        config = self.get_test_config()
        repo = self.get_test_repo(config)
        pipeline = self.create_test_pipeline(config, repo)
        pipeline.validate_config()

    # TODO: these have been moved to the script from the pipeline
    #       modify tests accordingly

    # def test_start_stop_logging(self):
    #     config = self.get_test_config()
    #     repo = self.get_test_repo(config)
    #     pipeline = self.create_test_pipeline(config, repo)

    #     pipeline._Pipeline__start_logging()
    #     pipeline._Pipeline__stop_logging()

    #     self.assertTrue(os.path.isfile(pipeline._Pipeline__logfile))
    #     os.remove(pipeline._Pipeline__logfile)

    def test_start_stop_tracing(self):
        config = self.get_test_config()
        repo = self.get_test_repo(config)
        pipeline = self.create_test_pipeline(config, repo)

        pipeline._Pipeline__start_tracing()
        pipeline._Pipeline__stop_tracing()

    def test_step_init(self):
        config = self.get_test_config()
        repo = self.get_test_repo(config)
        pipeline = self.create_test_pipeline(config, repo)
        pipeline.reset()

        pipeline._Pipeline__start_tracing()
        context = pipeline.create_context(trace=pipeline._Pipeline__trace)

        InitStep().run(context)

        pipeline._Pipeline__stop_tracing()

    def test_step_load(self):
        config = self.get_test_config()
        repo = self.get_test_repo(config)
        pipeline = self.create_test_pipeline(config, repo)
        pipeline.reset()

        pipeline._Pipeline__start_tracing()
        context = pipeline.create_context(trace=pipeline._Pipeline__trace)

        InitStep().run(context)
        LoadStep().run(context)
        LoadStep().validate(context)

        pipeline._Pipeline__stop_tracing()

        self.assertEqual(2, len(pipeline.product_cache[PfsSingle]))

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