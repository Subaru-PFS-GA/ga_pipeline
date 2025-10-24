import os
from unittest import TestCase

from pfs.datamodel import *
from pfs.ga.pfsspec.survey.pfs import PfsGen3Repo, PfsGen3ButlerConfig, PfsGen3FileSystemConfig
from pfs.ga.pfsspec.survey.repo import ButlerRepo, FileSystemRepo
from pfs.ga.pipeline.repo import GAPipeWorkdirConfig
from pfs.ga.pipeline.gapipe import GAPipeline, GAPipelineTrace
from pfs.ga.pipeline.gapipe.config import GAPipelineConfig
from pfs.ga.pipeline.gapipe.steps import *
from pipeline.gapipe.config.configs import *

class TestGAPipeline(TestCase):
    def get_test_config(self):
        config = GAPipelineConfig()
        config.load(TEST_CONFIG_RUN21_JUNE2025_10092, ignore_collisions=True)

        workdir = os.path.expandvars(os.path.join('./tmp/test/work', f'{config.target.identity.objId:016x}'))

        config.workdir = workdir
        config.logdir = os.path.join(workdir, 'log')
        config.figdir = os.path.join(workdir, 'fig')
        config.outdir = workdir
           
        return config
    
    def get_test_repo(self, config):
        # return PfsGen3FileSystemRepo(GAPipeWorkdirConfig)
        input_repo =  PfsGen3Repo(
            repo_type = ButlerRepo,
            config = PfsGen3ButlerConfig
        )

        work_repo = PfsGen3Repo(
            repo_type = FileSystemRepo,
            config = GAPipeWorkdirConfig
        )

        return input_repo, work_repo
        
    def create_test_pipeline(self, config, input_repo, work_repo):
        trace = GAPipelineTrace(config.figdir)
        pipeline = GAPipeline(config=config, trace=trace, input_repo=input_repo, work_repo=work_repo)
        return pipeline

    # TODO: these have been moved to the script from the pipeline
    #       modify tests accordingly

    # def test_start_stop_logging(self):
    #     config = self.get_test_config()
    #     input_repo, work_repo = self.get_test_repo(config)
    #     pipeline = self.create_test_pipeline(config, input_repo, work_repo)

    #     pipeline._Pipeline__start_logging()
    #     pipeline._Pipeline__stop_logging()

    #     self.assertTrue(os.path.isfile(pipeline._Pipeline__logfile))
    #     os.remove(pipeline._Pipeline__logfile)

    def test_start_stop_tracing(self):
        config = self.get_test_config()
        input_repo, work_repo = self.get_test_repo(config)
        pipeline = self.create_test_pipeline(config, input_repo, work_repo)

        pipeline._Pipeline__start_tracing()
        pipeline._Pipeline__stop_tracing()

    def test_steps(self):
        config = self.get_test_config()
        input_repo, work_repo = self.get_test_repo(config)
        pipeline = self.create_test_pipeline(config, input_repo, work_repo)
        pipeline.reset()

        pipeline._Pipeline__start_tracing()
        state = pipeline.create_state()
        context = pipeline.create_context(state=state, trace=pipeline._Pipeline__trace)

        ValidateStep().run(context)
        InitStep().run(context)
        LoadStep().run(context)
        LoadStep().validate(context)
        TempFitStep().init(context)
        TempFitStep().load(context)
        TempFitStep().validate_data(context)
        TempFitStep().preprocess(context)
        TempFitStep().guess(context)
        TempFitStep().run(context)
        TempFitStep().calculate_error(context)
        TempFitStep().calculate_covariance(context)
        TempFitStep().finish(context)
        TempFitStep().map_log_L(context)
        TempFitStep().cleanup(context)
        CoaddStep().init(context)
        CoaddStep().run(context)
        CoaddStep().cleanup(context)
        ChemFitStep().run(context)
        SaveStep().run(context)
        CleanupStep().run(context)

        pipeline._Pipeline__stop_tracing()

        self.assertEqual(2, len(pipeline.product_cache[PfsSingle]))
