import os

import pfs.datamodel
from pfs.datamodel import *

from ...common import Pipeline, PipelineError, PipelineStep, PipelineStepResults

from ...setup_logger import logger

class ValidateStep(PipelineStep):
    def __init__(self, name=None):
        super().__init__(name)

    def run(self, context):
        self.__validate_config(context)
        self.__validate_libs(context)

        return PipelineStepResults(success=True, skip_remaining=False, skip_substeps=False)

    def __validate_config(self, context):
        """
        Validates the configuration and the existence of all necessary input data. Returns
        `True` if the pipeline can proceed or 'False' if it cannot.

        Return
        -------
        :bool:
            `True` if the pipeline can proceed or 'False' if it cannot.
        """

        # TODO: this should go to the init step

        # Verify output and log directories
        context.pipeline.test_dir('output', context.config.outdir, must_exist=False)
        context.pipeline.test_dir('work', context.config.workdir, must_exist=False)
        context.pipeline.test_dir('log', context.pipeline.get_product_logdir(), must_exist=False)
        context.pipeline.test_dir('figure', context.pipeline.get_product_figdir(), must_exist=False)
        context.pipeline.test_dir('data',
                                  context.repo.get_resolved_variable('datadir'))
        context.pipeline.test_dir('rerun',
                                  os.path.join(
                                      context.repo.get_resolved_variable('datadir'),
                                      context.repo.get_resolved_variable('rerundir')))
        
        return True
    
    def __validate_libs(self, context):
        # TODO: write code to validate library versions and log git hash for each when available

        # TODO: this should go to the init step

        pass