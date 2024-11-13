import os

import pfs.datamodel
from pfs.datamodel import *

from ...common import Pipeline, PipelineError, PipelineStep, PipelineStepResults
from ..config import GAPipelineConfig

from ...setup_logger import logger

class ChemFitStep(PipelineStep):
    def __init__(self, name=None):
        super().__init__(name)

    def run(self, context):
        if not context.config.run_chemfit:
            logger.info('Chemical abundance fitting is disabled, skipping...')
            return PipelineStepResults(success=True, skip_remaining=False, skip_substeps=True)
        
        # TODO: run abundance fitting
        raise NotImplementedError()