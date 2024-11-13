import os

from ...common import PipelineError, PipelineStep, PipelineStepResults

from ...setup_logger import logger

class CleanupStep(PipelineStep):
    def __init__(self, name=None):
        super().__init__(name)

    def run(self, context):
        # TODO: Perform any cleanup
        return PipelineStepResults(success=True, skip_remaining=False, skip_substeps=False)
