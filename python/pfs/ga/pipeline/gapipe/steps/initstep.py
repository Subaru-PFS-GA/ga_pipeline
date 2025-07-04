import os

from pfs.ga.pfsspec.core.obsmod.snr import SNR_TYPES

from ...common import Pipeline, PipelineError, PipelineStep, PipelineStepResults
from ..config import GAPipelineConfig

from ...setup_logger import logger

class InitStep(PipelineStep):
    def __init__(self, name=None):
        super().__init__(name)
    
    def run(self, context):

        # Create output directories, although these might already exists since
        # the log files are already being written
        context.pipeline.create_dir('output', context.pipeline.get_product_outdir())
        context.pipeline.create_dir('work', context.pipeline.get_product_workdir())
        context.pipeline.create_dir('log', context.pipeline.get_product_logdir())
        context.pipeline.create_dir('figure', context.pipeline.get_product_figdir())
        
        # Save the full configuration to the output directory, if it's not already there
        context.pipeline.save_output_product(
            context.config,
            identity = context.config.target.identity,
            create_dir = True,
            exist_ok = True
        )
        
        # Initialize the SNR calculator objects
        context.pipeline.snr = {}
        for arm in context.config.arms:
            snr = SNR_TYPES[context.config.arms[arm]['snr']['type']](**context.config.arms[arm]['snr']['args'])
            context.pipeline.snr[arm] = snr

        return PipelineStepResults(success=True, skip_remaining=False, skip_substeps=False)
    