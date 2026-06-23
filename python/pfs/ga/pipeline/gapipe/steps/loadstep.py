import os

from pfs.ga.pfsspec.survey.pfs.datamodel import *

from ...common import Pipeline, PipelineError, PipelineStep, PipelineStepResults
from ..config import GAPipelineConfig

from ...setup_logger import logger

class LoadStep(PipelineStep):
    def __init__(self, name=None):
        super().__init__(name)

    #region Load

    def run(self, context):
        """
        Load the input data necessary for the pipeline. This does not include
        the spectrum grid, etc.

        The function only loads the raw data products that are required for the pipeline.
        The actual spectra will be extracted later.
        """

        # Load required data products that aren't already in the cache       
        for product in context.state.required_product_types:
            context.pipeline.load_input_products(product, arms=context.config.tempfit.fit_arms)

        # TODO: load photometry / prior files

        # TODO: add trace hook?

        return PipelineStepResults(success=True, skip_remaining=False, skip_substeps=False)

    #endregion
    #region Validate
    
    def validate(self, context):
        # Extract info from pfsSingle objects one by one and perform
        # some validation steps

        for product in context.state.required_product_types:
            context.pipeline.validate_input_products(product, arms=context.config.tempfit.fit_arms)

        return PipelineStepResults(success=True, skip_remaining=False, skip_substeps=False)
    
    #endregion