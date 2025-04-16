import os

import pfs.datamodel
from pfs.datamodel import *

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
        for t in context.pipeline.required_product_types:
            context.pipeline.load_input_products(t)

        # TODO: load photometry / prior files

        # TODO: add trace hook?

        return PipelineStepResults(success=True, skip_remaining=False, skip_substeps=False)

    #endregion
    #region Validate
    
    def validate(self, context):
        # Extract info from pfsSingle objects one by one and perform
        # some validation steps

        target = None

        for i, visit, identity in context.config.enumerate_visits():
            for product in context.pipeline.required_product_types:
                if issubclass(product, PfsFiberArray):
                    data = context.pipeline.product_cache[product][visit][identity.objId]

                    # Make sure that targets are the same
                    if target is None:
                        target = data.target
                    elif not target == data.target:
                        raise PipelineError(f'Target information in PfsSingle files do not match.')

                elif issubclass(product, PfsFiberArraySet):
                    data = context.pipeline.product_cache[product][visit]
                elif issubclass(product, PfsDesign):
                    data = context.pipeline.product_cache[product][visit]
                else:
                    raise NotImplementedError('Product type not recognized.')

                self.__validate_product(context, product, visit, data)

        # TODO: Count spectra per arm and write report to log

        return PipelineStepResults(success=True, skip_remaining=False, skip_substeps=False)
    
    def __validate_product(self, context, product, visit, data):       
        identity = context.repo.get_identity(data)
        filename = context.repo.format_filename(type(data), identity=identity)

        if issubclass(product, PfsFiberArray):
            # Verify that it is a single visit and not a co-add
            if data.nVisit != 1:
                raise PipelineError('More than one visit found in `{pfsSingle.filename}`')
            
            # Verify that visit numbers match
            if visit not in data.observations.visit:
                raise PipelineError(f'Visit does not match visit ID found in `{filename}`.')
            
            if data.target.catId != context.config.target.identity.catId:
                raise PipelineError(f'catId in config `{context.config.target.catId}` does not match catID in `{filename}`.')

            if data.target.objId != context.config.target.identity.objId:
                raise PipelineError(f'objId in config `{context.config.target.objId}` does not match objID in `{filename}`.')
        elif issubclass(product, PfsFiberArraySet):
            if visit != data.identity.visit:
                raise PipelineError(f'Visit does not match visit ID found in `{filename}`.')
        elif issubclass(product, PfsDesign):
            if issubclass(product, PfsConfig):
                # Verify that visit numbers match
                if visit != data.visit:
                    raise PipelineError(f'Visit does not match visit ID found in `{filename}`.')
                
            if context.config.target.identity.catId not in data.catId:
                raise PipelineError(f'catId in config `{context.config.target.identity.catId}` does not match catID in `{filename}`.')
            
            if context.config.target.identity.objId not in data.objId:
                raise PipelineError(f'objId in config `{context.config.target.identity.objId}` does not match objID in `{filename}`.')
        else:
            raise NotImplementedError('Product type not recognized.')
        
        # TODO: compare flags and throw a warning if bits are not the same in every file

        # TODO: write log message
    
    #endregion