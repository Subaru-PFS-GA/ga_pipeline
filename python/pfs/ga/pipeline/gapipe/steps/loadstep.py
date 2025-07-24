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
                data = context.pipeline.get_product_from_cache(product, visit, identity)
                if issubclass(product, PfsFiberArray):
                    # Make sure that targets are the same
                    if target is not None and (target != data.target):
                        raise PipelineError(f'Target information in PfsSingle files do not match.')
                elif issubclass(product, PfsFiberArraySet):
                    pass
                elif issubclass(product, PfsTargetSpectra):
                    pass
                elif issubclass(product, PfsDesign):
                    pass
                else:
                    raise NotImplementedError('Product type not recognized.')

                self.__validate_product(context, product, visit, data)

        # TODO: Count spectra per arm and write report to log

        return PipelineStepResults(success=True, skip_remaining=False, skip_substeps=False)
    
    def __validate_product(self, context, product, visit, data):       
        identity = context.pipeline.get_product_identity(data)

        if issubclass(product, PfsFiberArray):
            # Verify that it is a single visit and not a co-add
            if data.nVisit != 1:
                raise PipelineError(f'More than one visit found in `{product.__name__}` for `{identity}`.')
            
            # Verify that visit numbers match
            if visit not in data.observations.visit:
                raise PipelineError(f'Visit does not match visit ID found in `{product.__name__}` for `{identity}`.')
            
            if data.target.catId != context.config.target.identity.catId:
                raise PipelineError(f'catId in config `{context.config.target.catId}` does not match catID in `{product.__name__}` for `{identity}`.')

            if data.target.objId != context.config.target.identity.objId:
                raise PipelineError(f'objId in config `{context.config.target.objId}` does not match objID in `{product.__name__}` for `{identity}`.')
        elif issubclass(product, PfsFiberArraySet):
            if visit != data.identity.visit:
                raise PipelineError(f'Visit does not match visit ID found in `{product.__name__}` for `{identity}`.')
        elif issubclass(product, PfsTargetSpectra):
            # Verify that visit numbers match
            if visit != data[list(data.keys())[0]].observations.visit[0]:
                raise PipelineError(f'Visit does not match visit ID found in `{product.__name__}` for `{identity}`.')
        elif issubclass(product, PfsDesign):
            if issubclass(product, PfsConfig):
                # Verify that visit numbers match
                if visit != data.visit:
                    raise PipelineError(f'Visit does not match visit ID found in `{product.__name__}` for `{identity}`.')
                
            if context.config.target.identity.catId not in data.catId:
                raise PipelineError(f'catId in config `{context.config.target.identity.catId}` does not match catID in `{product.__name__}` for `{identity}`.')
            
            if context.config.target.identity.objId not in data.objId:
                raise PipelineError(f'objId in config `{context.config.target.identity.objId}` does not match objID in `{product.__name__}` for `{identity}`.')
        else:
            raise NotImplementedError('Product type not recognized.')
        
        # TODO: compare flags and throw a warning if bits are not the same in every file

        # TODO: write log message
    
    #endregion