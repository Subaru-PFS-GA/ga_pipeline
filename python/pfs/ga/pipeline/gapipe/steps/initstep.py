import os

import pfs.datamodel
from pfs.datamodel import *
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
        dir = context.pipeline.get_product_workdir()
        fn = context.repo.format_filename(GAPipelineConfig, context.config.target.identity)
        fn = os.path.join(dir, fn)
        if not os.path.isfile(fn):
            context.config.save(fn)
            logger.info(f'Runtime configuration file saved to `{fn}`.')

        # Initialize the SNR calculator objects
        context.pipeline.snr = {}
        for arm in context.config.arms:
            snr = SNR_TYPES[context.config.arms[arm]['snr']['type']](**context.config.arms[arm]['snr']['args'])
            context.pipeline.snr[arm] = snr

        # Verify stellar template grids and PSF files
        if context.config.run_rvfit:
            for arm in context.config.rvfit.fit_arms:
                # Verify that the synthetic spectrum grids are available
                if isinstance(context.config.rvfit.model_grid_path, dict):
                    fn = context.config.rvfit.model_grid_path[arm].format(arm=arm)
                else:
                    fn = context.config.rvfit.model_grid_path.format(arm=arm)
                
                if not os.path.isfile(fn):
                    raise FileNotFoundError(f'Synthetic spectrum grid `{fn}` not found.')
                else:
                    logger.info(f'Using synthetic spectrum grid `{fn}` for arm `{arm}`.')
                
                # Verify that the PSF files are available

                # TODO: update this when using observed PSF files

                if isinstance(context.config.rvfit.psf_path, dict):
                    fn = context.config.rvfit.psf_path[arm].format(arm=arm)
                elif context.config.rvfit.psf_path is not None:
                    fn = context.config.rvfit.psf_path.format(arm=arm)

                if context.config.rvfit.psf_path is not None:
                    if not os.path.isfile(fn):
                        raise FileNotFoundError(f'PSF file `{fn}` not found.')
                    else:
                        logger.info(f'Using PSF file `{fn}` for arm `{arm}`.')

        # TODO: verify chemfit template paths, factor out the two into their respective functions

        # Compile the list of required input data products. The data products
        # are identified by their type. The class definitions are located in pfs.datamodel
        context.pipeline.required_product_types = set()

        if context.config.run_rvfit:
            context.pipeline.required_product_types.update(
                [ getattr(pfs.datamodel, t) for t in context.config.rvfit.required_products ])
            
        if context.config.run_chemfit:
            context.pipeline.required_product_types.update(
                [ getattr(pfs.datamodel, t) for t in context.config.chemfit.required_products ])

        # Verify that input data files are available or the input products
        # are already in the cache
        for t in context.pipeline.required_product_types:
            self.__validate_input_product(context, t)

        # TODO: Verify photometry / prior files

        # TODO: add validation steps for CHEMFIT

        return PipelineStepResults(success=True, skip_remaining=False, skip_substeps=False)
    
    def __validate_input_product(self, context, product, required=True):

        # Check the availability of the required data products. They're either
        # already in the product cache on the pipeline level, or they must be
        # available in the data repository. We only identify the products here,
        # do no load them.

        for i, visit, identity in context.config.enumerate_visits():
            if context.pipeline.product_cache is not None and product in context.pipeline.product_cache:
                if issubclass(product, PfsFiberArray):
                    # Data product contains a single object
                    if visit in context.pipeline.product_cache[product]:
                        if identity.objId in context.pipeline.product_cache[product][visit]:
                            # Product is already in the cache, skip
                            continue
                elif issubclass(product, PfsFiberArraySet):
                    # Data product contains multiple objects
                    if visit in context.pipeline.product_cache[product]:
                        # Product is already in the cache, skip
                        continue
                else:
                    raise NotImplementedError('Product type not recognized.')
                
            # Product not found in cache of cache is empty, look up the file location
            try:
                context.repo.locate_product(product, **identity.__dict__)
            except FileNotFoundError:
                msg = f'{product.__name__} file for identity `{identity}` not available.'
                if required:
                    raise PipelineError(msg)
                else:
                    logger.warning(msg)