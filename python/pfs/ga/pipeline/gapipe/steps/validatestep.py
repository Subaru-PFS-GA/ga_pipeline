import os

import pfs.ga.pfsspec.survey.pfs.datamodel as datamodel
from pfs.ga.pfsspec.survey.pfs.datamodel import *

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

        # Verify input, output, log and figure directories
        self.__validate_output_directories(context)
        self.__validate_input_directories(context)

        # Verify that stellar template grids and PSF files exist
        self.__validate_tempfit_input_files(context)

        # TODO: Verify that the input files for chemfit exist
        self.__validate_chemfit_input_files(context)

        # Verify that all necessary data products are available
        self.__validate_input_data_products(context)

    def __validate_output_directories(self, context):
        context.pipeline.test_dir('output', context.config.outdir, must_exist=False)
        context.pipeline.test_dir('work', context.config.workdir, must_exist=False)
        context.pipeline.test_dir('log', context.pipeline.get_product_logdir(), must_exist=False)
        context.pipeline.test_dir('figure', context.pipeline.get_product_figdir(), must_exist=False)

    def __validate_input_directories(self, context):
        # If we're not using Butler, verify that the input repo is set up correctly
        # and the input directories exist.
        if context.input_repo.is_filesystem_repo:
            context.pipeline.test_dir('data', context.input_repo.get_resolved_variable('datadir'))
            context.pipeline.test_dir('rerun', os.path.join(
                context.repo.get_resolved_variable('datadir'),
                context.repo.get_resolved_variable('rerundir')))

    def __validate_tempfit_input_files(self, context):
        if context.config.run_tempfit:
            for arm in context.config.tempfit.fit_arms:
                # Verify that the synthetic spectrum grids are available
                if isinstance(context.config.tempfit.model_grid_path, dict):
                    fn = context.config.tempfit.model_grid_path[arm].format(arm=arm)
                else:
                    fn = context.config.tempfit.model_grid_path.format(arm=arm)
                
                if not os.path.isfile(fn):
                    raise FileNotFoundError(f'Synthetic spectrum grid `{fn}` not found.')
                else:
                    logger.info(f'Using synthetic spectrum grid `{fn}` for arm `{arm}`.')
                
                # Verify that the PSF files are available

                if isinstance(context.config.tempfit.psf_path, dict):
                    fn = context.config.tempfit.psf_path[arm].format(arm=arm)
                elif context.config.tempfit.psf_path is not None:
                    fn = context.config.tempfit.psf_path.format(arm=arm)

                if context.config.tempfit.psf_path is not None:
                    if not os.path.isfile(fn):
                        raise FileNotFoundError(f'PSF file `{fn}` not found.')
                    else:
                        logger.info(f'Using PSF file `{fn}` for arm `{arm}`.')

    def __validate_chemfit_input_files(self, context):
        if context.config.run_chemfit:
            raise NotImplementedError()

    def __validate_input_data_products(self, context):

        required_products = set()
        
        if context.config.run_tempfit:
            required_products.update(context.config.tempfit.required_products)

        if context.config.run_chemfit:
            required_products.update(context.config.chemfit.required_products)

        # Compile the list of required input data products. The data products
        # are identified by their type. The class definitions are located in pfs.datamodel
        context.state.required_product_types = set()

        for t in required_products:
            found = False
            for repo in [ context.input_repo, context.work_repo ]:
                try:
                    t = repo.parse_product_type(t)
                    context.state.required_product_types.add(t)
                    found = True
                    break
                except ValueError as e:
                    pass

            if not found:
                raise PipelineError(f'Unknown product type `{t}` in configuration.')

        # Verify that input data files are available or the input products
        # are already in the cache
        for t in context.state.required_product_types:
            self.__validate_input_data_product(context, t)

    def __validate_input_data_product(self, context, product, required=True):
        # Check the availability of the required data products. They're either
        # already in the product cache on the pipeline level, or they must be
        # available in the data repository. We only identify the products here,
        # do no load them.

        context.pipeline.locate_data_products(product, required=required)
    
    def __validate_libs(self, context):
        # TODO: write code to validate library versions and log git hash for each when available

        # TODO: this should go to the init step

        pass