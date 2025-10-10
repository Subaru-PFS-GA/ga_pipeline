import os

from pfs.ga.pfsspec.survey.pfs.datamodel import *
from pfs.ga.pfsspec.core.obsmod.resampling import Binning
from pfs.ga.pfsspec.core.obsmod.psf import GaussPsf, PcaPsf
from pfs.ga.pfsspec.core.obsmod.stacking import SpectrumStacker, SpectrumStackerTrace
from pfs.ga.pfsspec.stellar.grid import ModelGrid
from pfs.ga.pfsspec.stellar.tempfit import TempFit, ModelGridTempFit, ModelGridTempFitTrace, CORRECTION_MODELS, EXTINCTION_MODELS
from pfs.ga.pfsspec.survey.pfs import PfsStellarSpectrum
from pfs.ga.pfsspec.survey.pfs.utils import *

from ...common import PipelineError, PipelineStep, PipelineStepResults

from ...setup_logger import logger

class TempFitStep(PipelineStep):
    def __init__(self, name=None):
        super().__init__(name)

    #region Run

    def init(self, context):

        if not context.config.run_tempfit:
            logger.info('RV fitting is disabled, skipping step.')
            return PipelineStepResults(success=True, skip_remaining=False, skip_substeps=True)

        # Find the set of available arms in the available files
        avail_arms = set()
        for t in context.pipeline.required_product_types:
            # If it is a container type, use the last element
            if isinstance(t, tuple):
                t = t[-1]

            if issubclass(t, (PfsFiberArray, PfsFiberArraySet, PfsTargetSpectra)):
                avail_arms = avail_arms.union(context.pipeline.get_avail_arms(t))

        # Verify that all arms required in the config are available
        context.pipeline.tempfit_arms = set()
        for arm in context.config.tempfit.fit_arms:
            message = f'TempFit requires arm `{arm}` which is not observed.'
            if context.config.tempfit.require_all_arms and arm not in avail_arms:
                raise PipelineError(message)
            elif arm not in avail_arms:
                logger.warning(message)
            else:
                context.pipeline.tempfit_arms.add(arm)
        
        return PipelineStepResults(success=True, skip_remaining=False, skip_substeps=False)
    
    #endregion
    #region Load
    
    def load(self, context):
        # Load template grids and PSFs
        context.pipeline.tempfit_grids = self.__tempfit_load_grid(
            context,
            context.pipeline.tempfit_arms)

        # TODO: this will change once we get real PSFs from the 2DRP
        # TODO: add trace hook to plot the PSFs
        context.pipeline.tempfit_psfs = self.__tempfit_load_psf(
            context,
            context.pipeline.tempfit_arms,
            context.pipeline.tempfit_grids)
        
        # Initialize the TempFit object
        context.pipeline.tempfit, context.pipeline.tempfit_trace = self.__tempfit_init(
            context,
            context.pipeline.tempfit_grids,
            context.pipeline.tempfit_psfs)

        # Read the spectra from the data products
        spectra = context.pipeline.read_spectra(
            context.pipeline.required_product_types,
            context.pipeline.tempfit_arms)

        # Calculate the signal to noise for each exposure
        for arm in spectra:
            for visit, spec in spectra[arm].items():
                mask_bits = spec.get_mask_bits(context.config.arms[arm]['snr']['mask_flags'])
                spec.calculate_snr(context.state.snr[arm], mask_bits=mask_bits)
        
        # Collect spectra in a format that can be passed to TempFit, i.e
        # handle missing spectra, fully masked spectra, etc.
        context.pipeline.tempfit_spectra = self.__tempfit_collect_spectra(
            context,
            spectra,
            context.pipeline.tempfit_arms,
            skip_mostly_masked=False,
            mask_flags=context.config.tempfit.mask_flags)
        
        if context.trace is not None:
            context.trace.on_load(context.pipeline.tempfit_spectra)

        return PipelineStepResults(success=True, skip_remaining=False, skip_substeps=False)
    
    def __tempfit_init(self, context, template_grids, template_psfs):
        """
        Initialize the RV fit object.
        """

        # Initialize the trace that will be used for logging and plotting
        if context.trace is not None:
            trace = ModelGridTempFitTrace(id=context.id)
            trace.init_from_args(None, None, context.config.tempfit.trace_args)

            # Set output directories based on pipeline trace
            trace.figdir = context.trace.figdir
            trace.logdir = context.trace.logdir

            # Set the figure output file format
            trace.figure_formats = context.trace.figure_formats
        else:
            trace = None

        # Create the correction model which determines if we apply flux correction to
        # the templates or continuum-normalize the observations.
        correction_model = CORRECTION_MODELS[context.config.tempfit.correction_model]()
        extinction_model = EXTINCTION_MODELS[context.config.tempfit.extinction_model]()
        
        # Create the template fit object that will perform the RV fitting
        tempfit = ModelGridTempFit(
            correction_model=correction_model,
            extinction_model=extinction_model,
            trace=trace)

        tempfit.template_grids = template_grids
        tempfit.template_psf = template_psfs

        tempfit.wave_include = context.config.tempfit.wave_include
        tempfit.wave_exclude = context.config.tempfit.wave_exclude

        # Initialize the components from the configuration
        tempfit.init_from_args(None, None, context.config.tempfit.tempfit_args)
        tempfit.correction_model.init_from_args(None, None, context.config.tempfit.correction_model_args)
        tempfit.extinction_model.init_from_args(None, None, context.config.tempfit.extinction_model_args)

        return tempfit, trace
    
    def __tempfit_collect_spectra(self,
                                context,
                                input_spectra,
                                use_arms, 
                                skip_mostly_masked=False,
                                mask_flags=None):
        """
        Collect spectra that will be used to fit the RV and stacking.

        If all spectra are missing or fully masked in a visit, the visit will be skipped.
        If all spectra are missing or fully masked in an arm, the arm will be skipped.
        """
    
        spectra = { arm: {} for arm in use_arms }
        for arm in use_arms:
            for i, visit, identity in context.config.enumerate_visits():
                spec = input_spectra[arm][visit]
                if spec is not None:
                    # Calculate mask bits
                    if mask_flags is not None:
                        spec.mask_bits = spec.get_mask_bits(mask_flags)
                    else:
                        spec.mask_bits = None

                    # Calculate mask. True values mean pixels are not masked and to be
                    # included in the fit.
                    mask = context.pipeline.tempfit.get_full_mask(spec)
                    masked_count = (~mask).sum()

                    if masked_count == 0:
                        logger.warning(f'All pixels in spectrum {spec.get_name()} are masked.')
                        spec = None
                    elif skip_mostly_masked and (mask.size - masked_count < context.config.tempfit.min_unmasked_pixels):
                        logger.warning(f'Not enough unmasked pixels in spectrum {spec.get_name()}, '
                                       f'required at least {context.config.tempfit.min_unmasked_pixels}, '
                                       f'found only {mask.size - masked_count}.')
                        spec = None

                spectra[arm][visit] = spec

        # Remove all None visits
        for i, visit, identity in context.config.enumerate_visits():
            non_zero = False
            for arm in use_arms:
                if spectra[arm][visit] is not None:
                    non_zero = True
                    break
            if not non_zero:
                for arm in spectra.keys():
                    del spectra[arm][visit]

        # Remove all None arms
        for arm in use_arms:
            non_zero = False
            for visit in spectra[arm].keys():
                if spectra[arm][visit] is not None:
                    non_zero = True
                    break
            if not non_zero:
                del spectra[arm]

        # Convert dict of visits into lists for each arm
        for arm in use_arms:
            if arm in spectra:
                spectra[arm] = [ spectra[arm][visit] for visit in sorted(spectra[arm].keys()) ]

        return spectra
    
    def __tempfit_load_grid(self, context, arms):
        # Load template grids. Make sure each grid is only loaded once, if grid is
        # independent of arm.

        grids = {}
        for arm in arms:
            if isinstance(context.config.tempfit.model_grid_path, dict):
                fn = context.config.tempfit.model_grid_path[arm].format(arm=arm)
            else:
                fn = context.config.tempfit.model_grid_path.format(arm=arm)

            skip = False
            for _, grid in grids.items():
                if grid.filename == fn:
                    grids[arm] = grid
                    skip = True
                    break

            if not skip:
                grid = ModelGrid.from_file(fn, 
                                           preload_arrays=context.config.tempfit.model_grid_preload,
                                           mmap_arrays=context.config.tempfit.model_grid_mmap, 
                                           args=context.config.tempfit.model_grid_args,
                                           slice_from_args=False)
                if grid.wave_edges is None:
                    grid.wave_edges = Binning.find_wave_edges(grid.wave)

                grids[arm] = grid

        return grids
    
    def __tempfit_load_psf(self, context, arms, grids):
        # Right now load a PSF file generate by the ETC        
        # TODO: Modify this to use PSF from 2D pipeline instead of ETC
        
        psfs = {}
        for arm in arms:
            fn = context.config.tempfit.psf_path.format(arm=arm)
            gauss_psf = GaussPsf()
            gauss_psf.load(fn)

            if grids is not None:
                wave = grids[arm].wave
            else:
                raise NotImplementedError()

            s = gauss_psf.get_optimal_size(wave)
            logger.info(f'Optimal kernel size for PSF in arm `{arm}` is {s}.')

            pca_psf = PcaPsf.from_psf(gauss_psf, wave, size=s, truncate=5)
            psfs[arm] = pca_psf

        return psfs

    
    #endregion
    #region Validate data

    def validate_data(self, context):
        """
        """

        spectra = context.pipeline.tempfit_spectra

        # Make sure that the bit flags are the same for all spectra
        # TODO: any more validation here?
        mask_flags = None
        for arm in spectra:
            for s in spectra[arm]:
                if s is not None:
                    if mask_flags is None:
                        mask_flags = s.mask_flags
                    elif mask_flags != s.mask_flags:
                        logger.warning('Mask flags are not the same for all spectra.')

        return PipelineStepResults(success=True, skip_remaining=False, skip_substeps=False)
    
    #region Preprocess

    def preprocess(self, context):
        # TODO: validate available spectra here and throw warning if any of the arms are missing after
        #       filtering based on masks
        
        return PipelineStepResults(success=True, skip_remaining=False, skip_substeps=False)
    
    #endregion
    #region Run
    
    def run(self, context):
        # Determine the normalization factor to be used to keep continuum coefficients unity
        context.pipeline.tempfit.spec_norm, context.pipeline.tempfit.temp_norm = context.pipeline.tempfit.get_normalization(context.pipeline.tempfit_spectra)

        # Run the maximum likelihood fitting
        # TODO: add MCMC
        context.pipeline.tempfit_results = context.pipeline.tempfit.run_ml(context.pipeline.tempfit_spectra)

        context.pipeline.tempfit_spectra, _ = context.pipeline.tempfit.append_corrections_and_templates(
            context.pipeline.tempfit_spectra, None,
            context.pipeline.tempfit_results.rv_fit,
            context.pipeline.tempfit_results.params_fit,
            context.pipeline.tempfit_results.a_fit,
            match='template',
            apply_correction=False,
        )

        if context.trace is not None:
            context.trace.on_tempfit_finish_fit(context.pipeline.tempfit_spectra)

        return PipelineStepResults(success=True, skip_remaining=False, skip_substeps=False)
    
    #endregion
    #region Log L map

    def map_log_L(self, context):

        # TODO: bring out parameters: params, ranges

        # Generate a map of log L around the best fit values
        if not context.config.tempfit.map_log_L:
            logger.info('Log L map is disabled, skipping step.')
            return PipelineStepResults(success=True, skip_remaining=False, skip_substeps=False)

        params = context.pipeline.tempfit_results.params_fit
        params_free = context.pipeline.tempfit.determine_free_params(context.pipeline.tempfit.params_fixed)

        params_map = []        
        params_bounds = {}
        for p, step in zip(['M_H', 'T_eff', 'log_g'], [0.5, 500, 1.0]):
            if p in params_free:
                params_map.append(p)
                params_bounds[p] = [params[p] - step, params[p] + step]

        if len(params_map) >= 2:
            for i in range(len(params_map) - 1):
                pb = {
                    params_map[i]: params_bounds[params_map[i]],
                    params_map[i + 1]: params_bounds[params_map[i + 1]]
                }
                pf = { p: v for p, v in params.items() if p not in pb }

                context.pipeline.tempfit.map_log_L(
                    context.pipeline.tempfit_spectra,
                    size=10,
                    rv=context.pipeline.tempfit_results.rv_fit,
                    params_fixed=pf,
                    params_bounds=pb,
                    squeeze=True)

        return PipelineStepResults(success=True, skip_remaining=False, skip_substeps=False)

    #endregion
    #region Cleanup
    
    def cleanup(self, context):
        # TODO: free up memory after tempfit
        return PipelineStepResults(success=True, skip_remaining=False, skip_substeps=False)

    #endregion