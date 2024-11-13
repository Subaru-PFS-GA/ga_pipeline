import os

from pfs.datamodel import *
from pfs.ga.pfsspec.core.obsmod.resampling import Binning
from pfs.ga.pfsspec.core.obsmod.psf import GaussPsf, PcaPsf
from pfs.ga.pfsspec.core.obsmod.stacking import Stacker, StackerTrace
from pfs.ga.pfsspec.stellar.grid import ModelGrid
from pfs.ga.pfsspec.stellar.tempfit import TempFit, ModelGridTempFit, ModelGridTempFitTrace, CORRECTION_MODELS
from pfs.ga.pfsspec.survey.pfs import PfsStellarSpectrum
from pfs.ga.pfsspec.survey.pfs.utils import *

from ...common import PipelineError, PipelineStep, PipelineStepResults

from ...setup_logger import logger

class RVFitStep(PipelineStep):
    def __init__(self, name=None):
        super().__init__(name)

    #region Run

    def validate(self, context):
        """
        Initialize the RV fitting step.
        """
        
        if not context.config.run_rvfit:
            logger.info('RV fitting is disabled, skipping step.')
            return PipelineStepResults(success=True, skip_remaining=False, skip_substeps=True)
        
        # Find the set of available arms in the available files
        avail_arms = set()
        for t in context.pipeline.required_product_types:
            if issubclass(t, (PfsFiberArray, PfsFiberArraySet)):
                avail_arms = avail_arms.union(context.pipeline.get_avail_arms(t))

        # Verify that all arms required in the config are available
        context.pipeline.rvfit_arms = set()
        for arm in context.config.rvfit.fit_arms:
            message = f'RVFIT requires arm `{arm}` which is not observed.'
            if context.config.rvfit.require_all_arms and arm not in avail_arms:
                raise PipelineError(message)
            elif arm not in avail_arms:
                logger.warning(message)
            else:
                context.pipeline.rvfit_arms.add(arm)
        
        return PipelineStepResults(success=True, skip_remaining=False, skip_substeps=False)
    
    #endregion
    #region Load
    
    def load(self, context):
        # Load template grids and PSFs
        context.pipeline.rvfit_grids = self.__rvfit_load_grid(
            context,
            context.pipeline.rvfit_arms)

        # TODO: this will change once we get real PSFs from the 2DRP
        # TODO: add trace hook to plot the PSFs
        context.pipeline.rvfit_psfs = self.__rvfit_load_psf(
            context,
            context.pipeline.rvfit_arms,
            context.pipeline.rvfit_grids)
        
        # Initialize the RVFit object
        context.pipeline.rvfit, context.pipeline.rvfit_trace = self.__rvfit_init(
            context,
            context.pipeline.rvfit_grids,
            context.pipeline.rvfit_psfs)

        # Read the spectra from the data products
        spectra = context.pipeline.read_spectra(
            context.pipeline.required_product_types,
            context.pipeline.rvfit_arms)
        
        # Collect spectra in a format that can be passed to RVFit
        context.pipeline.rvfit_spectra = self.__rvfit_collect_spectra(
            context,
            spectra,
            context.pipeline.rvfit_arms,
            skip_mostly_masked=False,
            mask_flags=context.config.rvfit.mask_flags)
        
        if context.trace is not None:
            context.trace.on_load(context.pipeline.rvfit_spectra)

        return PipelineStepResults(success=True, skip_remaining=False, skip_substeps=False)
    
    def __rvfit_init(self, context, template_grids, template_psfs):
        """
        Initialize the RV fit object.
        """

        # Initialize the trace that will be used for logging and plotting
        if context.trace is not None:
            trace = ModelGridTempFitTrace(id=context.id)
            trace.init_from_args(None, None, context.config.rvfit.trace_args)

            # Set output directories based on pipeline trace
            trace.figdir = context.trace.figdir
            trace.logdir = context.trace.logdir

            # Set the figure output file format
            trace.figure_formats = context.trace.figure_formats
        else:
            trace = None

        # Create the correction model which determines if we apply flux correction to
        # the templates or continuum-normalize the observations.
        correction_model = CORRECTION_MODELS[context.config.rvfit.correction_model]()
        
        # Create the template fit object that will perform the RV fitting
        rvfit = ModelGridTempFit(correction_model=correction_model, trace=trace)

        rvfit.template_grids = template_grids
        rvfit.template_psf = template_psfs

        # Initialize the components from the configuration
        rvfit.init_from_args(None, None, context.config.rvfit.rvfit_args)
        rvfit.correction_model.init_from_args(None, None, context.config.rvfit.correction_model_args)

        return rvfit, trace
    
    def __rvfit_collect_spectra(self,
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
                        mask_bits = spec.get_mask_bits(mask_flags)
                    else:
                        mask_bits = None

                    # Calculate mask
                    mask = context.pipeline.rvfit.get_full_mask(spec, mask_bits=mask_bits)

                    if mask.sum() == 0:
                        logger.warning(f'All pixels in spectrum {spec.get_name()} are masked.')
                        spec = None
                    elif mask.sum() < context.config.rvfit.min_unmasked_pixels:
                        logger.warning(f'Not enough unmasked pixels in spectrum {spec.get_name()}.')
                        if skip_mostly_masked:
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
            spectra[arm] = [ spectra[arm][visit] for visit in sorted(spectra[arm].keys()) ]

        return spectra
    
    def __rvfit_load_grid(self, context, arms):
        # Load template grids. Make sure each grid is only loaded once, if grid is
        # independent of arm.

        grids = {}
        for arm in arms:
            if isinstance(context.config.rvfit.model_grid_path, dict):
                fn = context.config.rvfit.model_grid_path[arm].format(arm=arm)
            else:
                fn = context.config.rvfit.model_grid_path.format(arm=arm)

            skip = False
            for _, grid in grids.items():
                if grid.filename == fn:
                    grids[arm] = grid
                    skip = True
                    break

            if not skip:
                grid = ModelGrid.from_file(fn, 
                                           preload_arrays=context.config.rvfit.model_grid_preload,
                                           mmap_arrays=context.config.rvfit.model_grid_mmap, 
                                           args=context.config.rvfit.model_grid_args,
                                           slice_from_args=False)
                if grid.wave_edges is None:
                    grid.wave_edges = Binning.find_wave_edges(grid.wave)

                grids[arm] = grid

        return grids
    
    def __rvfit_load_psf(self, context, arms, grids):
        # Right now load a PSF file generate by the ETC        
        # TODO: Modify this to use PSF from 2D pipeline instead of ETC
        
        psfs = {}
        for arm in arms:
            fn = context.config.rvfit.psf_path.format(arm=arm)
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
    #region Preprocess

    def preprocess(self, context):
        # TODO: validate available spectra here and throw warning if any of the arms are missing after
        #       filtering based on masks
        
        return PipelineStepResults(success=True, skip_remaining=False, skip_substeps=False)
    
    #endregion
    #region Fit
    
    def fit(self, context):
        # Determine the normalization factor to be used to keep continuum coefficients unity
        context.pipeline.rvfit.spec_norm, context.pipeline.rvfit.temp_norm = context.pipeline.rvfit.get_normalization(context.pipeline.rvfit_spectra)

        # Run the maximum likelihood fitting
        context.pipeline.rvfit_results = context.pipeline.rvfit.fit_rv(context.pipeline.rvfit_spectra)

        return PipelineStepResults(success=True, skip_remaining=False, skip_substeps=False)
    
    #region Coadd

    def coadd(self, context):
        if not context.config.run_rvfit:
            logger.info('Spectrum stacking required RV fitting which is disabled, skipping step.')
            return PipelineStepResults(success=True, skip_remaining=True, skip_substeps=True)
        elif not context.config.run_coadd:
            logger.info('Spectrum stacking is disabled, skipping step.')
            return PipelineStepResults(success=True, skip_remaining=True, skip_substeps=True)
        
        # Use the same input as for RV fitting and evaluate the templates and the
        # continuum or flux correction function
        spectra = context.pipeline.rvfit_spectra
        templates = self.__rvfit_coadd_get_templates(context, spectra)
        corrections = self.__rvfit_coadd_eval_correction(context, spectra, templates)
        
        # We can only coadd arms that have been used for RV fitting
        coadd_arms = set(context.config.coadd.coadd_arms).intersection(spectra.keys())

        if len(coadd_arms) < len(context.config.coadd.coadd_arms):
            logger.warning('Not all arms required for co-adding are available from rvfit.')

        # Make sure that the bit flags are the same for all spectra
        # TODO: any more validation here?
        no_data_bit = None
        mask_flags = None
        for arm in spectra:
            for s in spectra[arm]:
                if s is not None:
                    if no_data_bit is None:
                        no_data_bit = s.get_mask_bits([ context.config.coadd.no_data_flag ])

                    if mask_flags is None:
                        mask_flags = s.mask_flags
                    elif mask_flags != s.mask_flags:
                        logger.warning('Mask flags are not the same for all spectra.')

        # Initialize the stacker algorithm
        context.pipeline.stacker = self.__rvfit_coadd_init(context)

        # TODO: add trace hook to plot the templates and corrections?

        coadd_spectra = {}
        for arm in spectra:
            # Only stack those spectra that are not masked or otherwise None
            ss = [ s for s in spectra[arm] if s is not None ]
            fc = [ f for f in corrections[arm] if f is not None ]
            if len(ss) > 0:
                stacked_wave, stacked_wave_edges, stacked_flux, stacked_error, stacked_weight, stacked_mask = \
                    context.pipeline.stacker.stack(ss, flux_corr=fc)
                
                # Mask out bins where the weight is zero
                # TODO: move this to the stacker algorithm
                stacked_mask = np.where(stacked_weight == 0, stacked_mask | no_data_bit, stacked_mask)

                # Create a spectrum
                spec = PfsStellarSpectrum()

                # TODO: fill in ids?
                #       calculate S/N etc.

                spec.wave = stacked_wave
                spec.wave_edges = stacked_wave_edges
                spec.flux = stacked_flux
                spec.flux_err = stacked_error
                spec.mask = stacked_mask

                coadd_spectra[arm] = spec
            else:
                coadd_spectra[arm] = None

        # Merge arms into a single spectrum
        # TODO: this won't work with overlapping arms! Need to merge them properly.
        arms = sort_arms(spectra.keys())
        coadd_merged = PfsStellarSpectrum()
        coadd_merged.wave = np.concatenate([ coadd_spectra[arm].wave for arm in arms ])
        coadd_merged.wave_edges = np.concatenate([ coadd_spectra[arm].wave_edges for arm in arms ])
        coadd_merged.flux = np.concatenate([ coadd_spectra[arm].flux for arm in arms ])
        coadd_merged.flux_err = np.concatenate([ coadd_spectra[arm].flux_err for arm in arms ])
        coadd_merged.mask = np.concatenate([ coadd_spectra[arm].mask for arm in arms ])

        # TODO: sky? covar? covar2? - these are required for a valid PfsFiberArray
        coadd_merged.sky = np.zeros(coadd_merged.wave.shape)
        coadd_merged.covar = np.zeros((3,) + coadd_merged.wave.shape)
        coadd_merged.covar2 = np.zeros((1, 1), dtype=np.float32)

        # Merge observation metadata
        observations = []
        target = None
        mask_flags = None
        for arm in context.pipeline.rvfit_spectra:
            for s in context.pipeline.rvfit_spectra[arm]:
                if s is not None:
                    observations.append(s.observations)
                    if target is None:
                        target = s.target
                        mask_flags = s.mask_flags
        
        # Merge all observations into a final list
        coadd_merged.target = target
        coadd_merged.observations = merge_observations(observations)
        coadd_merged.mask_flags = mask_flags

        context.pipeline.coadd_spectra = coadd_spectra
        context.pipeline.coadd_merged = coadd_merged

        # TODO: do we need mode metadata?

        # TODO: trace hook
        
        return PipelineStepResults(success=True, skip_remaining=False, skip_substeps=False)
    
    def __rvfit_coadd_init(self, context):
        # Initialize the trace object if tracing is enabled for the pipeline
        if context.trace is not None:
            trace = StackerTrace(id=context.id)
            trace.init_from_args(None, None, context.config.coadd.trace_args)

            # Set output directories based on pipeline trace
            trace.figdir = context.trace.figdir
            trace.logdir = context.trace.logdir

            # Set the figure output file format
            trace.figure_formats = context.trace.figure_formats
        else:
            trace = None

        # Initialize the stacker object
        stacker = Stacker(trace)
        stacker.init_from_args(None, None, context.config.coadd.stacker_args)

        return stacker
    
    def __rvfit_coadd_get_templates(self, context, spectra):
        # Return the templates at the best fit parameters

        # Interpolate the templates to the best fit parameters
        templates, missing = context.pipeline.rvfit.get_templates(
            spectra,
            context.pipeline.rvfit_results.params_fit)
        
        if context.trace is not None:
            context.trace.on_coadd_get_templates(spectra, templates)

        return templates
    
    def __rvfit_coadd_eval_correction(self, context, spectra, templates):
        # Evaluate the correction for every exposure of each arm.
        # Depending on the configuration, the correction is either a multiplicative
        # flux correction, or a model fitted to continuum pixels. The correction model
        # is used to normalize the spectra before coadding.

        corr = context.pipeline.rvfit.eval_correction(
            spectra,
            templates,
            context.pipeline.rvfit_results.rv_fit,
            a=context.pipeline.rvfit_results.a_fit)
        
        if context.trace is not None:
            context.trace.on_coadd_eval_correction(spectra, templates, corr,
                                                context.pipeline.rvfit.spec_norm, context.pipeline.rvfit.temp_norm)
        
        return corr
    
    #endregion
    #region Cleanup
    
    def cleanup(self, context):
        # TODO: free up memory after rvfit
        return PipelineStepResults(success=True, skip_remaining=False, skip_substeps=False)

    #endregion