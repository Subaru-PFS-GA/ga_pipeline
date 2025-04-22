import os

from pfs.datamodel import *
from pfs.ga.pfsspec.core.obsmod.resampling import Binning
from pfs.ga.pfsspec.core.obsmod.psf import GaussPsf, PcaPsf
from pfs.ga.pfsspec.core.obsmod.stacking import SpectrumStacker, SpectrumStackerTrace
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

    def validate_config(self, context):
        """
        RV fitting pre-validation.
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

        # Calculate the signal to noise for each exposure
        for arm in spectra:
            for visit, spec in spectra[arm].items():
                mask_bits = spec.get_mask_bits(context.config.arms[arm]['snr']['mask_flags'])
                spec.calculate_snr(context.pipeline.snr[arm], mask_bits=mask_bits)
        
        # Collect spectra in a format that can be passed to RVFit, i.e
        # handle missing spectra, fully masked spectra, etc.
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

        rvfit.wave_include = context.config.rvfit.wave_include
        rvfit.wave_exclude = context.config.rvfit.wave_exclude

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
                        spec.mask_bits = spec.get_mask_bits(mask_flags)
                    else:
                        spec.mask_bits = None

                    # Calculate mask. True values mean pixels are not masked and to be
                    # included in the fit.
                    mask = context.pipeline.rvfit.get_full_mask(spec)
                    masked_count = (~mask).sum()

                    if masked_count == 0:
                        logger.warning(f'All pixels in spectrum {spec.get_name()} are masked.')
                        spec = None
                    elif skip_mostly_masked and (mask.size - masked_count < context.config.rvfit.min_unmasked_pixels):
                        logger.warning(f'Not enough unmasked pixels in spectrum {spec.get_name()}, '
                                       f'required at least {context.config.rvfit.min_unmasked_pixels}, '
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
    #region Validate data

    def validate_data(self, context):
        """
        """

        spectra = context.pipeline.rvfit_spectra

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
    #region Fit
    
    def fit(self, context):
        # Determine the normalization factor to be used to keep continuum coefficients unity
        context.pipeline.rvfit.spec_norm, context.pipeline.rvfit.temp_norm = context.pipeline.rvfit.get_normalization(context.pipeline.rvfit_spectra)

        # Run the maximum likelihood fitting
        context.pipeline.rvfit_results = context.pipeline.rvfit.fit_rv(context.pipeline.rvfit_spectra)

        return PipelineStepResults(success=True, skip_remaining=False, skip_substeps=False)
    
    #endregion
    #region Coadd

    def coadd(self, context):
        if not context.config.run_rvfit:
            logger.info('Spectrum stacking required RV fitting which is disabled, skipping step.')
            return PipelineStepResults(success=True, skip_remaining=True, skip_substeps=True)
        elif not context.config.run_coadd:
            logger.info('Spectrum stacking is disabled, skipping step.')
            return PipelineStepResults(success=True, skip_remaining=True, skip_substeps=True)
        
        # We can only coadd arms that have been used for RV fitting
        coadd_arms = set(context.config.coadd.coadd_arms).intersection(context.pipeline.rvfit_spectra.keys())

        if len(coadd_arms) < len(context.config.coadd.coadd_arms):
            logger.warning('Not all arms required for co-adding are available from rvfit.')
        
        # Use the same input as for RV fitting but limit the arms to those we want to coadd
        context.pipeline.coadd_spectra = { arm: [ s for s in context.pipeline.rvfit_spectra[arm] if s is not None ] for arm in coadd_arms }
        spectra = context.pipeline.coadd_spectra
        
        # Add the extra mask flags to the spectra and then determine the mask values
        self.__coadd_add_extra_flags(context, spectra)
        no_data_bit, no_continuum_bit, exclude_bits, mask_flags = self.__coadd_get_mask_flags(context, spectra)

        # Plot the input spectra
        if context.trace is not None:
            context.trace.on_coadd_start_stack(
                { arm: [ s for s in spectra[arm] if s is not None ] for arm in spectra },
                no_continuum_bit, exclude_bits)
                
        # TODO: move these below to the tempfit class instead?

        # Evaluate the templates and the correction model for each arm and exposure at the best fit parameters
        templates = self.__coadd_get_templates(context, spectra)
        corrections, correction_masks = self.__coadd_eval_correction(context, spectra, templates)

        # Calculate the normalization factor that rescales the correction models to the original
        # physical flux unit of the spectra.
        norm = context.pipeline.rvfit.spec_norm if context.pipeline.rvfit.spec_norm is not None else 1.0
        norm /= context.pipeline.rvfit.temp_norm if context.pipeline.rvfit.temp_norm is not None else 1.0

        # Append the correction model to the spectra. This will leaver the mask and flux
        # intact and only attach the `cont` or `flux_corr` attribute to the spectra.
        context.pipeline.rvfit.correction_model.apply_correction(spectra, None, corrections, correction_masks,
                                                                 apply_flux=False, apply_mask=True,
                                                                 mask_bit=no_continuum_bit,
                                                                 normalization=norm)
        
        # Plot the corrected spectra
        # if context.trace is not None:
        #     context.trace.on_coadd_start_stack(
        #         { arm: [ s for s in spectra[arm] if s is not None ] for arm in context.config.coadd.coadd_arms },
        #         no_continuum_bit, exclude_bits)

        # Initialize the stacker algorithm
        context.pipeline.stacker = self.__coadd_init_stacker(context,
                                                             no_data_bit=no_data_bit,
                                                             exclude_bits=exclude_bits)

        # TODO: add trace hook to plot the templates and corrections?

        # TODO: when working with pfsMerged files, verify that errors are properly taken into account
        #       and correct weighting with the errors is done by the stacker

        # Generate the stacked spectrum
        # Contract spectra into a single list
        ss = []
        for arm in context.config.coadd.coadd_arms:
            if arm in spectra and spectra[arm] is not None:
                for s in spectra[arm]:
                    if s is not None:
                        ss.append(s)
        coadd_spectrum = context.pipeline.stacker.stack(ss)
        
        # TODO: evaluate the best fit model, continuum, etc that might be interesting

        if context.trace is not None:
            context.trace.on_coadd_finish_stack(coadd_spectrum, context.config.coadd.coadd_arms, no_continuum_bit, exclude_bits)

        # TODO: sky? covar? covar2? - these are required for a valid PfsFiberArray
        coadd_spectrum.sky = np.zeros(coadd_spectrum.wave.shape)
        coadd_spectrum.covar = np.zeros((3,) + coadd_spectrum.wave.shape)
        coadd_spectrum.covar2 = np.zeros((1, 1), dtype=np.float32)

        # Merge observation metadata
        # TODO: move this to the final save step, do not attach metadata to
        #       the coadded spectrum because it might not have all arms that
        #       we used in the processing

        # TODO: actually, we might want to save the coadd file separately from the RVFit results

        observations = []
        target = None
        for arm in context.pipeline.rvfit_spectra:
            for s in context.pipeline.rvfit_spectra[arm]:
                if s is not None:
                    observations.append(s.observations)
                    if target is None:
                        target = s.target
        
        # Merge all observations into a final list
        coadd_spectrum.target = target
        coadd_spectrum.observations = merge_observations(observations)
        coadd_spectrum.mask_flags = mask_flags

        context.pipeline.coadd_results = SimpleNamespace(
            spectrum = coadd_spectrum
        )

        # TODO: do we need more metadata?

        # TODO: trace hook
        
        return PipelineStepResults(success=True, skip_remaining=False, skip_substeps=False)
    
    def __coadd_add_extra_flags(self, context, spectra):
        for arm in spectra:
            for s in spectra[arm]:
                if s is not None:
                    s.mask_flags.update(context.config.coadd.extra_mask_flags)
    
    def __coadd_get_mask_flags(self, context, spectra):
        """
        Return mask bits and mask flags which are supposed to be valid for all spectra.

        Returns
        -------
        no_data_bit : int
            The bit mask for the no data flag.
        no_continuum_bit : int
            The bit mask for the no continuum flag.
        mask_flags : dict
            The dictionary of mask flags
        """

        for arm in spectra:
            for s in spectra[arm]:
                if s is not None:
                    mask_flags = s.mask_flags

                    no_data_bit = s.get_mask_bits([ context.config.coadd.mask_flag_no_data ])
                    no_continuum_bit = s.get_mask_bits([ context.config.coadd.mask_flag_no_continuum ])
                    exclude_bits = s.get_mask_bits(context.config.coadd.mask_flags_exclude)

                    return no_data_bit, no_continuum_bit, exclude_bits, mask_flags
    
    def __coadd_init_stacker(self, context,
                             no_data_bit=1,
                             exclude_bits=0):
        """
        Initialize the trace object if tracing is enabled for the pipeline
        """

        if context.trace is not None:
            trace = SpectrumStackerTrace(id=context.id)
            trace.init_from_args(None, None, context.config.coadd.trace_args)

            # Set output directories based on pipeline trace
            trace.figdir = context.trace.figdir
            trace.logdir = context.trace.logdir

            # Set the figure output file format
            trace.figure_formats = context.trace.figure_formats
        else:
            trace = None

        # Initialize the stacker object
        stacker = SpectrumStacker(trace)
        stacker.spectrum_type = PfsStellarSpectrum
        stacker.mask_no_data_bit = no_data_bit
        stacker.mask_exclude_bits = exclude_bits
        stacker.init_from_args(None, None, context.config.coadd.stacker_args)

        return stacker
    
    def __coadd_get_templates(self, context, spectra):
        # Return the templates at the best fit parameters

        # Interpolate the templates to the best fit parameters
        templates, missing = context.pipeline.rvfit.get_templates(
            spectra,
            context.pipeline.rvfit_results.params_fit)
        
        if context.trace is not None:
            context.trace.on_coadd_get_templates(spectra, templates)

        return templates
    
    def __coadd_eval_correction(self, context, spectra, templates):
        # Evaluate the correction for every exposure of each arm.
        # Depending on the configuration, the correction is either a multiplicative
        # flux correction, or a model fitted to continuum pixels. The correction model
        # is used to normalize the spectra before coadding.

        pp_specs, pp_temps, corrections, correction_masks = context.pipeline.rvfit.eval_correction(
            spectra,
            templates,
            context.pipeline.rvfit_results.rv_fit,
            a=context.pipeline.rvfit_results.a_fit)

        # Attach the correction model to the spectra but do not multiply
        context.pipeline.rvfit.correction_model.apply_correction(spectra, None, corrections, correction_masks,
                                                                 apply_flux=False, apply_mask=True)
        
        if context.trace is not None:
            context.trace.on_coadd_eval_correction(spectra, templates, corrections, correction_masks,
                                                   context.pipeline.rvfit.spec_norm, context.pipeline.rvfit.temp_norm)
        
        return corrections, correction_masks
    
    #endregion
    #region Cleanup
    
    def cleanup(self, context):
        # TODO: free up memory after rvfit
        return PipelineStepResults(success=True, skip_remaining=False, skip_substeps=False)

    #endregion