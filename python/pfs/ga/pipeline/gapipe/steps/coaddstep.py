import numpy as np
from types import SimpleNamespace

from pfs.ga.pfsspec.survey.pfs.datamodel import *
from pfs.ga.pfsspec.core.obsmod.resampling import Binning
from pfs.ga.pfsspec.core.obsmod.psf import GaussPsf, PcaPsf
from pfs.ga.pfsspec.core.obsmod.stacking import SpectrumStacker, SpectrumStackerTrace
from pfs.ga.pfsspec.stellar.grid import ModelGrid
from pfs.ga.pfsspec.stellar.tempfit import TempFit, ModelGridTempFit, ModelGridTempFitTrace, CORRECTION_MODELS
from pfs.ga.pfsspec.survey.pfs import PfsStellarSpectrum
from pfs.ga.pfsspec.survey.pfs.utils import *

from ...common import PipelineError, PipelineStep, PipelineStepResults

from ...setup_logger import logger

class CoaddStep(PipelineStep):
    def __init__(self, name=None):
        super().__init__(name)

    def init(self, context):
        if not context.config.run_rvfit:
            logger.info('Spectrum stacking required RV fitting which is disabled, skipping step.')
            return PipelineStepResults(success=True, skip_remaining=True, skip_substeps=True)
        elif not context.config.run_coadd:
            logger.info('Spectrum stacking is disabled, skipping step.')
            return PipelineStepResults(success=True, skip_remaining=True, skip_substeps=True)
        else:
            return PipelineStepResults(success=True, skip_remaining=False, skip_substeps=False)

    #region Coadd

    def run(self, context):       
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
        context.pipeline.rvfit.correction_model.apply_correction(spectra,
                                                                 corrections, correction_masks, None,
                                                                 apply_flux=False, apply_mask=True,
                                                                 mask_bit=no_continuum_bit,
                                                                 template=False)
        
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
        context.pipeline.rvfit.correction_model.apply_correction(spectra, 
                                                                 corrections, correction_masks, None,
                                                                 apply_flux=False, apply_mask=True,
                                                                 template=False)
        
        if context.trace is not None:
            context.trace.on_coadd_eval_correction(spectra, templates, corrections, correction_masks,
                                                   context.pipeline.rvfit.spec_norm, context.pipeline.rvfit.temp_norm)
        
        return corrections, correction_masks
    
    #endregion
    #region Cleanup
    
    def cleanup(self, context):
        # TODO: free up memory after coadd
        return PipelineStepResults(success=True, skip_remaining=False, skip_substeps=False)

    #endregion