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
        
        # Get the spectra from rvfit and append the flux correction model.
        # The correction is not applied yet, only appended to the spectra.
        spectra = context.pipeline.rvfit.append_corrections_and_templates(
            context.pipeline.rvfit_spectra, None,
            context.pipeline.rvfit_results.rv_fit,
            context.pipeline.rvfit_results.params_fit,
            context.pipeline.rvfit_results.a_fit,
            match='template',
            apply_correction=False,
        )

        # Only consider the arms that we want to coadd
        context.pipeline.coadd_spectra = spectra = { arm: spectra[arm] for arm in coadd_arms }
        
        # Add the extra mask flags to the spectra and then determine the mask values
        self.__add_extra_mask_flags(spectra, context.config.coadd.extra_mask_flags)
        no_data_bit, no_continuum_bit, exclude_bits, mask_flags = self.__get_mask_flags(context, spectra)

        # Call the trace hook to plot the input spectra
        if context.trace is not None:
            context.trace.on_coadd_start_stack(
                { arm: [ s for s in spectra[arm] if s is not None ] for arm in spectra },
                no_continuum_bit, exclude_bits)
            
        # Initialize the stacker algorithm
        context.pipeline.stacker = stacker = self.__init_stacker(context,
                                                       no_data_bit=no_data_bit,
                                                       exclude_bits=exclude_bits)

        # TODO: when working with pfsMerged files, verify that errors are properly taken into account
        #       and correct weighting with the errors is done by the stacker

        # Contract spectra into a single list
        ss = []
        for arm in context.config.coadd.coadd_arms:
            if arm in spectra and spectra[arm] is not None:
                for s in spectra[arm]:
                    if s is not None:
                        ss.append(s)

        # Generate the stacked spectrum
        coadd_spectrum = stacker.stack(ss)
        
        # TODO: evaluate the best fit model, continuum, etc that might be interesting

        # TODO: sky? covar? covar2? - these are required for a valid PfsFiberArray
        # TODO: these should go into the stacker class
        coadd_spectrum.sky = np.zeros(coadd_spectrum.wave.shape)
        coadd_spectrum.covar = np.zeros((3,) + coadd_spectrum.wave.shape)
        coadd_spectrum.covar2 = np.zeros((1, 1), dtype=np.float32)

        # Evaluate the best fit template at the best fit parameters
        templates = self.__get_templates(context, spectra)

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

        # Call the trace hook
        if context.trace is not None:
            context.trace.on_coadd_finish_stack(coadd_spectrum, context.config.coadd.coadd_arms, no_continuum_bit, exclude_bits)
        
        return PipelineStepResults(success=True, skip_remaining=False, skip_substeps=False)
    
    def __add_extra_mask_flags(self, spectra, flags):
        for arm in spectra:
            for s in spectra[arm]:
                if s is not None:
                    s.mask_flags.update(flags)
    
    def __get_mask_flags(self, context, spectra):
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
    
    def __init_stacker(self, context,
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
    
    def __get_templates(self, context, spectra):
        # Return the templates at the best fit parameters

        # Interpolate the templates to the best fit parameters
        templates, missing = context.pipeline.rvfit.get_templates(
            spectra,
            context.pipeline.rvfit_results.params_fit)
        
        if context.trace is not None:
            context.trace.on_coadd_get_templates(spectra, templates)

        return templates
    
    #endregion
    #region Cleanup
    
    def cleanup(self, context):
        # TODO: free up memory after coadd
        return PipelineStepResults(success=True, skip_remaining=False, skip_substeps=False)

    #endregion