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
            
        # We can only coadd arms that have been used for RV fitting
        context.state.coadd_arms = set(context.config.coadd.coadd_arms).intersection(context.pipeline.rvfit_spectra.keys())

        if len(context.state.coadd_arms) < len(context.config.coadd.coadd_arms):
            logger.warning('Not all arms required for co-adding are available from rvfit.')

        return PipelineStepResults(success=True, skip_remaining=False, skip_substeps=False)

    #region Coadd

    def run(self, context):       
        input_spectra, no_data_bit, no_continuum_bit, exclude_bits, mask_flags = self.__load_spectra(context)
            
        coadd_spectra = self.__stack_spectra(context, input_spectra,
                                             no_data_bit, no_continuum_bit, exclude_bits)

        merged_spectrum = self.__merge_spectra(context, coadd_spectra)

        self.__append_metadata(context, coadd_spectra, merged_spectrum, mask_flags)
        
        context.state.coadd_results = SimpleNamespace(
            coadd_spectra = coadd_spectra,
            merged_spectrum = merged_spectrum,
        )

        if context.trace is not None:
            context.trace.on_coadd_finish_coadd(coadd_spectra)
        
        return PipelineStepResults(success=True, skip_remaining=False, skip_substeps=False)

    def __load_spectra(self, context):
        # Only consider the arms that we want to coadd
        input_spectra = { arm: context.pipeline.rvfit_spectra[arm] for arm in context.state.coadd_arms }
        
        # Add the extra mask flags to the spectra and then determine the mask values
        self.__add_extra_mask_flags(input_spectra, context.config.coadd.extra_mask_flags)
        no_data_bit, no_continuum_bit, exclude_bits, mask_flags = self.__get_mask_flags(context, input_spectra)

        return input_spectra, no_data_bit, no_continuum_bit, exclude_bits, mask_flags

    def __add_extra_mask_flags(self, spectra, flags):
        for arm in spectra:
            for s in spectra[arm]:
                if s is not None:
                    s.mask_flags.update(flags)

    def __stack_spectra(self, context, input_spectra, no_data_bit, no_continuum_bit, exclude_bits):
        # Initialize the stacker algorithm for each arm separately
        stackers = {}
        for arm in context.state.coadd_arms:
            if arm in input_spectra:
                stackers[arm] = self.__init_stacker(context,
                                                    arm,
                                                    no_data_bit=no_data_bit,
                                                    exclude_bits=exclude_bits)

        # TODO: when working with pfsMerged files, verify that errors are properly taken into account
        #       and correct weighting with the errors is done by the stacker

        # Generate the stacked spectra
        stacked_spectra = {}
        for arm, stacker in stackers.items():
            spec = stacker.stack([s for s in input_spectra[arm] if s is not None])
            stacked_spectra[arm] = [spec]

        # TODO: move everything below to the stacker class

        # Evaluate the best fit model, continuum, etc that might be interesting
        rvfit = context.pipeline.rvfit
        rvfit.reset()
        
        # Append the flux correction model to the coadded spectra
        stacked_spectra, _ = rvfit.append_corrections_and_templates(
            stacked_spectra, None,
            context.pipeline.rvfit_results.rv_fit,
            context.pipeline.rvfit_results.params_fit,
            a_fit=None,
            match='template',
            apply_correction=True,
        )

        # TODO: call the trace hook at this point but from the stacker class

        return stacked_spectra

    def __merge_spectra(self, context, coadd_spectra):
        # Merge the single arm spectra into a single spectrum
        # TODO: Now we assume that there is no overlap between the arms
        #       If we want to process observations with overlapping arms, we need to modify this
        merger = self.__init_merger(context)
        merged_spectrum = merger.merge(coadd_spectra)

        # TODO: sky? covar? covar2? - these are required for a valid PfsFiberArray
        # TODO: these should go into the stacker class
        merged_spectrum.sky = np.zeros(merged_spectrum.wave.shape)
        merged_spectrum.covar = np.zeros((3,) + merged_spectrum.wave.shape)
        merged_spectrum.covar[1, :] = merged_spectrum.flux_err**2
        merged_spectrum.covar2 = np.zeros((1, 1), dtype=np.float32)

        return merged_spectrum

    def __append_metadata(self, context, coadd_spectra, merged_spectrum, mask_flags):
        
        def append_target(spectrum, target):
            spectrum.target = target
            spectrum.mask_flags = mask_flags
            spectrum.catid = target.catId
            spectrum.id = target.objId
        
        # Append observation metadata, this is PFS-specific
        all_observations = []
        target = None
        for arm in context.pipeline.rvfit_spectra:
            observations = []
            for s in context.pipeline.rvfit_spectra[arm]:
                if s is not None:
                    observations.append(s.observations)
                    all_observations.append(s.observations)
                    if target is None:
                        target = s.target

            if arm in coadd_spectra:
                for s in coadd_spectra[arm]:
                    if s is not None:
                        s.observations = merge_observations(observations)
                        append_target(s, target)

        merged_spectrum.observations = merge_observations(all_observations)
        append_target(merged_spectrum, target)
    
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

    def __init_stacker_trace(self, context, arm=None):
        """
        Initialize the trace object if tracing is enabled for the pipeline
        """

        if context.trace is not None:
            trace = SpectrumStackerTrace(id=context.id)
            trace.init_from_args(None, None, context.config.coadd.trace_args)
            if arm is not None:
                id = f'{context.id}-{arm}'
            else:
                id = context.id
            trace.update(figdir=context.trace.figdir, logdir=context.trace.logdir, id=id)

            # Set the figure output file format
            trace.figure_formats = context.trace.figure_formats
        else:
            trace = None

        return trace
    
    def __init_stacker(self,
                       context,
                       arm,
                       no_data_bit=1,
                       exclude_bits=0):

        # Initialize the stacker object
        trace = self.__init_stacker_trace(context, arm)
        stacker = SpectrumStacker(trace)
        stacker.spectrum_type = PfsStellarSpectrum
        stacker.snr = context.state.snr[arm]
        stacker.snr_mask_flags = context.config.arms[arm]['snr']['mask_flags']
        stacker.mask_no_data_bit = no_data_bit
        stacker.mask_exclude_bits = exclude_bits
        stacker.init_from_args(None, None, context.config.coadd.stacker_args)

        return stacker

    def __init_merger(self, context,
                       no_data_bit=1,
                       exclude_bits=0):

        # Initialize the stacker object
        trace = self.__init_stacker_trace(context)
        stacker = SpectrumStacker(trace)
        stacker.spectrum_type = PfsStellarSpectrum
        stacker.mask_no_data_bit = no_data_bit
        stacker.mask_exclude_bits = exclude_bits
        stacker.init_from_args(None, None, context.config.coadd.stacker_args)

        return stacker
    
    #endregion
    #region Cleanup
    
    def cleanup(self, context):
        # TODO: free up memory after coadd
        return PipelineStepResults(success=True, skip_remaining=False, skip_substeps=False)

    #endregion