import numpy as np

from pfs.ga.pfsspec.survey.pfs.utils import *

class CoaddStepMixin():

    def _merge_spectra(self, context, coadd_spectra):
        # Merge the single arm spectra into a single spectrum
        # TODO: Now we assume that there is no overlap between the arms
        #       If we want to process observations with overlapping arms, we need to modify this
        merger = self._init_merger(context)
        merged_spectrum = merger.merge(coadd_spectra)

        # TODO: sky? covar? covar2? - these are required for a valid PfsFiberArray
        # TODO: these should go into the stacker class
        merged_spectrum.sky = np.zeros(merged_spectrum.wave.shape)
        merged_spectrum.covar = np.zeros((3,) + merged_spectrum.wave.shape)
        merged_spectrum.covar[1, :] = merged_spectrum.flux_err**2
        merged_spectrum.covar2 = np.zeros((1, 1), dtype=np.float32)

        return merged_spectrum
    
    def _get_mask_flags(self, context, spectra):
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

    def _append_metadata(self, context, coadd_spectra, merged_spectrum, mask_flags):
        
        def append_target(spectrum, target):
            spectrum.target = target
            spectrum.mask_flags = mask_flags
            spectrum.catid = target.catId
            spectrum.id = target.objId
        
        # Append observation metadata, this is PFS-specific
        all_observations = []
        target = None
        for arm in context.state.tempfit_spectra:
            observations = []
            for s in context.state.tempfit_spectra[arm]:
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