import os
from collections import defaultdict

from pfs.ga.pfsspec.survey.pfs.datamodel import *
from pfs.ga.pfsspec.core.obsmod.resampling import Binning
from pfs.ga.pfsspec.core.obsmod.psf import GaussPsf, PcaPsf
from pfs.ga.pfsspec.core.obsmod.stacking import SpectrumStacker, SpectrumStackerTrace
from pfs.ga.pfsspec.core import Filter
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
        for t in context.state.required_product_types:
            # If it is a container type, use the last element
            if isinstance(t, tuple):
                t = t[-1]

            if issubclass(t, (PfsFiberArray, PfsFiberArraySet, PfsTargetSpectra)):
                avail_arms = avail_arms.union(context.pipeline.get_avail_arms(t))

        # Verify that all arms required in the config are available
        context.state.tempfit_arms = set()
        for arm in context.config.tempfit.fit_arms:
            message = f'TempFit requires arm `{arm}` which is not observed.'
            if context.config.tempfit.require_all_arms and arm not in avail_arms:
                raise PipelineError(message)
            elif arm not in avail_arms:
                logger.warning(message)
            else:
                context.state.tempfit_arms.add(arm)
        
        return PipelineStepResults(success=True, skip_remaining=False, skip_substeps=False)
    
    #endregion
    #region Load
    
    def load(self, context):
        # Load template grids and PSFs
        context.state.tempfit_grids = self.__tempfit_load_grid(
            context,
            context.state.tempfit_arms)

        # TODO: this will change once we get real PSFs from the 2DRP
        # TODO: add trace hook to plot the PSFs
        context.state.tempfit_psfs = self.__tempfit_load_psf(
            context,
            context.state.tempfit_arms,
            context.state.tempfit_grids)

        # Load the filter curves to fit extinction, if needed
        context.state.tempfit_synthmag_filters, context.state.tempfit_synthmag_grids = self.__tempfile_load_filters(context)
        
        # Initialize the TempFit object
        context.state.tempfit, context.state.tempfit_trace = self.__tempfit_init(context)

        # Read the spectra from the data products
        spectra = context.pipeline.read_spectra(
            context.state.required_product_types,
            context.state.tempfit_arms)

        # Calculate the signal to noise for each exposure
        for arm in spectra:
            for visit, spec in spectra[arm].items():
                mask_bits = spec.get_mask_bits(context.config.arms[arm]['snr']['mask_flags'])
                spec.calculate_snr(context.state.snr[arm], mask_bits=mask_bits)
        
        # Collect spectra in a format that can be passed to TempFit, i.e
        # handle missing spectra, fully masked spectra, etc.
        context.state.tempfit_spectra = self.__tempfit_collect_spectra(
            context,
            spectra,
            context.state.tempfit_arms,
            skip_mostly_masked=False,
            mask_flags=context.config.tempfit.mask_flags)

        # If fitting the broadband magnitudes, collect the fluxes
        if context.config.tempfit.fit_photometry:
            context.state.tempfit_fluxes = self.__tempfit_collect_fluxes(context)
        else:
            context.state.tempfit_fluxes = None
        
        if context.trace is not None:
            context.trace.on_load(context.state.tempfit_spectra)

        return PipelineStepResults(success=True, skip_remaining=False, skip_substeps=False)
    
    def __tempfit_init(self, context):
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

        tempfit.template_grids = context.state.tempfit_grids
        tempfit.template_psf = context.state.tempfit_psfs

        tempfit.synthmag_filters = context.state.tempfit_synthmag_filters
        tempfit.synthmag_grids = context.state.tempfit_synthmag_grids

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
                    mask = context.state.tempfit.get_full_mask(spec)
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

    def __tempfit_collect_fluxes(self, context):
        """
        Collect photometric fluxes from the spectra.
        """

        # TODO: do we need to do some kind of mapping here between filter names?

        fluxes = defaultdict(dict)
        for band, photometry in context.config.tempfit.photometry.items():
            # TODO: handle the case when we only have magnitudes
            fluxes[photometry.instrument][band] = (
                photometry.flux,
                photometry.flux_error)

        return fluxes
    
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

    def __tempfile_load_filters(self, context):

        filters = defaultdict(dict)
        if context.config.tempfit.photometry is not None:
            for band, photometry in context.config.tempfit.photometry.items():
                if photometry.filter_path is not None:
                    fn = os.path.expandvars(photometry.filter_path)
                    if not os.path.isfile(fn):
                        raise FileNotFoundError(f'Filter file `{fn}` for band `{band}` not found.')
                    else:
                        f = Filter()
                        f.read(fn)
                        f.trim(tol=context.config.tempfit.filter_cutoff)
                        f.calculate_wave_eff()
                        filters[photometry.instrument][band] = f
                        logger.info(f'Using filter curve `{fn}` for band `{band}`.')
                else:
                    raise ValueError(f'No filter path specified for band `{band}` in photometry config.')

        # For each filter, look up the model grid that covers the filter wavelength range
        grids = defaultdict(dict)
        for instrument in filters:
            for band, f in filters[instrument].items():
                found = False
                for arm, grid in context.state.tempfit_grids.items():
                    wave, _, _ = grid.get_wave()
                    if wave[0] < f.wave[0] and f.wave[-1] < wave[-1]:
                        found = True
                        break
                if found:
                    grids[instrument][band] = grid
                else:
                    grids[instrument][band] = None
                    logger.warning(f'No model grid covers the wavelength range {f.wave[[0, -1]]} of filter `{f.name}`.')

        return filters, grids
    
    #endregion
    #region Validate data

    def validate_data(self, context):
        """
        """

        spectra = context.state.tempfit_spectra

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
        context.state.tempfit.spec_norm, context.state.tempfit.temp_norm = context.state.tempfit.get_normalization(context.state.tempfit_spectra)

        # Run the maximum likelihood fitting
        # TODO: add MCMC
        context.state.tempfit_results, context.state.tempfit_state = context.state.tempfit.run_ml(
            context.state.tempfit_spectra,
            fluxes=context.state.tempfit_fluxes
        )

        return PipelineStepResults(success=True, skip_remaining=False, skip_substeps=False)

    def calculate_error(self, context):

        # Calculate the asymptotic error of the individual parameters
        context.state.tempfit_results, context.state.tempfit_state = context.state.tempfit.calculate_error_ml(
            context.state.tempfit_spectra,
            fluxes=context.state.tempfit_fluxes,
            state=context.state.tempfit_state
        )

        return PipelineStepResults(success=True, skip_remaining=False, skip_substeps=False)

    def calculate_covariance(self, context):

        # Calculate the covariance matrix of rv and the template parameters
        context.state.tempfit_results, context.state.tempfit_state = context.state.tempfit.calculate_cov_ml(
            context.state.tempfit_spectra,
            fluxes=context.state.tempfit_fluxes,
            state=context.state.tempfit_state
        )

        return PipelineStepResults(success=True, skip_remaining=False, skip_substeps=False)

    def finish(self, context):
        context.state.tempfit_results, context.state.tempfit_state = context.state.tempfit.finish_ml(
            context.state.tempfit_spectra,
            fluxes=context.state.tempfit_fluxes,
            state=context.state.tempfit_state
        )

        context.state.tempfit_spectra, _ = context.state.tempfit.append_corrections_and_templates(
            context.state.tempfit_spectra, None,
            context.state.tempfit_results.rv_fit,
            context.state.tempfit_results.params_fit,
            context.state.tempfit_results.a_fit,
            match='template',                       # Pull flux to templates instead of pulling templates to observations
            apply_correction=False,
        )

        if context.trace is not None:
            context.trace.on_tempfit_finish_fit(context.state.tempfit_spectra)

        return PipelineStepResults(success=True, skip_remaining=False, skip_substeps=False)
    
    #endregion
    #region Log L map

    def map_log_L(self, context):

        # TODO: bring out parameters: params, ranges

        # Generate a map of log L around the best fit values
        if not context.config.tempfit.map_log_L:
            logger.info('Log L map is disabled, skipping step.')
            return PipelineStepResults(success=True, skip_remaining=False, skip_substeps=False)

        params = context.state.tempfit_results.params_fit
        params_free = context.state.tempfit.determine_free_params(context.state.tempfit.params_fixed)

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

                context.state.tempfit.map_log_L(
                    context.state.tempfit_spectra,
                    size=10,
                    rv=context.state.tempfit_results.rv_fit,
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