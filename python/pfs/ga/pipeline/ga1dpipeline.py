import os
import time
from datetime import datetime
import pytz
import numpy as np

from pfs.datamodel import PfsSingle, Target, Observations, MaskHelper
from pfs.ga.datamodel import PfsGAObject, PfsGAObjectNotes, StellarParams, VelocityCorrections, Abundances

from pfs.ga.pfsspec.core import Physics, Astro
from pfs.ga.pfsspec.core.obsmod.resampling import Binning, FluxConservingResampler, Interp1dResampler
from pfs.ga.pfsspec.core.obsmod.psf import GaussPsf, PcaPsf
from pfs.ga.pfsspec.core.obsmod.snr import QuantileSnr
from pfs.ga.pfsspec.stellar.grid import ModelGrid
from pfs.ga.pfsspec.stellar.rvfit import RVFit, ModelGridRVFit, ModelGridRVFitTrace
from pfs.ga.pfsspec.core.obsmod.stacking import Stacker, StackerTrace

from .setup_logger import logger
from .scripts.script import Script
from .pipeline import Pipeline
from .pipelineerror import PipelineError
from .config import GA1DPipelineConfig
from .ga1dpipelinetrace import GA1DPipelineTrace
from .ga1dspectrum import GA1DSpectrum


class GA1DPipeline(Pipeline):
    """
    Implements the Galactic Archeology Spectrum Processing Pipeline.

    Inputs are the `PfsSingle` files of all individual exposures belonging to the same
    `objId` as a dictionary indexed by `visit`.

    The pipeline processes all exposures of a single object at a time and produces a
    PfsGAObject file which contains the measured parameters, their errors and full covariance
    matrix as well as a co-added spectrum, a continuum-normalized co-added spectrum and a
    best fit continuum model.

    The three main steps of the pipeline are i) line-of-sight velocity estimation,
    ii) abundance estimation and iii) spectrum stacking.
    """

    def __init__(self,
                 script: Script,
                 config: GA1DPipelineConfig,
                 trace: GA1DPipelineTrace = None,
                 pfsSingle: dict = None):
        """
        Initializes a GA Pipeline object for processing of individual exposures of a
        single object.

        Parameters
        ----------
        config: :obj:`GA1DPipelineConfig`
            Configuration of the GA pipeline
        objId: :int:
            Unique object identifier
        pfsSingle : :dict:`int`,`PfsSingle`
            Dictionary of PfsSingle object containing the individual exposures,
            keyed by ˙visit˙.
        """
        
        super().__init__(script=script, config=config, trace=trace)

        self._steps = [
            {
                'name': 'init',
                'func': self.__step_init,
                'critical': True
            },
            {
                'name': 'load',
                'func': self.__step_load,
                'critical': True
            },
            {
                'name': 'vcorr',
                'func': self.__step_vcorr,
                'critical': False
            },
            {
                'name': 'rvfit',
                'func': self.__step_rvfit,
                'critical': False
            },
            {
                'name': 'coadd',
                'func': self.__step_coadd,
                'critical': False
            },
            {
                'name': 'chemfit',
                'func': self.__step_chemfit,
                'critical': False
            },
            {
                'name': 'save',
                'func': self.__step_save,
                'critical': False
            },
            {
                'name': 'cleanup',
                'func': self.__step_cleanup,
                'critical': False
            },
        ]

        self.__pfsSingle = pfsSingle
        self.__identity = None

        self.__spectra = None                   # spectra in PFSSPEC class for each class and visit
        self.__v_corr = None                    # velocity correction for each visit

        self.__rvfit = None                     # RVFit object
        self.__rvfit_results = None             # Results from RVFit

        self.__stacking_results = None          # Results from stacking

        self.__chemfit_results = None           # Results from ChemFit

        self.__pfsGAObject = None

    def __get_pfsGAObject(self):
        return self.__pfsGAObject
    
    pfsGAObject = property(__get_pfsGAObject)

    def _get_log_filename(self):
        return f'gapipe_{self.config.object.objId:016x}.log'
    
    def _get_log_message_step_start(self, name):
        return f'Executing GA pipeline step `{name}` for objID={self.config.object.objId:016x}.'

    def _get_log_message_step_stop(self, name, elapsed_time):
        return f'GA pipeline step `{name}` for objID={self.config.object.objId:016x} completed successfully in {elapsed_time:.3f} seconds.'

    def _get_log_message_step_error(self, name, ex):
        return f'GA pipeline step `{name}` for objID={self.config.object.objId:016x} failed with error `{type(ex).__name__}`.'

    def _validate_config(self):
        """
        Validates the configuration and the existence of all necessary input data. Returns
        `True` if the pipeline can proceed or 'False' if it cannot.

        Return
        -------
        :bool:
            `True` if the pipeline can proceed or 'False' if it cannot.
        """

        # TODO: put this back but check if outdir is to be created automatically
        #       also add option to test if outfile exists
        # if not os.path.isdir(self.config.workdir):
        #     raise FileNotFoundError(f'Working directory `{self.config.workdir}` does not exist.')
        
        if not os.path.isdir(self.config.datadir):
            raise FileNotFoundError(f'Data directory `{self.config.datadir}` does not exist.')

        if not os.path.isdir(os.path.join(self.config.datadir, self.config.rerundir)):
            raise FileNotFoundError(f'Rerun directory `{self.config.rerundir}` does not exist.')
        
        if self.config.run_rvfit:
            for arm in self.config.arms:
                fn = self.config.rvfit.model_grid_path.format(arm=arm)
                if not os.path.isfile(fn):
                    raise FileNotFoundError(f'Synthetic spectrum grid `{fn}` not found.')
                
                if self.config.rvfit.psf_path is not None:
                    fn = self.config.rvfit.psf_path.format(arm=arm)
                    if not os.path.isfile(fn):
                        raise FileNotFoundError(f'PSF file `{fn}` not found.')

        return True
    
    def validate_libs(self):
        # TODO: write code to validate library versions and log git hash for each when available
        pass

    #region Object and visit identity

    def __copy_target(self, orig):
        # Copy the target object
        return Target(
            catId = orig.catId,
            tract = orig.tract,
            patch = orig.patch,
            objId = orig.objId,
            ra = orig.ra,
            dec = orig.dec,
            targetType = orig.targetType,
            fiberFlux = orig.fiberFlux,
        )
    
    def __merge_observations(self, observations):
        # Merge observations into a single object
        visit = np.concatenate([ o.visit for o in observations ])
        arm = np.concatenate([ o.arm for o in observations ])
        spectrograph = np.concatenate([ o.spectrograph for o in observations ])
        pfsDesignId = np.concatenate([ o.pfsDesignId for o in observations ])
        fiberId = np.concatenate([ o.fiberId for o in observations ])
        pfiNominal = np.concatenate([ o.pfiNominal for o in observations ])
        pfiCenter = np.concatenate([ o.pfiCenter for o in observations ])

        return Observations(
            visit = visit,
            arm = arm,
            spectrograph = spectrograph,
            pfsDesignId = pfsDesignId,
            fiberId = fiberId,
            pfiNominal = pfiNominal,
            pfiCenter = pfiCenter,
        )
    
    def __get_identity(self):
        """
        Returns an identity generated from the pfsSingle objects.
        """

        first_visit = sorted(list(self.__pfsSingle.keys()))[0]
        target = self.__copy_target(self.__pfsSingle[first_visit].target)
        observations = self.__merge_observations([ self.__pfsSingle[visit].observations for visit in sorted(self.__pfsSingle.keys()) ])

        identity = target.identity
        identity.update(observations.getIdentity())

        return target, observations, identity

    #endregion

    def __step_init(self):
        # Create output directories
        self._create_dir(self.config.outdir, 'output')
        self._create_dir(self.config.figdir, 'figure')

    def __step_load(self):
        # Load each PfsSingle file.

        # TODO: skip loading files if the object are already passed to the pipeline
        #       from the outside

        self.__pfsSingle = {}

        start_time = time.perf_counter()

        for i, visit in enumerate(sorted(self.config.object.visits.keys())):
            identity = {
                'objId': self.config.object.objId,
                'catId': self.config.object.catId,
                'tract': self.config.object.tract,
                'patch': self.config.object.patch,
                'visit': visit
            }            
            self.__pfsSingle[visit] = self.__load_pfsSingle(identity)

        self.__target, self.__observations, self.__identity = self.__get_identity()
        self.__id = ('{catId:05d}-{tract:05d}-{patch}-{objId:016x}' + \
                    '-{nVisit:03d}-0x{pfsVisitHash:016x}').format(**self.__identity)
        
        if self.trace is not None:
            self.trace.id = self.__id

        # Extract the spectra of individual arms from the pfsSingle objects
        avail_arms = self.__get_avail_arms()
        # TODO: union with abund use arms and coadd use arms?
        fit_arms = set(self.config.rvfit.fit_arms)
        use_arms = avail_arms.intersection(fit_arms)

        if len(use_arms) < len(fit_arms):
            # TODO: list missing arms, include visit IDs
            logger.warning(f'Not all arms required to run the pipeline are available in the observations for Object ID `{self.config.object.objId}.')

        # TODO: do we want to load arms that we don't fit?
        #       consider taking the intersection of avail_arms and fit_arms
        #       and raise a warning when an arm is missing

        self.__spectra = self.__read_spectra(use_arms)

        self.__load_validate()

        stop_time = time.perf_counter()
        logger.info(f'PFS data files loaded successfully for {len(self.__pfsSingle)} exposures in {stop_time - start_time:.3f} s.')

        if self.trace is not None:
            spectra = self.__rvfit_collect_spectra(use_arms)
            self.trace.on_load(spectra)

    def __load_pfsSingle(self, identity):
        dir = os.path.join(self.config.datadir,
                           self.config.rerundir,
                           'pfsSingle/{catId:05d}/{tract:05d}/{patch}'.format(**identity))
        fn = PfsSingle.filenameFormat % identity
        
        logger.info(f'Loading PfsSingle from `{os.path.join(dir, fn)}`.')

        start_time = time.perf_counter()
        pfsSingle = PfsSingle.read(identity, dirName=dir)
        stop_time = time.perf_counter()

        logger.info(f'Loaded PfsSingle from `{os.path.join(dir, fn)}` in {stop_time - start_time:.3f} s.')

        return pfsSingle
    
    def __load_validate(self):
        # Extract info from pfsSingle objects one by one and perform
        # some validation steps

        target = None

        for visit in self.__pfsSingle.keys():
            pfsSingle = self.__pfsSingle[visit]
            self.__load_validate_pfsSingle(visit, pfsSingle)

            # Make sure that targets are the same
            if target is None:
                target = pfsSingle.target
            elif not target == pfsSingle.target:
                raise PipelineError(f'Target information in PfsSingle files do not match.')

        # TODO: Count spectra per arm and write report to log

        self.__rvfit_validate()
    
    def __load_validate_pfsSingle(self, visit, pfsSingle):
        fn = pfsSingle.filenameFormat % {**pfsSingle.getIdentity(), 'visit': visit}

        # Verify that it is a single visit and not a co-add
        if pfsSingle.nVisit != 1:
            raise PipelineError('More than one visit found in `{pfsSingle.filename}`')
        
        # Verify that visit numbers match
        if visit not in pfsSingle.observations.visit:
            raise PipelineError(f'Visit does not match visit ID found in `{fn}`.')
        
        if pfsSingle.target.catId != self.config.object.catId:
            raise PipelineError(f'catId in config `{self.config.object.catId}` does not match catID in `{fn}`.')

        if pfsSingle.target.objId != self.config.object.objId:
            raise PipelineError(f'objId in config `{self.config.object.objId}` does not match objID in `{fn}`.')
        
        # TODO: compare flags and throw a warning if bits are not the same in every file

        # TODO: write log message

    def __get_avail_arms(self):
        """
        Return a set of arms that are available in the observations.
        """

        # TODO: add option to require that all observations contain all arms

        # Collect all arms available in observations

        arms = set()
        for visit, pfsSingle in self.__pfsSingle.items():
            for arm in pfsSingle.observations.arm[0]:
                if arm not in arms:
                    arms.add(arm)

        return arms
    
    def __read_spectra(self, arms):
        # Extract spectra from the fluxtables of pfsSingle objects

        start_time = time.perf_counter()
        read = 0
        skipped = 0

        spectra = { arm: {} for arm in arms }

        for i, visit in enumerate(sorted(self.__pfsSingle.keys())):
            pfsSingle = self.__pfsSingle[visit]

            # nm -> A            
            wave = Physics.nm_to_angstrom(pfsSingle.fluxTable.wavelength)

            # Slice up by arm
            for arm in arms:
                # Generate a mask based on arm limits
                arm_mask = (self.config.arms[arm]['wave'][0] <= wave) & (wave <= self.config.arms[arm]['wave'][1])

                if arm not in pfsSingle.observations.arm[0] or arm_mask.sum() == 0:
                    s = None
                    skipped += 1
                else:
                    s = self.__read_spectrum(i, arm, visit, pfsSingle, arm_mask)
                    read += 1

                spectra[arm][visit] = s

        stop_time = time.perf_counter()
        logger.info(f'Extracted {read} and skipped {skipped} spectra from PfsSingle files in {stop_time - start_time:.3f} s.')

        return spectra
    
    def __read_spectrum(self, i, arm, visit, pfsSingle, arm_mask):
        # TODO: consider moving this routine to pfs.ga.pfsspec.survey

        # TODO: What if the arms overlap?

        # TODO: What if arm doesn't exist for a certain exposure? How to tell?
        #       Is metadata consistent with the fluxtable?
        
        s = GA1DSpectrum()

        # Extract header information

        s.index = i
        s.arm = arm
        s.catId = pfsSingle.target.catId
        s.objId = s.id = pfsSingle.target.objId
        s.tract = pfsSingle.target.tract
        s.patch = pfsSingle.target.patch
        s.visit = pfsSingle.observations.visit[0]
        s.spectrograph = pfsSingle.observations.spectrograph[0]
        s.fiberid = pfsSingle.observations.fiberId[0]

        # TODO: where do we take these from?
        # s.exp_count = 1
        # s.exp_time = 0
        # s.seeing = 0
        # s.obs_time

        # Get coordinates, observation time, airmass
        s.ra = pfsSingle.target.ra
        s.dec = pfsSingle.target.dec

        # Extract the spectrum
        wave = Physics.nm_to_angstrom(pfsSingle.fluxTable.wavelength)
        # nJy -> erg s-1 cm-2 A-1
        flux = 1e-32 * Physics.fnu_to_flam(wave, pfsSingle.fluxTable.flux)
        flux_err = 1e-32 * Physics.fnu_to_flam(wave, pfsSingle.fluxTable.error)
        mask = pfsSingle.fluxTable.mask

        s.wave = wave[arm_mask]
        s.wave_edges = Binning.find_wave_edges(s.wave)
        s.flux = flux[arm_mask]
        s.flux_err = flux_err[arm_mask]

        # TODO: add logic to accept some masked pixels if the number of unmasked pixels is low
        s.mask = mask[arm_mask]

        # Consider using different mask bits for each pipeline step
        # TODO: are mask bits the same for a rerun or they vary from file to file?
        s.mask_bits = self.__get_mask_bits(pfsSingle, self.config.mask_flags)

        # Make sure pixels with nan and inf are masked
        s.mask = np.where(np.isnan(s.flux) | np.isinf(s.flux) | np.isnan(s.flux_err) | np.isinf(s.flux_err),
                          s.mask | pfsSingle.flags['UNMASKEDNAN'],
                          s.mask)

        # SNR
        # TODO: make this configurable?
        s.calculate_snr(QuantileSnr(0.75, binning=self.config.arms[arm]['pix_per_res']))

        # Target PSF magnitude from metadata
        if self.config.ref_mag in pfsSingle.target.fiberFlux:
            # Convert nJy to ABmag
            flux = 1e-9 * pfsSingle.target.fiberFlux[self.config.ref_mag]
            mag = Physics.jy_to_abmag(flux)
            s.mag = mag
        else:
            s.mag = np.nan

        # Write number of masked/unmasked pixels to log
        # TODO: review message and add IDs
        mm = s.mask_as_bool()
        logger.info(f'Spectrum contains masked pixels, {np.sum(~mm)} unmasked pixels.')

        self.__calc_spectrum_params(s)

        return s
    
    def __calc_spectrum_params(self, s):
        # s.airmass

        # TODO: read MJD from somewhere
        # Convert datetime to MJD using astropy
        # Create datetime with UTC time zone (Hawaii: UTC - 10)
        s.mjd = Astro.datetime_to_mjd(datetime(2023, 7, 24, 14, 0, 0, tzinfo=pytz.timezone('UTC')))
        s.alt, s.az = Astro.radec_to_altaz(s.ra, s.dec, s.mjd)
        
        pass

    def __get_mask_bits(self, pfsSingle, mask_flags):
        if mask_flags is None:
            return None
        else:
            mask_bits = 0
            for flag in mask_flags:
                mask_bits |= pfsSingle.flags[flag]
            return mask_bits

    #region v_corr

    # TODO: these have to be moved to the 2D pipeline

    def __step_vcorr(self):
        if self.config.v_corr is not None and self.config.v_corr.lower() != 'none':
            self.__v_corr_calculate()
            self.__v_corr_apply()
        else:
            logger.info('Velocity correction for geocentric frame is set to `none`, skipping corrections.')
        
    def __v_corr_calculate(self):
        
        # TODO: logging + perf counter
        
        self.__v_corr = {}

        # Calculate the velocity correction for each spectrum
        for arm in self.__spectra:
            for visit in self.__spectra[arm]:
                s = self.__spectra[arm][visit]
                if s is not None and visit not in self.__v_corr:
                    self.__v_corr[visit] = Astro.v_corr(self.config.v_corr, s.ra, s.dec, s.mjd)

    def __v_corr_apply(self):

        # Apply the velocity correction for each spectrum
        for arm in self.__spectra:
            for visit in self.__spectra[arm]:
                s = self.__spectra[arm][visit]

                if s is not None:
                    # Apply the correction to the spectrum
                    # TODO: verify this and convert into a function on spectrum
                    z = Physics.vel_to_z(self.__v_corr[visit])
                    s.apply_v_corr(z=z)

    #endregion     
    #region RVFIT
        
    def __step_rvfit(self):
        if not self.config.run_rvfit:
            logger.info('RV fitting is disabled, skipping step.')
            
        # Run RV fitting

        logger.info(f'Starting RVFit...')
        start_time = time.perf_counter()

        # Collect arms that can be used for fitting
        avail_arms = set(self.__spectra.keys())
        fit_arms = set(self.config.rvfit.fit_arms)
        use_arms = avail_arms.intersection(fit_arms)

        template_grids = self.__rvfit_load_grid(use_arms)
        template_psfs = self.__rvfit_load_psf(use_arms, template_grids)
        self.__rvfit = self.__rvfit_init(template_grids, template_psfs)

        # Collect spectra in a format that can be passed to RVFit
        spectra = self.__rvfit_collect_spectra(use_arms)

        # TODO: validate available spectra here and throw warning if any of the arms are missing after
        #       filtering based on masks

        self.__rvfit_preprocess(spectra)

        # Determine the normalization factor to be used to keep continuum coefficients unity
        self.__rvfit.spec_norm, self.__rvfit.temp_norm = self.__rvfit.get_normalization(spectra)

        # Run the maximum likelihood fitting
        self.__rvfit_results = self.__rvfit.fit_rv(spectra)

        self.__rvfit_cleanup()

        stop_time = time.perf_counter()
        logger.info(f'Successfully executed RVFit in {stop_time - start_time:.3f} s.')
    
    def __rvfit_validate(self):
        # Find a unique set of available arms in the pfsSingle files

        # NOTE: for some reason, pfsSingle.observations.arm is a char array that contains a single string
        #       in item 0 and not an array of characters

        # Verify that all arms defined in the config are available
        avail_arms = set(self.__spectra.keys())
        fit_arms = set()
        for arm in self.config.rvfit.fit_arms:
            if self.config.rvfit.require_all_arms and arm not in avail_arms:
                raise PipelineError(f'RVFIT requires arm `{arm}` which is not observed.')
            elif arm not in avail_arms:
                logger.warning(f'RVFIT requires arm `{arm}` which is not observed.')
    
    def __rvfit_load_grid(self, arms):
        # Load template grids. Make sure each grid is only loaded once, if grid is
        # independent of arm.

        grids = {}
        for arm in arms:
            fn = self.config.rvfit.model_grid_path.format(arm)
            skip = False
            for _, ˇgrid in grids.items():
                if ˇgrid.filename == fn:
                    grids[arm] = ˇgrid
                    skip = True
                    break

            if not skip:
                grid = ModelGrid.from_file(fn, 
                                           preload_arrays=self.config.rvfit.model_grid_preload,
                                           mmap_arrays=self.config.rvfit.model_grid_mmap, 
                                           args=self.config.rvfit.model_grid_args,
                                           slice_from_args=False)
                if grid.wave_edges is None:
                    grid.wave_edges = FluxConservingResampler().find_wave_edges(grid.wave)

                grids[arm] = grid

        return grids
    
    def __rvfit_load_psf(self, arms, grids):
        # Right now load a PSF file generate by the ETC        
        # TODO: Modify this to use PSF from 2D pipeline instead of ETC
        
        psfs = {}
        for arm in arms:
            fn = self.config.rvfit.psf_path.format(arm=arm)
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
    
    def __rvfit_init(self, template_grids, template_psfs):
        trace = ModelGridRVFitTrace(
            id=self.__id,
            figdir=self.config.figdir,
            logdir=self.config.logdir)
        
        if self.trace is not None:
            trace.figure_formats = self.trace.figure_formats
        
        rvfit = ModelGridRVFit(trace)

        rvfit.template_grids = template_grids
        rvfit.template_psf = template_psfs

        rvfit.init_from_args(None, None, self.config.rvfit.rvfit_args)
        rvfit.trace.init_from_args(None, None, self.config.rvfit.trace_args)

        return rvfit
    
    def __rvfit_collect_spectra(self, use_arms, skip_fully_masked=False, skip_mosly_masked=False, skip_none=False, mask_bits=None):
        # Collect spectra that will be used to fit the RV and stacking
        # Only add those arms where at least on spectrum is available

        spectra = {}
        for arm in use_arms:
            for visit in sorted(self.__spectra[arm].keys()):
                spec = self.__spectra[arm][visit]
                if spec is not None:

                    if self.__rvfit is not None:
                        mask = self.__rvfit.get_full_mask(spec, mask_bits=mask_bits)
                    else:
                        mask = spec.mask_as_bool(bits=mask_bits)

                    if mask.sum() == 0:
                        logger.warning(f'All pixels in arm `{arm}` for visit `{visit}` are masked.')
                        if skip_fully_masked:
                            continue
                        else:
                            # Skip this spectrum because it is fully masked
                            spec = None
                    elif mask.sum() < self.config.rvfit.min_unmasked_pixels:
                        logger.warning(f'Not enough unmasked pixels in arm `{arm}` for visit `{visit}`.')
                        if skip_mosly_masked:
                            continue

                if not skip_none or spec is not None:
                    if arm not in spectra:
                        spectra[arm] = []
                    spectra[arm].append(spec)

        return spectra
    
    def __rvfit_preprocess(self, spectra):
        pass

    def __rvfit_cleanup(self):
        pass

    #endregion
    #region Co-add

    def __step_coadd(self):
        if not self.config.run_rvfit:
            logger.info('Spectrum stacking required RV fitting which is disabled, skipping step.')
            return
        elif not self.config.run_coadd:
            logger.info('Spectrum stacking is disabled, skipping step.')
            return
        
        # Coadd the spectra

        first_visit = sorted(list(self.__pfsSingle.keys()))[0]
        no_data_bit = self.__pfsSingle[first_visit].flags['NO_DATA']

        # Collect arms that can be used for fitting
        # We can only coadd spectra that have been processed through RV fit

        avail_arms = set(self.__spectra.keys())
        rvfit_arms = set(self.config.rvfit.fit_arms)
        stack_arms = set(self.config.coadd.coadd_arms)
        use_arms = avail_arms.intersection(rvfit_arms.intersection(stack_arms))

        spectra = self.__rvfit_collect_spectra(use_arms)

        # TODO: Validate here

        # TODO: v_corr has already been applied to the spectra, so we only have to apply the
        #       flux correction here. Since the flux correction normalizes the spectra to an
        #       arbitrary value of unity, we'll have to recalibrate the flux based on broadband
        #       magnitudes or else.

        self.__stacker = self.__coadd_init()

        # TODO: now we stack each arm separately and merge at the end. This will have to change
        #       we want to process spectra that overlap between arms

        templates = self.__coadd_get_templates(spectra)
        flux_corr = self.__coadd_eval_flux_corr(spectra, templates)

        self.__stacking_results = {}
        for arm in spectra:
            # Only stack those spectra that are not None
            ss = [ s for s in spectra[arm] if s is not None ]
            fc = [ f for f in flux_corr[arm] if f is not None ]
            if len(ss) > 0:
                stacked_wave, stacked_wave_edges, stacked_flux, stacked_error, stacked_weight, stacked_mask = \
                    self.__stacker.stack(ss, flux_corr=fc)
                
                # Mask out bins where the weight is zero
                stacked_mask = np.where(stacked_weight == 0, stacked_mask | no_data_bit, stacked_mask)

                # Create a spectrum
                spec = GA1DSpectrum()
                spec.wave = stacked_wave
                spec.wave_edges = stacked_wave_edges
                spec.flux = stacked_flux
                spec.flux_err = stacked_error
                spec.mask = stacked_mask

                self.__stacking_results[arm] = spec
            else:
                self.__stacking_results[arm] = None

        # TODO: trace hook
        pass
    
    def __coadd_init(self):
        trace = StackerTrace(
            id=self.__id,
            figdir=self.config.figdir,
            logdir=self.config.logdir)
        
        if self.trace is not None:
            trace.figure_formats = [ '.png' ]

        stacker = Stacker(trace)

        stacker.init_from_args(None, None, self.config.coadd.stacker_args)
        stacker.trace.init_from_args(None, None, self.config.coadd.trace_args)

        return stacker
    
    def __coadd_get_templates(self, spectra):
        # Return the templates at the best fit parameters

        # Interpolate the templates to the best fit parameters
        templates, missing = self.__rvfit.get_templates(
            spectra,
            self.__rvfit_results.params_fit)
        
        if self.trace is not None:
            self.trace.on_coadd_get_templates(spectra, templates)

        return templates
    
    def __coadd_eval_flux_corr(self, spectra, templates):
        # Evaluate the flux correction for every exposure of each arm.

        flux_corr = RVFit.eval_flux_corr(
            self.__rvfit,
            spectra,
            templates,
            self.__rvfit_results.rv_fit,
            a=self.__rvfit_results.a_fit)
        
        if self.trace is not None:
            self.trace.on_coadd_eval_flux_corr(spectra, templates, flux_corr, self.__rvfit.spec_norm, self.__rvfit.temp_norm)
        
        return flux_corr

    #endregion
    #region CHEMFIT

    def __step_chemfit(self):
        if not self.config.run_chemfit:
            logger.info('Chemical abundance fitting is disabled, skipping...')
            return
        
        # TODO: run abundance fitting
        raise NotImplementedError()
    
    #endregion

    def __step_save(self):
        # Construct the output object based on the results from the pipeline steps

        # Copy target from any of the PfsSingle objects
        first_visit = sorted(list(self.__pfsSingle.keys()))[0]
        target = self.__copy_target(self.__pfsSingle[first_visit].target)
        observations = self.__merge_observations([ self.__pfsSingle[visit].observations for visit in sorted(self.__pfsSingle.keys()) ])
        flags = self.__copy_flags(self.__pfsSingle[first_visit].flags)
        metadata = {}       # TODO


        # TODO: replace this with the stacked spectrum
        wavelength = self.__pfsSingle[first_visit].wavelength
        flux = self.__pfsSingle[first_visit].flux
        mask = self.__pfsSingle[first_visit].mask
        sky = self.__pfsSingle[first_visit].sky
        covar = self.__pfsSingle[first_visit].covar
        covar2 = self.__pfsSingle[first_visit].covar2

        # TODO: replace this with the stacked spectrum
        flux_table = None

        stellar_params = self.__get_stellar_params()
        stellar_params_covar = self.__rvfit_results.cov
        velocity_corrections = self.__get_velocity_corrections()
        abundances = self.__get_abundances()
        abundances_covar = None
        notes = PfsGAObjectNotes()

        self.__pfsGAObject = PfsGAObject(target, observations,
                                  wavelength, flux, mask, sky, covar, covar2,
                                  flags, metadata,
                                  flux_table,
                                  stellar_params,
                                  velocity_corrections,
                                  abundances,
                                  stellar_params_covar,
                                  abundances_covar,
                                  notes)

        # Save output FITS file
        id = self.__pfsGAObject.getIdentity()
        fn = os.path.join(self.config.outdir, PfsGAObject.filenameFormat % id)
        self.__pfsGAObject.writeFits(fn)
    
    def __copy_flags(self, flags):
        return MaskHelper(**flags.flags)
    
    def __get_stellar_params(self):
        # Extract stellar parameters from rvfit results

        # Collect parameters
        units = {
            'T_eff': 'K',
            'log_g': 'dex',
            'M_H': 'dex',
            'a_M': 'dex',
            'v_los': 'km s-1',
        }
        params_fit = self.__rvfit_results.params_free + [ 'v_los' ]
        params_all = params_fit + [ p for p in self.__rvfit_results.params_fit if p not in params_fit ]

        # Construct columns
        method = [ 'ga1dpipe' for p in params_all ]
        frame = [ 'bary' for p in params_all ]
        param = [ p for p in params_all ]
        covarId = [ params_fit.index(p) if p in params_fit else 255 for p in params_all ]
        unit = [ units[p] for p in params_all ]
        value = [ self.__rvfit_results.params_fit[p] for p in self.__rvfit_results.params_free ] + \
                [ self.__rvfit_results.rv_fit ] + \
                [ self.__rvfit_results.params_fit[p] for p in self.__rvfit_results.params_fit if p not in params_fit ]
        value_err = [ self.__rvfit_results.params_err[p] for p in self.__rvfit_results.params_free ] + \
                [ self.__rvfit_results.rv_err ] + \
                [ self.__rvfit_results.params_err[p] for p in self.__rvfit_results.params_fit if p not in params_fit ]
        flag = [ False for p in params_all ]

        # TODO: we currently have no means of detecting bad fits
        status = [ '' for p in params_all ]

        return StellarParams(
            method=np.array(method),
            frame=np.array(frame),
            param=np.array(param),
            covarId=np.array(covarId),
            unit=np.array(unit),
            value=np.array(value),
            valueErr=np.array(value_err),
            flag=np.array(flag),
            status=np.array(status),
        )
    
    def __get_velocity_corrections(self):
        visit = list(sorted(self.__pfsSingle.keys()))

        # TODO: not obs time data in any of the headers!
        JD = [ 0.0 for v in visit]
        helio = [ 0.0 for v in visit]
        bary = [ 0.0 for v in visit]

        return VelocityCorrections(
            visit=np.array(visit),
            JD=np.array(JD),
            helio=np.array(helio),
            bary=np.array(bary),
        )
    
    def __get_abundances(self):
        # TODO: implement this
        return Abundances(
            method = np.array([], dtype=str),
            element = np.array([], dtype=str),
            covarId = np.array([], dtype=np.int8),
            value = np.array([], dtype=np.float32),
            valueErr = np.array([], dtype=np.float32),
        )

    def __step_cleanup(self):
        # TODO: Perform any cleanup
        pass
