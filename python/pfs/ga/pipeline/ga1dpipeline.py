import os
import time
from datetime import datetime
import pytz
import numpy as np
from types import SimpleNamespace

from pfs.datamodel import *
from pfs.datamodel import PfsGAObject, PfsGAObjectNotes, StellarParams, VelocityCorrections, Abundances

from pfs.ga.pfsspec.core import Physics, Astro
from pfs.ga.pfsspec.core.obsmod.resampling import FluxConservingResampler, Interp1dResampler
from pfs.ga.pfsspec.core.obsmod.psf import GaussPsf, PcaPsf
from pfs.ga.pfsspec.core.obsmod.snr import QuantileSnr
from pfs.ga.pfsspec.core.obsmod.stacking import Stacker, StackerTrace
from pfs.ga.pfsspec.stellar.grid import ModelGrid
from pfs.ga.pfsspec.stellar.tempfit import TempFit, ModelGridTempFit, ModelGridTempFitTrace, CORRECTION_MODELS
from pfs.ga.pfsspec.survey.pfs import PfsStellarSpectrum
from pfs.ga.pfsspec.survey.pfs.io import PfsStellarSpectrumReader

from .setup_logger import logger

from .constants import Constants
from .util import Timer
from .scripts.script import Script
from .pipeline import Pipeline, StepResults
from .pipelineerror import PipelineError
from .config import GA1DPipelineConfig
from .ga1dpipelinetrace import GA1DPipelineTrace
from .data import FileSystemConnector

class GA1DPipeline(Pipeline):
    """
    Implements the Galactic Archeology Spectrum Processing Pipeline.

    Inputs are the `PfsSingle` files of all individual exposures belonging to the same
    `objId` as a dictionary indexed by `visit`. If the inputs are not provided on initialization,
    the files will be loaded based on the configuration object.

    The pipeline processes all exposures of a single object at a time and produces a
    PfsGAObject file which contains the measured parameters, their errors and full covariance
    matrix as well as a co-added spectrum, a continuum-normalized co-added spectrum and a
    best fit continuum model.

    The three main steps of the pipeline are i) line-of-sight velocity estimation,
    ii) abundance estimation and iii) spectrum stacking.
    """

    def __init__(self,
                 script: Script = None,
                 config: GA1DPipelineConfig = None,
                 connector: FileSystemConnector = None,
                 trace: GA1DPipelineTrace = None,
                 id: str = None):
        """
        Initializes a GA Pipeline object for processing of individual exposures of a
        single object.

        Parameters
        ----------
        script: :obj:`Script`
            Script object for logging and command-line arguments
        config: :obj:`GA1DPipelineConfig`
            Configuration of the GA pipeline
        connector: :obj:`FileSystemConnector`
        trace: :obj:`GA1DPipelineTrace`
            Trace object for logging and plotting
        """
        
        super().__init__(script=script, config=config, trace=trace)

        self.__id = id                          # Identity represented as string
        self.__connector = connector

        self._steps = self.__create_steps()
       
        # list of data products to be loaded
        self.__required_product_types = [ PfsSingle, PfsMerged ]
        self.__product_cache = None             # cache of loaded products

        self.__output_product_type = PfsGAObject
        self.__output_product = None            # output product

        self.__pfsSingle = None                 # dict of pfsSingle, indexed by visit
        self.__pfsMerged = None                 # dict of pfsMerged, indexed by visit
        self.__pfsObject = None                 # dict of pfsObject, indexed by visit
        self.__pfsConfig = None                 # dict of pfsConfig, indexed by visit
        self.__pfsGAObject = None

        self.__spectra = None                   # spectra in PFSSPEC class for each class and visit
        self.__v_corr = None                    # velocity correction for each visit

        self.__rvfit = None                     # RVFit object
        self.__rvfit_arms = None                # Arms used for RV fitting
        self.__rvfit_spectra = None             # Spectra used for RV fitting
        self.__rvfit_grids = None               # Template grids for RV fitting
        self.__rvfit_psfs = None                # PSFs for RV fitting
        self.__rvfit_results = None             # Results from RVFit

        self.__stacking_results = None          # Results from stacking

        self.__chemfit_results = None           # Results from ChemFit

    def reset(self):
        super().reset()

    def update(self, script=None, config=None, connector=None, trace=None, id=None):
        super().update(script=script, config=config, trace=trace)

        self.__connector = connector if connector is not None else self.__connector
        self.__id = id if id is not None else self.__id

        if self.trace is not None:
            self.trace.update(id=self.__id)

        # TODO: reset anything else?

    def __create_steps(self):
        return [
            {
                'name': 'init',
                'func': self.__step_init,
                'critical': True,
            },
            {
                'name': 'load',
                'func': self.__step_load,
                'critical': True,
                'substeps': [
                    {
                        'name': 'load_validate',
                        'func': self.__step_load_validate,
                        'critical': True
                    }
                ]
            },
            {
                'name': 'vcorr',
                'func': self.__step_vcorr,
                'critical': False
            },
            {
                'name': 'rvfit',
                'func': self.__step_rvfit,
                'critical': False,
                'substeps': [
                    {
                        'name': 'rvfit_load',
                        'func': self.__step_rvfit_load,
                        'critical': True,
                    },
                    {
                        'name': 'rvfit_preprocess',
                        'func': self.__step_rvfit_preprocess,
                        'critical': True,
                    },
                    {
                        'name': 'rvfit_fit',
                        'func': self.__step_rvfit_fit,
                        'critical': True,
                    },
                    {
                        'name': 'rvfit_cleanup',
                        'func': self.__step_rvfit_cleanup,
                        'critical': True,
                    },
                ]
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

    #region Properties

    def __get_id(self):
        return self.__id
    
    def __set_id(self, value):
        self.__id = value

    id = property(__get_id, __set_id)

    def __get_pfsGAObject(self):
        return self.__pfsGAObject
    
    pfsGAObject = property(__get_pfsGAObject)

    #endregion

    def get_product_workdir(self):
        return self.__connector.format_dir(GA1DPipelineConfig,
                                           self.config.target.identity,
                                           variables={ 'datadir': self.config.workdir })

    def get_product_outdir(self):
        return self.__connector.format_dir(PfsGAObject,
                                           self.config.target.identity,
                                           variables={ 'datadir': self.config.outdir })

    def get_loglevel(self):
        return self.config.loglevel
    
    def get_product_logdir(self):
        """
        Return the directory were the log belonging to the product of this
        pipeline will be stored.
        """

        dir = self.get_product_workdir()
        if self.config.logdir is not None:
            return os.path.join(dir, self.config.logdir)
        else:
            return dir

    def get_product_logfile(self):
        """
        Return the full path to the logfile for the currently processed product.
        """

        dir = self.get_product_workdir()
        filename = self.__connector.format_filename(GA1DPipelineConfig, self.config.target.identity)
        filename, _ = os.path.splitext(filename)
        if self.config.logdir is not None:
            return os.path.join(dir, self.config.logdir, filename + '.log')
        else:
            return os.path.join(dir, filename + '.log')
        
    def get_product_figdir(self):
        """
        Return the directory were the figures belonging to the product of this
        pipeline will be stored.
        """

        dir = self.get_product_workdir()
        if self.config.figdir is not None:
            return os.path.join(dir, self.config.figdir)
        else:
            return dir
    
    def _get_log_message_step_start(self, name):
        return f'Executing GA pipeline step `{name}` for {self.__id}'

    def _get_log_message_step_stop(self, name):
        return f'GA pipeline step `{name}` for {self.__id} completed successfully in {{elapsed_time:.3f}} seconds.'

    def _get_log_message_step_error(self, name, ex):
        return f'GA pipeline step `{name}` for {self.__id} failed with error `{type(ex).__name__}`.'

    def validate_config(self):
        """
        Validates the configuration and the existence of all necessary input data. Returns
        `True` if the pipeline can proceed or 'False' if it cannot.

        Return
        -------
        :bool:
            `True` if the pipeline can proceed or 'False' if it cannot.
        """

        # Verify output and log directories
        self._test_dir('output', self.config.outdir, must_exist=False)
        self._test_dir('work', self.config.workdir, must_exist=False)
        self._test_dir('log', self.get_product_logdir(), must_exist=False)
        self._test_dir('figure', self.get_product_figdir(), must_exist=False)
        
        self._test_dir('data', self.__connector.get_resolved_variable('datadir'))
        self._test_dir('rerun', os.path.join(self.__connector.get_resolved_variable('datadir'),
                                             'rerun',
                                             self.__connector.get_resolved_variable('rerundir')))
        
        return True
    
    def validate_libs(self):
        # TODO: write code to validate library versions and log git hash for each when available
        pass

    #region Object and visit identity

    def __copy_target(self, orig):
        # TODO: do we need this?
        raise NotImplementedError()

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
        # TODO: do we need this? We already have Observations object
        raise NotImplementedError()

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

        # TODO: is this function needed?
        raise NotImplementedError()

        first_visit = sorted(list(self.__pfsSingle.keys()))[0]
        target = self.__copy_target(self.__pfsSingle[first_visit].target)
        observations = self.__merge_observations([ self.__pfsSingle[visit].observations for visit in sorted(self.__pfsSingle.keys()) ])

        identity = target.identity
        identity.update(observations.getIdentity())

        return target, observations, identity
    
    def __enumerate_visits(self):
        """
        Enumerate the visits in the configs and return an identity for each.
        """

        for i, visit in enumerate(sorted(self.config.target.observations.visit)):
            identity = SimpleNamespace(**self.config.target.get_identity(visit))
            yield i, visit, identity

    #endregion
    #region Step: Init

    def __step_init(self):
        # Save the full configuration to the output directory, if it's not already there
        dir = self.get_product_workdir()
        fn = self.__connector.format_filename(GA1DPipelineConfig, self.config.target.identity)
        fn = os.path.join(dir, fn)
        if not os.path.isfile(fn):
            self.config.save(fn)
            logger.info(f'Runtime configuration file saved to `{fn}`.')

        # Verify stellar template grids and PSF files
        if self.config.run_rvfit:
            for arm in self.config.rvfit.fit_arms:
                if isinstance(self.config.rvfit.model_grid_path, dict):
                    fn = self.config.rvfit.model_grid_path[arm].format(arm=arm)
                else:
                    fn = self.config.rvfit.model_grid_path.format(arm=arm)
                
                if not os.path.isfile(fn):
                    raise FileNotFoundError(f'Synthetic spectrum grid `{fn}` not found.')
                else:
                    logger.info(f'Using synthetic spectrum grid `{fn}` for arm `{arm}`.')
                
                if self.config.rvfit.psf_path is not None:
                    fn = self.config.rvfit.psf_path.format(arm=arm)
                    if not os.path.isfile(fn):
                        raise FileNotFoundError(f'PSF file `{fn}` not found.')
                    else:
                        logger.info(f'Using PSF file `{fn}` for arm `{arm}`.')

        # Verify that input data files are available or the input products
        # are already in the cache
        for t in self.__required_product_types:
            self.__step_init_validate_input_product(t)

        # TODO: Verify photometry / prior files

        # TODO: add validation steps for CHEMFIT

        # Create output directories, although these might already exists since
        # the log files are already being written
        self._create_dir('output', self.get_product_outdir())
        self._create_dir('work', self.get_product_workdir())
        self._create_dir('log', self.get_product_logdir())
        self._create_dir('figure', self.get_product_figdir())

        return StepResults(success=True, skip_remaining=False, skip_substeps=False)
    
    def __step_init_validate_input_product(self, product):
        if self.__product_cache is None or product not in self.__product_cache:
            for i, visit, identity in self.__enumerate_visits():
                try:
                    self.__connector.locate_product(product, **identity.__dict__)
                except FileNotFoundError:
                    raise PipelineError(f'{product.__name__} file for visit `{visit}` not available.')
        else:
            for i, visit, identity in self.__enumerate_visits():
                if visit not in self.__product_cache[product]:
                    raise PipelineError(f'{product.__name__} for visit `{visit}` is not available.')
    
    #endregion
    #region Step: Load

    def __step_load(self):
        """
        Load the input data necessary for the pipeline. This does not include
        the spectrum grid, etc.
        """

        # Load required data products that aren't already in the cache       
        for t in self.__required_product_types:
            self.__load_input_products(t)

        # Extract the spectra of individual arms from the pfsSingle objects
        for t in self.__required_product_types:
            avail_arms = self.__get_avail_arms(t)
        
        # TODO: this should probably go to the respective steps
        #       because the data product we use depends on the template fitting method
        self.__spectra = self.__read_spectra(avail_arms)

        if self.trace is not None:
            self.trace.on_load(self.__spectra)

        return StepResults(success=True, skip_remaining=False, skip_substeps=False)
    
    def __load_input_products(self, product):
        """
        Load the source data product for each visit, if it's not already available.

        The identities of the products are read from the config.

        Parameters
        ----------
        product: :obj:`type`
            Type of the product to load
        cache: :obj:`dict`
            Cache of the loaded products
        """

        # TODO: consider factoring this logic out to a separate class

        if self.__product_cache is None:
            self.__product_cache = {}

        if product not in self.__product_cache:
            self.__product_cache[product] = {}

        q = 0
        for i, visit, identity in self.__enumerate_visits():
            if issubclass(product, PfsFiberArray):
                # Data product contains a single object
                if visit not in self.__product_cache[product]:
                    self.__product_cache[product][visit] = {}
                
                if identity.objId not in self.__product_cache[product][visit]:
                    data, id, filename = self.__connector.load_product(product, identity=identity)
                    self.__product_cache[product][visit][identity.objId] = data
                    q += 1
            elif issubclass(product, PfsFiberArraySet):
                # Data product contains multiple objects
                if visit not in self.__product_cache[product]:
                    data, id, filename = self.__connector.load_product(product, identity=identity)
                    self.__product_cache[product][visit] = data
                    q += 1
               
        logger.info(f'A total of {q} {product.__name__} data files loaded successfully for {self.__id}.')
    
    def __get_avail_arms(self, product):
        """
        Return a set of arms that are available in the observations.
        """

        avail_arms = set()

        for i, visit, identity in self.__enumerate_visits():
            if issubclass(product, PfsFiberArray):
                arms = self.__product_cache[product][visit][identity.objId].observations.arm[0]
            elif issubclass(product, PfsFiberArraySet):
                arms = self.__product_cache[product][visit].identity.arm
                    
            avail_arms.update([ a for a in arms ])

        # TODO: add option to require that all observations contain all arms

        # TODO: we actually know what arms are available from the config

        return avail_arms
    
    def __read_spectra(self, product, arms):
        # Extract spectra from the input products for each visit in a format
        # required by pfsspec

        read = 0
        skipped = 0

        spectra = { arm: {} for arm in arms }

        for i, visit, identity in self.__enumerate_visits():

            raise NotImplementedError()

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

        logger.info(f'Extracted {read} and skipped {skipped} spectra from PfsSingle files.')

        return spectra
    
    def __step_load_validate(self):
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

        if self.config.run_rvfit:
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

        # TODO: add validation for chemfit

        return StepResults(success=True, skip_remaining=False, skip_substeps=False)
    
    def __load_validate_pfsSingle(self, visit, pfsSingle):
        fn = pfsSingle.filenameFormat % {**pfsSingle.getIdentity(), 'visit': visit}

        # Verify that it is a single visit and not a co-add
        if pfsSingle.nVisit != 1:
            raise PipelineError('More than one visit found in `{pfsSingle.filename}`')
        
        # Verify that visit numbers match
        if visit not in pfsSingle.observations.visit:
            raise PipelineError(f'Visit does not match visit ID found in `{fn}`.')
        
        if pfsSingle.target.catId != self.config.target.catId:
            raise PipelineError(f'catId in config `{self.config.target.catId}` does not match catID in `{fn}`.')

        if pfsSingle.target.objId != self.config.target.objId:
            raise PipelineError(f'objId in config `{self.config.target.objId}` does not match objID in `{fn}`.')
        
        # TODO: compare flags and throw a warning if bits are not the same in every file

        # TODO: write log message
    
    def __read_spectrum(self, i, arm, visit, pfsSingle, arm_mask):
        # TODO: consider moving this routine to pfs.ga.pfsspec.survey

        r = PfsStellarSpectrumReader()
        s = r.read_from_pfsSingle(pfsSingle, i,
                                  arm=arm, arm_mask=arm_mask,
                                  ref_mag=self.config.ref_mag)

        # Generate the ID string
        s.id = Constants.PFSARM_ID_FORMAT.format(
            catId=s.catId,
            objId=s.objId,
            tract=s.tract,
            patch=s.patch,
            visit=s.visit,
            arm=s.arm,
            spectrograph=s.spectrograph
        )

        # TODO: Consider using different mask bits for each pipeline step
        # TODO: are mask bits the same for a rerun or they vary from file to file?
        s.mask_bits = self.__get_mask_bits(pfsSingle, self.config.mask_flags)
        
        # TODO: add logic to accept some masked pixels if the number of unmasked pixels is low

        # SNR
        # TODO: make this configurable?
        s.calculate_snr(QuantileSnr(0.75, binning=self.config.arms[arm]['pix_per_res']))

        # Write number of masked/unmasked pixels to log
        # TODO: review message and add IDs
        mm = s.mask_as_bool()
        logger.info(f'Spectrum {s.id} contains {np.sum(mm)} masked pixels and {np.sum(~mm)} unmasked pixels.')

        self.__calc_spectrum_params(s)

        return s
    
    def __calc_spectrum_params(self, s):
        # s.airmass

        # TODO: read MJD from somewhere
        # Convert datetime to MJD using astropy
        # Create datetime with UTC time zone (Hawaii: UTC - 10)
        s.mjd = Astro.datetime_to_mjd(datetime(2023, 7, 24, 14, 0, 0, tzinfo=pytz.timezone('UTC')))
        s.alt, s.az = Astro.radec_to_altaz(s.ra, s.dec, s.mjd)

    def __get_mask_bits(self, pfsSingle, mask_flags):
        if mask_flags is None:
            return None
        else:
            mask_bits = 0
            for flag in mask_flags:
                mask_bits |= pfsSingle.flags[flag]
            return mask_bits

    #endregion
    #region Step: vcorr

    # TODO: these have to be moved to the 2D pipeline

    def __step_vcorr(self):
        if self.config.v_corr is not None and self.config.v_corr.lower() != 'none':
            self.__v_corr_calculate()
            self.__v_corr_apply()
            return StepResults(success=True, skip_remaining=False, skip_substeps=False)
        else:
            logger.info('Velocity correction for geocentric frame is set to `none`, skipping corrections.')
            return StepResults(success=True, skip_remaining=False, skip_substeps=True)
        
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
    #region Step: RVFIT
        
    def __step_rvfit(self):
        """
        Perform the RV fitting step.
        """
        
        if not self.config.run_rvfit:
            logger.info('RV fitting is disabled, skipping step.')
            return StepResults(success=True, skip_remaining=False, skip_substeps=True)

        # Collect arms that can be used for fitting
        avail_arms = self.__get_avail_arms()
        fit_arms = set(self.config.rvfit.fit_arms)
        self.__rvfit_arms = avail_arms.intersection(fit_arms)

        if len(self.__rvfit_arms) < len(fit_arms):
            # TODO: list missing arms, include visit IDs
            logger.warning(f'Not all arms required to run RVFit are available in the observations for `{self.__id}.')

        # Collect spectra in a format that can be passed to RVFit
        self.__rvfit_spectra = self.__rvfit_collect_spectra(self.__rvfit_arms)

        return StepResults(success=True, skip_remaining=False, skip_substeps=False)
    
    def __step_rvfit_load(self):
        # Load template grids and PSFs
        self.__rvfit_grids = self.__rvfit_load_grid(self.__rvfit_arms)
        self.__rvfit_psfs = self.__rvfit_load_psf(self.__rvfit_arms, self.__rvfit_grids)
        return StepResults(success=True, skip_remaining=False, skip_substeps=False)
    
    def __rvfit_load_grid(self, arms):
        # Load template grids. Make sure each grid is only loaded once, if grid is
        # independent of arm.

        grids = {}
        for arm in arms:
            if isinstance(self.config.rvfit.model_grid_path, dict):
                fn = self.config.rvfit.model_grid_path[arm].format(arm=arm)
            else:
                fn = self.config.rvfit.model_grid_path.format(arm=arm)

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
    
    def __step_rvfit_preprocess(self):
        # TODO: validate available spectra here and throw warning if any of the arms are missing after
        #       filtering based on masks
        
        return StepResults(success=True, skip_remaining=False, skip_substeps=False)

    def __step_rvfit_fit(self):
        # Initialize the RVFit object
        self.__rvfit = self.__rvfit_init(self.__rvfit_grids, self.__rvfit_psfs)

        # Determine the normalization factor to be used to keep continuum coefficients unity
        self.__rvfit.spec_norm, self.__rvfit.temp_norm = self.__rvfit.get_normalization(self.__rvfit_spectra)

        # Run the maximum likelihood fitting
        self.__rvfit_results = self.__rvfit.fit_rv(self.__rvfit_spectra)

        return StepResults(success=True, skip_remaining=False, skip_substeps=False)
    
    def __rvfit_init(self, template_grids, template_psfs):
        """
        Initialize the RV fit object.
        """

        # Initialize the trace that will be used for logging and plotting
        trace = ModelGridTempFitTrace(
            id=self.__id,
            figdir=self.config.figdir,
            logdir=self.config.logdir)
        
        # Set the figure output file format
        if self.trace is not None:
            trace.figure_formats = self.trace.figure_formats

        # Create the correction model which determines if we apply flux correction to
        # the templates or continuum-normalize the observations.
        correction_model = CORRECTION_MODELS[self.config.rvfit.correction_model]()
        
        # Create the template fit object that will perform the RV fitting
        rvfit = ModelGridTempFit(correction_model=correction_model, trace=trace)

        rvfit.template_grids = template_grids
        rvfit.template_psf = template_psfs

        # Initialize the components from the configuration
        rvfit.init_from_args(None, None, self.config.rvfit.rvfit_args)
        rvfit.trace.init_from_args(None, None, self.config.rvfit.trace_args)
        rvfit.correction_model.init_from_args(None, None, self.config.rvfit.correction_model_args)

        return rvfit
    
    def __rvfit_collect_spectra(self, use_arms, skip_fully_masked=False, skip_mostly_masked=False, skip_none=False, mask_bits=None):
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
                        logger.warning(f'All pixels in spectrum {spec.id} are masked.')
                        if skip_fully_masked:
                            continue
                        else:
                            # Skip this spectrum because it is fully masked
                            spec = None
                    elif mask.sum() < self.config.rvfit.min_unmasked_pixels:
                        logger.warning(f'Not enough unmasked pixels in spectrum {spec.id}.')
                        if skip_mostly_masked:
                            continue

                if not skip_none or spec is not None:
                    if arm not in spectra:
                        spectra[arm] = []
                    spectra[arm].append(spec)

        return spectra

    def __step_rvfit_cleanup(self):
        # TODO: free up memory after rvfit
        return StepResults(success=True, skip_remaining=False, skip_substeps=False)

    #endregion
    #region Co-add

    def __step_coadd(self):
        if not self.config.run_rvfit:
            logger.info('Spectrum stacking required RV fitting which is disabled, skipping step.')
            return StepResults(success=True, skip_remaining=True, skip_substeps=True)
        elif not self.config.run_coadd:
            logger.info('Spectrum stacking is disabled, skipping step.')
            return StepResults(success=True, skip_remaining=True, skip_substeps=True)
        
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
        flux_corr = self.__coadd_eval_correction(spectra, templates)

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
                spec = PfsStellarSpectrum()

                # TODO: fill in ids?

                spec.wave = stacked_wave
                spec.wave_edges = stacked_wave_edges
                spec.flux = stacked_flux
                spec.flux_err = stacked_error
                spec.mask = stacked_mask

                self.__stacking_results[arm] = spec
            else:
                self.__stacking_results[arm] = None

        # TODO: trace hook
        
        return StepResults(success=True, skip_remaining=False, skip_substeps=False)
    
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
    
    def __coadd_eval_correction(self, spectra, templates):
        # Evaluate the correction correction for every exposure of each arm.
        # Depending on the configuration, the correction is either a multiplicative
        # flux correction, or a model fitted to continuum pixels. The correction model
        # is used to normalize the spectra before coadding.

        corr = self.__rvfit.eval_correction(
            spectra,
            templates,
            self.__rvfit_results.rv_fit,
            a=self.__rvfit_results.a_fit)
        
        if self.trace is not None:
            self.trace.on_coadd_eval_correction(spectra, templates, corr, self.__rvfit.spec_norm, self.__rvfit.temp_norm)
        
        return corr

    #endregion
    #region CHEMFIT

    def __step_chemfit(self):
        if not self.config.run_chemfit:
            logger.info('Chemical abundance fitting is disabled, skipping...')
            return StepResults(success=True, skip_remaining=False, skip_substeps=True)
        
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
        dir, fn = self.__get_pfsGAObject_dir_filename()
        self.__pfsGAObject.writeFits(os.path.join(dir, fn))

        return StepResults(success=True, skip_remaining=False, skip_substeps=False)
    
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
            flag = np.array([], dtype=bool),
            status = np.array([], dtype=str),
        )

    def __step_cleanup(self):
        # TODO: Perform any cleanup
        return StepResults(success=True, skip_remaining=False, skip_substeps=False)
