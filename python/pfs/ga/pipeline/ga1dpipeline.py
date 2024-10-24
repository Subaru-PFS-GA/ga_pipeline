import os
import time
from datetime import datetime
import pytz
import numpy as np
from types import SimpleNamespace

import pfs.datamodel
from pfs.datamodel import *
from pfs.datamodel import PfsGAObject, PfsGAObjectNotes, StellarParams, VelocityCorrections, Abundances

from pfs.ga.pfsspec.core import Physics, Astro
from pfs.ga.pfsspec.core.obsmod.resampling import FluxConservingResampler, Interp1dResampler
from pfs.ga.pfsspec.core.obsmod.psf import GaussPsf, PcaPsf
from pfs.ga.pfsspec.core.obsmod.snr import QuantileSnr
from pfs.ga.pfsspec.core.obsmod.stacking import Stacker, StackerTrace
from pfs.ga.pfsspec.stellar.grid import ModelGrid
from pfs.ga.pfsspec.stellar.tempfit import TempFit, ModelGridTempFit, ModelGridTempFitTrace, CORRECTION_MODELS
from pfs.ga.pfsspec.survey.repo import FileSystemRepo
from pfs.ga.pfsspec.survey.pfs import PfsStellarSpectrum
from pfs.ga.pfsspec.survey.pfs.io import PfsSpectrumReader
from pfs.ga.pfsspec.survey.pfs.utils import *

from .setup_logger import logger

from .constants import Constants
from .util import Timer
from .scripts.script import Script
from .pipeline import Pipeline, StepResults
from .pipelineerror import PipelineError
from .config import GA1DPipelineConfig
from .ga1dpipelinetrace import GA1DPipelineTrace
from .repo import PfsFileSystemConfig

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
                 repo: FileSystemRepo = None,
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
        connector: :obj:`FileSystemRepo`
        trace: :obj:`GA1DPipelineTrace`
            Trace object for logging and plotting
        """
        
        super().__init__(script=script, config=config, trace=trace)

        self.__id = id                          # Identity represented as string
        self.__repo = repo

        self._steps = self.__create_steps()
       
        # list of data products to be loaded
        self.__required_product_types = None
        self.__product_cache = None             # cache of loaded products

        self.__output_product_type = PfsGAObject
        self.__output_product = None            # output product

        self.__pfsGAObject = None

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

    def update(self, script=None, config=None, repo=None, trace=None, id=None):
        super().update(script=script, config=config, trace=trace)

        self.__repo = repo if repo is not None else self.__repo
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
            # {
            #     'name': 'vcorr',
            #     'func': self.__step_vcorr,
            #     'critical': False
            # },
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
                        'name': 'rvfit_coadd',
                        'func': self.__step_rvfit_coadd,
                        'critical': False
                    },
                    {
                        'name': 'rvfit_cleanup',
                        'func': self.__step_rvfit_cleanup,
                        'critical': True,
                    },
                ]
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
        return self.__repo.format_dir(GA1DPipelineConfig,
                                           self.config.target.identity,
                                           variables={ 'datadir': self.config.workdir })

    def get_product_outdir(self):
        return self.__repo.format_dir(PfsGAObject,
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
        filename = self.__repo.format_filename(GA1DPipelineConfig, self.config.target.identity)
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
        
        self._test_dir('data', self.__repo.get_resolved_variable('datadir'))
        self._test_dir('rerun', os.path.join(self.__repo.get_resolved_variable('datadir'),
                                             'rerun',
                                             self.__repo.get_resolved_variable('rerundir')))
        
        return True
    
    def validate_libs(self):
        # TODO: write code to validate library versions and log git hash for each when available
        pass

    #region Object and visit identity
        
    def __enumerate_visits(self):
        """
        Enumerate the visits in the configs and return an identity for each.
        """

        for i, visit in enumerate(sorted(self.config.target.observations.visit)):
            identity = SimpleNamespace(**self.config.target.get_identity(visit))
            yield i, visit, identity

    #endregion
    #region Load and validate input products

    def __validate_input_product(self, product):
        for i, visit, identity in self.__enumerate_visits():
            if self.__product_cache is not None and product in self.__product_cache:
                if issubclass(product, PfsFiberArray):
                    # Data product contains a single object
                    if visit in self.__product_cache[product]:
                        if identity.objId in self.__product_cache[product][visit]:
                            # Product is already in the cache, skip
                            continue
                elif issubclass(product, PfsFiberArraySet):
                    # Data product contains multiple objects
                    if visit in self.__product_cache[product]:
                        # Product is already in the cache, skip
                        continue
                else:
                    raise NotImplementedError('Product type not recognized.')
                
            # Product not found in cache of cache is empty, look up the file location
            try:
                self.__repo.locate_product(product, **identity.__dict__)
            except FileNotFoundError:
                raise PipelineError(f'{product.__name__} file for visit `{visit}` not available.')
                
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
                    data, id, filename = self.__repo.load_product(product, identity=identity)
                    self.__product_cache[product][visit][identity.objId] = data
                    q += 1
            elif issubclass(product, (PfsFiberArraySet, PfsDesign)):
                # Data product contains multiple objects
                if visit not in self.__product_cache[product]:
                    data, id, filename = self.__repo.load_product(product, identity=identity)
                    self.__product_cache[product][visit] = data
                    q += 1
            else:
                raise NotImplementedError('Product type not recognized.')
               
        logger.info(f'A total of {q} {product.__name__} data files loaded successfully for {self.__id}.')

    def __unload_data_products(self):
        # TODO: implement clean-up logic for batch processing mode
        raise NotImplementedError()
    
    def __validate_product(self, product, visit, data):       
        identity = self.__repo.get_identity(data)
        filename = self.__repo.format_filename(type(data), identity=identity)

        if issubclass(product, PfsFiberArray):
            # Verify that it is a single visit and not a co-add
            if data.nVisit != 1:
                raise PipelineError('More than one visit found in `{pfsSingle.filename}`')
            
            # Verify that visit numbers match
            if visit not in data.observations.visit:
                raise PipelineError(f'Visit does not match visit ID found in `{filename}`.')
            
            if data.target.catId != self.config.target.catId:
                raise PipelineError(f'catId in config `{self.config.target.catId}` does not match catID in `{filename}`.')

            if data.target.objId != self.config.target.objId:
                raise PipelineError(f'objId in config `{self.config.target.objId}` does not match objID in `{filename}`.')
        elif issubclass(product, PfsFiberArraySet):
            if visit != data.identity.visit:
                raise PipelineError(f'Visit does not match visit ID found in `{filename}`.')
        elif issubclass(product, PfsDesign):
            if issubclass(product, PfsConfig):
                # Verify that visit numbers match
                if visit != data.visit:
                    raise PipelineError(f'Visit does not match visit ID found in `{filename}`.')
                
            if self.config.target.identity.catId not in data.catId:
                raise PipelineError(f'catId in config `{self.config.target.identity.catId}` does not match catID in `{filename}`.')
            
            if self.config.target.identity.objId not in data.objId:
                raise PipelineError(f'objId in config `{self.config.target.identity.objId}` does not match objID in `{filename}`.')
        else:
            raise NotImplementedError('Product type not recognized.')
        
        # TODO: compare flags and throw a warning if bits are not the same in every file

        # TODO: write log message
    
    #endregion
    #region Read spectra from the data products
    
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
            else:
                arms = ''
                    
            avail_arms = avail_arms.union([ a for a in arms ])

        # TODO: add option to require that all observations contain all arms

        # TODO: we actually know what arms are available from the config

        return avail_arms
    
    def __read_spectra(self, products, arms):
        # Extract spectra from the input products for each visit in a format
        # required by pfsspec. Also return observation metadata which will be
        # required for the final output data product PfsGAObject.

        r = PfsSpectrumReader()

        read_count = 0
        skipped_count = 0

        spectra = { arm: {} for arm in arms }

        for i, visit, identity in self.__enumerate_visits():
            for arm in arms:
                wave_limits = self.config.arms[arm]['wave']
                spec = PfsStellarSpectrum()
                read = False

                # Make sure PfsDesign is read first if it's in the list of products because
                # other products might need information from it
                # Cannot read a spectrum from a design file but can update its metadata
                for t in products:
                    if issubclass(t, PfsDesign):
                        data = self.__product_cache[t][visit]
                        r.read_from_pfsDesign(data, spec, arm=arm,
                                                objid=identity.objId)
                
                for t in products:
                    if issubclass(t, PfsFiberArray):
                        data = self.__product_cache[t][visit][identity.objId]
                        # TODO is the arm available?
                        raise NotImplementedError()
                        r.read_from_pfsFiberArray(data, spec, arm=arm, wave_limits=wave_limits)
                        read = True
                    elif issubclass(t, PfsFiberArraySet):
                        data = self.__product_cache[t][visit]
                        if arm in data.identity.arm:
                            r.read_from_pfsFiberArraySet(data, spec, arm=arm,
                                                         fiberid=spec.fiberid,
                                                         wave_limits=wave_limits)
                            read = True
                    elif issubclass(t, PfsDesign):
                        pass
                    else:
                        raise NotImplementedError('Product type not recognized.')

                if read:
                    spectra[arm][visit] = spec
                    read_count += 1
                else:
                    spectra[arm][visit] = None
                    skipped_count += 1

        logger.info(f'Extracted {read_count} and skipped {skipped_count} spectra.')

        return spectra
    
    #endregion
    #region Step: Init

    def __step_init(self):
        # Save the full configuration to the output directory, if it's not already there
        dir = self.get_product_workdir()
        fn = self.__repo.format_filename(GA1DPipelineConfig, self.config.target.identity)
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

        # TODO: verify chemfit template paths, factor out the two into their respective functions

        # Compile the list of required input data products
        self.__required_product_types = set()
        if self.config.run_rvfit:
            self.__required_product_types.update([ getattr(pfs.datamodel, t) for t in self.config.rvfit.required_products ])
        if self.config.run_chemfit:
            self.__required_product_types.update([ getattr(pfs.datamodel, t) for t in self.config.chemfit.required_products ])

        # Verify that input data files are available or the input products
        # are already in the cache
        for t in self.__required_product_types:
            self.__validate_input_product(t)

        # TODO: Verify photometry / prior files

        # TODO: add validation steps for CHEMFIT

        # Create output directories, although these might already exists since
        # the log files are already being written
        self._create_dir('output', self.get_product_outdir())
        self._create_dir('work', self.get_product_workdir())
        self._create_dir('log', self.get_product_logdir())
        self._create_dir('figure', self.get_product_figdir())

        return StepResults(success=True, skip_remaining=False, skip_substeps=False)
    
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

        # TODO: load photometry / prior files

        # TODO: add trace hook?

        return StepResults(success=True, skip_remaining=False, skip_substeps=False)
    
    def __step_load_validate(self):
        # Extract info from pfsSingle objects one by one and perform
        # some validation steps

        target = None

        for i, visit, identity in self.__enumerate_visits():
            for product in self.__required_product_types:
                if issubclass(product, PfsFiberArray):
                    data = self.__product_cache[product][visit][identity.objId]

                    # Make sure that targets are the same
                    if target is None:
                        target = data.target
                    elif not target == data.target:
                        raise PipelineError(f'Target information in PfsSingle files do not match.')

                elif issubclass(product, PfsFiberArraySet):
                    data = self.__product_cache[product][visit]
                elif issubclass(product, PfsDesign):
                    data = self.__product_cache[product][visit]
                else:
                    raise NotImplementedError('Product type not recognized.')

                self.__validate_product(product, visit, data)

        # TODO: Count spectra per arm and write report to log

        return StepResults(success=True, skip_remaining=False, skip_substeps=False)

    #endregion
    #region Step: vcorr

    # TODO: these have to be moved to the 2D pipeline

    def __step_vcorr(self):
        # if self.config.v_corr is not None and self.config.v_corr.lower() != 'none':
        #     self.__v_corr_calculate()
        #     self.__v_corr_apply()
        #     return StepResults(success=True, skip_remaining=False, skip_substeps=False)
        # else:
        #     logger.info('Velocity correction for geocentric frame is set to `none`, skipping corrections.')
        #     return StepResults(success=True, skip_remaining=False, skip_substeps=True)

        return StepResults(success=True, skip_remaining=False, skip_substeps=True)
        
    def __v_corr_calculate(self):
        
        # TODO: logging + perf counter

        # TODO: review this
        raise NotImplementedError()
        
        self.__v_corr = {}

        # Calculate the velocity correction for each spectrum
        for arm in self.__spectra:
            for visit in self.__spectra[arm]:
                s = self.__spectra[arm][visit]
                if s is not None and visit not in self.__v_corr:
                    self.__v_corr[visit] = Astro.v_corr(self.config.v_corr, s.ra, s.dec, s.mjd)

    def __v_corr_apply(self):

        # TODO: review this
        raise NotImplementedError()

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
        Initialize the RV fitting step.
        """
        
        if not self.config.run_rvfit:
            logger.info('RV fitting is disabled, skipping step.')
            return StepResults(success=True, skip_remaining=False, skip_substeps=True)
        
        # Find the set of available arms in the available files
        avail_arms = set()
        for t in self.__required_product_types:
            if issubclass(t, (PfsFiberArray, PfsFiberArraySet)):
                avail_arms = avail_arms.union(self.__get_avail_arms(t))

        # Verify that all arms required in the config are available
        self.__rvfit_arms = set()
        for arm in self.config.rvfit.fit_arms:
            message = f'RVFIT requires arm `{arm}` which is not observed.'
            if self.config.rvfit.require_all_arms and arm not in avail_arms:
                raise PipelineError(message)
            elif arm not in avail_arms:
                logger.warning(message)
            else:
                self.__rvfit_arms.add(arm)
        
        return StepResults(success=True, skip_remaining=False, skip_substeps=False)
    
    def __step_rvfit_load(self):

        # Load template grids and PSFs
        self.__rvfit_grids = self.__rvfit_load_grid(self.__rvfit_arms)

        # TODO: this will change once we get real PSFs from the 2DRP
        # TODO: add trace hook to plot the PSFs
        self.__rvfit_psfs = self.__rvfit_load_psf(self.__rvfit_arms, self.__rvfit_grids)
        
        # Initialize the RVFit object
        self.__rvfit = self.__rvfit_init(self.__rvfit_grids, self.__rvfit_psfs)

        # Read the spectra from the data products
        spectra = self.__read_spectra(self.__required_product_types, self.__rvfit_arms)
        
        # Collect spectra in a format that can be passed to RVFit
        self.__rvfit_spectra = self.__rvfit_collect_spectra(spectra,
                                                            self.__rvfit_arms,
                                                            skip_mostly_masked=False,
                                                            mask_flags=self.config.rvfit.mask_flags)
        
        if self.trace is not None:
            self.trace.on_load(self.__rvfit_spectra)

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
    
    def __rvfit_collect_spectra(self,
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
            for i, visit, identity in self.__enumerate_visits():
                spec = input_spectra[arm][visit]
                if spec is not None:
                    # Calculate mask bits
                    if mask_flags is not None:
                        mask_bits = spec.get_mask_bits(mask_flags)
                    else:
                        mask_bits = None

                    # Calculate mask
                    mask = self.__rvfit.get_full_mask(spec, mask_bits=mask_bits)

                    if mask.sum() == 0:
                        logger.warning(f'All pixels in spectrum {spec.get_name()} are masked.')
                        spec = None
                    elif mask.sum() < self.config.rvfit.min_unmasked_pixels:
                        logger.warning(f'Not enough unmasked pixels in spectrum {spec.get_name()}.')
                        if skip_mostly_masked:
                            spec = None

                spectra[arm][visit] = spec

        # Remove all None visits
        for i, visit, identity in self.__enumerate_visits():
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
    
    def __step_rvfit_preprocess(self):
        # TODO: validate available spectra here and throw warning if any of the arms are missing after
        #       filtering based on masks
        
        return StepResults(success=True, skip_remaining=False, skip_substeps=False)
    
    def __rvfit_init(self, template_grids, template_psfs):
        """
        Initialize the RV fit object.
        """

        # Initialize the trace that will be used for logging and plotting
        if self.trace is not None:
            trace = ModelGridTempFitTrace(id=self.__id)
            trace.init_from_args(None, None, self.config.rvfit.trace_args)

            # Set output directories based on pipeline trace
            trace.figdir = self.trace.figdir
            trace.logdir = self.trace.logdir

            # Set the figure output file format
            trace.figure_formats = self.trace.figure_formats
        else:
            trace = None

        # Create the correction model which determines if we apply flux correction to
        # the templates or continuum-normalize the observations.
        correction_model = CORRECTION_MODELS[self.config.rvfit.correction_model]()
        
        # Create the template fit object that will perform the RV fitting
        rvfit = ModelGridTempFit(correction_model=correction_model, trace=trace)

        rvfit.template_grids = template_grids
        rvfit.template_psf = template_psfs

        # Initialize the components from the configuration
        rvfit.init_from_args(None, None, self.config.rvfit.rvfit_args)
        rvfit.correction_model.init_from_args(None, None, self.config.rvfit.correction_model_args)

        return rvfit

    def __step_rvfit_fit(self):

        # Determine the normalization factor to be used to keep continuum coefficients unity
        self.__rvfit.spec_norm, self.__rvfit.temp_norm = self.__rvfit.get_normalization(self.__rvfit_spectra)

        # Run the maximum likelihood fitting
        self.__rvfit_results = self.__rvfit.fit_rv(self.__rvfit_spectra)

        return StepResults(success=True, skip_remaining=False, skip_substeps=False)
    
    def __step_rvfit_coadd(self):
        if not self.config.run_rvfit:
            logger.info('Spectrum stacking required RV fitting which is disabled, skipping step.')
            return StepResults(success=True, skip_remaining=True, skip_substeps=True)
        elif not self.config.run_coadd:
            logger.info('Spectrum stacking is disabled, skipping step.')
            return StepResults(success=True, skip_remaining=True, skip_substeps=True)
        
        # Use the same input as for RV fitting and evaluate the templates and the
        # continuum or flux correction function
        spectra = self.__rvfit_spectra
        templates = self.__rvfit_coadd_get_templates(spectra)
        corrections = self.__rvfit_coadd_eval_correction(spectra, templates)
        
        # We can only coadd arms that have been used for RV fitting
        coadd_arms = set(self.config.coadd.coadd_arms).intersection(spectra.keys())

        if len(coadd_arms) < len(self.config.coadd.coadd_arms):
            logger.warning('Not all arms required for co-adding are available from rvfit.')

        # Make sure that the bit flags are the same for all spectra
        # TODO: any more validation here?
        no_data_bit = None
        mask_flags = None
        for arm in spectra:
            for s in spectra[arm]:
                if s is not None:
                    if no_data_bit is None:
                        no_data_bit = s.get_mask_bits([ self.config.coadd.no_data_flag ])

                    if mask_flags is None:
                        mask_flags = s.mask_flags
                    elif mask_flags != s.mask_flags:
                        logger.warning('Mask flags are not the same for all spectra.')

        # Initialize the stacker algorithm
        self.__stacker = self.__rvfit_coadd_init()

        # TODO: add trace hook to plot the templates and corrections?

        self.__coadd_spectra = {}
        for arm in spectra:
            # Only stack those spectra that are not masked or otherwise None
            ss = [ s for s in spectra[arm] if s is not None ]
            fc = [ f for f in corrections[arm] if f is not None ]
            if len(ss) > 0:
                stacked_wave, stacked_wave_edges, stacked_flux, stacked_error, stacked_weight, stacked_mask = \
                    self.__stacker.stack(ss, flux_corr=fc)
                
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

                self.__coadd_spectra[arm] = spec
            else:
                self.__coadd_spectra[arm] = None

        # Merge arms into a single spectrum
        # TODO: this won't work with overlapping arms! Need to merge them properly.
        arms = sort_arms(spectra.keys())
        self.__coadd_merged = PfsStellarSpectrum()
        self.__coadd_merged.wave = np.concatenate([ self.__coadd_spectra[arm].wave for arm in arms ])
        self.__coadd_merged.wave_edges = np.concatenate([ self.__coadd_spectra[arm].wave_edges for arm in arms ])
        self.__coadd_merged.flux = np.concatenate([ self.__coadd_spectra[arm].flux for arm in arms ])
        self.__coadd_merged.flux_err = np.concatenate([ self.__coadd_spectra[arm].flux_err for arm in arms ])
        self.__coadd_merged.mask = np.concatenate([ self.__coadd_spectra[arm].mask for arm in arms ])

        # TODO: sky? covar? covar2? - these are required for a valid PfsFiberArray
        self.__coadd_merged.sky = np.zeros(self.__coadd_merged.wave.shape)
        self.__coadd_merged.covar = np.zeros((3,) + self.__coadd_merged.wave.shape)
        self.__coadd_merged.covar2 = np.zeros((1, 1), dtype=np.float32)

        # Merge observation metadata
        observations = []
        target = None
        mask_flags = None
        for arm in self.__rvfit_spectra:
            for s in self.__rvfit_spectra[arm]:
                if s is not None:
                    observations.append(s.observations)
                    if target is None:
                        target = s.target
                        mask_flags = s.mask_flags
        
        # Merge all observations into a final list
        self.__coadd_merged.target = target
        self.__coadd_merged.observations = merge_observations(observations)
        self.__coadd_merged.mask_flags = mask_flags

        # TODO: do we need mode metadata?

        # TODO: trace hook
        
        return StepResults(success=True, skip_remaining=False, skip_substeps=False)
           
    def __rvfit_coadd_init(self):
        # Initialize the trace object if tracing is enabled for the pipeline
        if self.trace is not None:
            trace = StackerTrace(id=self.__id)
            trace.init_from_args(None, None, self.config.coadd.trace_args)

            # Set output directories based on pipeline trace
            trace.figdir = self.trace.figdir
            trace.logdir = self.trace.logdir

            # Set the figure output file format
            trace.figure_formats = self.trace.figure_formats
        else:
            trace = None

        # Initialize the stacker object
        stacker = Stacker(trace)
        stacker.init_from_args(None, None, self.config.coadd.stacker_args)

        return stacker
    
    def __rvfit_coadd_get_templates(self, spectra):
        # Return the templates at the best fit parameters

        # Interpolate the templates to the best fit parameters
        templates, missing = self.__rvfit.get_templates(
            spectra,
            self.__rvfit_results.params_fit)
        
        if self.trace is not None:
            self.trace.on_coadd_get_templates(spectra, templates)

        return templates
    
    def __rvfit_coadd_eval_correction(self, spectra, templates):
        # Evaluate the correction for every exposure of each arm.
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
    
    def __step_rvfit_cleanup(self):
        # TODO: free up memory after rvfit
        return StepResults(success=True, skip_remaining=False, skip_substeps=False)

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
        # TODO: 
        metadata = {}
        flux_table = None

        stellar_params = self.__get_stellar_params()
        stellar_params_covar = self.__rvfit_results.cov
        velocity_corrections = self.__get_velocity_corrections(self.__coadd_merged.observations)
        abundances = self.__get_abundances()
        abundances_covar = None
        notes = PfsGAObjectNotes()

        self.__pfsGAObject = PfsGAObject(
            self.__coadd_merged.target,
            self.__coadd_merged.observations,
            self.__coadd_merged.wave,
            self.__coadd_merged.flux,
            self.__coadd_merged.mask,
            self.__coadd_merged.sky,
            self.__coadd_merged.covar,
            self.__coadd_merged.covar2,
            MaskHelper(**{ v: k for k, v in self.__coadd_merged.mask_flags.items() }),
            metadata,
            flux_table,
            stellar_params,
            velocity_corrections,
            abundances,
            stellar_params_covar,
            abundances_covar,
            notes)

        # Save output FITS file
        identity, filename = self.__repo.save_product(
            self.__pfsGAObject,
            variables={ 'datadir': self.config.outdir })
        
        logger.info(f'Output file saved to `{filename}`.')

        return StepResults(success=True, skip_remaining=False, skip_substeps=False)
    
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
    
    def __get_velocity_corrections(self, observations):
        # Assume observations are sorted by visit

        # TODO: not obs time data in any of the headers!
        JD = [ 0.0 for v in observations.visit]
        helio = [ 0.0 for v in observations.visit]
        bary = [ 0.0 for v in observations.visit]

        return VelocityCorrections(
            visit=np.atleast_1d(observations.visit),
            JD=np.atleast_1d(JD),
            helio=np.atleast_1d(helio),
            bary=np.atleast_1d(bary),
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
