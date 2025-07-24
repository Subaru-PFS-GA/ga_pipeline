import os
import time
from datetime import datetime
import pytz
import numpy as np
from types import SimpleNamespace

from pfs.ga.pfsspec.survey.pfs.datamodel import *

from pfs.ga.pfsspec.survey.repo import Repo, FileSystemRepo
from pfs.ga.pfsspec.survey.pfs import PfsStellarSpectrum
from pfs.ga.pfsspec.survey.pfs.io import PfsSpectrumReader

from ..constants import Constants
from ..util import Timer
from ..common import Script, Pipeline, PipelineStepResults, PipelineError
from ..repo import GAPipeWorkdirConfig
from .config import GAPipelineConfig
from .gapipelinetrace import GAPipelineTrace
from .steps import *

from ..setup_logger import logger

class GAPipeline(Pipeline):
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

    def __init__(self, /,
                 script: Script = None,
                 config: GAPipelineConfig = None,
                 input_repo: Repo = None,
                 work_repo: FileSystemRepo = None,
                 trace: GAPipelineTrace = None,
                 id: str = None):
        """
        Initializes a GA Pipeline object for processing of individual exposures of a
        single object.

        Parameters
        ----------
        script: :obj:`Script`
            Script object for logging and command-line arguments
        config: :obj:`GAPipelineConfig`
            Configuration of the GA pipeline
        connector: :obj:`FileSystemRepo`
        trace: :obj:`GAPipelineTrace`
            Trace object for logging and plotting
        """

        # TODO: id and trace should be moved to the state object entirely
        # TODO: also move everything config related because that cannot be set on
        #       the pipeline level is the pipeline is stateless
        
        super().__init__(script=script, config=config, trace=trace)

        self.__id = id                          # Identity represented as string
        self.__input_repo = input_repo
        self.__work_repo = work_repo
       
    def reset(self):
        super().reset()

        # TODO: keep this cache around instead when processing multiple objects
        self.product_cache = None           

        # Pipeline state variables

        self.required_product_types = None

        self.output_product_type = PfsGAObject
        self.output_product = None

        self.snr = None                       # SNR calculator class for each arm

        self.rvfit = None                     # RVFit object
        self.rvfit_arms = None                # Arms used for RV fitting
        self.rvfit_spectra = None             # Spectra used for RV fitting
        self.rvfit_grids = None               # Template grids for RV fitting
        self.rvfit_psfs = None                # PSFs for RV fitting
        self.rvfit_results = None             # Results from RVFit

        self.stacker = None                   # SpectrumStacker object

        self.coadd_arms = None
        self.coadd_spectra = None             # Spectra used for coaddition
        self.coadd_results = None             # Results from coaddition

    def update(self, script=None, config=None, repo=None, trace=None, id=None):
        super().update(script=script, config=config, trace=trace)

        self.__input_repo = repo if repo is not None else self.__input_repo
        self.__id = id if id is not None else self.__id

        if self.trace is not None:
            self.trace.update(id=self.__id)

        # TODO: reset anything else?

    #region Properties

    def __get_id(self):
        return self.__id
    
    def __set_id(self, value):
        self.__id = value

    id = property(__get_id, __set_id)

    #endregion

    def create_context(self, state=None, trace=None):    
        context = SimpleNamespace(
            id = self.__id,
            pipeline = self,
            input_repo = self.__input_repo,
            work_repo = self.__work_repo,
            config = self.config,
            trace = trace
        )

        return context

    def create_steps(self):
        return [
            {
                'type': ValidateStep,
                'name': 'validate',
                'func': ValidateStep.run,
                'critical': True,
            },
            {
                'type': InitStep,
                'name': 'init',
                'func': InitStep.run,
                'critical': True,
            },
            {
                'type': LoadStep,
                'name': 'load',
                'func': LoadStep.run,
                'critical': True,
                'substeps': [
                    {
                        'name': 'load_validate',
                        'func': LoadStep.validate,
                        'critical': True
                    }
                ]
            },
            # {
            #     'type': VCorrStep,
            #     'name': 'vcorr',
            #     'func': self.__step_vcorr,
            #     'critical': False
            # },
            {
                'type': RVFitStep,
                'name': 'rvfit',
                'func': RVFitStep.init,
                'critical': True,
                'substeps': [
                    {
                        'name': 'rvfit_load',
                        'func': RVFitStep.load,
                        'critical': True,
                        'substeps': [
                            {
                                'name': 'rvfit_load_validate',
                                'func': RVFitStep.validate_data,
                                'critical': True
                            }
                        ]
                    },
                    {
                        'name': 'rvfit_preprocess',
                        'func': RVFitStep.preprocess,
                        'critical': True,
                    },
                    {
                        'name': 'rvfit_fit',
                        'func': RVFitStep.fit,
                        'critical': True,
                    },
                    {
                        'name': 'rvfit_map_log_L',
                        'func': RVFitStep.map_log_L,
                        'critical': False,
                    },
                    {
                        'name': 'rvfit_coadd',
                        'func': RVFitStep.coadd,
                        'critical': False
                    },
                    {
                        'name': 'rvfit_cleanup',
                        'func': RVFitStep.cleanup,
                        'critical': True,
                    },
                ]
            },
            # {
            #     # TODO: factor out co-add from rvfit
            #     'type': CoaddStep,
            # },
            {
                'type': ChemFitStep,
                'name': 'chemfit',
                'func': ChemFitStep.run,
                'critical': False
            },
            {
                'type': SaveStep,
                'name': 'save',
                'func': SaveStep.run,
                'critical': False
            },
            {
                'type': CleanupStep,
                'name': 'cleanup',
                'func': CleanupStep.run,
                'critical': False
            },
        ]
    
    def get_product_workdir(self):
        return self.__work_repo.format_dir(GAPipelineConfig,
                                           self.config.target.identity,
                                           variables={ 'workdir': self.config.workdir })

    def get_product_outdir(self):
        return self.__work_repo.format_dir(PfsGAObject,
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
        filename = self.__work_repo.format_filename(GAPipelineConfig, self.config.target.identity)
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

    def _save_exceptions(self, exceptions, tracebacks):
        """
        Write the exceptions and the stack traces to a file.
        """

        if exceptions is not None and len(exceptions) > 0:
            # Get full path of log file without extension
            logfile = self.get_product_logfile()
            if logfile is not None:
                logdir = os.path.dirname(logfile)
                logfile = os.path.basename(logfile)
                logfile = os.path.splitext(logfile)[0]
                fn = os.path.join(logdir, logfile + '.traceback')
                with open(fn, 'a') as f:
                    for i in range(len(exceptions)):
                        f.write(repr(exceptions[i]))
                        f.write('\n')
                        f.writelines(tracebacks[i])
                        f.write('\n')

    def locate_data_products(self, product, required=True):
        """
        Try to look up each data product, first in the product cache, then
        in the data repositories. If `required` is `True`, raise an error if the
        product is not found in any of the repositories.
        """

        for i, visit, identity in self.config.enumerate_visits():
            # Try to look up the product in the cache first
            if self.product_cache is not None and product in self.product_cache:
                if issubclass(product, PfsFiberArray):
                    # Data product contains a single object
                    if visit in self.product_cache[product]:
                        if identity.objId in self.product_cache[product][visit]:
                            # Product is already in the cache, skip
                            return
                elif issubclass(product, PfsFiberArraySet):
                    # Data product contains multiple objects
                    if visit in self.product_cache[product]:
                        # Product is already in the cache, skip
                        return
                else:
                    raise NotImplementedError('Product type not recognized.')
                    
            # Product not found in cache of cache is empty, look up the file location using
            # the input repository with fall-back to the work repository, in case some of the
            # data products are already copied to the output directory.
            found = False
            for repo in [self.__input_repo, self.__work_repo]:
                try:
                    repo.locate_product(product, **identity.__dict__)
                    found = True
                    break
                except KeyError:
                    # The product type is not available in the repository, skip
                    logger.debug(f'Product type `{product.__name__}` not available in repository of type {type(repo).__name__}.')
                    continue
                except FileNotFoundError:
                    # The product file is not available in the repository, skip
                    logger.warning(f'{product.__name__} file for identity `{identity}` not available in repository of type {type(repo).__name__}.')
                    continue

            if not found:
                msg = f'{product.__name__} file for identity `{identity}` not available.'
                if required:
                    raise PipelineError(msg)
                else:
                    logger.warning(msg)

    def load_input_products(self, product):
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

        if self.product_cache is None:
            self.product_cache = {}

        if product not in self.product_cache:
            self.product_cache[product] = {}

        q = 0
        for i, visit, identity in self.config.enumerate_visits():
            if issubclass(product, PfsFiberArray):
                # Data product contains a single object
                if visit not in self.product_cache[product]:
                    self.product_cache[product][visit] = {}
                
                if identity.objId not in self.product_cache[product][visit]:
                    found = False
                    for repo in [self.__input_repo, self.__work_repo]:
                        try:
                            data, id, filename = repo.load_product(product, identity=identity)
                            data.filename = filename
                            self.product_cache[product][visit][identity.objId] = data
                            found = True
                            q += 1
                        except KeyError:
                            # The product type is not available in the repository, skip
                            continue

                    if not found:
                        msg = f'{product.__name__} file for identity `{identity}` not available.'
                        raise FileNotFoundError(msg)
            elif issubclass(product, (PfsFiberArraySet, PfsTargetSpectra, PfsConfig)):
                # Data product contains multiple objects
                if visit not in self.product_cache[product]:
                    found = False
                    for repo in [self.__input_repo, self.__work_repo]:
                        try:
                            data, id, filename = repo.load_product(product, identity=identity)
                            self.product_cache[product][visit] = data
                            found = True
                            q += 1
                        except KeyError:
                            # The product type is not available in the repository, skip
                            continue

                    if not found:
                        msg = f'{product.__name__} file for identity `{identity}` not available.'
                        raise FileNotFoundError(msg)
            else:
                raise NotImplementedError('Product type not recognized.')
               
        logger.info(f'A total of {q} {product.__name__} data files loaded successfully for {self.__id}.')

    def get_product_from_cache(self, product, visit, identity):
        if issubclass(product, PfsFiberArray):
            return self.product_cache[product][visit][identity.objId]
        elif issubclass(product, PfsFiberArraySet):
            return self.product_cache[product][visit]
        elif issubclass(product, PfsTargetSpectra):
            return self.product_cache[product][visit]
        elif issubclass(product, PfsDesign):
            return self.product_cache[product][visit]
        else:
            raise NotImplementedError('Product type not recognized.')

    def get_product_identity(self, data):
        found = False
        for repo in [self.__input_repo, self.__work_repo]:
            try:
                return repo.get_identity(data)
            except KeyError:
                # The product type is not available in the repository, skip
                continue

        raise NotImplementedError()

    def unload_data_products(self):
        # TODO: implement clean-up logic for batch processing mode
        raise NotImplementedError()
    
    def save_output_product(self, product, identity=None, create_dir=True, exist_ok=True):
        """
        Save the output product to the output directory.
        """

        identity, filename = self.__work_repo.save_product(
            product,
            identity = identity,
            variables = { 'datadir': self.config.outdir },
            create_dir = create_dir,
            exist_ok = exist_ok)
    
        logger.info(f'Output file saved to `{filename}`.')

        return identity, filename
    
    def get_avail_arms(self, product):
        """
        Return a set of arms that are available in the observations.
        """

        avail_arms = set()

        for i, visit, identity in self.config.enumerate_visits():
            if issubclass(product, PfsFiberArray):
                arms = self.product_cache[product][visit][identity.objId].observations.arm[0]
            elif issubclass(product, PfsFiberArraySet):
                arms = self.product_cache[product][visit].identity.arm
            elif issubclass(product, PfsTargetSpectra):
                data = self.product_cache[product][visit]
                arms = data[list(self.product_cache[product][visit].keys())[0]].observations.arm[0]
            else:
                arms = ''
                    
            avail_arms = avail_arms.union([ a for a in arms ])

        # TODO: add option to require that all observations contain all arms

        # TODO: we actually know what arms are available from the config

        return avail_arms
    
    def __read_spectrum(self, products, reader, visit, arm, identity, wave_limits):
        
        # To read everything about a spectrum we usually need the corresponding PfsConfig file
        
        spec = PfsStellarSpectrum()
        found = False

        # Make sure PfsConfig is read first if it's in the list of products because
        # other products might need information from it
        # Cannot read a spectrum from a config file but can update its metadata
        for t in products:
            if issubclass(t, PfsConfig):
                data = self.product_cache[t][visit]
                if reader.is_available(data, arm=arm, objid=identity.objId):
                    reader.read_from_pfsConfig(data, spec, arm=arm, objid=identity.objId)
                else:
                    return False, None
        
        for t in products:
            if issubclass(t, PfsFiberArray):            # PfsSingle etc
                data = self.product_cache[t][visit][identity.objId]
                if reader.is_available(data, arm=arm):
                    reader.read_from_pfsFiberArray(data, spec, arm=arm, wave_limits=wave_limits)
                    found = True
                else:
                    return False, None
            elif issubclass(t, PfsFiberArraySet):       # PfsMerged, etc
                data = self.product_cache[t][visit]
                if reader.is_available(data, arm=arm):
                    reader.read_from_pfsFiberArraySet(data, spec, arm=arm,
                                                      fiberid=spec.fiberid,
                                                      wave_limits=wave_limits)
                    found = True
                else:
                    return False, None
            elif issubclass(t, PfsTargetSpectra):       # PfsCalibrated etc
                data = self.product_cache[t][visit]
                if reader.is_available(data, arm=arm, objid=identity.objId):
                    reader.read_from_pfsTargetSpectra(data, spec, arm=arm,
                                                      objid=identity.objId,
                                                      wave_limits=wave_limits)
                    found = True
                else:
                    return False, None
            elif issubclass(t, PfsConfig):
                pass
            else:
                raise NotImplementedError('Product type not recognized.')

        # TODO: add other parameters from the config file such as megnitude etc.
            
        return found, spec
    
    def read_spectra(self, products, arms):
        # Extract spectra from the input products for each visit in a format
        # required by pfsspec. Also return observation metadata which will be
        # required for the final output data product PfsGAObject.

        reader = PfsSpectrumReader()

        read_count = 0
        skipped_count = 0

        spectra = { arm: {} for arm in arms }

        for i, visit, identity in self.config.enumerate_visits():
            for arm in arms:
                wave_limits = self.config.arms[arm]['wave']
                found, spec = self.__read_spectrum(products, reader, visit, arm, identity, wave_limits)

                if found:
                    spectra[arm][visit] = spec
                    read_count += 1
                else:
                    spectra[arm][visit] = None
                    skipped_count += 1

        logger.info(f'Extracted {read_count} and skipped {skipped_count} spectra.')

        return spectra