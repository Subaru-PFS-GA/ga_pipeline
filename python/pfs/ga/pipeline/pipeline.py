import os
import logging
import traceback
import time

from pfs.datamodel import PfsConfig, PfsSingle
from pfs.ga.datamodel import PfsGAObject

from .config import Config

class Pipeline():
    """
    Implements the Galactic Archeology Spectrum Processing Pipeline.

    Inputs are the `PfsSingle` files of all individual exposures belonging to the same
    `objId` and all related pfsConfig files as dictionaries indexed by `visit`.

    The pipeline processes all exposures of a single object at a time and produces a
    PfsGAObject file which contains the measured parameters, their errors and full covariance
    matrix as well as a co-added spectrum, a continuum-normalized co-added spectrum and a
    best fit continuum model.

    The three main steps of the pipeline are i) line-of-sight velocity estimation,
    ii) abundance estimation and iii) spectrum stacking.
    """

    def __init__(self, config: Config, objId = None, pfsConfig: dict = None, pfsSingle: dict = None):
        """
        Initializes a GA Pipeline object for processing of individual exposures of a
        single object.

        Parameters
        ----------
        config: :obj:`Config`
            Configuration of the GA pipeline
        objId: :int:
            Unique object identifier
        pfsConfig: :dict:`int`,`PfsConfig`
            Dictionary of PfsConfig objects for each visit, keyed by ˙visit`.
        pfsSingle : :dict:`int`,`PfsSingle`
            Dictionary of PfsSingle object containing the individual exposures,
            keyed by ˙visit˙.
        """
        
        self.__config = config
        
        self.__logger = logging.getLogger('gapipe')
        self.__logfile = None
        self.__logFormatter = None
        self.__logFileHandler = None
        self.__logConsoleHandler = None

        self.__exceptions = []
        self.__tracebacks = []

        self.__steps = [
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
                'name': 'rvfit',
                'func': self.__step_rvfit,
                'critical': False
            },
            {
                'name': 'chemfit',
                'func': self.__step_chemfit,
                'critical': False
            },
            {
                'name': 'coadd',
                'func': self.__step_coadd,
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

        self.__pfsConfig = pfsConfig          
        self.__pfsSingle = pfsSingle
        self.__pfsGAObject = None

    def __get_pfsGAObject(self):
        return self.__pfsGAObject
    
    pfsGAObject = property(__get_pfsGAObject)

    def __get_exceptions(self):
        return self.__exceptions
    
    exceptions = property(__get_exceptions)

    def __get_tracebacks(self):
        return self.__tracebacks

    def validate(self):
        """
        Validates the configuration and the existence of all necessary input data. Returns
        `True` if the pipeline can proceed or 'False' if it cannot.

        Return
        -------
        :bool:
            `True` if the pipeline can proceed or 'False' if it cannot.
        """

        if not os.path.isdir(self.__config.workdir):
            raise FileNotFoundError(f'Working directory `{self.__config.workdir}` does not exist.')
        
        if not os.path.isdir(self.__config.rerundir):
            raise FileNotFoundError(f'Data directory `{self.__config.rerundir}` does not exist.')
        
        if not os.path.isfile(self.__config.modelGridPath):
            raise FileNotFoundError(f'Synthetic spectrum grid `{self.__config.modelGridPath}` does not exist.')

        return True

    def execute(self):
        """
        Execute the pipeline steps sequentially and return the output PfsGAObject containing
        the inferred parameters and the co-added spectrum.
        """

        if not self.__start_logging():
            return None
        
        for i, step in enumerate(self._steps):
            success = self._execute_step(step['name'], step['func'])
            if not success and step['critical']:
                break

        self._stop_logging()
        
        return self._pfsGAObject
    
    def __create_dir(self, dir, name):
        if not os.path.isdir(dir):
            os.makedirs(dir, exist_ok=True)
            logging.debug(f'Created {name} directory `{dir}`.')
        else:
            logging.debug(f'Found existing {name} directory `{dir}`.')

    def __start_logging(self):
        self.__create_dir(self.__config.logdir, 'log')

        self.__logfile = os.path.join(self.__config.logdir, f'gapipe_{self.__config.objId:016x}.log')
        self.__logFormatter = logging.Formatter("%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s", datefmt='%H:%M:%S')
        self.__logFileHandler = logging.FileHandler(self.__logfile)
        self.__logFileHandler.setFormatter(self.__logFormatter)
        self.__logConsoleHandler = logging.StreamHandler()
        self.__logConsoleHandler.setFormatter(self.__logFormatter)

        # Configure root logger
        root = logging.getLogger()
        root.handlers = []
        root.setLevel(self.__config.loglevel)
        root.addHandler(self.__logFileHandler)
        root.addHandler(self.__logConsoleHandler)
 
        self.__logger.propagate = True
        self.__logger.setLevel(self.__config.loglevel)

        self.__logger.info(f'Logging started to `{self.__logfile}`.')

    def __stop_logging(self):
        self.__logger.info(f'Logging finished to `{self.__logfile}`.')

        # Disconnect file logger and re-assign stderr
        root = logging.getLogger()
        root.handlers = []
        root.addHandler(logging.StreamHandler())

        # Destroy logging objects (but keep last filename)
        self.__logFormatter = None
        self.__logFileHandler = None
        self.__logConsoleHandler = None

    def __execute_step(self, name, step):
        """
        Execute a single processing step. Handle exceptions and return `True` if the
        execution succeeded.
        """

        try:
            self.__logger.info(f'Executing GA pipeline step `{name}` for objID={self._objId}.')
            step()
            self.__logger.info(f'GA pipeline step `{name}` for objID={self._objId} completed successfully.')
            return True
        except Exception as ex:
            self.__logger.info(f'GA pipeline step `{name}` for objID={self._objId} failed with error `{type(ex).__name__}`.')
            self.__logger.exception(ex)
            
            self.__exceptions.append(ex)
            self.__tracebacks.append(traceback.format_tb(ex.traceback))
            return False

    def __step_init(self):
        # Create output directories
        self.__create_dir(self.__config.outdir, 'output')
        self.__create_dir(self.__config.figdir, 'figure')

    def __step_load(self):
        # Load each PfsConfig and PfsSingle file. Make sure that PfsConfig loaded only once if
        # repeated between exposures.

        self.__pfsConfig = {}
        self.__pfsSingle = {}

        start_time = time.perf_counter()

        for i, visit in enumerate(self.__config.visit):
            identity = {
                'catId': self.__config.catId[visit],
                'tract': self.__config.tract[visit],
                'patch': self.__config.patch[visit],
                'objId': self.__config.objId,
                'visit': visit
            }

            dir = os.path.join(self.__config.datadir, 'pfsSingle/{catId:05d}/{tract:05d}/{patch}'.format(**identity))
            fn = PfsSingle.filenameFormat % identity
            self.__logger.info(f'Loading PfsSingle from `{os.path.join(dir, fn)}`.')
            single =  PfsSingle.read(identity, dirName=dir)
            self.__pfsSingle[visit] = single

            dir = os.path.join(self.__config.datadir, 'pfsConfig/{date}'.format(date=self.__config.date[visit]))
            fn = PfsConfig.fileNameFormat % (self.__config.designId[visit], visit)
            self.__logger.info(f'Loading PfsConfig from `{os.path.join(dir, fn)}`.')
            config = PfsConfig.read(self.__config.designId[visit], visit, dirName=dir)
            self.__pfsConfig[visit] = config

        stop_time = time.perf_counter()
        self.__logger.info(f'PFS data files loaded successfully for {len(self.__pfsSingle)} exposures in {stop_time - start_time:.3f} s.')

        # Construct the output object that will filled up by subsequent steps
        self.__pfsGAObject = PfsGAObject()

    def __step_rvfit(self):
        # TODO: run RV fitting
        raise NotImplementedError()

    def __step_chemfit(self):
        # TODO: run abundance fitting
        raise NotImplementedError()

    def __step_coadd(self):
        # TODO: run spectrum co-adding
        raise NotImplementedError()

    def __step_save(self):
        # TODO: save any outputs
        raise NotImplementedError()

    def __step_cleanup(self):
        # TODO: Perform any cleanup
        raise NotImplementedError()
