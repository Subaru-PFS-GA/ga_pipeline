import os
import logging
import traceback

from pfs.datamodel import PfsConfig

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

    def __init__(self, config, objId = None, pfsConfig: dict = None, pfsSingle: dict = None):
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
        self.__objId = objId
        self.__pfsConfig = pfsConfig          
        self.__pfsSingle = pfsSingle
        self.__pfsGAObject = None

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

    def __get_pfsGAObject(self):
        return self.__pfsGAObject
    
    pfsGAObject = property(__get_pfsGAObject)

    def __get_exceptions(self):
        return self.__exceptions
    
    exceptions = property(__get_exceptions)

    def __get_tracebacks(self):
        return self.__tracebacks

    def verify(self):
        """
        Verifies the configuration and the existence of all necessary input data. Returns
        `True` if the pipeline can proceed or 'False' if it cannot.

        Return
        -------
        :bool:
            `True` if the pipeline can proceed or 'False' if it cannot.
        """

        return True

    def execute(self):
        """
        Execute the pipeline steps sequentially and return the output PfsGAObject containing
        the inferred parameters and the co-added spectrum.
        """

        if not self._start_logging():
            return None
        
        for i, step in enumerate(self._steps):
            success = self._execute_step(step['name'], step['func'])
            if not success and step['critical']:
                break

        self._stop_logging()
        
        return self._pfsGAObject

    def __start_logging(self):
        pass

    def __stop_logging(self):
        pass

    def __execute_step(self, name, step):
        """
        Execute a single processing step. Handle exceptions and return `True` if the
        execution succeeded.
        """

        try:
            logging.info(f'Executing GA pipeline step `{name}` for objID={self._objId}.')



            logging.info(f'GA pipeline step `{name}` for objID={self._objId} completed successfully.')
            return True
        except Exception as ex:
            logging.info(f'GA pipeline step `{name}` for objID={self._objId} failed with error `{type(ex).__name__}`.')
            self.__exceptions.append(ex)
            self.__tracebacks.append(traceback.format_tb(ex.traceback))
            return False

    def __step_init(self):
        # TODO: create output directories
        raise NotImplementedError()

    def __step_load(self):
        # TODO: open synthetic grid
        raise NotImplementedError()

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
