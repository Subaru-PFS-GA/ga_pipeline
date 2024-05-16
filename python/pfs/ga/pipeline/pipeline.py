import os
import time
import logging
import traceback
try:
    import debugpy
except ModuleNotFoundError:
    debugpy = None

from .constants import *
from .config.pipelineconfig import PipelineConfig
from .pipelinetrace import PipelineTrace

class Pipeline():
    def __init__(self, config: PipelineConfig, trace: PipelineTrace = None):

        self.__config = config
        self.__trace = trace

        self.__logger = logging.getLogger(GA_PIPELINE_LOGNAME)
        self.__logfile = None
        self.__logFormatter = None
        self.__logFileHandler = None
        self.__logConsoleHandler = None

        self.__exceptions = []
        self.__tracebacks = []

        self._steps = None

    def __get_config(self):
        return self.__config
    
    config = property(__get_config)

    def __get_trace(self):
        return self.__trace
    
    trace = property(__get_trace)

    def __get_logger(self):
        return self.__logger
    
    logger = property(__get_logger)

    def __get_exceptions(self):
        return self.__exceptions
    
    exceptions = property(__get_exceptions)

    def __get_tracebacks(self):
        return self.__tracebacks
    
    tracebacks = property(__get_tracebacks)
    
    def _validate_config(self):
        raise NotImplementedError()
    
    def execute(self):
        """
        Execute the pipeline steps sequentially and return the output PfsGAObject containing
        the inferred parameters and the co-added spectrum.
        """

        self.__start_logging()
        self.__start_tracing()
        
        for i, step in enumerate(self._steps):
            success = self.__execute_step(step['name'], step['func'])
            if not success and step['critical']:
                break

        self.__stop_tracing()
        self.__stop_logging()

    def _create_dir(self, dir, name):
        if not os.path.isdir(dir):
            os.makedirs(dir, exist_ok=True)
            logging.debug(f'Created {name} directory `{dir}`.')
        else:
            logging.debug(f'Found existing {name} directory `{dir}`.')

    def _get_log_filename(self):
        raise NotImplementedError()

    def __start_logging(self):
        self._create_dir(self.__config.logdir, 'log')

        self.__logfile = os.path.join(self.__config.logdir, self._get_log_filename())
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

    def __start_tracing(self):
        if self.__trace is not None:
            self.__trace.figdir = self.__config.figdir
            self.__logger.info(f'Tracing initialized. Figure directory is `{self.__config.figdir}`.')

    def __stop_tracing(self):
        if self.__trace is not None:
            self.__logger.info(f'Tracing stopped.')

    def __execute_step(self, name, step):
        """
        Execute a single processing step. Handle exceptions and return `True` if the
        execution succeeded.
        """

        try:
            self.__logger.info(self._get_log_message_step_start(name))
            start_time = time.perf_counter()
            step()
            stop_time = time.perf_counter()
            self.__logger.info(self._get_log_message_step_stop(name, stop_time - start_time))
            return True
        except Exception as ex:
            # Break into debugger if available
            if debugpy is not None and debugpy.is_client_connected():
                raise ex

            self.__logger.info(self._get_log_message_step_error(name, ex))
            self.__logger.exception(ex)
            
            self.__exceptions.append(ex)
            self.__tracebacks.append(traceback.format_tb(ex.__traceback__))
            return False

    def _get_log_message_step_start(self, name):
        return f'Executing pipeline step `{name}`'

    def _get_log_message_step_stop(self, name, elapsed_time):
        return f'Pipeline step `{name}` completed successfully.'

    def _get_log_message_step_error(self, name, ex):
        return f'Pipeline step `{name}` failed with error `{type(ex).__name__}`.'