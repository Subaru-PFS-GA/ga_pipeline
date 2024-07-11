import os
import traceback
import logging
try:
    import debugpy
except ModuleNotFoundError:
    debugpy = None

from .setup_logger import logger
from .util import Timer
from .scripts.script import Script
from .constants import *
from .config.pipelineconfig import PipelineConfig
from .pipelinetrace import PipelineTrace
from .pipelineexception import PipelineException

class Pipeline():
    def __init__(self, script: Script, config: PipelineConfig, trace: PipelineTrace = None):

        self.__script = script
        self.__config = config
        self.__trace = trace

        self.__exceptions = []
        self.__tracebacks = []

        self._steps = None

    def __get_script(self): 
        return self.__script
    
    script = property(__get_script)

    def __get_config(self):
        return self.__config
    
    config = property(__get_config)

    def __get_trace(self):
        return self.__trace
    
    trace = property(__get_trace)

    def __get_exceptions(self):
        return self.__exceptions
    
    exceptions = property(__get_exceptions)

    def __get_tracebacks(self):
        return self.__tracebacks
    
    tracebacks = property(__get_tracebacks)
    
    def validate_config(self):
        raise NotImplementedError()
    
    def execute(self):
        """
        Execute the pipeline steps sequentially and return the output PfsGAObject containing
        the inferred parameters and the co-added spectrum.
        """

        self.__start_tracing()

        self.__execute_steps(self._steps)

        self.__stop_tracing()

    def _create_dir(self, name, dir):
        self.script._create_dir(name, dir, logger=logger)

    def _test_dir(self, name, dir, must_exist=True):
        """Verify that a directory exists and is accessible."""

        if not os.path.isdir(dir):
            if must_exist:
                raise FileNotFoundError(f'{name.title()} directory `{dir}` does not exist.')
            else:
                logger.info(f'{name.title()} directory `{dir}` does not exist, will be automatically created.')
        else:
            logger.info(f'Using {name} directory `{dir}`.')

    def _test_file(self, name, filename, must_exists=True):
        """Verify that a file exists and is accessible."""
        
        if not os.path.isfile(filename):
            if must_exists:
                raise FileNotFoundError(f'{name.title()} file `{filename}` does not exist.')
            else:
                logger.info(f'{name.title()} file `{filename}` does not exist, will be automatically created.')
        else:
            logger.info(f'Using {name} file `{filename}`.')

    def get_log_filename(self):
        raise NotImplementedError()
    
    def get_log_level(self):
        # Override log level from the command-line
        loglevel = self.__config.loglevel

        if self.__script.log_level is not None and self.__script.log_level < loglevel:
            loglevel = self.__script.log_level
        
        if self.__script.debug and logging.DEBUG < loglevel:
            loglevel = logging.DEBUG
        
        return loglevel

    def __start_tracing(self):
        if self.__trace is not None:
            self.__trace.figdir = self.__config.figdir
            logger.info(f'Tracing initialized. Figure directory is `{self.__config.figdir}`.')

    def __stop_tracing(self):
        if self.__trace is not None:
            logger.info(f'Tracing stopped.')

    def __execute_steps(self, steps):
        """Execute a list of processing steps, and optionally any substeps"""

        success = True
        for i, step in enumerate(steps):
            if 'func' in step:
                suc, skip_remaining, skip_substeps = self.__execute_step(step['name'], step['func'], step['critical'])
                success = success and suc
                if skip_remaining:
                    break

            # Call recursively for substeps
            if not skip_substeps and 'substeps' in step:
                suc = self.__execute_steps(step['substeps'])
                success = success and suc

        return success

    def __execute_step(self, name, func, critical):
        """
        Execute a single processing step. Handle exceptions and return `True` if the
        execution succeeded.
        """

        with Timer() as timer:
            start_message = self._get_log_message_step_start(name)
            stop_message = self._get_log_message_step_stop(name)

            try:
                logger.info(start_message)
                
                success, skip_remaining, skip_substeps = func()
                if not success and critical:
                    raise PipelineException(f'Pipeline step `{name}` failed and is critical. Stopping pipeline.')

                logger.info(timer.format_message(stop_message))
                return success, skip_remaining, skip_substeps
            except Exception as ex:
                # Break into debugger, if available
                if debugpy is not None and debugpy.is_client_connected():
                    raise ex

                error_message = self._get_log_message_step_error(name, ex)
                logger.error(timer.format_message(error_message))
                logger.exception(ex)
                
                self.__exceptions.append(ex)
                self.__tracebacks.append(traceback.format_tb(ex.__traceback__))
                return False, True, True

    def _get_log_message_step_start(self, name):
        return f'Executing pipeline step `{name}`'

    def _get_log_message_step_stop(self, name):
        return f'Pipeline step `{name}` completed successfully in {{:.3f}} sec.'

    def _get_log_message_step_error(self, name, ex):
        return f'Pipeline step `{name}` failed with error `{type(ex).__name__}`.'