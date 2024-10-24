import os
import traceback
from collections import namedtuple
try:
    import debugpy
except ModuleNotFoundError:
    debugpy = None

from .util import Timer
from .scripts.script import Script
from .constants import *
from .config.pipelineconfig import PipelineConfig
from .pipelinetrace import PipelineTrace
from .pipelineexception import PipelineException

from .setup_logger import logger

# Create a named tuple for the pipeline step
StepResults = namedtuple('StepResults', ['success', 'skip_remaining', 'skip_substeps'])

class Pipeline():
    def __init__(self,
                 script: Script = None,
                 config: PipelineConfig = None,
                 trace: PipelineTrace = None):

        self.__script = script
        self.__config = config
        self.__trace = trace

        self.__exceptions = []
        self.__tracebacks = []

        self._steps = None

    def reset(self):
        pass

    def update(self, 
               script: Script = None,  
               config: PipelineConfig = None, 
               trace: PipelineTrace = None):
        
        self.__script = script if script is not None else self.__script
        self.__config = config if config is not None else self.__config
        self.__trace = trace if trace is not None else self.__trace

        # TODO: reset anything else?

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

        # Save exceptions to a file and reset them so that
        # they are not reported again on the driver script level
        self.__save_exceptions()
        self.__reset_exceptions()

    def _create_dir(self, name, dir):
        self.script._create_dir(name, dir, logger=logger)

    def _test_dir(self, name, dir, must_exist=True):
        """Verify that a directory exists and is accessible."""

        if dir is None:
            logger.info(f'{name.title()} directory is not set, will use default.')
        elif not os.path.isdir(dir):
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

    def get_loglevel(self):
        raise NotImplementedError()

    def get_product_logfile(self):
        raise NotImplementedError()
    
    def __start_tracing(self):
        if self.__trace is not None:
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
                
                # TODO: Create named tuple for the return values
                step_results = func()
                if not step_results.success and critical:
                    raise PipelineException(f'Pipeline step `{name}` failed and is critical. Stopping pipeline.')

                logger.info(timer.format_message(stop_message))
                return step_results
            except Exception as ex:
                error_message = self._get_log_message_step_error(name, ex)
                logger.error(timer.format_message(error_message))
                logger.exception(ex)
                
                self.__exceptions.append(ex)
                self.__tracebacks.append(traceback.format_tb(ex.__traceback__))

                # TODO: Add trace hook for the exception

                # Enable these lines to automatically break into debugger, if available
                if debugpy is not None and debugpy.is_client_connected():
                    raise ex

                return False, True, True

    def _get_log_message_step_start(self, name):
        return f'Executing pipeline step `{name}`'

    def _get_log_message_step_stop(self, name):
        return f'Pipeline step `{name}` completed successfully in {{:.3f}} sec.'

    def _get_log_message_step_error(self, name, ex):
        return f'Pipeline step `{name}` failed with error `{type(ex).__name__}`.'
    
    def __save_exceptions(self):
        if self.__exceptions is not None and len(self.__exceptions) > 0:
            
            # Get full path of log file without extension
            logfile = self.get_product_logfile()
            if logfile is not None:
                logdir = os.path.dirname(logfile)
                logfile = os.path.basename(logfile)
                logfile = os.path.splitext(logfile)[0]
                fn = os.path.join(logdir, logfile + '.traceback')
                with open(fn, 'a') as f:
                    for i in range(len(self.__exceptions)):
                        f.write(repr(self.__exceptions[i]))
                        f.write('\n')
                        f.writelines(self.__tracebacks[i])
                        f.write('\n')

    def __reset_exceptions(self):
        self.__exceptions = []
        self.__tracebacks = []