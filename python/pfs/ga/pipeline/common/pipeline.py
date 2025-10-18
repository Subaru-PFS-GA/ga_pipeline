import os
import traceback
from collections import namedtuple
from types import SimpleNamespace, MethodType
try:
    import debugpy
except ModuleNotFoundError:
    debugpy = None

from pfs.ga.common.scripts import Script

from ..util import Timer
from .pipelineerror import PipelineError
from .pipelineconfig import PipelineConfig
from .pipelinetrace import PipelineTrace
from .pipelinestep import PipelineStep, PipelineStepResults

from .setup_logger import logger

class Pipeline():
    # TODO: make pipeline stateless by moving config and trace to the context

    def __init__(self, /,
                 script: Script = None,
                 config: PipelineConfig = None,
                 trace: PipelineTrace = None):

        self.__script = script          # Caller command-line script

        self.__config = config          # Pipeline configuration
        self.__trace = trace            # Pipeline tracing

        self.__exceptions = []          # Stores exceptions raised during execution
        self.__tracebacks = []          # Stores tracebacks for exceptions

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

    #region Utility function

    def create_dir(self, name, dir):
        """Create a directory if it does not exist."""

        dir = os.path.join(os.getcwd(), dir)
        if not os.path.isdir(dir):
            os.makedirs(dir, exist_ok=True)
            logger.debug(f'Created {name} directory `{dir}`.')
            return True
        else:
            logger.debug(f'Found existing {name} directory `{dir}`.')
            return False

    def test_dir(self, name, dir, must_exist=True):
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

    def test_file(self, name, filename, must_exists=True):
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

    #endregion
    
    def create_context(self, state=None, trace=None):
        """
        Returns a dictionary of objects that are passed to the worker functions of the
        steps. The worker functions can use these context objects to read and write
        data that is shared across steps.
        """

        context = SimpleNamespace(
            pipeline = self,
            config = self.__config,
            state = state,
            trace = trace
        )

        return context
    
    def create_state(self, pipeline=None, config=None):
        """
        Instantiate a state object that will be passed to each step of the pipeline.
        """
        return None
    
    def destroy_state(self, state):
        """Clean up the state object after the pipeline execution."""
        pass

    def create_steps(self):
        raise NotImplementedError()
    
    def execute(self):
        """
        Execute the pipeline steps sequentially and return the output PfsStar containing
        the inferred parameters and the co-added spectrum.
        """

        self.__start_tracing()

        # Call the pipeline to return the step definitions
        steps = self.create_steps()

        # Create a new state for the pipeline and wrap it into a context that will
        # be passed to the worker functions of the steps
        state = self.create_state()
        context = self.create_context(state=state, trace=self.__trace)

        # Execute the steps one by one
        self.__execute_steps(steps, context)

        # Clean up the state
        self.destroy_state(state)
        
        self.__stop_tracing()

        # Save exceptions to a file and reset them so that
        # they are not reported again on the driver script level
        self._save_exceptions(self.__exceptions, self.__tracebacks)
        self.__reset_exceptions()
    
    def __start_tracing(self):
        if self.__trace is not None:
            logger.info(f'Tracing initialized. Figure directory is `{self.__config.figdir}`.')

    def __stop_tracing(self):
        if self.__trace is not None:
            logger.info(f'Tracing stopped.')

    def __execute_steps(self, steps, context, step_instance=None):
        """Execute a list of processing steps, and optionally any substeps"""

        top_level = step_instance is None

        success = True
        for i, step in enumerate(steps):
            
            # If this is a top level step, instantiate the step class
            if top_level:
                step_instance = step['type']()

            if 'func' in step:
                suc, skip_remaining, skip_substeps = self.__execute_step(
                    step_instance,
                    context,
                    step['name'],
                    step['func'],
                    step['critical'])
                
                success = success and suc
                if skip_remaining:
                    break
            else:
                PipelineError(f'Pipeline step `{step["name"]}` does not have a worker function.')

            # Call recursively for substeps
            if not skip_substeps and 'substeps' in step:
                suc = self.__execute_steps(step['substeps'], context, step_instance=step_instance)
                success = success and suc

            # If this is a top level step, delete the instance
            if top_level:
                del step_instance

        return success

    def __execute_step(self, step_instance, context, name, func, critical):
        """
        Execute a single processing step. Handle exceptions and return `True` if the
        execution succeeded.
        """

        with Timer() as timer:
            start_message = self._get_log_message_step_start(name)
            stop_message = self._get_log_message_step_stop(name)

            try:
                logger.info(start_message)
                
                # Call the step or substep worker function of the step instance
                # ff = MethodType(func, step_instance)            # Bind the method to the instance
                step_results = func(step_instance, context)

                if not step_results.success and critical:
                    raise PipelineError(f'Pipeline step `{name}` failed and is critical. Stopping pipeline.')

                logger.info(timer.format_message(stop_message))
                return step_results
            except Exception as ex:
                error_message = self._get_log_message_step_error(name, ex)
                logger.error(timer.format_message(error_message))
                logger.exception(ex)
                
                self.__exceptions.append(ex)
                self.__tracebacks.append(traceback.format_tb(ex.__traceback__))

                # TODO: Add trace hook for the exception

                # Enable these lines to automatically break into debugger and get a valid
                # stack trace, if the debugger is attached
                if debugpy is not None and debugpy.is_client_connected():
                    raise ex

                return False, True, True

    def _get_log_message_step_start(self, name):
        return f'Executing pipeline step `{name}`'

    def _get_log_message_step_stop(self, name):
        return f'Pipeline step `{name}` completed successfully in {{:.3f}} sec.'

    def _get_log_message_step_error(self, name, ex):
        return f'Pipeline step `{name}` failed with error `{type(ex).__name__}`.'
    
    def _save_exceptions(self, exceptions, tracebacks):
        pass

    def __reset_exceptions(self):
        self.__exceptions = []
        self.__tracebacks = []