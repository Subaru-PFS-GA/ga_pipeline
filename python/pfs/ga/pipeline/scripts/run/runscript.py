#!/usr/bin/env python3

import os
import logging

from pfs.ga.common.scripts import Batch, Progress

from ...gapipe import GAPipeline, GAPipelineTrace
from ..pipelinescript import PipelineScript
from ...gapipe.config import *

from ...setup_logger import logger

class RunScript(PipelineScript, Batch, Progress):
    """
    Runs the pipeline from a configuration file. The configuration file is either
    passed to it as a parameter, or the script can look it up by the indentity
    of the object being processed.

    If a config file is specified on the command-line it overrides
    the default behavior of looking up the configuration files in the repo
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.__config_files = None
        self.__pipeline = None          # Pipeline object
        self.__trace = None             # Pipeline trace object

    def _add_args(self):
        self.add_arg('--config', type=str, nargs='+', help='Configuration files')

        PipelineScript._add_args(self)
        Progress._add_args(self)
        Batch._add_args(self)

    def _init_from_args(self, args):
        self.__config_files = self.get_arg('config', args, self.__config_files)

        PipelineScript._init_from_args(self, args)
        Progress._init_from_args(self, args)
        Batch._init_from_args(self, args)

    def prepare(self):
        super().prepare()

        # Override logging directory to use the same as the pipeline workdir,
        # unless the log file is set from the command-line.
        if not self.is_arg('log_file'):
            self._set_log_file_to_workdir()

    def run(self):
        if self.is_batch():
            self.__submit()
        else:
            self.__run()

    def __get_config_files(self):
        """
        Get a list of config files
        """
        
        # If a config file is provided on the command-line, we only process a single one.
        # If no config file is provided, we search for configs based on the command line
        # search filters using the data store connector.
        if self.__config_files is not None:
            config_files = self.__config_files
        else:
            logger.info('No config files provided on the command-line. Searching for config files in the work repository.')
            config_files, _ = self.work_repo.find_product(GAPipelineConfig)

        if len(config_files) == 0:
            logger.warning('No config files found matching the filters.')
            return []

        return config_files

    def __submit(self):
        """
        Submit a batch job for each config file or object matching the filters.
        """

        config_files = self.__get_config_files()

        logger.info(f'Found {len(config_files)} config files matching the filters. Scheduling for batch submission.')

        # Submit a job for each config file
        for i, config_file in enumerate(self._wrap_in_progressbar(config_files, total=len(config_files), logger=logger)):
            item = os.path.splitext(os.path.basename(config_file))[0]
            log_dir = os.path.dirname(config_file)
            log_file = os.path.splitext(config_file)[0] + '.log'

            command = f'python -m pfs.ga.pipeline.scripts.run.runscript'
            command += f' --config {config_file}'
            command += f' --no-log-to-console'
            command += f' --log-file {log_file}'

            self._submit_job(command, item, output_dir=log_dir)

            if self.top is not None and i >= self.top:
                logger.info(f'Stop after processing {self.top} objects.')
                break

    def __run(self):
        """
        Execute the pipeline in-process for each config file or object matching the filters.
        """
        
        config_files = self.__get_config_files()

        # Create the pipeline and the trace object
        self.__trace = GAPipelineTrace()
        self.__pipeline = GAPipeline(
            script = self,
            input_repo = self.input_repo,
            work_repo = self.work_repo,
            trace = self.__trace)

        for i, config_file in enumerate(self._wrap_in_progressbar(config_files, total=len(config_files), logger=logger)):
            self.__run_pipeline(config_file)

            # TODO: Add other command-line arguments

            if self.top is not None and i >= self.top:
                logger.info(f'Stop after processing {self.top} objects.')
                break

    def __run_pipeline(self, config_file):

        # TODO: this function now takes a single config file and runs the pipeline on it.
        #       We should probably refactor this to allow for multiple config files

        # Load the configuration
        config = GAPipelineConfig()
        config.load(config_file, ignore_collisions=True)
        self._update_directories(config)

        # Generate a string ID for the object being processed
        id = str(config.target.identity)

        # Update the pipeline object
        self.__pipeline.reset()
        self.__pipeline.update(config=config, id=id)

        # Get the log file name and set figdir and logdir of the trace to the same directory
        logfile = self.__pipeline.get_product_logfile()
        logdir = os.path.dirname(logfile)

        # Update the trace object used for logging and plotting
        self.__trace.reset()
        self.__trace.init_from_args(self, config.trace_args)
        if self.plot_level is not None:
            self.__trace.plot_level = self.plot_level
        self.__trace.update(figdir=logdir, logdir=logdir, id=id)

        # Reconfigure logging according to the configuration
        self.push_log_settings()
        self.stop_logging()

        loglevel = self.__pipeline.get_loglevel()
        if self.log_level is not None and self.log_level < loglevel:
            loglevel = self.log_level
        if self.debug and logging.DEBUG < loglevel:
            loglevel = logging.DEBUG
        self.log_level = loglevel
        self.log_file = logfile
        self.start_logging()

        logger.info(f'Using configuration file(s) `{self.config.config_files}`.')
        
        # Execute the pipeline
        self.__pipeline.execute()

        # Restore the logging to the main log file
        self.stop_logging()
        self.pop_log_settings()
        self.start_logging()

    def _update_directories(self, config):
        """
        Ensure the precedence of the configuration settings
        """
        
        #   1. Command-line arguments
        #   2. Configuration file
        #   3. Default values

        # Override configuration with command-line arguments
        if self.is_arg('workdir'):
            config.workdir = self.get_arg('workdir')
        if self.is_arg('outdir'):
            config.outdir = self.get_arg('outdir')
        if self.is_arg('datadir'):
            config.datadir = self.get_arg('datadir')
        if self.is_arg('rerundir'):
            config.rerundir = self.get_arg('rerundir')

        # Override data store connector with configuration values
        if config.workdir is not None:
            self.input_repo.set_variable('workdir', config.workdir)
        if config.outdir is not None:
            self.input_repo.set_variable('outdir', config.outdir)
        if config.datadir is not None:
            self.input_repo.set_variable('datadir', config.datadir)
        if config.rerundir is not None:
            self.input_repo.set_variable('rerundir', config.rerundir)
        if config.rerun is not None:
            self.input_repo.set_variable('rerun', config.rerun)

def main():
    script = RunScript()
    script.execute()

if __name__ == "__main__":
    main()