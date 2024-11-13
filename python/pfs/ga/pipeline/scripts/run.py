#!/usr/bin/env python3

import os
import logging

from pfs.ga.pfsspec.survey.repo import FileSystemRepo

from ..common.script import Script
from ..constants import Constants
from ..gapipe import GAPipeline, GAPipelineTrace
from ..gapipe.config import GAPipelineConfig
from ..repo import PfsFileSystemConfig

from ..setup_logger import logger

class Run(Script):
    """
    Runs the pipeline from a configuration file. The configuration file is either
    passed to it as a parameter, or the script can look it up by the indentity
    of the object being processed.
    """

    def __init__(self):
        super().__init__()

        self.__config = None            # Configuration file or list of files
        self.__dry_run = False          # Dry run mode

        self.__repo = self.__create_data_repo()
        self.__pipeline = None          # Pipeline object
        self.__trace = None             # Pipeline trace object

        # Pipeline configuration files defined on the command line
        # The pipeline will be executed for each of these files
        self.__config_files = None      

    def _add_args(self):
        self.add_arg('--config', type=str, nargs='?', help='Configuration file')
        self.add_arg('--outdir', type=str, help='Output directory')
        self.add_arg('--dry-run', action='store_true', help='Dry run mode')

        # Register data store arguments, do not include search filters
        self.__repo.add_args(self, include_variables=True, include_filters=True)

        super()._add_args()

    def _init_from_args(self, args):
        # Parse the data store arguments
        self.__repo.init_from_args(self)

        self.__config = self.get_arg('config', args)
        self.__dry_run = self.get_arg('dry_run', args, self.__dry_run)

        super()._init_from_args(args)

    def __create_data_repo(self):
        """
        Create a connector to the file system.
        """

        return FileSystemRepo(config=PfsFileSystemConfig)

    def prepare(self):
        super().prepare()

        # Create the pipeline and the trace object
        self.__trace = GAPipelineTrace()
        self.__pipeline = GAPipeline(script=self, repo=self.__repo, trace=self.__trace)

        # Override logging directory to use the same as the workdir
        # This is not the location where the pipeline itself will write the logs
        # because that's the workdir of the output product
        log_file = os.path.basename(self.log_file)
        self.log_file = os.path.join(self.__repo.get_resolved_variable('workdir'), log_file)

    def run(self):

        # If a config file is provided on the command-line, we only process a single one.
        # If no config file is provided, we search for configs based on the command line
        # search filters using the data store connector.
        if self.__config is not None:
            self.__config_files = [ self.__config ]
        else:
            self.__config_files, _ = self.__repo.locate_product(GAPipelineConfig)

        for i, config_file in enumerate(self.__config_files):
            self.__run_pipeline(config_file)

    def __run_pipeline(self, config_file):
        
        # Load the configuration
        config = GAPipelineConfig()
        config.load(config_file, ignore_collisions=True)
        self.__update_directories(config)

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
        self.__trace.update(figdir=logdir, logdir=logdir, id=id)

        # Reconfigure logging according to the configuration
        self.push_log_settings()
        self.stop_logging()

        loglevel = self.__pipeline.get_loglevel()
        if self.log_level is not None and self.log_level < loglevel:
            loglevel = self.log_level
        if self.debug and logging.DEBUG < loglevel:
            loglevel = logging.DEBUG
        self.loglevel = loglevel
        self.logfile = logfile
        self.start_logging()

        logger.info(f'Using configuration file(s) `{config.config_files}`.')
        
        # Execute the pipeline
        self.__pipeline.execute()

        # Restore the logging to the main log file
        self.stop_logging()
        self.pop_log_settings()
        self.start_logging()

    def __update_directories(self, config):
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
            self.__repo.set_variable('workdir', config.workdir)
        if config.outdir is not None:
            self.__repo.set_variable('outdir', config.outdir)
        if config.datadir is not None:
            self.__repo.set_variable('datadir', config.datadir)
        if config.rerundir is not None:
            self.__repo.set_variable('rerundir', config.rerundir)

def main():
    script = Run()
    script.execute()

if __name__ == "__main__":
    main()