#!/bin/env python3

import os

from .script import Script
from ..constants import Constants

class Run(Script):
    """
    Run the pipeline from a configuration file.
    """

    def __init__(self):
        super().__init__()

        self.__config = None

    def _add_args(self):
        super()._add_args()

        self._add_arg('--config', type=str, help='Configuration file', required=True)
        self._add_arg('--datadir', type=str, help='Data directory')
        self._add_arg('--workdir', type=str, help='Working directory')
        self._add_arg('--rerundir', type=str, help='Rerun directory')

    def _init_from_args(self, args):
        super()._init_from_args(args)

        self.__config = self._get_arg('config', args, self.__config)

    def run(self):
        # Create the pipeline object and initialize it from a config file
        from pfs.ga.pipeline import GA1DPipeline, GA1DPipelineTrace
        from pfs.ga.pipeline.config import GA1DPipelineConfig

        config = GA1DPipelineConfig()
        config.load(self.__config, ignore_collisions=True)

        identity = config.target.get_identity()
        
        # Override a few settings from the command line
        config.datadir = self._get_arg('datadir', default=config.datadir).format(**identity)
        config.workdir = self._get_arg('workdir', default=config.workdir).format(**identity)
        config.rerundir = self._get_arg('rerundir', default=config.rerundir)
        config.outdir = self._get_arg('outdir', default=config.outdir).format(**identity)
        config.logdir = os.path.join(config.workdir, 'log')
        config.figdir = os.path.join(config.workdir, 'fig')

        # Intialize the trace object used for logging and plotting
        trace = GA1DPipelineTrace()
        trace.init_from_args(self, None, config.trace_args)
        
        # Initialize the pipeline object
        pipeline = GA1DPipeline(script=self, config=config, trace=trace)        
        
        # Set the object IDs
        id = Constants.PFSOBJECT_ID_FORMAT.format(**identity)
        pipeline.id = id
        trace.id = id

        # Validate and execute the pipeline
        pipeline.validate_config()
        pipeline.validate_libs()
        pipeline.execute()

def main():
    script = Run()
    script.execute()

if __name__ == "__main__":
    main()