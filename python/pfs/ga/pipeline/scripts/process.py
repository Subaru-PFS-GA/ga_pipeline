#!/bin/env python3

import os

from pfs.ga.pipeline import GA1DPipeline, GA1DPipelineTrace
from pfs.ga.pipeline.config import GA1DPipelineConfig
from .script import Script

class Process(Script):
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

        config = GA1DPipelineConfig()
        config.load(self.__config)
        
        # Override a few settings from the command line
        config.datadir = self._get_arg('datadir', default=config.datadir)
        config.workdir = self._get_arg('workdir', default=config.workdir)
        config.rerundir = self._get_arg('rerundir', default=config.rerundir)
        config.outdir = self._get_arg('outdir', default=config.outdir)
        config.logdir = os.path.join(config.workdir, 'log')
        config.figdir = os.path.join(config.workdir, 'fig')

        trace = GA1DPipelineTrace(figdir=config.figdir, logdir=config.logdir)
        trace.init_from_args(self, None, config.trace_args)

        pipeline = GA1DPipeline(config, trace)
        
        pipeline._validate_config()
        pipeline.execute()

def main():
    script = Process()
    script.execute()

if __name__ == "__main__":
    main()