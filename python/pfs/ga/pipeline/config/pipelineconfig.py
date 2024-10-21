import logging

from .config import Config

class PipelineConfig(Config):
    def __init__(self):
        self.trace_args = {}

        self.workdir = self._get_env('GAPIPE_WORKDIR')        # Working directory, must be writable         
        self.outdir = self._get_env('GAPIPE_WORKDIR')         # Directory for output data files, must be writable
        self.logdir = None                                    # Log directory, must be writable
        self.figdir = None                                    # Figure directory, must be writable

        self.loglevel = logging.INFO

        super().__init__()