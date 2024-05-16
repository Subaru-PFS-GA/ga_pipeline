import logging

from .config import Config

class PipelineConfig(Config):
    def __init__(self, config=None):
        self.workdir = self._get_env('GAPIPE_WORKDIR')        # Working directory, must be writable         
        self.datadir = self._get_env('GAPIPE_DATADIR')        # PFS survey data directory root
        self.logdir = self._get_env('GAPIPE_WORKDIR')         # Log directory, must be writable
        self.figdir = self._get_env('GAPIPE_WORKDIR')         # Figure directory, must be writable
        self.outdir = self._get_env('GAPIPE_WORKDIR')         # Directory for output data files, must be writable

        self.loglevel = logging.INFO

        super().__init__(config=config)