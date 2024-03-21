import logging

from .config import Config

class PipelineConfig(Config):
    def __init__(self, config=None):
        self.workdir = None
        self.datadir = None
        self.logdir = None
        self.figdir = None
        self.outdir = None

        self.loglevel = logging.INFO

        super().__init__(config=config)