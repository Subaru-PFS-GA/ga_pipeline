import logging

from .config import Config

class PipelineConfig(Config):
    def __init__(self):
        self.trace_args = {}

        self.logdir = None                                    # Log directory, relative to workdir
        self.figdir = None                                    # Figure directory, relative to workdir

        self.loglevel = logging.INFO

        super().__init__()