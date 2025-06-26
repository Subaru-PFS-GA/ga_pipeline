import logging

from .config import Config

class PipelineConfig(Config):
    def __init__(self):
        self.trace_args = {}

        self.logdir = None                                    # Log directory
        self.figdir = None                                    # Figure directory

        self.loglevel = logging.INFO

        super().__init__()