import logging

class Config():
    """
    Galactic Archeology Spectrum Processing Pipeline configuration.
    """

    def __init__(self):
        self.workdir = None
        self.logdir = None
        self.rerundir = None
        self.figdir = None
        self.outdir = None
        self.modelGridPath = None
        self.loglevel = logging.INFO

        self.designId = None
        self.visit = None