import logging

class Config():
    """
    Galactic Archeology Spectrum Processing Pipeline configuration.
    """

    def __init__(self):
        self.workdir = None
        self.logdir = None
        self.datadir = None
        self.figdir = None
        self.outdir = None
        self.modelGridPath = None
        self.loglevel = logging.INFO

        self.objId = None
        self.visit = None
        self.catId = None
        self.tract = None
        self.patch = None
        self.designId = None
        self.date = None
        self.fiber = None