from .config import Config

class ChemfitConfig(Config):
    """
    Configuration class for the chemfit step of the pipeline.
    """

    def __init__(self):
        super().__init__()