from pfs.ga.common.config import Config

class PhotometryConfig(Config):
    """
    Configuration class for an AB magnitude measurement.
    """

    def __init__(self):

        # Name of the instrument, e.g. 'ps1', 'hsc' etc.
        self.instrument = None

        # Name of the filter, within the photometric system
        self.filter_name = None

        # Response curve file
        self.filter_path = None

        # Magnitude value, error and optional zero point
        self.magnitude = None
        self.magnitude_error = None
        self.magnitude_zero = None

        # Flux value and error
        self.flux = None
        self.flux_error = None

        super().__init__()