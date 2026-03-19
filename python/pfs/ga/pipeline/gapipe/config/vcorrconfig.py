from pfs.ga.common.config import Config

class VCorrConfig(Config):
    """
    Configuration class for velocity corrections.
    """

    def __init__(self):

        # Telescope site (astropy id)
        self.site = 'Subaru'

        # Initial frame, i.e. 'observed' or 'geocentric'
        self.from_frame = None

        # Target frame, i.e. 'heliocentric', 'barycentric', 'geocentric'
        self.to_frame = None

        super().__init__()