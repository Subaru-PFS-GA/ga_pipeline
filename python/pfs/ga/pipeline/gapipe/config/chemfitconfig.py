from ...common.config import Config

class ChemfitConfig(Config):
    """
    Configuration class for the chemfit step of the pipeline.
    """

    def __init__(self):

        # CHEMFIT global parameters
        
        self.min_unmasked_pixels = 3000           # Only fit if there's enough non-masked pixels

        # Flags to treat as masked pixel, combined with logical or.
        self.mask_flags = [
            'BAD',
            'BAD_FIBERTRACE',
            'BAD_FLAT',
            'BAD_FLUXCAL',
            'BAD_SKY',
            'CR',
            'DETECTED',
            'DETECTED_NEGATIVE',
            'EDGE',
            'FIBERTRACE',
            'INTRP',
            'IPC',
            'NO_DATA',
            'REFLINE',
            'SAT',
            'SUSPECT',
            'UNMASKEDNAN'
        ]

        self.required_products = [ 'pfsConfig', 'pfsMerged' ]

        super().__init__()