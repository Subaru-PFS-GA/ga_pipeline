from .idfilter import IDFilter

class HexIDFilter(IDFilter):
    """
    Implements an argument parser for hex ID filters and logic to match
    ranges of hex IDs within file names.
    """

    def __init__(self, name, format=None, orig=None):

        format = format if format is not None else '0x{:x}'

        super().__init__(name, format, orig)

    def _parse_value(self, value):
        return int(value, 16)