from .idfilter import IDFilter

class IntIDFilter(IDFilter):
    """
    Implements an argument parser for integer ID filters and logic to match
    ranges of integer IDs within file names.
    """

    def _parse_value(self, value):
        return int(value)