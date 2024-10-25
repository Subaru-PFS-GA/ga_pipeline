import numpy as np
from datetime import datetime, date
from json import JSONEncoder

class ConfigJSONEncoder(JSONEncoder):
    """
    JSON encoder for configuration files that allows encoding of NumPy arrays.
    """

    def default(self, obj):
        """
        Convert NumPy arrays to built-in Python lists.
        """

        if isinstance(obj, np.ndarray):
            return obj.tolist()
        elif isinstance(obj, np.generic):
            return obj.item()
        elif isinstance(obj, datetime):
            return obj.isoformat()
        elif isinstance(obj, date):
            return obj.isoformat()
        else:
            return super().default(obj)