class IDFilter():
    """
    Implements and argument parser for ID filters and logic to match
    ranges of IDs within file names.
    """

    def __init__(self, format=None, orig=None):
        if not isinstance(orig, IDFilter):
            self.__format = format if format is not None else '{}'
        else:
            self.__format = format if format is not None else orig.__format

        self.__values = None

    def __get_format(self):
        return self.__format
    
    def __set_format(self, value):
        self.__format = value

    format = property(__get_format, __set_format)

    def __get_values(self):
        return self.__values
    
    values = property(__get_values)

    def _parse_value(self, value):
        raise NotImplementedError()
    
    def parse_value(self, value):
        return self._parse_value(value)

    def parse(self, arg: list):
        """
        Parse a list of arguments into individual IDs and ID ranges.
        """

        self.__values = []

        if arg is not None:
            for a in arg:
                if '-' in a:
                    start, end = a.split('-')
                    self.__values.append((self._parse_value(start), self._parse_value(end)))
                else:
                    self.__values.append(self._parse_value(a))

    def match(self, arg):
        # The filter matches the value if the filter is empty or the value
        # is equal to one of the values or within the inclusive range of one
        # of the ranges in the filter.

        if isinstance(arg, str):
            value = self._parse_value(arg)
        else:
            value = arg

        if len(self.__values) == 0:
            return True
        else:
            for v in self.__values:
                if isinstance(v, tuple) and value >= v[0] and value <= v[1]:
                        return True
                elif value == v:
                        return True

            return False
            
    def get_glob_pattern(self):
        """
        Return a glob pattern that matches all IDs in the filter.
        """

        if len(self.__values) == 1 and not isinstance(self.__values[0], tuple):
            return self.__format.format(self.__values[0])
        else:
            return '*'
        
    def get_regex_pattern(self):
        raise NotImplementedError()