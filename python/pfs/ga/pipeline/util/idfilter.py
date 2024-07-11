class IDFilter():
    """
    Implements and argument parser for ID filters and logic to match
    ranges of IDs within file names.

    ID filters are used to select a subset of files based on their IDs and are
    specified on the command-line as strings which are parsed into a list of strings
    by `ArgParse`. The strings within a list can be single IDs or ranges of IDs
    separated by a hyphen. For example, the argument `--visit 120 123-127` would be
    parsed into the list `['123', ('123', '127')]`.
    """

    def __init__(self, name, format=None, orig=None):
        if not isinstance(orig, IDFilter):
            self.__name = name
            self.__format = format if format is not None else '{}'
        else:
            self.__name = name if name is not None else orig.__name
            self.__format = format if format is not None else orig.__format

        self.__values = None

    def __str__(self):
        """
        Convert the ID filter into a string.
        """
        
        res = ''
        for a in self.__values:
            if res != '':
                res += ' '

            if isinstance(a, tuple):
                res += '{}-{}'.format(self.__format.format(a[0]), self.__format.format(a[1]))
            else:
                res += self.__format.format(a)
        return res

    def __get_name(self):
        return self.__name
    
    def __set_name(self, value):
        self.__name = value

    name = property(__get_name, __set_name)

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