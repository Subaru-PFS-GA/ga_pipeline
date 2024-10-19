import os
import re
from glob import glob
from types import SimpleNamespace

from pfs.datamodel import *

from ..setup_logger import logger

from ..constants import Constants
from .intfilter import IntFilter
from .hexfilter import HexFilter
from .datefilter import DateFilter
from .stringfilter import StringFilter
from .config import config as default_config

class FileSystemConnector():
    """
    Implements routines to find data products in the file system.
    This is a replacement of Butler for local development.

    The class works by querying the file system with glob for files that match the specified parameters.
    The parameters can either be defined on the class level or passed as arguments to the methods.
    Method arguments always take precedence over class-level parameters.

    Variables
    ---------
    datadir : str
        Path to the data directory. Set to GAPIPE_DATADIR by default.
    rerundir : str
        Path to the rerun directory. Set to GAPIPE_RERUNDIR by default.
    PfsDesignId : HexFilter
        Filter for PfsDesign identifier.
    catId : IntFilter
        Filter for catalog identifier.
    tract : IntFilter
        Filter for tract number.
    patch : StringFilter
        Filter for patch identifier.
    objId : HexFilter
        Filter for object identifier.
    visit : IntFilter
        Filter for visit number.
    date : DateFilter
        Filter for observation date.
    """

    def __init__(self,
                 config=None,
                 orig=None):
        
        if not isinstance(orig, FileSystemConnector):
            self.__config = config if config is not None else default_config

            self.__pfsDesignId = HexFilter(name='pfsDesignId', format='{:016x}')
            self.__catId = IntFilter(name='catid', format='{:05d}')
            self.__tract = IntFilter(name='tract', format='{:05d}')
            self.__patch = StringFilter(name='patch')
            self.__objId = HexFilter(name='objid', format='{:016x}')
            self.__visit = IntFilter(name='visit', format='{:06d}')
            self.__date = DateFilter(name='date', format='{:%Y-%m-%d}')
        else:
            self.__config = config if config is not None else orig.__config

            self.__pfsDesignId = orig.__pfsDesignId
            self.__catId = orig.__catId
            self.__tract = orig.__tract
            self.__patch = orig.__patch
            self.__objId = orig.__objId
            self.__visit = orig.__visit
            self.__date = orig.__date

    #region Properties

    def __get_config(self):
        return self.__config
    
    config = property(__get_config)

    def __get_pfsDesignId(self):
        return self.__pfsDesignId
    
    pfsDesignId = property(__get_pfsDesignId)

    def __get_catId(self):
        return self.__catId
    
    catId = property(__get_catId)

    def __get_tract(self):
        return self.__tract
    
    tract = property(__get_tract)

    def __get_patch(self):
        return self.__patch
    
    def __set_patch(self, value):
        self.__patch = value
    
    patch = property(__get_patch, __set_patch)

    def __get_objId(self):
        return self.__objId
    
    objId = property(__get_objId)

    def __get_visit(self):
        return self.__visit
    
    visit = property(__get_visit)

    #endregion
    #region Utility functions

    def __throw_or_warn(self, message, required, exception_type=ValueError):
        """
        If required is True, raises an exception with the specified message, otherwise logs a warning.

        Arguments
        ---------
        message : str
            Message to log or raise.
        required : bool
            If True, an exception is raised. Otherwise, a warning is logged.
        exception_type : Exception
            Type of the exception to raise. Default is ValueError.
        """

        if required:
            raise exception_type(message)
        else:
            logger.warning(message)

    def __ensure_one_arg(self, **kwargs):
        """
        Ensures that only one parameter is specified in the keyword arguments.

        Arguments
        ---------
        kwargs : dict
            Keyword arguments to check.

        Returns
        -------
        str
            Name of the parameter that is specified.
        """

        if sum([1 for x in kwargs.values() if x is not None]) != 1:
            names = ', '.join(kwargs.keys())
            raise ValueError(f'Only one of the parameters {names} can be specified .')
        
    def __parse_identity(self, regex, path: str, params: SimpleNamespace):
        """
        Parses parameters from the filename using the specified regex pattern.

        Arguments
        ---------
        path : str
            Path to the file.
        """

        # Match the filename pattern to find the IDs
        match = re.search(regex, path)
        if match is not None:
            groups = match.groupdict()
            values = { k: p.parse_value(groups[k]) for k, p in params.items() if k in groups }
            return SimpleNamespace(**values)
        else:
            return None

    def __find_files_and_match_params(self,
                                      patterns: list,
                                      params: SimpleNamespace,
                                      param_values: SimpleNamespace,
                                      params_regex: list):
        """
        Given a list of directory name glob pattern template strings, substitute the parameters
        and find files that match the glob pattern. Match IDs is in the file names with the
        parameters and return the matched IDs, as well as the paths to the files. The final
        list is filtered by the parameters.

        Arguments
        ---------
        patterns : str
            List of directory name glob pattern template strings.
        params : SimpleNamespace
            Parameters to match the IDs in the file names.
        param_values : SimpleNamespace
            Values of the parameters to match the IDs in the file names.
        params_regex : str
            Regular expression patterns to match the filename. The regex should contain named groups
            that correspond to the parameters of the product identity. The list should consist of
            more restrictive patterns first.

        Returns
        -------
        list of str
            List of paths to the files that match the query.
        SimpleNamespace
            List of identifiers that match the query.
        """

        # Unwrap the parameters
        params = { k: p.copy() for k, p in params.__dict__.items() }

        # Update the parameters with the values
        for k, v in param_values.items():
            if k in params:
                if v is not None:
                    params[k].values = v
                elif hasattr(self, k):
                    params[k].values = getattr(self, k)

        # Evaluate the glob pattern for each filter parameter
        glob_pattern_parts = { k: p.get_glob_pattern() for k, p in params.items() }

        # Compose the full glob pattern
        glob_pattern = os.path.join(self.__config.root, *[ p.format(**glob_pattern_parts) for p in patterns ])

        # Substitute config variables into the glob pattern
        glob_pattern = os.path.expandvars(glob_pattern)

        # Find the files that match the glob pattern
        paths = glob(glob_pattern)

        ids = { k: [] for k in params.keys() }
        values = { k: None for k in params.keys() }
        filenames = []
        for path in paths:
            for regex in params_regex:
                # Match the filename pattern to find the IDs
                match = re.search(regex, path)
                
                if match is not None:
                    # If all parameters match the param filters, add the IDs to the list
                    good = True
                    for k, param in params.items():
                        # Parse the string value from the match and convert it to the correct type
                        values[k] = param.parse_value(match.group(k))

                        # Match the parameter against the filter. This is a comparison
                        # against the values and value ranges specified in the filter.
                        good &= param.match(values[k])

                    if good:
                        filenames.append(path)
                        for k, v in values.items():
                            ids[k].append(v)
                        
                    break # for regex in regex_list

        return filenames, SimpleNamespace(**ids)
    
    def __get_single_file(self, files, identities):
        """
        Given a list of files and a list of identifiers, returns the single file that matches the query.
        If more than one file is found, an exception is raised. If no files are found, an exception is raised.

        Arguments
        ---------
        files : list of str
            List of paths to the files that match the query.
        identities : SimpleNamespace
            List of identifiers that match the query.

        Returns
        -------
        str
            Path to the file that matches the query.
        SimpleNamespace
            Identifiers that match the query.
        """

        if len(files) == 0:
            raise FileNotFoundError(f'No file found matching the query.')
        elif len(files) > 1:
            raise FileNotFoundError(f'Multiple files found matching the query.')
        else:
            return files[0], SimpleNamespace(**{ k: v[0] for k, v in identities.__dict__.items() })
        
    #endregion
    #region Products

    def parse_product_identity(self, product, path: str, required=True):
        """
        Parses parameters from the filename using the specified regex pattern.

        Arguments
        ---------
        path : str
            Path to the file.
        params : SimpleNamespace
            Parameters to parse from the filename.
        regex : str
            Regular expression pattern to match the filename. The regex should contain named groups
            that correspond to the parameters in the SimpleNamespace.

        Returns
        -------
        SimpleNamespace
            Parsed identity parameters, or None, when the filename does not match the expected format.
        """

        # Unwrap the parameters
        params = self.__config.products[product].params.__dict__

        # Try to parse with each regular expression defined in the config
        for regex in self.__config.products[product].params_regex:
            identity = self.__parse_identity(regex, path, params)
            if identity is not None:
                return identity
        
        # If no match is found
        self.__throw_or_warn(f'Filename does not match expected format: {path}', required)
        return None
    
    def find_product(self, product, **kwargs):
        """
        Finds profuct files that match the specified filters.

        Arguments
        ---------
        product : type
            Type of the product to find.
        kwargs : dict
            Additional parameters to match the product identity. Can be of one of scalar type,
            or a SearchFilter instance.

        Returns
        -------
        list of str
            List of paths to the files that match the query.
        SimpleNamespace
            List of identities that match the query.
        """

        return self.__find_files_and_match_params(
            patterns = [
                self.__config.products[product].dir_format,
                self.__config.products[product].filename_format,
            ],
            params_regex = self.__config.products[product].params_regex,
            params = self.__config.products[product].params,
            param_values = kwargs)
    
    def locate_product(self, product, **kwargs):
        """
        Finds a specific product file.

        Arguments
        ---------
        product : type
            Type of the product to locate.
        kwargs : dict
            Additional parameters to match the product identity. Can be of one of scalar type,
            or a SearchFilter instance.

        Returns
        -------
        str
            Path to the file that matches the query.
        SimpleNamespace
            The identity of the product that matches the query.
        """

        files, ids = self.find_product(product, **kwargs)
        return self.__get_single_file(files, ids)
    
    def load_product(self, product, *, filename=None, identity=None):
        """
        Loads a product from a file or based on identity.

        Arguments
        ---------
        product : type
            Type of the product to load.
        filename : str
            Path to the file to load.
        identity : SimpleNamespace
            Identity of the product to load.
        """

        self.__ensure_one_arg(filename=filename, identity=identity)

        # Some products cannot be loaded by filename, so we need to parse the identity
        if filename is not None:
            identity = self.parse_product_identity(product, filename, required=True)

        # The file name might not contain all information necessary to load the
        # product, so given the parsed identity, we need to locate the file.
        filename, identity = self.locate_product(product, **identity.__dict__)
        dir = os.path.dirname(filename)

        # Load the product via the dispatcher
        product = self.__config.products[product].load(identity, filename, dir)

        return product, identity, filename

    #endregion
