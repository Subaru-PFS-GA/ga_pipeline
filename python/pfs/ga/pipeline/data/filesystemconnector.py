import os
import re
from glob import glob
from types import SimpleNamespace

from pfs.datamodel import *

from ..constants import Constants
from .intfilter import IntFilter
from .hexfilter import HexFilter
from .datefilter import DateFilter
from .stringfilter import StringFilter

class FileSystemConnector():
    """
    Implements routines to find data products in the file system.
    This is a replacement of Butler for local development.
    """

    def __init__(self,
                 datadir=None,
                 rerundir=None,
                 orig=None):
        
        if not isinstance(orig, FileSystemConnector):
            self.__datadir = datadir
            self.__rerundir = rerundir

            self.__pfsDesignId = HexFilter(name='pfsDesignId', format='{:016x}')
            self.__catId = IntFilter(name='catid', format='{:05d}')
            self.__tract = IntFilter(name='tract', format='{:05d}')
            self.__patch = StringFilter(name='patch')
            self.__objId = HexFilter(name='objid', format='{:016x}')
            self.__visit = IntFilter(name='visit', format='{:06d}')
            self.__date = DateFilter(name='date')
        else:
            self.__datadir = datadir if datadir is not None else orig.__datadir
            self.__rerundir = rerundir if rerundir is not None else orig.__rerundir

            self.__pfsDesignId = orig.__pfsDesignId
            self.__catId = orig.__catId
            self.__tract = orig.__tract
            self.__patch = orig.__patch
            self.__objId = orig.__objId
            self.__visit = orig.__visit
            self.__date = orig.__date

    #region Properties

    def __get_datadir(self):
        return self.__datadir
    
    def __set_datadir(self, value):
        self.__datadir = value

    datadir = property(__get_datadir, __set_datadir)

    def __get_rerundir(self):
        return self.__rerundir
    
    def __set_rerundir(self, value):
        self.__rerundir = value

    rerundir = property(__get_rerundir, __set_rerundir)

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

    def __parse_filename_params(self, path: str, params: SimpleNamespace, regex: str):
        # Unwrap the parameters
        params = params.__dict__

        # Match the filename pattern to find the IDs
        match = re.search(regex, path)
        if match is not None:
            return SimpleNamespace(**{ k: p.parse_value(match.group(k)) for k, p in params.items() })
        else:
            raise ValueError(f'Filename does not match expected format: {path}')

    def __find_files_and_match_params(self, *patterns, params: SimpleNamespace, regex: str):
        """
        Given a list of directory name glob pattern template strings, substitute the parameters
        and find files that match the glob pattern. Match IDs is in the file names with the
        parameters and return the matched IDs, as well as the paths to the files.
        """

        # Unwrap the parameters
        params = params.__dict__

        # Evaluate the glob patterns for each filter parameter
        glob_patterns = { k: p.get_glob_pattern() for k, p in params.items() }

        # Compose the full glob pattern
        glob_pattern = os.path.join(*[ p.format(**glob_patterns) for p in patterns ])

        # Find the files that match the glob pattern
        paths = glob(glob_pattern)

        # Match the IDs in the files found
        ids = { k: [] for k in params.keys() }
        values = { k: None for k in params.keys() }
        filenames = []

        for path in paths:
            # Match the filename pattern to find the IDs
            match = re.search(regex, path)
            
            if match is not None:
                # If all parameters match the param filters, add the IDs to the list
                good = True
                for k, param in params.items():
                    values[k] = param.parse_value(match.group(k))
                    good &= param.match(values[k])

                if good:
                    filenames.append(path)
                    for k, v in values.items():
                        ids[k].append(v)

        return filenames, SimpleNamespace(**ids)
    
    def __get_single_file(self, files, ids):
        if len(files) == 0:
            raise FileNotFoundError(f'No file found matching the query.')
        elif len(files) > 1:
            raise FileNotFoundError(f'Multiple files found matching the query.')
        else:
            return files[0], SimpleNamespace(**{ k: v[0] for k, v in ids.__dict__.items() })

    def find_datadir(self, reference_path=None):
        if reference_path is not None:
            # Split path into list of directories
            dirs = reference_path.split(os.sep)

            # Find the parent directory of data in filename
            if 'rerun' in dirs:
                rerun_index = dirs.index('rerun')
            else:
                raise ValueError('Data directory cannot be found based on reference path.')

            return os.path.abspath(os.sep.join(dirs[:rerun_index]))
        else:
            return os.path.abspath(self.__datadir)

    def find_rerundir(self, reference_path=None):      
        if reference_path is not None:
            # Split path into list of directories
            dirs = reference_path.split(os.sep)

            # Find the parent directory of any of the PFS products
            rerun_index = -1
            for product in [ 'pfsArm', 'pfsMerged', 'pfsSingle', 'pfsObject', 'pfsGAObject' ]:
                if product in dirs:
                    rerun_index = dirs.index(product)
                    break

            if rerun_index == -1:
                raise ValueError('Rerun directory cannot be found based on reference path.')

            return os.path.abspath(os.sep.join(dirs[:rerun_index]))
        else:
            return os.path.abspath(os.path.join(self.__datadir, self.__rerundir))

    def find_pfsDesign(self, pfsDesignId=None, reference_path=None):
        """
        Find PfsDesign files.

        Arguments
        ---------
        pfsDesignId : HexIDFilter or int or None
            PfsDesign identifier.
        reference_path : str
            Path to a file referencing the PfsDesign file. The path will be used to
            discover the path to the PfsDesign file.
        """

        return self.__find_files_and_match_params(
            self.find_datadir(reference_path=reference_path),
            Constants.PFSDESIGN_DIR_GLOB,
            Constants.PFSDESIGN_FILENAME_GLOB,
            regex = Constants.PFSDESIGN_FILENAME_REGEX,
            params = SimpleNamespace(
                pfsDesignId = HexFilter(pfsDesignId if pfsDesignId is not None else self.__pfsDesignId)
            ))
    
    def get_pfsDesign(self, pfsDesignId, reference_path=None):
        """
        Find a specific PfsDesign file.

        Arguments
        ---------
        pfsDesignId : int
            PfsDesign identifier.
        reference_path : str
            Path to a file referencing the PfsDesign file. The path will be used to
            discover the path to the PfsDesign file.
        """

        files, ids = self.find_pfsDesign(
            pfsDesignId = pfsDesignId,
            reference_path = reference_path)
        return self.__get_single_file(files, ids)
    
    def load_pfsDesign(self, path=None, identity=None):
            
        if sum([1 for x in [path, identity] if x is not None]) > 1:
            raise ValueError('Only one of filename or identity can be specified.')
        
        if path is not None:
            # PfsConfig cannot read from a file directly, so figure out parameters
            # from the filename
            dir, basename = os.path.split(path)

            # Extract pfsDesignId and visit from the filename
            identity = self.__parse_filename_params(
                basename,
                params = SimpleNamespace(
                    pfsDesignId = HexFilter(),
                ),
                regex = Constants.PFSDESIGN_FILENAME_REGEX
            )
        elif identity is not None:
            dir = ''

        dir = os.path.join(
            self.__datadir,
            Constants.PFSDESIGN_DIR_FORMAT.format(**identity.__dict__),
            dir)

        return PfsDesign.read(identity.pfsDesignId, dirName=dir,)
    
    def find_pfsConfig(self, pfsDesignId=None, visit=None, date=None, reference_path=None):
        """
        Find PfsConfig files.

        Arguments
        ---------
        pfsDesignId : HexIDFilter or int or None
            PfsDesign identifier.
        visit : IntIDFilter or int or None
            Visit number.
        reference_path : str
            Path to a file referencing the PfsConfig file. The path will be used to
            discover the path to the PfsConfig file.
        """

        return self.__find_files_and_match_params(
            self.find_datadir(reference_path=reference_path),
            Constants.PFSCONFIG_DIR_GLOB,
            Constants.PFSCONFIG_FILENAME_GLOB,
            regex = Constants.PFSCONFIG_PATH_REGEX,
            params = SimpleNamespace(
                pfsDesignId = HexFilter(pfsDesignId if pfsDesignId is not None else self.__pfsDesignId),
                visit = IntFilter(visit if visit is not None else self.__visit),
                date = DateFilter(date if date is not None else self.__date)
            ))
    
    def get_pfsConfig(self, visit, pfsDesignId=None, reference_path=None):
        """
        Find a specific PfsConfig file.

        Arguments
        ---------
        pfsDesignId : int
            PfsDesign identifier.
        visit : int
            Visit number.
        reference_path : str
            Path to a file referencing the PfsConfig file. The path will be used to
            discover the path to the PfsConfig file.
        filename : str
            Name of the file to find. If None, the first file found is returned.
        """

        files, ids = self.find_pfsConfig(
            visit = visit,
            pfsDesignId = pfsDesignId,
            reference_path = reference_path)
        return self.__get_single_file(files, ids)
    
    def load_pfsConfig(self, path=None, identity=None):
        
        if sum([1 for x in [path, identity] if x is not None]) > 1:
            raise ValueError('Only one of path or identity can be specified.')
        
        if path is not None:
            # PfsConfig cannot read from a file directly, so figure out parameters
            # from the filename
            dir, basename = os.path.split(path)

            # Extract pfsDesignId and visit from the filename
            identity = self.__parse_filename_params(
                path,
                params = SimpleNamespace(
                    pfsDesignId = HexFilter(),
                    visit = IntFilter(),
                    date = DateFilter(),
                ),
                regex = Constants.PFSCONFIG_PATH_REGEX
            )
        elif identity is not None:
            dir = ''

        dir = os.path.join(
            self.__datadir,
            Constants.PFSCONFIG_DIR_FORMAT.format(**identity.__dict__),
            dir)

        return PfsConfig.read(identity.pfsDesignId, identity.visit, dirName=dir,)
        
    def find_pfsArm(self, catId, tract, patch, objId, visit, arm):
        raise NotImplementedError()
    
    def find_pfsMerged(self, visit):
        raise NotImplementedError()
        
    def find_pfsSingle(self, catId, tract, patch, objId, visit):
        raise NotImplementedError()
    
    def find_pfsObject(self, catId, tract, patch, objId, nVisit, pfsVisitHash):
        raise NotImplementedError()