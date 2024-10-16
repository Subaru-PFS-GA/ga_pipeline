import os
import re
from glob import glob

from pfs.datamodel import *

from ..constants import Constants
from ..util import IntIDFilter, HexIDFilter

class FileSystemDiscovery():
    """
    Implements routines to find data products in the file system.
    This is a replacement of Butler for local development.
    """

    def __init__(self,
                 datadir=None,
                 rerundir=None,
                 orig=None):
        
        if not isinstance(orig, FileSystemDiscovery):
            self.__datadir = datadir
            self.__rerundir = rerundir

            self.__pfsDesignId = HexIDFilter(name='pfsDesignId', format='{:016x}')
            self.__catId = IntIDFilter(name='catid', format='{:05d}')
            self.__tract = IntIDFilter(name='tract', format='{:05d}')
            self.__patch = None
            self.__objId = HexIDFilter(name='objid', format='{:016x}')
            self.__visit = IntIDFilter(name='visit', format='{:06d}')
        else:
            self.__datadir = datadir if datadir is not None else orig.__datadir
            self.__rerundir = rerundir if rerundir is not None else orig.__rerundir

            self.__pfsDesignId = orig.__pfsDesignId
            self.__catId = orig.__catId
            self.__tract = orig.__tract
            self.__patch = orig.__patch
            self.__objId = orig.__objId
            self.__visit = orig.__visit

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
        """

        pfsDesignId = HexIDFilter(pfsDesignId if pfsDesignId is not None else self.__pfsDesignId)

        pattern = os.path.join(
            self.find_datadir(reference_path=reference_path),
            Constants.PFSDESIGN_DIR_GLOB,
            Constants.PFSDESIGN_FILENAME_GLOB.format(
                pfsDesignId=pfsDesignId.get_glob_pattern()))

        return glob(pattern)
    
    def get_pfsDesign(self, pfsDesignId, reference_path=None):
        pfsDesignId = pfsDesignId if pfsDesignId is not None else self.__pfsDesignId.value

        files = self.find_pfsDesign(pfsDesignId=pfsDesignId, reference_path=reference_path)

        if len(files) == 0:
            raise FileNotFoundError(f'No pfsConfig file found for pfsDesignId {str(pfsDesignId)}.')
        elif len(files) > 1:
            raise FileNotFoundError(f'Multiple pfsConfig files found for pfsDesignId {str(pfsDesignId)}.')
        else:
            return files[0]
    
    def find_pfsConfig(self, pfsDesignId=None, visit=None, reference_path=None):
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

        pfsDesignId = HexIDFilter(pfsDesignId if pfsDesignId is not None else self.__pfsDesignId)
        visit = IntIDFilter(visit if visit is not None else self.__visit)

        pattern = os.path.join(
            self.find_datadir(reference_path=reference_path),
            Constants.PFSCONFIG_DIR_GLOB,
            Constants.PFSCONFIG_FILENAME_GLOB.format(
                pfsDesignId=pfsDesignId.get_glob_pattern(),
                visit=visit.get_glob_pattern()))
        
        return glob(pattern)
    
    def get_pfsConfig(self, pfsDesignId, visit, reference_path=None):
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

        pfsDesignId = pfsDesignId if pfsDesignId is not None else self.__pfsDesignId.value
        visit = visit if visit is not None else self.__visit.value

        files = self.find_pfsConfig(pfsDesignId=pfsDesignId, visit=visit, reference_path=reference_path)

        if len(files) == 0:
            raise FileNotFoundError(f'No pfsConfig file found for pfsDesignId {str(pfsDesignId)} and visit {str(visit)}')
        elif len(files) > 1:
            raise FileNotFoundError(f'Multiple pfsConfig files found for pfsDesignId {str(pfsDesignId)} and visit {str(visit)}')
        else:
            return files[0]
        
    def find_pfsArm(self, catId, tract, patch, objId, visit, arm):
        raise NotImplementedError()
    
    def find_pfsMerged(self, visit):
        raise NotImplementedError()
        
    def find_pfsSingle(self, catId, tract, patch, objId, visit):
        raise NotImplementedError()
    
    def find_pfsObject(self, catId, tract, patch, objId, nVisit, pfsVisitHash):
        raise NotImplementedError()