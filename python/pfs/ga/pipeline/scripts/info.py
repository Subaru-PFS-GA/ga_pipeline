#!/usr/bin/env python3

import os
import re
from glob import glob
from types import SimpleNamespace
import numpy as np

from pfs.datamodel import PfsDesign, PfsConfig, PfsObject, PfsSingle, PfsMerged
from pfs.datamodel.utils import calculatePfsVisitHash, wraparoundNVisit

from ..constants import Constants
from ..data import FileSystemConnector
from .script import Script

from ..setup_logger import logger

class Info(Script):
    """
    Print useful info about a PFS data file.

    This script works by reading the contents of FITS files only, without relying on Butler.
    """

    def __init__(self):
        super().__init__()

        self.__file_types = {
            'pfsSingle': [
                self.__print_pfsSingle
            ],
            'pfsObject': [
                self.__print_pfsObject
            ],
            'pfsDesign': [
                self.__print_pfsDesign
            ],
            'pfsConfig': [
                self.__print_pfsConfig
            ],
            'pfsMerged': [
                self.__print_pfsMerged
            ]
        }

        self.__connector = None
        self.__filename = None          # Path of the input file

    def _add_args(self):
        super()._add_args()

        self._add_arg('--in', type=str, help='Input file')

    def _init_from_args(self, args):
        super()._init_from_args(args)

        self.__filename = self._get_arg('in')

    def _configure_logging(self):
        super()._configure_logging()

        self.log_file = None
        self.log_to_console = False

    def prepare(self):
        return super().prepare()
    
    def run(self):
        """
        Depending on the type of file being processed, print different types of information.
        """

        # Split filename into path, basename and extension
        path, basename = os.path.split(self.__filename)
        name, ext = os.path.splitext(basename)

        self.__filetype = name.split('-')[0]

        if self.__filetype in self.__file_types:
            for func in self.__file_types[self.__filetype]:
                func(self.__filename)
        else:
            raise NotImplementedError(f'File type not recognized: {basename}')
        
    def __get_data_connector(self, reference_path=None):
        """
        Create a connector to the file system.
        """

        if self.__connector is None:
            self.__connector = FileSystemConnector()
            self.__connector.datadir = self.__connector.find_datadir(reference_path=reference_path)
            self.__connector.rerundir = self.__connector.find_rerundir(reference_path=reference_path)

        return self.__connector

    def __print_info(self, object, filename):
        print(f'File type: {type(object).__name__}')
        print(f'Filename: {filename}')

    def __print_identity(self, identity):
        print(f'Identity')
        d = identity.__dict__
        for key in d:
            # Check if pfsDesignId is in the key
            if 'pfsdesignid' in key.lower():
                print(f'  {key}: 0x{d[key]:016x}')
            else:
                print(f'  {key}: {d[key]}')

    def __print_pfsSingle(self, filename):
        pass

    def __print_pfsObject(self, filename):
        pass

    def __print_pfsDesign(self, filename):
        pass

    def __print_pfsConfig(self, filename):
        connector = self.__get_data_connector(filename)
        pfsConfig = connector.load_pfsConfig(filename)

        self.__print_info(pfsConfig, filename)
        print(f'  DesignName: {pfsConfig.designName}')
        print(f'  PfsDesignId: 0x{pfsConfig.pfsDesignId:016x}')
        print(f'  Visit: {pfsConfig.visit}')
        print(f'  Center: {pfsConfig.raBoresight:0.5f}, {pfsConfig.decBoresight:0.5f}')
        print(f'  PosAng: {pfsConfig.posAng:0.5f}')
        print(f'  Arms: {pfsConfig.arms}')
        print(f'  Tract: {np.unique(pfsConfig.tract)}')
        print(f'  Patch: {np.unique(pfsConfig.patch)}')
        print(f'  CatId: {np.unique(pfsConfig.catId)}')
        print(f'  ProposalId: {np.unique(pfsConfig.proposalId)}')

    def __print_pfsMerged(self, filename):
        connector = self.__get_data_connector(filename)
        # TODO: use connector here
        merged = PfsMerged.readFits(filename)

        self.__print_info(merged, filename)
        self.__print_identity(merged.identity)
        print(f'Arrays')
        print(f'  Wavelength: {merged.wavelength.shape}')
        print(f'  Flux:       {merged.wavelength.shape}')

        # Try to locate the corresponding pfsConfig file
        try:
            filename, identity = connector.get_pfsConfig(
                visit = merged.identity.visit,
                pfsDesignId = merged.identity.pfsDesignId,
            )
            self.__print_pfsConfig(filename=filename)
        except Exception as e:
            raise e

def main():
    script = Info()
    script.execute()

if __name__ == "__main__":
    main()