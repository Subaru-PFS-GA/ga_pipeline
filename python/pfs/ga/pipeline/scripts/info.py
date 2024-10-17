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

        self.__discovery = FileSystemConnector()
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
        # TODO: move this to discovery
        # PfsConfig cannot read from a file directly, so figure out parameters
        # from the filename
        dir, basename = os.path.split(filename)

        # Extract pfsDesignId and visit from the filename
        # Format is "pfsConfig-0x%016x-%06d.fits"
        match = re.match(r'pfsConfig-0x([0-9a-f]{16})-(\d{6}).fits', basename)
        if match is None:
            raise ValueError(f'Filename does not match expected format: {basename}')
        pfsDesignId = int(match.group(1), 16)
        visit = int(match.group(2))
        
        config = PfsConfig.read(pfsDesignId, visit, dirName=dir)

        self.__print_info(config, filename)
        print(f'  DesignName: {config.designName}')
        print(f'  PfsDesignId: 0x{config.pfsDesignId:016x}')
        print(f'  Visit: {config.visit}')
        print(f'  Center: {config.raBoresight:0.5f}, {config.decBoresight:0.5f}')
        print(f'  PosAng: {config.posAng:0.5f}')
        print(f'  Arms: {config.arms:0.5f}')
        print(f'  Tract, patch: {config.tract:0.5f}, {config.patch}')
        print(f'  CatId: {np.unique(config.catId)}')
        print(f'  ProposalId: {np.unique(config.proposalId)}')

    def __print_pfsMerged(self, filename):
        merged = PfsMerged.readFits(filename)

        self.__print_info(merged, filename)
        self.__print_identity(merged.identity)
        print(f'Arrays')
        print(f'  Wavelength: {merged.wavelength.shape}')
        print(f'  Flux:       {merged.wavelength.shape}')

        # Try to locate the corresponding pfsConfig file
        try:
            config_file = self.__discovery.find_pfsConfig(
                merged.identity.pfsDesignId, merged.identity.visit,
                reference_path=filename)
            self.__print_pfsConfig(config_file)
        except Exception as e:
            raise e

def main():
    script = Info()
    script.execute()

if __name__ == "__main__":
    main()