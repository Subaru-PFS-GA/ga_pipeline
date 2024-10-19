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
            'pfsSingle': SimpleNamespace(
                type = PfsSingle,
                print = [ self.__print_pfsSingle ]
            ),
            'pfsObject': SimpleNamespace(
                type = PfsObject,
                print = [ self.__print_pfsObject ]
            ),
            'pfsDesign': SimpleNamespace(
                type = PfsDesign,
                print = [ self.__print_pfsDesign ]
            ),
            'pfsConfig': SimpleNamespace(
                type = PfsConfig,
                print = [ self.__print_pfsConfig ]
            ),
            'pfsMerged': SimpleNamespace(
                type = PfsMerged,
                print = [ self.__print_pfsMerged ]
            ),
        }

        self.__connector = None
        self.__filename = None          # Path of the input file
        self.__file_type = None

    def _add_args(self):
        self._add_arg('in', type=str, help='Input file')

        super()._add_args()

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

        self.__connector = self.__create_data_connector()

        # Split filename into path, basename and extension
        path, basename = os.path.split(self.__filename)
        name, ext = os.path.splitext(basename)
        t = name.split('-')[0]

        if t in self.__file_types:
            product_type = self.__file_types[t].type
            product, identity, filename = self.__connector.load_product(product_type, filename=self.__filename)
            for func in self.__file_types[t].print:
                func(product, identity, filename)
        else:
            raise NotImplementedError(f'File type not recognized: {basename}')
        
    def __create_data_connector(self):
        """
        Create a connector to the file system.
        """

        connector = FileSystemConnector()
        return connector

    def __print_info(self, object, filename):
        print(f'{type(object).__name__}')
        print(f'  Full path: {filename}')

    def __print_identity(self, identity):
        print(f'Identity')
        d = identity.__dict__
        for key in d:
            # Check if pfsDesignId is in the key
            if 'pfsdesignid' in key.lower():
                print(f'  {key}: 0x{d[key]:016x}')
            else:
                print(f'  {key}: {d[key]}')

    def __print_identity(self, identity):
        print(f'Identity')
        d = identity.__dict__
        for key in d:
            # Check if pfsDesignId is in the key
            if 'pfsdesignid' in key.lower() or 'objid' in key.lower():
                print(f'  {key}: 0x{d[key]:016x}')
            else:
                print(f'  {key}: {d[key]}')

    def __print_target(self, target):
        print(f'Target')
        d = target.__dict__
        for key in d:
            # Check if pfsDesignId is in the key
            if 'objid' in key.lower() or 'pfsdesignid' in key.lower():
                print(f'  {key}: 0x{d[key]:016x}')
            else:
                print(f'  {key}: {d[key]}')

    def __print_observations(self, observations, s=()):
        print(f'Observations')
        print(f'  num: {observations.num}')
        d = observations.__dict__
        for key in d:
            # Check if pfsDesignId is in the key
            if key == 'num':
                pass
            elif key == 'arm':
                print(f'  {key}: {d[key]}')
            elif 'objid' in key.lower() or 'pfsdesignid' in key.lower():
                v = ' '.join(f'{x:016x}' for x in d[key][s])
                print(f'  {key}: {v}')
            else:
                v = ' '.join(str(x) for x in d[key][s])
                print(f'  {key}: {v}')

    def __print_pfsDesign(self, filename):
        pass

    def __print_pfsConfig(self, product, identity, filename):
        self.__print_info(product, filename)
        print(f'  DesignName: {product.designName}')
        print(f'  PfsDesignId: 0x{product.pfsDesignId:016x}')
        print(f'  Variant: {product.variant}')
        print(f'  Visit: {product.visit}')
        print(f'  Date: {identity.date:%Y-%m-%d}')
        print(f'  Center: {product.raBoresight:0.5f}, {product.decBoresight:0.5f}')
        print(f'  PosAng: {product.posAng:0.5f}')
        print(f'  Arms: {product.arms}')
        print(f'  Tract: {np.unique(product.tract)}')
        print(f'  Patch: {np.unique(product.patch)}')
        print(f'  CatId: {np.unique(product.catId)}')
        print(f'  ProposalId: {np.unique(product.proposalId)}')

    def __print_pfsSingle(self, product, identity, filename):
        self.__print_info(product, filename)

        print(f'  nVisit: {product.nVisit}')
        print(f'  Wavelength: {product.wavelength.shape}')
        print(f'  Flux: {product.wavelength.shape}')
        
        self.__print_target(product.target)
        self.__print_observations(product.observations, s=0)

        f, id = self.__connector.locate_product(
            PfsConfig,
            pfsDesignId=product.observations.pfsDesignId[0],
            visit=product.observations.visit[0]
        )
        p, id, f = self.__connector.load_product(PfsConfig, identity=id)
        self.__print_pfsConfig(p, id, f)

    def __print_pfsObject(self, product, identity, filename):
        self.__print_info(product, filename)

        print(f'  nVisit: {product.nVisit}')
        print(f'  Wavelength: {product.wavelength.shape}')
        print(f'  Flux: {product.wavelength.shape}')

        self.__print_target(product.target)
        self.__print_observations(product.observations, s=())

    def __print_pfsMerged(self, filename):
        merged = PfsMerged.readFits(filename)

        self.__print_info(merged, filename)
        self.__print_identity(merged.identity)
        print(f'Arrays')
        print(f'  Wavelength: {merged.wavelength.shape}')
        print(f'  Flux:       {merged.wavelength.shape}')

        # Try to locate the corresponding pfsConfig file
        try:
            filename, identity = self.__connector.locate_pfsConfig(
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