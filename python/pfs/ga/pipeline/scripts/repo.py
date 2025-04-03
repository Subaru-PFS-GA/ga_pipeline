#!/usr/bin/env python3

import os, sys
from types import SimpleNamespace
import logging
import numpy as np
import pandas as pd
import commentjson as json

from pfs.datamodel import *
from pfs.ga.pfsspec.survey.pfs import PfsGen3FileSystemRepo

from ..constants import Constants
from ..common import Script, ConfigJSONEncoder

from ..setup_logger import logger

class Repo(Script):
    """
    Search PFS repo for data files and print useful information about them.

    This script works by reading the file system and contents of FITS files only,
    without relying on Butler.
    """

    def __init__(self):
        super().__init__(log_level=logging.WARNING, log_to_file=False)

        self.__commands = {
            'info': SimpleNamespace(
                help = 'Print information about the data root and rerun directory',
                run = self.__run_info
            ),
            'find-product': SimpleNamespace(
                help = 'Search for files of a given product type',
                run = self.__run_find_product
            ),
            'find-object': SimpleNamespace(
                help = 'Search for object withing pfsConfig files',
                run = self.__run_find_object
            ),
            'show': SimpleNamespace(
                help = 'Print information about a given file',
                run = self.__run_show
            )
        }

        self.__products = {
            PfsSingle: SimpleNamespace(
                print = [ self.__print_pfsSingle ]
            ),
            PfsObject: SimpleNamespace(
                print = [ self.__print_pfsObject ]
            ),
            PfsDesign: SimpleNamespace(
                print = [ self.__print_pfsDesign ]
            ),
            PfsConfig: SimpleNamespace(
                print = [ self.__print_pfsConfig ]
            ),
            PfsMerged: SimpleNamespace(
                print = [ self.__print_pfsMerged ]
            ),
        }

        self.__command = None           # Command to execute
        self.__filename = None          # Path of the input file
        self.__product = None           # Product to be processed
        self.__format = 'table'         # Output format
        self.__top = None

        self.__connector = self.__create_data_connector()

    def _add_args(self):
        self.add_arg('command', type=str,
                     choices=[ k for k in self.__commands.keys() ],
                     help='Command')
        self.add_arg('in', type=str, nargs='?',
                     help='Product type or filename')
        self.add_arg('--format', type=str)
        self.add_arg('--top', type=int)
        self.__connector.add_args(self)

        super()._add_args()

    def _init_from_args(self, args):
        self.__command = self.get_arg('command')

        # See if the very first argument can be interpreted as a product type.
        # If not, interpret it as a filename
        if self.is_arg('in'):
            try:
                self.__product = self.__connector.parse_product_type(self.get_arg('in'))
            except ValueError:
                self.__filename = self.get_arg('in')
        
        self.__connector.init_from_args(self)

        self.__format = self.get_arg('format', args, self.__format)
        self.__top = self.get_arg('top', args, self.__top)

        super()._init_from_args(args)

    def __create_data_connector(self):
        """
        Create a connector to the file system.
        """

        # TODO: create different connectors here if working with
        #       data sets other than PFS

        connector = PfsGen3FileSystemRepo()

        return connector

    def prepare(self):
        return super().prepare()
    
    def run(self):
        self.__commands[self.__command].run()

    def __run_info(self):
        datadir = self.__connector.get_resolved_variable('datadir')
        rerundir = self.__connector.get_resolved_variable('rerundir')
        
        print(f'Data root directory: {datadir}')
        print(f'Rerun directory: {rerundir}')

    def __run_find_product(self):
        if self.__product is None:
            raise ValueError('Product type not provided')
        
        filenames, identities = self.__connector.find_product(self.__product)
        identities.filename = filenames

        # TODO: return values in different formats
            
        json.dump(identities.__dict__,
                  sys.stdout,
                  sort_keys=False,
                  indent=2,
                  cls=ConfigJSONEncoder)

    def __run_find_object(self):
        identities = self.__connector.find_object(groupby='none')
        if identities is not None:
            if self.__top is not None:
                for k, v in identities.__dict__.items():
                    setattr(identities, k, v[:self.__top])
            self.__print_identities(identities)

    def __print_identities(self, identities, format=None):
        format = format if format is not None else self.__format

        if format == 'table':
            self.__print_identities_table(identities)
        elif format == 'json':
            self.__print_identities_json(identities)
        else:
            raise NotImplementedError()

    def __print_identities_table(self, identities):
        # Pretty-print all columns and rows of the data frame
        pd.set_option('display.max_columns', None)
        pd.set_option('display.max_rows', None)
        pd.set_option('display.width', None)
        pd.set_option('display.expand_frame_repr', False)
        pd.set_option('display.float_format', '{:.2f}'.format)
        pd.set_option('display.precision', 2)
        pd.set_option('display.colheader_justify', 'right')
        pd.set_option('display.show_dimensions', False)
        pd.set_option('display.max_colwidth', None)

        
        df = pd.DataFrame(identities.__dict__)

        if 'objId' in df.columns:
            df['objId'] = df['objId'].apply(lambda x: f'0x{x:016x}')
        if 'pfsDesignId' in df.columns:
            df['pfsDesignId'] = df['pfsDesignId'].apply(lambda x: f'0x{x:016x}')

        # Print the summary
        print(df.to_string(index=False))

    def __print_identities_json(self, identities):
        json.dump(identities.__dict__,
                  sys.stdout,
                  sort_keys=False,
                  indent=2,
                  cls=ConfigJSONEncoder)

    def __run_show(self):
        """
        Depending on the type of file being processed, print different types of information.
        """

        if self.__filename is not None:
            # Split filename into path, basename and extension
            path, basename = os.path.split(self.__filename)
            name, ext = os.path.splitext(basename)
            product_type = self.__connector.parse_product_type(name.split('-')[0])

            if product_type in self.__products:
                product, identity, filename = self.__connector.load_product(product_type, filename=self.__filename)
            else:
                raise NotImplementedError(f'File type not recognized: {basename}')
        elif self.__product is not None:
            product, identity, filename = self.__connector.load_product(self.__product)
        else:
            raise ValueError('No input file or product type provided')

        for func in self.__products[type(product)].print:
            func(product, identity, filename)

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
    script = Repo()
    script.execute()

if __name__ == "__main__":
    main()