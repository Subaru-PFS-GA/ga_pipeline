#!/usr/bin/env python3

import os, sys
import re
from types import SimpleNamespace
import logging
import numpy as np
import pandas as pd
import commentjson as json

from pfs.ga.pfsspec.survey.pfs.datamodel import *
from ..pipelinescript import PipelineScript
from ...common import Script, PipelineError, ConfigJSONEncoder

from ...setup_logger import logger

class RepoScript(PipelineScript):
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
            'extract-product': SimpleNamespace(
                help = 'Extract spectra from the specified type of product',
                run = self.__run_extract_product
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

        self.__command = None           # Command to execute
        self.__filename = None          # Path of the input file
        self.__product = None           # Product to be processed
        self.__format = 'table'         # Output format
        self.__top = None

    def _add_args(self):
        self.add_arg('command', type=str,
                     choices=[ k for k in self.__commands.keys() ],
                     help='Command')
        self.add_arg('in', type=str, nargs='?',
                     help='Product type or filename')
        self.add_arg('--format', type=str)
        self.add_arg('--top', type=int)

        super()._add_args()

    def _init_from_args(self, args):
        self.__command = self.get_arg('command')

        # See if the very first argument can be interpreted as a product type.
        # If not, interpret it as a filename
        if self.is_arg('in'):
            try:
                self.__product = self.repo.parse_product_type(self.get_arg('in'))
            except ValueError:
                self.__filename = self.get_arg('in')

        self.__format = self.get_arg('format', args, self.__format)
        self.__top = self.get_arg('top', args, self.__top)

        super()._init_from_args(args)

    def prepare(self):
        return super().prepare()
    
    def run(self):
        self.__commands[self.__command].run()

    def __run_info(self):
        datadir = self.repo.get_resolved_variable('datadir')
        rerundir = self.repo.get_resolved_variable('rerundir')
        
        print(f'Data root directory: {datadir}')
        print(f'Rerun directory: {rerundir}')

    def __run_find_product(self):
        if self.__product is None:
            raise ValueError('Product type not provided')
        
        filenames, identities = self.repo.find_product(self.__product)
        identities.filename = filenames

        # TODO: return values in different formats
            
        json.dump(identities.__dict__,
                  sys.stdout,
                  sort_keys=False,
                  indent=2,
                  cls=ConfigJSONEncoder)

    def __run_extract_product(self):
        if self.__product is None:
            raise ValueError('Product type not provided')
        
        # Depending on the product type, extract different types of data
        if not hasattr(self.__product, 'extract'):
            raise ValueError(f'Product type does not support extracting sub-product: {self.__product}')

        filenames, identities = self.repo.find_product(self.__product)

        # Load the products one by one and extract all sub-products
        for i, fn in enumerate(filenames):
            prod, _, _ = self.repo.load_product(self.__product, filename=fn)
            subprods, subids = prod.extract()

            for subprod, subid in zip(subprods, subids):
                # Match filters
                if self.repo.filters_match_object(subid):
                    _, filename = self.repo.save_product(
                        subprod, identity=subid,
                        variables={'datadir': self.config.workdir})

    def __run_find_object(self):
        identities = self.repo.find_object(groupby='none')
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
            product_type = self.repo.parse_product_type(re.split('[-_]', name)[0])

            if product_type in self.products:
                product, identity, filename = self.repo.load_product(product_type, filename=self.__filename)
            else:
                raise NotImplementedError(f'File type not recognized: {basename}')
        elif self.__product is not None:
            product, identity, filename = self.repo.load_product(self.__product)
        else:
            raise ValueError('No input file or product type provided')

        for func in self.products[type(product)].print:
            func(product, identity, filename)

def main():
    script = RepoScript()
    script.execute()

if __name__ == "__main__":
    main()