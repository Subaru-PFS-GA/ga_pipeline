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
from ..progress import Progress
from ..batchscript import BatchScript
from ...common import Script, PipelineError, ConfigJSONEncoder

from ...setup_logger import logger

class RepoScript(PipelineScript, BatchScript, Progress):
    """
    Search PFS repo for data files and print useful information about them.

    This script works by reading the file system and contents of FITS files only,
    without relying on Butler.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.__commands = {
            'info': SimpleNamespace(
                help = 'Print information about the data root and rerun directory',
                run = self.__run_info,
                submit = None
            ),
            'find-product': SimpleNamespace(
                help = 'Search for files of a given product type',
                run = self.__run_find_product,
                submit = None
            ),
            'extract-product': SimpleNamespace(
                help = 'Extract spectra from the specified type of product',
                run = self.__run_extract_product,
                submit = self.__submit_extract_product,
            ),
            'find-object': SimpleNamespace(
                help = 'Search for object withing pfsConfig files',
                run = self.__run_find_object,
                submit = None
            ),
            'show': SimpleNamespace(
                help = 'Print information about a given file',
                run = self.__run_show,
                submit = None
            )
        }

        self.__command = None           # Command to execute
        self.__filename = None          # Path of the input file
        self.__product = None           # Product to be processed
        self.__format = 'table'         # Output format

    def _add_args(self):
        self.add_arg('command', type=str,
                     choices=[ k for k in self.__commands.keys() ],
                     help='Command')
        self.add_arg('in', type=str, nargs='?',
                     help='Product type or filename')
        self.add_arg('--format', type=str, choices=['table', 'json', 'path'])

        PipelineScript._add_args(self)
        Progress._add_args(self)
        BatchScript._add_args(self)

    def _init_from_args(self, args):
        self.__command = self.get_arg('command')

        # See if the very first argument can be interpreted as a product type.
        # If not, interpret it as a filename
        if self.is_arg('in'):
            try:
                self.__product = self.input_repo.parse_product_type(self.get_arg('in'))
            except ValueError:
                self.__filename = self.get_arg('in')

        self.__format = self.get_arg('format', args, self.__format)

        PipelineScript._init_from_args(self, args)
        Progress._init_from_args(self, args)
        BatchScript._init_from_args(self, args)

    def prepare(self):
        return PipelineScript.prepare(self)
    
    def run(self):
        if self.is_batch():
            submit = self.__commands[self.__command].submit
            if submit is None:
                raise NotImplementedError(f'Command {self.__command} does not support batch submission')
            submit()
        else:
            self.__commands[self.__command].run()

    def __run_info(self):
        datadir = self.input_repo.get_resolved_variable('datadir')
        rerundir = self.input_repo.get_resolved_variable('rerundir')
        
        print(f'Data root directory: {datadir}')
        print(f'Rerun directory: {rerundir}')

    def __run_find_product(self):
        if self.__product is None:
            raise ValueError('Product type not provided')
        
        filenames, identities = self.input_repo.find_product(self.__product)
        identities.filename = filenames

        # Print results in different formats
        if self.__format == 'table':
            self.__print_identities_table(identities)
        elif self.__format == 'json':
            self.__print_identities_json(identities)
        elif self.__format == 'path':
            self.__print_paths(filenames)

    def __validate_extract_product(self):

        filename = self.__filename
        product = self.__product

        if filename is None and product is None:
            raise ValueError('Neither the product type, nor a filename was provided')
        
        # Depending on the product type, extract different types of data
        if isinstance(product, tuple):
            if len(product) != 2:
                raise ValueError(f'Product type must be a tuple of (container, sub-product): {product}')
            
            product, _ = product

        if product is not None and not hasattr(product, 'extract'):       
            raise ValueError(f'Product type does not support extracting sub-product: {product}')

    def __get_extract_product_filenames(self):
        if isinstance(self.__filename, str):
            filenames, identities = [self.__filename], None
        elif self.__filename is not None:
            filenames, identities = self.__filename, None
        else:
            filenames, identities = self.input_repo.find_product(self.__product)

        return filenames, identities

    def __run_extract_product(self):
        self.__validate_extract_product()
        filenames, identities = self.__get_extract_product_filenames()

        # Load the products one by one and extract all sub-products
        for i, fn in enumerate(self._wrap_in_progressbar(filenames)):
            product = self.input_repo.match_container_product_type(os.path.basename(fn))
            subprods = self.input_repo.load_products_from_container(
                *product,
                filename=fn,
                ignore_missing_files=True)

            if subprods is not None:
                for subprod, subid, _ in subprods:
                    # If the sub-product matches the object filters, save it
                    # to the work directory
                    if self.input_repo.match_object_filters(subid):
                        _, filename = self.work_repo.save_product(
                            subprod, identity=subid,
                            variables={'datadir': self.config.workdir})

            if self.top is not None and i >= self.top:
                logger.info(f'Stop after processing {self.top} objects.')
                break

    def __submit_extract_product(self):
        self.__validate_extract_product()
        filenames, identities = self.__get_extract_product_filenames()

        # Submit a job for each product matching the filters
        for i, fn in enumerate(self._wrap_in_progressbar(filenames)):
            command = f'python -m pfs.ga.pipeline.scripts.repo.reposcript extract-product {fn}'
            
            # Add the filters
            for key, filter in self.input_repo.object_filters.__dict__.items():
                args = filter.render(lower=True)
                if args is not None:
                    command += f' {args}'
            
            self._submit_job(command, fn)

            if self.top is not None and i >= self.top:
                logger.info(f'Stop after processing {self.top} objects.')
                break

    def __run_find_object(self):
        identities = self.input_repo.find_objects(groupby='none')
        if identities is not None:
            if self.top is not None:
                for k, v in identities.__dict__.items():
                    setattr(identities, k, v[:self.top])
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

    def __print_paths(self, filenames):
        for fn in filenames:
            print(fn)

    def __run_show(self):
        """
        Depending on the type of file being processed, print different types of information.
        """

        if self.__filename is not None:
            # Split filename into path, basename and extension
            path, basename = os.path.split(self.__filename)
            name, ext = os.path.splitext(basename)
            product_type = self.input_repo.parse_product_type(re.split('[-_]', name)[0])

            if product_type in self.products:
                product, identity, filename = self.input_repo.load_product(product_type, filename=self.__filename)
            else:
                raise NotImplementedError(f'File type not recognized: {basename}')
        elif self.__product is not None:
            product, identity, filename = self.input_repo.load_product(self.__product)
        else:
            raise ValueError('No input file or product type provided')

        for func in self.products[type(product)].print:
            func(product, identity, filename)

def main():
    script = RepoScript()
    script.execute()

if __name__ == "__main__":
    main()