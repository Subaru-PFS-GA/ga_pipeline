#!/usr/bin/env python3

import os, sys
import re
from types import SimpleNamespace
import logging
import numpy as np
import pandas as pd
import commentjson as json

from pfs.ga.common.config import ConfigJSONEncoder
from pfs.ga.common.scripts import Batch, Progress
from pfs.ga.pfsspec.survey.pfs.datamodel import *

from ..pipelinescript import PipelineScript
from ...gapipe.config import RepoConfig
from ...common import PipelineError

from ...setup_logger import logger

class RepoScript(PipelineScript, Batch, Progress):
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
            PfsCalibrated: SimpleNamespace(
                print = [ self.__print_pfsCalibrated ]
            ),
            PfsStar: SimpleNamespace(
                print = [ self.__print_pfsStar]
            ),
            PfsStarCatalog: SimpleNamespace(
                print = [ self.__print_pfsStarCatalog ]
            )
        }

        self.__command = None           # Command to execute
        self.__in = None                # Input parameter, product type or filename as a string
        self.__format = 'table'         # Output format

    def __get_products(self):
        return self.__products

    products = property(__get_products)

    def _add_args(self):
        self.add_arg('command', type=str,
                     choices=[ k for k in self.__commands.keys() ],
                     help='Command')
        self.add_arg('in', type=str, nargs='?',
                     help='Product type or filename')
        self.add_arg('--format', type=str, choices=['table', 'json', 'path'])

        PipelineScript._add_args(self)
        Progress._add_args(self)
        Batch._add_args(self)

    def _init_from_args(self, args):
        self.__command = self.get_arg('command')
        self.__in = self.get_arg('in')
        self.__format = self.get_arg('format', args, self.__format)

        PipelineScript._init_from_args(self, args)
        Progress._init_from_args(self, args)
        Batch._init_from_args(self, args)

    def __find_product_in_repos(self, product_or_filename, repo_list=None):

        # See if the very first argument can be interpreted as a product type.
        # If not, interpret it as a filename
        product = None
        repo = None
        for i, r, name in self._enumerate_repos(repo_list=repo_list):
            try:
                if r is not None:
                    product = r.parse_product_type(product_or_filename)
                    if not r.has_product(product):
                        product = None
                        continue
                    else:
                        # Found a repo that has the product type, no need to continue searching
                        repo = r
                        break
            except ValueError:
                # Butler throws ValueError if the product type is not recognized, catch it and try the next repo
                continue
        
        # If the input argument cannot be interpreted as a product type, interpret it as a filename
        if product is None:
            filename = product_or_filename
        else:
            filename = None

        return product, repo, filename

    def _create_config(self):
        return RepoConfig()

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

        print(f'Data root directory:  {self.input_repo.get_resolved_variable("datadir")}')
        print(f'pipe2d run directory: {self.input_repo.get_resolved_variable("rundir")}')
        print(f'pipe2d run:           {self.input_repo.filters.run.value}')
        print(f'pfsConfig run:        {self.config_repo.filters.run.value}')

        print()

        print(f'Work directory:       {self.work_repo.get_resolved_variable("datadir")}')
        print(f'Output directory:     {self.output_repo.get_resolved_variable("datadir")}')
        print(f'gapipe run directory: {self.work_repo.get_resolved_variable("garundir")}')
        print(f'gapipe run:           {self.work_repo.filters.garun.value}')

        print()

        if self.use_butler:
            print('Using Butler for data access.')
            butler_configdir = self.input_repo.get_resolved_variable('butlerconfigdir')
            butler_collections = self.input_repo.get_resolved_variable('butlercollections')
            print(f'Butler config dir:   {butler_configdir}')
            print(f'Butler collections:  {butler_collections}')
        else:
            print('Not using Butler for data access.')

    def __run_find_product(self):

        product, repo, filename = self.__find_product_in_repos(self.__in)

        if product is None:
            raise ValueError('Product type not provided or could not be inferred from the input arguments.')
        
        # TODO: DELETE
        # # Find the first repository that has the product type.
        # repo = None
        # for i, repo, repo_name in self._enumerate_repos():
        #     if repo is not None and repo.has_product(product):
        #         logger.debug(f'Product type {self.__product} found in repo {repo_name}.')
        #         break

        if repo is None:
            raise ValueError(f'Product type {product} not found in any repo.')
        
        filenames, identities = repo.find_product(product)
        identities.filename = filenames

        # Print results in different formats
        if self.__format == 'table':
            self.__print_identities_table(identities)
        elif self.__format == 'json':
            self.__print_identities_json(identities)
        elif self.__format == 'path':
            self.__print_paths(filenames)

    def __validate_extract_product(self, product, filename):

        if filename is None and product is None:
            raise ValueError('Neither the product type, nor a filename was provided')
        
        # Depending on the product type, extract different types of data
        if isinstance(product, tuple):
            if len(product) != 2:
                raise ValueError(f'Product type must be a tuple of (container, sub-product): {product}')
            
            product, _ = product

        if product is not None and not hasattr(product, 'extract'):       
            raise ValueError(f'Product type does not support extracting sub-product: {product}')

    def __get_extract_product_filenames(self, product, filename):
        # Depending on the command-line arguments, figure out the list of input files to be processed.
        # This script can take two types of arguments as the first positional argument:
        # a pair of product types (e.g. pfsCalibrated,pfsSingle) or a filename (e.g. pfsCalibrated_*.fits)
        # The function `init_from_args` decides if the argument can be interpreted as a product type,
        # and if not, it is interpreted as a filename. So here we just need to check which one we got
        # and act accordingly.

        if isinstance(filename, str):
            filenames, identities = [filename], None
        elif filename is not None:
            filenames, identities = filename, None
        else:
            filenames, identities = self.input_repo.find_product(product)

        return filenames, identities

    def __run_extract_product(self):
        
        product, repo, filename = self.__find_product_in_repos(self.__in, repo_list=['input'])
        self.__validate_extract_product(product, filename)
        filenames, identities = self.__get_extract_product_filenames(product, filename)

        # Load the products one by one and extract all sub-products to the work repo
        for i, fn in enumerate(
            self._wrap_in_progressbar(filenames, total=len(filenames), logger=logger)):
            
            if product is None:
                product = self.input_repo.match_container_product_type(os.path.basename(fn))

            subprods = self.input_repo.load_products_from_container(
                *product,
                filename=fn,
                ignore_missing_files=True)

            if subprods is not None:
                for subprod, subid, _ in subprods:
                    # If the sub-product matches the object filters, save it to the work repository
                    if self.input_repo.match_object_filters(subid):
                        if self.dry_run:
                            _, filename, _ = self.work_repo.get_data_path(subprod, identity=subid)
                            logger.info(f'Dry run: would save sub-product {type(subprod).__name__} to {filename}')
                        else:
                            _, filename = self.work_repo.save_product(subprod, identity=subid)

            if self.top is not None and i >= self.top:
                logger.info(f'Stop after processing {self.top} objects.')
                break

    def __submit_extract_product(self):

        product, repo, filename = self.__find_product_in_repos(self.__in)
        self.__validate_extract_product(product, filename)
        filenames, identities = self.__get_extract_product_filenames(product, filename)

        # Submit a job for each product matching the filters
        for i, fn in enumerate(self._wrap_in_progressbar(filenames, total=len(filenames), logger=logger)):
            command = f'python -m pfs.ga.pipeline.scripts.repo.reposcript extract-product {fn}'
            
            # Add the repo filters
            for key, filter in self.input_repo.object_filters.__dict__.items():
                args = filter.render(lower=True)
                if args is not None:
                    command += f' {args}'

            # TODO: Add other command-line arguments
            command += ' --log-to-console'

            # TODO: filter out wrong arguments instead
            raise NotImplementedError()
        
            for a in ['workdir', 'outdir', 'datadir', 'run', 'rundir']:
                if self.is_arg(a, args):
                    command += f' --{a} {self.get_arg(a, args)}'
            
            self._submit_job(command, fn)

            if self.top is not None and i >= self.top:
                logger.info(f'Stop after processing {self.top} objects.')
                break

    def __run_find_object(self):       
        # identities = self.config_repo.find_objects(groupby='none', configrun=self.config.configrun)

        # Find the objects matching the command-line arguments. Arguments
        # are parsed by the repo object itself, so no need to pass them in here.
        logger.info('Finding objects matching the filters. This requires loading all PfsConfig files for the given visits and can take a while.')
        pfs_configs = self.config_repo.load_pfsConfigs()

        # Get the dictionary of object identities matching the filters, keyed by objid
        identities = self.input_repo.find_objects(pfs_configs=pfs_configs, groupby='none')

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

        if not isinstance(identities, dict):
            identities = identities.__dict__
        
        df = pd.DataFrame(identities)

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

            # Figure out what repo has the product
            found = False
            for repo in [self.input_repo, self.work_repo, self.output_repo]:
                product_type = repo.parse_product_type(re.split('[-_]', name)[0])
                if repo.has_product(product_type):
                    product, identity, filename = repo.load_product(product_type,
                                                                    filename=self.__filename,
                                                                    skip_locate=True)
                    found = True
                    break

            if not found:
                raise NotImplementedError(f'File type not recognized: {basename}')
        elif self.__product is not None:
            found = False
            for repo in [self.input_repo, self.work_repo, self.output_repo]:
                if repo.has_product(self.__product):
                    product, identity, filename = repo.load_product(self.__product)
                    found = True
                    break
            if not found:
                raise NotImplementedError(f'Product type not found in any repo: {self.__product}')
        else:
            raise ValueError('No input file or product type provided')

        for func in self.products[type(product)].print:
            func(product, identity, filename)

    #region Print functions for different product types

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
                v = ' '.join(f'{x:016x}' for x in np.atleast_1d(d[key][s]))
                print(f'  {key}: {v}')
            else:
                v = ' '.join(str(x) for x in np.atleast_1d(np.array(d[key])[s]))
                print(f'  {key}: {v}')

    def __print_pfsDesign(self, product, identity, filename):
        pass

    def __load_pfsConfig(self, identity):
        config, identity, filename = self.config_repo.load_product(PfsConfig, identity={'visit': identity.visit})
        return config, identity, filename

    def __print_pfsConfig(self, product, identity, filename):
        self.__print_info(product, filename)
        print(f'  DesignName: {product.designName}')
        print(f'  PfsDesignId: 0x{product.pfsDesignId:016x}')
        print(f'  Variant: {product.variant}')
        print(f'  Visit: {product.visit}')
        if identity.date is not None:
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

        # Try to locate the corresponding pfsConfig file
        try:
            config, identity, filename = self.__load_pfsConfig(identity)
            self.__print_pfsConfig(config, identity, filename)
        except Exception as e:
            raise e

    def __print_pfsObject(self, product, identity, filename):
        self.__print_info(product, filename)

        print(f'  nVisit: {product.nVisit}')
        print(f'  Wavelength: {product.wavelength.shape}')
        print(f'  Flux: {product.wavelength.shape}')

        self.__print_target(product.target)
        self.__print_observations(product.observations, s=())

    def __print_pfsCalibrated(self, product, identity, filename):
        self.__print_info(product, filename)
        self.__print_identity(identity)
        print(f'Items')
        print(f'  Spectra: {len(product.spectra)}')

        # Try to locate the corresponding pfsConfig file
        try:
            config, identity, filename = self.__load_pfsConfig(identity)
            self.__print_pfsConfig(config, identity, filename)
        except Exception as e:
            raise e

    def __print_pfsMerged(self, product, identity, filename):
        self.__print_info(product, filename)
        self.__print_identity(identity)
        print(f'Arrays')
        print(f'  Wavelength: {product.wavelength.shape}')
        print(f'  Flux:       {product.wavelength.shape}')

        # Try to locate the corresponding pfsConfig file
        try:
            config, identity, filename = self.__load_pfsConfig(identity)
            self.__print_pfsConfig(config, identity, filename)
        except Exception as e:
            raise e

    def __print_pfsStar(self, product, identity, filename):
        self.__print_info(product, filename)

        print(f'  nVisit: {product.nVisit}')
        print(f'  Wavelength: {product.wavelength.shape}')
        print(f'  Flux: {product.wavelength.shape}')

        self.__print_target(product.target)
        self.__print_observations(product.observations, s=())

    def __print_pfsStarCatalog(self, product, identity, filename):
        raise NotImplementedError()

    #endregion

def main():
    script = RepoScript()
    script.execute()

if __name__ == "__main__":
    main()