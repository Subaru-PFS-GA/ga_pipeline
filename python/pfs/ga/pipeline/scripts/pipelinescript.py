import os
from types import SimpleNamespace
import pandas as pd
from copy import deepcopy

from pfs.ga.pfsspec.survey.pfs.datamodel import *
from pfs.ga.pfsspec.survey.repo import FileSystemRepo, ButlerRepo
from pfs.ga.pfsspec.survey.pfs import PfsGen3Repo
from ..gapipe.config import *
from ..repo import GAPipeWorkdirConfig, PfsGen3ButlerConfig
from ..common import Script, PipelineError, ConfigJSONEncoder

from ..setup_logger import logger

class PipelineScript(Script):
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

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

        self.__config = self._create_config()
        self.__input_repo = self._create_input_repo()
        self.__work_repo = self._create_work_repo()

    def __get_products(self):
        return self.__products

    products = property(__get_products)

    def __get_config(self):
        return self.__config
    
    config = property(__get_config)

    def __get_input_repo(self):
        return self.__input_repo

    input_repo = property(__get_input_repo)

    def __get_work_repo(self):
        return self.__work_repo
    
    work_repo = property(__get_work_repo)

    def _add_args(self):
        self.__input_repo.add_args(self, ignore_duplicates=True)
        self.__work_repo.add_args(self, ignore_duplicates=True)
        super()._add_args()

    def _init_from_args(self, args):
        # Load the configuration
        if self.is_arg('config', args):
            config_files = self.get_arg('config', args)
            self.__config.load(config_files, ignore_collisions=True)

        # Override configuration with command-line arguments
        if self.is_arg('workdir', args):
            self.__config.workdir = self.get_arg('workdir', args, self.get_env('GAPIPE_WORKDIR'))
        if self.is_arg('outdir', args):
            self.__config.outdir = self.get_arg('outdir', args, self.get_env('GAPIPE_OUTDIR'))
        if self.is_arg('datadir', args):
            self.__config.datadir = self.get_arg('datadir', args)
        if self.is_arg('rerun', args):
            self.__config.rerun = self.get_arg('rerun', args)
        if self.is_arg('rerundir', args):
            self.__config.rerundir = self.get_arg('rerundir', args)

        # Initialize the data repository, first from the configuration,
        # then from the command-line arguments
        self._init_input_repo()
        self._init_work_repo()

        super()._init_from_args(args)

    def _create_config(self):
        return GAPipelineConfig()

    def _create_input_repo(self):
        """
        Create a data repository connector to the file system.
        """

        # TODO: create different connectors here if working with
        #       data sets other than PFS

        # TODO: figure out how to define the repo type in the config
        #       the issue is that we need the repo before loading the config
        #       in order to register the command-line arguments

        # repo = PfsGen3Repo(
        #     repo_type = FileSystemRepo,
        #     config = PfsGen3FileSystemConfig
        # )

        repo = PfsGen3Repo(
            repo_type = ButlerRepo,
            config = PfsGen3ButlerConfig
        )

        return repo

    def _create_work_repo(self):
        repo = PfsGen3Repo(
            repo_type = FileSystemRepo,
            config = GAPipeWorkdirConfig
        )

        return repo

    def _init_input_repo(self):
        # When configured, allow for certain input files to be missing
        # This is useful when the pipeline is run on a subset of data
        # This setting can be overridden in the command line
        self.__input_repo.ignore_missing_files = self.__config.ignore_missing_files
        self.__input_repo.init_from_args(self)

    def _init_work_repo(self):
        # When configured, allow for certain input files to be missing
        # This is useful when the pipeline is run on a subset of data
        # This setting can be overridden in the command line
        self.__work_repo.ignore_missing_files = self.__config.ignore_missing_files
        self.__work_repo.init_from_args(self)

    def _set_log_file_to_workdir(self):
        # Override logging directory to use the same as the pipeline workdir
        logfile = os.path.basename(self.log_file)
        self.log_file = os.path.join(
            self.work_repo.get_resolved_variable('workdir'),
            self.work_repo.get_resolved_variable('rerundir'),
            logfile)

    def _load_obs_params_file(self, obs_params_file, obs_params_id, obs_params_visit):
        if obs_params_file is not None:
            logger.info(f'Loading observation parameters from {obs_params_file}.')
            obs_params = pd.read_feather(obs_params_file)

            logger.info(f'Found {len(obs_params)} entries in observation parameter file.')

            if obs_params_id not in obs_params.columns:
                raise ValueError(f'ID column {obs_params_id} not found in observation parameter file.')
            if obs_params_visit not in obs_params.columns:
                raise ValueError(f'Visit column {obs_params_visit} not found in observation parameter file.')

            obs_params = obs_params.set_index(obs_params_id) 

            return obs_params
        else:
            return None

    def _load_stellar_params_file(self, stellar_params_file, stellar_params_id):
        # TODO: update this if multiple files are needed or the file format changes
        if stellar_params_file is not None:
            logger.info(f'Loading stellar parameters from {stellar_params_file}.')
            stellar_params = pd.read_feather(stellar_params_file)

            logger.info(f'Found {len(stellar_params)} entries in stellar parameter file.')

            if stellar_params_id not in stellar_params.columns:
                raise ValueError(f'ID column {stellar_params_id} not found in stellar parameter file.')

            stellar_params = stellar_params.set_index(stellar_params_id) 

            return stellar_params
        else:
            return None

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

        f, id = self.input_repo.locate_product(
            PfsConfig,
            pfsDesignId=product.observations.pfsDesignId[0],
            visit=product.observations.visit[0]
        )
        p, id, f = self.input_repo.load_product(PfsConfig, identity=id)
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
            filename, identity = self.input_repo.locate_pfsConfig(
                visit = merged.identity.visit,
                pfsDesignId = merged.identity.pfsDesignId,
            )
            self.__print_pfsConfig(filename=filename)
        except Exception as e:
            raise e