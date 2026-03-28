import os
from glob import glob
from types import SimpleNamespace
import numpy as np
import pandas as pd
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from dateutil import parser as dateparser
from collections import defaultdict
from astropy.coordinates import SkyCoord
import astropy.units as u

from pfs.ga.common.scripts import Script
from pfs.ga.common.config import ConfigJSONEncoder
from pfs.ga.pfsspec.core import Physics
from pfs.ga.pfsspec.core import Trace
from pfs.ga.pfsspec.survey.pfs.datamodel import *
from pfs.ga.pfsspec.survey.repo import FileSystemRepo, ButlerRepo
from pfs.ga.pfsspec.survey.pfs import PfsGen3Repo
from ..gapipe.config import *
from ..repo import GAPipeWorkdirConfig, PfsGen3ButlerConfig, PfsGen3FileSystemConfig, PfsGAFileSystemConfig
from ..common import PipelineError

from ..setup_logger import logger

class PipelineScript(Script):
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.__repo_types = {
            'butler_repo': dict(
                repo_type = ButlerRepo,
                config = PfsGen3ButlerConfig
            ),
            'input_repo': dict(
                repo_type = FileSystemRepo,
                config = PfsGen3FileSystemConfig
            ),
            'work_repo': dict(
                repo_type = FileSystemRepo,
                config = GAPipeWorkdirConfig
            ),
            'output_repo': dict(
                repo_type = FileSystemRepo,
                config = PfsGAFileSystemConfig
            ),
        }

        self.__plot_level = None

        self.__config = self._create_config()
        self.__use_butler = False
        self.__input_repo = None
        self.__work_repo = None
        self.__output_repo = None

    def __get_plot_level(self):
        return self.__plot_level

    plot_level = property(__get_plot_level)

    def __get_use_butler(self):
        return self.__use_butler

    use_butler = property(__get_use_butler)

    def __get_config(self):
        return self.__config
    
    config = property(__get_config)

    def __get_input_repo(self):
        return self.__input_repo

    input_repo = property(__get_input_repo)

    def __get_work_repo(self):
        return self.__work_repo
    
    work_repo = property(__get_work_repo)

    def __get_output_repo(self):
        return self.__output_repo

    output_repo = property(__get_output_repo)

    def _add_args(self):
        self.add_arg('--plot-level', type=str, choices=['NONE', 'INFO', 'DEBUG', 'TRACE'], help='Plot level for tracing')
        self.add_arg('--butler', action='store_true', dest='use_butler', help='Whether to use Butler for data access.')
        self.add_arg('--no-butler', action='store_false', dest='use_butler', help='Whether to use Butler for data access.')

        # Register custom directories, these will specialize the work and output repos
        self.add_arg('--workdir', type=str, help='Work directory for the pipeline.')
        self.add_arg('--outdir', type=str, help='Output directory for the pipeline.')
        self.add_arg('--garun', type=str, help='Run name for the GA pipeline.')
        self.add_arg('--garundir', type=str, help='Rerun directory for the GA pipeline.')

        # Instantiate all repo types to register their command-line arguments
        for k in self.__repo_types:
            r = PfsGen3Repo(**self.__repo_types[k])
            r.add_args(self, ignore_duplicates=True)

        super()._add_args()

    def _init_from_args_pre_logging(self, args):
        super()._init_from_args_pre_logging(args)

        # TODO: repo variables are not updated based on the command-line arguments yet
        #       because of this, the log file will go to the default location which
        #       is determined by the environment variables instead of any config files or
        #       command-line arguments. This needs to be fixed.

        self.__use_butler = self.get_arg('use_butler', args, self.__use_butler)
        self.__input_repo = self._create_input_repo()
        self.__work_repo = self._create_work_repo()
        self.__output_repo = self._create_output_repo()

    def _init_from_args(self, args):

        self.__plot_level = self.get_arg('plot_level', args, self.__plot_level)
        if self.__plot_level is not None:
            level_map = {
                'NONE': Trace.PLOT_LEVEL_NONE,
                'INFO': Trace.PLOT_LEVEL_INFO,
                'DEBUG': Trace.PLOT_LEVEL_DEBUG,
                'TRACE': Trace.PLOT_LEVEL_TRACE
            }
            self.__plot_level = level_map[self.__plot_level]

        # Load the configuration
        if self.is_arg('config', args):
            config_files = self.get_arg('config', args)
            self.__config.load(config_files, ignore_collisions=True)

        # Override configuration with command-line arguments
        self.__config.workdir = self.get_arg('workdir', args, self.get_env('GAPIPE_WORKDIR'))
        self.__config.outdir = self.get_arg('outdir', args, self.get_env('GAPIPE_OUTDIR'))
        if self.is_arg('datadir', args):
            self.__config.datadir = self.get_arg('datadir', args)
        if self.is_arg('rundir', args):
            self.__config.rundir = self.get_arg('rundir', args)
        if self.is_arg('garundir', args):
            self.__config.garundir = self.get_arg('garundir', args)

        # Initialize the data repository, first from the configuration,
        # then from the command-line arguments
        
        self._init_input_repo()
        self._init_work_repo()
        self._init_output_repo()

        # Update the repo directories based on the config and the command-line arguments
        self._update_repo_directories(self.__config)

        super()._init_from_args(args)

    def get_command_name(self):
        """
        Returns the command name based on the script class name.

        Returns
        -------
        str
            Command name.
        """

        name = self.__class__.__name__.lower()
        name = name.replace('script', '')
        return f'gapipe-{name}'

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

        if self.__use_butler:
            repo = PfsGen3Repo(**self.__repo_types['butler_repo'])
        else:
            repo = PfsGen3Repo(**self.__repo_types['input_repo'])

        return repo

    def _create_work_repo(self):
        repo = PfsGen3Repo(**self.__repo_types['work_repo'])

        return repo

    def _create_output_repo(self):
        repo = PfsGen3Repo(**self.__repo_types['output_repo'])

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

    def _init_output_repo(self):
        self.__output_repo.init_from_args(self)

        self.__output_repo.ignore_missing_files = self.__config.ignore_missing_files
        self.__output_repo.init_from_args(self)

    def _update_repo_directories(self, config):
        """
        Ensure the precedence of the configuration settings
        """
        
        #   1. Command-line arguments
        #   2. Configuration file
        #   3. Default values

        # Override configuration with command-line arguments
        if self.is_arg('datadir'):
            config.datadir = self.get_arg('datadir')
        if self.is_arg('workdir'):
            config.workdir = self.get_arg('workdir')
        if self.is_arg('outdir'):
            config.outdir = self.get_arg('outdir')
        if self.is_arg('rundir'):
            config.rundir = self.get_arg('rundir')
        if self.is_arg('run'):
            config.run = self.get_arg('run')
        if self.is_arg('garundir'):
            config.garundir = self.get_arg('garundir')
        if self.is_arg('garun'):
            config.garun = self.get_arg('garun')

        if config.datadir is not None:
            self.__input_repo.set_variable('datadir', config.datadir)
        if config.rundir is not None:
            self.__input_repo.set_variable('rundir', config.rundir)

        if config.workdir is not None:
            self.__work_repo.set_variable('datadir', config.workdir)
        if config.garundir is not None:
            self.__work_repo.set_variable('rundir', config.garundir)

        if config.outdir is not None:
            self.__output_repo.set_variable('datadir', config.outdir)
        if config.garundir is not None:
            self.__output_repo.set_variable('rundir', config.garundir)

    def _set_log_file_to_workdir(self):
        # Override logging directory to use the same as the pipeline workdir
        logfile = os.path.basename(self.log_file)
        self.log_file = os.path.join(
            self.work_repo.get_resolved_variable('workdir'),
            self.work_repo.get_resolved_variable('rerun'),
            logfile)

    def _load_obs_log_files(self, obs_logs_path):
        if isinstance(obs_logs_path, str):
            obs_logs_path = [ obs_logs_path ]

        obs_log = None

        for path in obs_logs_path:
            files = glob(path)
            for f in files:
                logger.info(f'Loading observation log from {f}.')

                columns = {
                    '# visit_id': pd.Int32Dtype(),
                    'pfs_design_id': str,
                    'sequence_name': pd.StringDtype(),
                    'issued_at': str,                  # HST
                    'avg_exptime': float,
                    'seeing_median': float,
                    'transparency_median': float,
                    'eet_b': float,
                    'eet_r': float,
                    'eet_n': float,
                    'eet_m': float
                }

                column_names = {

                }

                df = pd.read_csv(f,
                         delimiter=',',
                         header=0,
                         usecols=list(columns.keys()),
                         dtype=columns)

                # Rename all columns starting with '#' to remove the hash and
                # then use the mapping to rename the columns
                df.columns = df.columns.str.replace('# ', '', regex=False)

                # df['pfs_design_id'] = df['pfs_design_id'].map(lambda x: f'0x{int(x, 16):016x}')
                df['issued_at'] = df['issued_at'].map(lambda x: dateparser.parse(x))

                # Convert from HST to UTC
                tz = timedelta(hours=-10)
                df['issued_at'] = df['issued_at'].map(lambda x: x - tz)

                if obs_log is None:
                    obs_log = df
                else:
                    obs_log = pd.concat([obs_log, df], ignore_index=True)

        # Remove duplicates
        obs_log.drop_duplicates(subset=['visit_id'], inplace=True, keep='first')

        # Index by visit_id
        obs_log.set_index('visit_id', inplace=True)
        
        return obs_log

    def _load_obs_params_file(self, obs_params_file, obs_params_id, obs_params_visit):
        logger.info(f'Loading observation parameters from {obs_params_file}.')
        obs_params = pd.read_feather(obs_params_file)

        logger.info(f'Found {len(obs_params)} entries in observation parameter file.')

        if obs_params_id not in obs_params.columns:
            raise ValueError(f'ID column {obs_params_id} not found in observation parameter file.')
        if obs_params_visit not in obs_params.columns:
            raise ValueError(f'Visit column {obs_params_visit} not found in observation parameter file.')

        obs_params = obs_params.set_index(obs_params_id) 

        return obs_params

    def _load_stellar_params_file(self, stellar_params_file, stellar_params_id):
        # TODO: update this if multiple files are needed or the file format changes
        logger.info(f'Loading stellar parameters from {stellar_params_file}.')
        stellar_params = pd.read_feather(stellar_params_file)

        logger.info(f'Found {len(stellar_params)} entries in stellar parameter file.')

        if stellar_params_id not in stellar_params.columns:
            raise ValueError(f'ID column {stellar_params_id} not found in stellar parameter file.')

        stellar_params = stellar_params.set_index(stellar_params_id) 

        return stellar_params

    def _load_target_list_files(self, target_lists_files):
        """
        Load target list files from the netflow output directory.
        """

        target_list = None

        for target_lists_file in target_lists_files if isinstance(target_lists_files, list) else [target_lists_files]:
            for f in glob(target_lists_file):

                # Skip sky files because they're too large
                if f.endswith('sky.feather'):
                    logger.info(f'Skipping sky target list file {f} because it is too large.')
                    continue
                
                logger.info(f'Loading target list from {f}.')

                df = pd.read_feather(f)

                if target_list is None:
                    target_list = df
                else:
                    target_list = pd.concat([target_list, df], ignore_index=True)

        # Remove duplicates
        if target_list is not None:
            target_list.drop_duplicates(inplace=True)
            logger.info(f'Found {len(target_list)} unique entries in target list files.')

        return target_list

    def _find_matching_targets(self, target_list, obcode, objid, max_separation=0.1):
        # Find objects in the target lists.

        # The object can be present multiple times in the target list because we cross-match each input catalog
        # and store each match in the target list. However, only the primary occurance of the target will have
        # an obcode associated with. Yet, the values in the column __target_idx should match. We can use all
        # entries in the target list to load fluxes and magnitudes.

        # Some early runs of netflow used a cross-match radius too large, so we need to verify if duplicate
        # entries in the target list are actually duplicates or if they are wrong matches.

        # First, find the unique matching target based on obcode. For this, we need to look up the obcode from
        # the pfsConfig file.

        if obcode == 'N/A':
            # This is probably a calibration target
            mask = (target_list['targetid'] == objid)
        else:
            # Science targets always have a valid obcode
            mask = (target_list['obcode'] == obcode) & (target_list['__target_idx'] == (objid & 0xFFFFFFFF))

        primary_target = np.where(mask)[0].item()
        target_idx = target_list.loc[primary_target, '__target_idx']
        primary_ra = target_list.loc[primary_target, 'RA']
        primary_dec = target_list.loc[primary_target, 'Dec']

        # Find other targets with matching __target_idx
        secondary_targets = []
        for secondary_target in np.where(target_list['__target_idx'] == target_idx)[0]:
            # Skip primary target because we will use it to override other targets
            if secondary_target == primary_target:
                continue

            # Calculate the separation between the primary and secondary target
            # and skip if they are too far apart because they are likely wrong matches
            secondary_ra = target_list.loc[secondary_target, 'RA']
            secondary_dec = target_list.loc[secondary_target, 'Dec']

            primary_coord = SkyCoord(ra=primary_ra * u.deg, dec=primary_dec * u.deg)
            secondary_coord = SkyCoord(ra=secondary_ra * u.deg, dec=secondary_dec * u.deg)
            separation = primary_coord.separation(secondary_coord)
            if separation.arcsec > max_separation:
                logger.warning(f'Separation between primary and secondary target with __target_idx {target_idx} is {separation.arcsec:.2f} arcsec, which is larger than the threshold. Skipping secondary target.')
                continue

            secondary_targets.append(secondary_target)

        return primary_target, secondary_targets

    def _enumerate_target_list_fluxes(self, photometry, target_list, idx):
        for filter, config in photometry.items():
            if config.filter_name is None:
                filter_names = [filter]
            else:
                filter_names = config.filter_name if isinstance(config.filter_name, (list, tuple)) else [config.filter_name]
            
            # Search among possible filter names for the same instrument band
            for filter_name in filter_names:
                flux_found = False
                if f'{filter_name}_flux' in target_list.columns:
                    # The flux is available in it's own column
                    # ~np.isnan(target_list.loc[primary_target, f'{filter_name}_flux']):
                    flux = target_list.loc[idx, f'{filter_name}_flux'].item()
                    flux_err = target_list.loc[idx, f'{filter_name}_flux_err'].item()
                    flux_found = True
                else:
                    for cc in [ c for c in target_list.columns if c.startswith('filter_')]:
                        if target_list.loc[idx, cc] == filter_name:
                            # The filter name is available in one of the bands
                            filter = cc[len('filter_'):]
                            flux = target_list.loc[idx, f'psf_flux_{filter}'].item()
                            flux_err = target_list.loc[idx, f'psf_flux_error_{filter}'].item()
                            flux_found = True

                if flux_found:
                    break

            if flux_found and flux is not None and flux_err is not None and \
                np.isfinite(flux) and np.isfinite(flux_err):

                yield filter, config, flux, flux_err

    def _find_magnitudes_in_target_list(self, photometry, target_list, idx, magnitudes=None, force_update=False):
        # Check if any magnitudes with filter curves are defined in the config template
        # and if so, try to match them to the filters available in pfsConfig

        # TODO: the targets lists actually should have the magnitudes, not just the fluxes,
        #       but earlier netflow runs don't keep track of the column they're stored in and
        #       it's not straighforward to match the filter names, so for now we just look for
        #       fluxes in the target list.

        magnitudes = magnitudes if magnitudes is not None else defaultdict(SimpleNamespace)

        for filter, config, flux, flux_err in self._enumerate_target_list_fluxes(photometry, target_list, idx):
            # Set the config values based on the target list values
            if filter not in magnitudes or force_update:
                magnitudes[filter].flux = flux
                magnitudes[filter].flux_err = flux_err
                magnitudes[filter].mag, magnitudes[filter].mag_err = Physics.jy_to_abmag(1e-9 * flux, 1e-9 * flux_err)

        return magnitudes

    def _find_matching_assignment(self, assignments, obcode, objid):
        # Match with the assignments by obCode and objId
        if obcode == 'N/A':
            # This is a calibration target with a missing obCode
            assignments_idx = (assignments['targetid'] == (objid & 0xFFFFFFFF))
        else:
            # This is a science target, we also match by obcode
            assignments_idx = (assignments['obcode'] == obcode) # & (assignments['__target_idx'] == (objid & 0xFFFFFFFF))
        
        if np.sum(assignments_idx) == 0:
            logger.warning(f'No matching assignment found for obCode {obcode}.')
        elif np.sum(assignments_idx) > 1:
            logger.warning(f'Multiple matching assignments found for obCode {obcode}, taking the one with the highest stage.')

        return assignments_idx
