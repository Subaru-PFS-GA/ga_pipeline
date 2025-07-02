#!/usr/bin/env python3

import os
import re
from copy import deepcopy
from glob import glob
from types import SimpleNamespace
from datetime import datetime
import numpy as np

from pfs.datamodel import *
from pfs.datamodel.utils import calculatePfsVisitHash, wraparoundNVisit

from ..pipelinescript import PipelineScript
from ...gapipe.config import *

from ...setup_logger import logger

class ConfigureScript(PipelineScript):
    """
    Generate the job configuration file for a set of observations.

    The script works by finding all pfsSingle files that match the filters specified
    on the command-line and then reads an existing configuration file which is used
    as a template. The script then updates the configuration file with the identities
    of the data products determined from the pfsSingle files and saves the updated
    configuration file to the output directory.

    Although the script registers all the identity parameters as filters, it only
    uses the catId, tract, patch, objId, and visit filters to find the pfsSingle files.
    The corresponding pfsConfig files are then found by matching the pfsSingle files
    to look up the fiberId etc.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.__params = None                # Params file with stellar parameters to derive the priors from
        self.__params_id = '__target_idx'   # ID column of the params file
        self.__dry_run = False              # Dry run mode
        self.__top = None                   # Stop after this many objects

    def _add_args(self):
        self.add_arg('--config', type=str, nargs='*', required=True, help='Configuration file')
        self.add_arg('--params', type=str, help='Path to stellar parameters file')
        self.add_arg('--params-id', type=str, help='ID column of the stellar parameters to use')
        self.add_arg('--dry-run', action='store_true', help='Dry run mode')
        self.add_arg('--top', type=int, help='Stop after this many objects')

        super()._add_args()

    def _init_from_args(self, args):
        self.__params = self.get_arg('params', args, self.__params)
        self.__params_id = self.get_arg('params_id', args, self.__params_id)
        self.__dry_run = self.get_arg('dry_run', args, self.__dry_run)
        self.__top = self.get_arg('top', args, self.__top)

        super()._init_from_args(args)

    def prepare(self):
        super().prepare()

        # Override logging directory to use the same as the pipeline workdir
        self._set_log_file_to_workdir()

    def run(self):
        """
        Find all the pfsSingle or pfsConfig files that match the filters and generate a config file for each.
        """

        files = ' '.join(self.config.config_files)
        logger.info(f'Using configuration template file(s) {files}.')

        # Load the stellar parameters
        params = self._load_params_file(self.__params, self.__params_id)

        # Find the objects matching the command-line arguments. Arguments
        # are parsed by the repo object itself, so no need to pass them in here.
        identities = self.input_repo.find_objects(groupby='objid')

        if len(identities) == 0:
            logger.error('No objects found matching the filters.')
            return
        else:
            logger.info(f'Found {len(identities)} objects matching the filters.')

        # Create the target configuration objects
        configs, filenames = self.__create_output_configs(identities, params)
        
        # Generate the configuration file for each target
        self.__save_config_files(configs, filenames)

    def __create_output_configs(self, identities, params=None):
        configs = {}
        filenames = {}
        for objid, id in identities.items():
            # Get target identity and list of observations to be included
            target = self.__get_target_config(objid, id)

            # Create general pipeline configuration
            config, filename = self.__get_pipeline_config(objid, target.identity)
            config.target = target

            # Look up the stellar parameters in the params file and configure
            # template fitting
            if params is not None and objid in params.index:
                self.__configure_rvfit(config, params, objid)

            # TODO: add further steps

            configs[objid] = config
            filenames[objid] = filename

        return configs, filenames

    def __get_target_config(self, objid, id):
        """
        Return the configuration section objects for each target
        """

        # Look up the necessary input files from the list of observations and
        # make sure they exists. If the don't, exclude them from the list.
        visit_mask = self.__locate_required_products(objid, id)

        if visit_mask.sum() == 0:
            logger.warning(f'No required products found for object {objid}, skipping.')
            return None

        target = GATargetConfig(
            proposalId = id.proposalId[0],
            targetType = id.targetType[0],
            identity = GAObjectIdentityConfig(
                catId = id.catId[0],
                tract = id.tract[0],
                patch = id.patch[0],
                objId = objid,
                nVisit = wraparoundNVisit(len(id.visit[visit_mask])),
                pfsVisitHash = calculatePfsVisitHash(id.visit[visit_mask]),
            ),
            observations = GAObjectObservationsConfig(
                visit = id.visit[visit_mask],
                arms = id.arms[visit_mask],
                spectrograph = id.spectrograph[visit_mask],
                pfsDesignId = id.pfsDesignId[visit_mask],
                fiberId = id.fiberId[visit_mask],
                fiberStatus = id.fiberStatus[visit_mask],
                obstime = id.obstime[visit_mask],
                exptime = id.exptime[visit_mask],
            )
        )

        return target

    def __locate_required_products(self, objid, identity):
        # self.config is the prototype configuration object that we can use
        # to get a list of required input files.

        mask = np.full_like(identity.visit, True, dtype=bool)

        # Check if the required products are available in any of the data
        # repositories. If not, set the mask to False for those visits.
        for product_name in self.config.rvfit.required_products:
            m = np.full_like(mask, False, dtype=bool)

            # Try each repository in order
            for repo in [self.input_repo, self.work_repo]:
                # Look up the product type based on its name as a string. If the
                # product is not available in a certain repo, skip to the next one.
                try:
                    prod = repo.parse_product_type(product_name)
                except ValueError:
                    logger.debug(f'Product type `{product_name}` not available in repo of type {type(repo.repo).__name__}.')
                    continue

                for i in range(len(identity.visit)):
                    try:
                        fn, _ = repo.locate_product(
                            prod,
                            visit = identity.visit[i],
                            pfsDesignId = identity.pfsDesignId[i],
                            catId = identity.catId[i],
                            objid = identity.objId[i])
                    except FileNotFoundError:
                        fn = None

                    if fn is None or not os.path.isfile(fn):
                        logger.warning(f'Required product `{product_name}` for object {objid} not found: {fn}, ignoring.')
                    else:
                        m[i] = True

                break   # If we found the product in one of the repositories, we can stop looking further.

            mask &= m
                
        return mask

    def __get_pipeline_config(self, objid, identity, ext='.yaml'):
        """
        Initialze a pipeline configuration object based on the template and the target.
        """

        # TODO: should we make a deep copy here?
        config = deepcopy(self.config)

        # Compose the directory and file names for the identity of the object
        # The file should be written somewhere under the work directory

        dir = self.work_repo.format_dir(GAPipelineConfig, identity)
        config_file = self.work_repo.format_filename(GAPipelineConfig, identity)

        # Name of the output pipeline configuration
        filename = os.path.join(dir, config_file)

        # Update config with directory names

        # Input data directories
        config.datadir = self.input_repo.get_resolved_variable('datadir')
        config.rerundir = self.input_repo.get_resolved_variable('rerundir')

        logger.debug(f'Configured data directory for object {identity}: {config.datadir}')
        logger.debug(f'Configured rerun directory for object {identity}: {config.rerundir}')
        logger.debug(f'Configured work directory for object {identity}: {config.workdir}')
        logger.debug(f'Configured output directory for object {identity}: {config.outdir}')

        return config, filename

    def __configure_rvfit(self, config, params, objid):
        pp = params.loc[objid]

        for k in ['M_H', 'T_eff', 'log_g', 'a_M', 'rv']:
            k_min = f'{k}_min'
            k_max = f'{k}_max'
            k_dist = f'{k}_dist'
            k_dist_mean = f'{k}_dist_mean'
            k_dist_sigma = f'{k}_dist_sigma'

            # Limits

            if k in config.rvfit.rvfit_args and config.rvfit.rvfit_args[k] is not None:
                values = config.rvfit.rvfit_args[k]
            else:
                values = None

            if k in pp and pp[k] is not None and not np.isnan(pp[k]):
                # Constant value, overrides limits
                values = float(pp[k])
            else:
                if k_min in pp and pp[k_min] is not None and not np.isnan(pp[k_min]):
                    # Lower limit
                    if not isinstance(values, list):
                        values = [-np.inf, np.inf]
                    values[0] = pp[k_min]

                if k_max in pp and pp[k_max] is not None and not np.isnan(pp[k_max]):
                    # Upper limit
                    if not isinstance(values, list):
                        values = [-np.inf, np.inf]
                    values[1] = pp[k_max]

            if values is not None:
                config.rvfit.rvfit_args[k] = values

            # Distribution

            if k_dist in config.rvfit.rvfit_args and config.rvfit.rvfit_args[k_dist] is not None:
                dist = config.rvfit.rvfit_args[k_dist][0]
                dist_args = config.rvfit.rvfit_args[k_dist][1:]
            else:
                dist = None
                dist_args = None
            
            if k_dist in pp and pp[k_dist] is not None:
                if pp[k_dist] == 'uniform':
                    dist = 'uniform'
                    dist_args = []
                elif pp[k_dist] == 'normal':
                    dist = 'normal'
                    dist_args = [pp[k_dist_mean], pp[k_dist_sigma]]
                else:
                    raise NotImplementedError()

            if dist is not None and dist_args is not None:
                config.rvfit.rvfit_args[k_dist] = [dist] + dist_args
    
    def __save_config_files(self, configs, filenames):
        """
        Generate a config file for each of the inputs.

        While the result of the final processing is a single FITS file, we need
        a separate work directory for each object to store the auxiliary files.
        """

        q = 0
        for objid in sorted(configs.keys()):
            config, filename = configs[objid], filenames[objid]
            if not self.__dry_run:
                logger.info(f'Saving configuration file `{filename}`.')
                os.makedirs(os.path.dirname(filename), exist_ok=True)
                config.save(filename)
            else:
                logger.info(f'Skipped saving configuration file `{filename}`.')

            q += 1
            if self.__top is not None and q >= self.__top:
                logger.info(f'Stopping after {q} objects.')
                break

def main():
    script = ConfigureScript()
    script.execute()

if __name__ == "__main__":
    main()