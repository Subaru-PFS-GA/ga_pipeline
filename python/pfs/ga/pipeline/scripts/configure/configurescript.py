#!/usr/bin/env python3

import os
import re
from copy import deepcopy
from glob import glob
from types import SimpleNamespace
from datetime import datetime
from collections import defaultdict
import numpy as np

from pfs.datamodel import *
from pfs.datamodel.utils import calculatePfsVisitHash, wraparoundNVisit

from pfs.ga.common.scripts import Progress

from ..pipelinescript import PipelineScript
from ...gapipe.config import *

from ...setup_logger import logger

class ConfigureScript(PipelineScript, Progress):
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

        self.__obs_logs = None                      # Observation log files
        self.__obs_params = None                    # Observation parameters file
        self.__obs_params_id = 'objid'              # ID column of the observation parameters
        self.__obs_params_visit = 'visit'           # Visit ID column of the observation parameters
        self.__stellar_params = None                # Params file with stellar parameters to derive the priors from
        self.__stellar_params_id = '__target_idx'   # ID column of the params file

    def _add_args(self):
        self.add_arg('--config', type=str, nargs='*', required=True, help='Configuration file')
        self.add_arg('--obs-logs', type=str, nargs='*', help='Observation log files')
        self.add_arg('--obs-params', type=str, help='Observation parameters file')
        self.add_arg('--obs-params-id', type=str, help='ObjID column of the observation parameters to use')
        self.add_arg('--obs-params-visit', type=str, help='Visit ID column of the observation parameters to use')
        self.add_arg('--stellar-params', type=str, help='Path to stellar parameters file')
        self.add_arg('--stellar-params-id', type=str, help='ID column of the stellar parameters to use')

        PipelineScript._add_args(self)
        Progress._add_args(self)

    def _init_from_args(self, args):
        self.__obs_logs = self.get_arg('obs_logs', args, self.__obs_logs)
        self.__obs_params = self.get_arg('obs_params', args, self.__obs_params)
        self.__obs_params_id = self.get_arg('obs_params_id', args, self.__obs_params_id)
        self.__obs_params_visit = self.get_arg('obs_params_visit', args, self.__obs_params_visit)
        self.__stellar_params = self.get_arg('stellar_params', args, self.__stellar_params)
        self.__stellar_params_id = self.get_arg('stellar_params_id', args, self.__stellar_params_id)

        PipelineScript._init_from_args(self, args)
        Progress._init_from_args(self, args)

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

        # Update the repo directories based on the config and the command-line arguments
        self._update_repo_directories(self.config)

        # Load the observation logs
        if self.__obs_logs is not None:
            obs_log = self._load_obs_log_files(self.__obs_logs)
        else:
            logger.warning('No observation log files specified, skipping loading obslog.')
            obs_log = None

        # Load the observational parameters such as broadband magnitudes, velocity corrections etc.
        if self.__obs_params is not None:
            obs_params = self._load_obs_params_file(self.__obs_params, self.__obs_params_id, self.__obs_params_visit)
        else:
            obs_params = None

        # Load the stellar parameters
        if self.__stellar_params is not None:
            stellar_params = self._load_stellar_params_file(self.__stellar_params, self.__stellar_params_id)
        else:
            stellar_params = None

        # Find the objects matching the command-line arguments. Arguments
        # are parsed by the repo object itself, so no need to pass them in here.
        logger.info('Finding objects matching the filters. This requires loading all PfsConfig files for the given visits and can take a while.')
        pfs_configs = self.input_repo.load_pfsConfigs()

        # Get the dictionary of object identities matching the filters, keyed by objid
        identities = self.input_repo.find_objects(pfs_configs=pfs_configs, groupby='objid')

        if len(identities) == 0:
            logger.error('No objects found matching the filters.')
            return
        else:
            logger.info(f'Found {len(identities)} objects matching the filters.')

        # Create the iterator that we will loop over to generate the pipeline configuration files.
        pipeline_configs = self.__create_pipeline_configs(identities, pfs_configs, obs_log, obs_params, stellar_params)

        logger.info(f'Ready to generate {len(identities)} configuration files for the pipeline.')

        if not self.yes:
            answer = input(f'Proceed to generate {len(identities)} configuration files? [y/N]: ')
            if answer.lower() != 'y':
                logger.info('Aborting configuration file generation.')
                return
        
        # Generate the configuration file for each target
        q = 0
        for objid, pipeline_config, filename in self._wrap_in_progressbar(pipeline_configs, total=len(identities), logger=logger):
            self.__save_pipeline_config(objid, pipeline_config, filename)

            q += 1
            if self.top is not None and q >= self.top:
                logger.warning(f'Stopping after {q} objects.')
                break

    def __create_pipeline_configs(self, identities, pfs_configs, obs_log=None, obs_params=None, stellar_params=None):
        for objid, identity in identities.items():
            # Update the identity from the observation log if available
            if obs_log is not None:
                # Update seeing and exptime from obs_log. Obstime is taken from
                # pfsConfig which is more accurate since the obslog only has the
                # time of the start of the command, not the start of the exposure.
                exptime = []
                seeing = []
                for visit in identity.visit:
                    exptime.append(obs_log.loc[visit, 'avg_exptime'])
                    seeing.append(obs_log.loc[visit, 'seeing_median'])

                identity.exptime = np.array(exptime, dtype=float)
                identity.seeing = np.array(seeing, dtype=float)
            else:
                # exptime is part of pfsConfig, but it might be all None
                identity.exptime = np.array([np.nan] * len(identity.visit), dtype=float)
                identity.seeing = np.array([np.nan] * len(identity.visit), dtype=float)

            # Get target identity and list of observations to be included
            target = self.__create_target_config(objid, identity)

            # Skip objects where the target configuration could not be created
            # (e.g. no required products found)
            if target is None:
                continue

            # Create a pipeline configuration object specific to the object
            pipeline_config, filename = self.__create_pipeline_config(objid, target.identity)
            pipeline_config.target = target

            # Look up the stellar parameters in the params file and configure
            # template fitting
            self.__configure_tempfit(objid, pipeline_config, pfs_configs, obs_params, stellar_params)

            # TODO: add further steps

            yield objid, pipeline_config, filename

    def __create_target_config(self, objid, id):
        """
        Return the configuration section objects for each target
        """

        # Look up the necessary input files from the list of observations and
        # make sure they exists. If the don't, exclude them from the list.
        visit_mask = self.__locate_required_products(objid, id)

        if visit_mask.sum() == 0:
            logger.error(f'No required products found for object {objid}, skipping.')
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
                obsTime = id.obstime[visit_mask],
                expTime = id.exptime[visit_mask],
                seeing = id.seeing[visit_mask],
            )
        )

        return target

    def __locate_required_products(self, objid, identity):
        """
        Iterate over all objects and check if the required products are available.

        Maintain a list of already located products to avoid too many queries to Butler.
        """

        mask = np.full_like(identity.visit, True, dtype=bool)

        # Check if the required products are available in any of the data
        # repositories. If not, set the mask to False for those visits.
        for product_name in self.config.tempfit.required_products:
            m = np.full_like(mask, False, dtype=bool)

            # Try each repository in order
            found = False
            for repo in [self.input_repo, self.work_repo]:
                # Look up the product type based on its name as a string. If the
                # product is not available in a certain repo, skip to the next one.
                try:
                    product_type = repo.parse_product_type(product_name)
                    found = True
                except ValueError:
                    logger.debug(f'Product type `{product_name}` not available in repo of type {type(repo.repo).__name__}.')
                    continue

                for i in range(len(identity.visit)):
                    id = { k: v[i] for k, v in identity.__dict__.items() }

                    try:
                        fn, _ = repo.locate_product(product_type, **id)
                    except FileNotFoundError:
                        fn = None

                    if fn is None:
                        logger.warning(f'Required product `{product_name}` for object 0x{objid:x}, visit {identity.visit[i]} not found.')
                    elif not os.path.isfile(fn):
                        logger.warning(f'Required file {fn} for product `{product_name}` for object {objid:x}, visit {identity.visit[i]} not found.')
                    else:
                        m[i] = True

                break   # If we found the product in one of the repositories, we can stop looking further.

            if not found:
                logger.warning(f'Required product type `{product_name}` not found in any of the repositories for object 0x{objid:x}.')
                continue

            mask &= m
                
        return mask

    def __create_pipeline_config(self, objid, identity, ext='.yaml'):
        """
        Initialize a pipeline configuration object based on the template and the target.
        """

        # TODO: should we make a deep copy here?
        config = deepcopy(self.config)

        # Compose the directory and file names for the identity of the object
        # The file should be written somewhere under the work directory

        # Name of the output pipeline configuration
        dir = self.work_repo.format_dir(GAPipelineConfig, identity)
        config_file = self.work_repo.format_filename(GAPipelineConfig, identity)
        filename = os.path.join(dir, config_file)

        # Update config with directory names

        # Input data directories; if not set, use the default from the config
        if 'datadir' in self.input_repo.variables:
            config.datadir = self.input_repo.get_resolved_variable('datadir')
        if 'rerundir' in self.input_repo.variables:
            config.rerundir = self.input_repo.get_resolved_variable('rerundir')

        logger.debug(f'Configured data directory for object {identity}: {config.datadir}')
        logger.debug(f'Configured rerun directory for object {identity}: {config.rerundir}')
        logger.debug(f'Configured work directory for object {identity}: {config.workdir}')
        logger.debug(f'Configured output directory for object {identity}: {config.outdir}')

        return config, filename

    def __configure_tempfit(self, objid, pipeline_config, pfs_configs, obs_params, stellar_params):
        # TODO: replace this with obs_log?
        self.__configure_tempfit_obs_params(objid, pipeline_config, pfs_configs, obs_params)

        self.__configure_tempfit_magnitudes_pfs_config(objid, pipeline_config, pfs_configs)

        if stellar_params is None:
            pass
        elif objid not in stellar_params.index:
            logger.warning(f'Stellar parameters for object 0x{objid:x} not found, skipping configuring priors.')
        else:
            self.__configure_tempfit_magnitudes_stellar_params(objid, pipeline_config, stellar_params)
            self.__configure_tempfit_stellar_param_priors(objid, pipeline_config, pfs_configs, stellar_params)

    def __configure_tempfit_obs_params(self, objid, pipeline_config, pfs_configs, obs_params):
        pass

    def __configure_tempfit_magnitudes_pfs_config(self, objid, pipeline_config, pfs_configs):
        # Look up fluxes in the pfs_config file and set the photometric fluxes
        # to constrain template fitting
        # The configuration template has a list of magnitudes that can be used. Match these
        # to the fluxes available in the pfsConfig file, set the values and the errors.

        # TODO: this takes the very first pfsConfig file, should we do something smarter?
        pfs_config = pfs_configs[pipeline_config.target.observations.visit[0]]
        idx = np.where(pfs_config.objId == objid)[0].item()

        # Check if any magnitudes with filter curves are defined in the config template
        # and if so, try to match them to the filters available in pfsConfig
        if pipeline_config.tempfit.photometry is not None:
            for filter_index, filter_name in enumerate(pfs_config.filterNames[idx]):
                if filter_name is not None and filter_name != 'none':
                    # Try to match the filter name to something in the configuration
                    filter_found = False
                    for fn, mag in pipeline_config.tempfit.photometry.items():
                        if mag.filter_name is None or isinstance(mag.filter_name, (list, tuple)) and len(mag.filter_name) == 0:
                            # If the filter name is not set, match on the key
                            if fn == filter_name:
                                filter_found = True
                                break
                        else:
                            # If the filter name is set, match on that
                            if isinstance(mag.filter_name, str):
                                names = [mag.filter_name]
                            else:
                                names = mag.filter_name

                            if filter_name in names:
                                filter_found = True
                                break
                        
                    if filter_found:
                        # Set the values from pfsConfig, with a preference order
                        # of the fluxes
                        flux_found = False
                        for flux, flux_error in [
                            (pfs_config.psfFlux[idx][filter_index], pfs_config.psfFluxErr[idx][filter_index]),
                            (pfs_config.fiberFlux[idx][filter_index], pfs_config.fiberFluxErr[idx][filter_index]),
                            (pfs_config.totalFlux[idx][filter_index], pfs_config.totalFluxErr[idx][filter_index]),
                        ]:
                            
                            if flux is not None and flux_error is not None and \
                                np.isfinite(flux) and np.isfinite(flux_error):

                                flux_found = True
                                mag.flux = flux
                                mag.flux_error = flux_error

                                # TODO: calculate magnitudes?
                                break

                        if not flux_found:
                            logger.warning(f'No fluxes found in pfsConfig for object 0x{objid:x}, filter {filter_name}, skipping setting magnitude.')

        # Only keep photometry that is available in pfsConfig or obs_params
        photometry = {}
        for k, v in pipeline_config.tempfit.photometry.items():
            if v.flux is not None:
                photometry[k] = v

        # TODO: verify if magnitudes are consistent because some fluxes in PfsConfig are wrong

        pipeline_config.tempfit.photometry = photometry

    def __configure_tempfit_magnitudes_stellar_params(self, objid, pipeline_config, stellar_params):
        # TODO: when a stellar_params file is provided, look up magnitudes from there as
        #       well and override magnitude taken from pfsConfig
        pass

    def __configure_tempfit_stellar_param_priors(self, objid, pipeline_config, pfs_configs, stellar_params):
        pp = stellar_params.loc[objid]
        for k in ['M_H', 'T_eff', 'log_g', 'a_M', 'rv']:
            k_min = f'{k}_min'
            k_max = f'{k}_max'
            k_dist = f'{k}_dist'
            k_dist_mean = f'{k}_dist_mean'
            k_dist_sigma = f'{k}_dist_sigma'

            # Limits

            if k in pipeline_config.tempfit.tempfit_args and pipeline_config.tempfit.tempfit_args[k] is not None:
                values = pipeline_config.tempfit.tempfit_args[k]
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
                pipeline_config.tempfit.tempfit_args[k] = values

            # Distribution

            if k_dist in pipeline_config.tempfit.tempfit_args and pipeline_config.tempfit.tempfit_args[k_dist] is not None:
                dist = pipeline_config.tempfit.tempfit_args[k_dist][0]
                dist_args = pipeline_config.tempfit.tempfit_args[k_dist][1:]
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
                pipeline_config.tempfit.tempfit_args[k_dist] = [dist] + dist_args
    
    def __save_pipeline_config(self, objid, pipeline_config, filename):
        """
        Generate a config file for each of the inputs.
        """

        if not self.dry_run:
            logger.info(f'Saving configuration file `{filename}`.')
            os.makedirs(os.path.dirname(filename), exist_ok=True)
            pipeline_config.save(filename)
        else:
            logger.info(f'Skipped saving configuration file `{filename}`.')

def main():
    script = ConfigureScript()
    script.execute()

if __name__ == "__main__":
    main()