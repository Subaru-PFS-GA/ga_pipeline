#!/usr/bin/env python3

import os
import re
from glob import glob
from types import SimpleNamespace
from datetime import datetime
import numpy as np

from pfs.datamodel import *
from pfs.datamodel.utils import calculatePfsVisitHash, wraparoundNVisit
from pfs.ga.pfsspec.survey.pfs import PfsGen3FileSystemRepo

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

    Variables
    ---------
    config : GAPipelineConfig
        Pipeline configuration template
    workdir : str
        Working directory for the pipeline job. The log file, as well as the config
        files, are written to this directory. When running the pipeline, the working
        directory will be used to store the individual log files and auxiliary files.
        Subdirectories composed of the objects' identity parameters will be created.
    outdir : str
        Output directory for the final data products.
    """

    def __init__(self):
        super().__init__()

        self.__config = None            # Pipeline configuration template

        self.__workdir = self.get_env('GAPIPE_WORKDIR')     # Working directory for the pipeline job
        self.__outdir = self.get_env('GAPIPE_OUTDIR')       # Output directory for the final data products
        self.__dry_run = False          # Dry run mode
        self.__top = None               # Stop after this many objects

    def _add_args(self):
        self.add_arg('--config', type=str, nargs='*', required=True, help='Configuration file')

        self.add_arg('--outdir', type=str, help='Output directory')
        self.add_arg('--dry-run', action='store_true', help='Dry run mode')
        self.add_arg('--top', type=int, help='Stop after this many objects')

        super()._add_args()

    def _init_from_args(self, args):
        self.__config = GAPipelineConfig()

        # Load the configuration template file
        config_files = self.get_arg('config', args)
        self.__config.load(config_files, ignore_collisions=True)

        # TODO: consider merging this part with the run script
        # Ensure the precendence of the directories:
        #   1. Command-line arguments
        #   2. Configuration file
        #   3. Default values

        # Override configuration with command-line arguments
        if self.is_arg('workdir', args):
            self.__config.workdir = self.get_arg('workdir', args)
        if self.is_arg('outdir', args):
            self.__config.outdir = self.get_arg('outdir', args)
        if self.is_arg('datadir', args):
            self.__config.datadir = self.get_arg('datadir', args)
        if self.is_arg('rerun', args):
            self.__config.rerun = self.get_arg('rerun', args)
        if self.is_arg('rerundir', args):
            self.__config.rerundir = self.get_arg('rerundir', args)

        # Override data store connector with configuration values
        # Also save workdir and outdir because these might be overwritten
        # in the configuration template
        if self.__config.workdir is not None:
            self.repo.set_variable('workdir', self.__config.workdir)
            self.__workdir = self.__config.workdir
        if self.__config.outdir is not None:
            self.repo.set_variable('outdir', self.__config.outdir)
            self.__outdir = self.__config.outdir
        if self.__config.datadir is not None:
            self.repo.set_variable('datadir', self.__config.datadir)
        if self.__config.rerun is not None:
            self.repo.set_variable('rerun', self.__config.rerun)
        if self.__config.rerundir is not None:
            self.repo.set_variable('rerundir', self.__config.rerundir)

        self.__dry_run = self.get_arg('dry_run', args, self.__dry_run)
        self.__top = self.get_arg('top', args, self.__top)

        super()._init_from_args(args)

    def prepare(self):
        super().prepare()

        # Override logging directory to use the same as the pipeline workdir
        logfile = os.path.basename(self.log_file)
        self.log_file = os.path.join(self.__workdir, logfile)

    def run(self):
        """
        Find all the pfsSingle or pfsConfig files that match the filters and generate a config file for each.
        """

        files = ' '.join(self.__config.config_files)
        logger.info(f'Using configuration template file(s) {files}.')

        identities = self.repo.find_object(groupby='objid')
        targets = self.__create_target_config(identities)

        if len(targets) == 0:
            logger.warning('No objects found matching the filters.')
            return
        
        # Generate the configuration file for each target
        self.__generate_config_files(targets)

    def __create_target_config(self, identities):
        """
        Return the configuration section objects for each target
        """

        targets = {}
        for objid, id in identities.items():
            targets[objid] = GATargetConfig(
                proposalId = id.proposalId[0],
                targetType = id.targetType[0],
                identity = GAObjectIdentityConfig(
                    catId = id.catId[0],
                    tract = id.tract[0],
                    patch = id.patch[0],
                    objId = objid,
                    nVisit = wraparoundNVisit(len(id.visit)),
                    pfsVisitHash = calculatePfsVisitHash(id.visit),
                ),
                observations = GAObjectObservationsConfig(
                    visit = id.visit,
                    arms = id.arms,
                    spectrograph = id.spectrograph,
                    pfsDesignId = id.pfsDesignId,
                    fiberId = id.fiberId,
                    fiberStatus = id.fiberStatus,
                    obstime = id.obstime,
                    exptime = id.exptime,
                )
            )

        return targets

    def __load_target_from_pfsConfig(self, target, objId, visit, pfsConfig, i, obsTime, expTime):
        if target.proposalId is None:
            target.proposalId = pfsConfig.proposalId[i]
        elif target.proposalId != pfsConfig.proposalId[i]:
            logger.warning(f'proposalId mismatch for objId {objId}: {target.proposalId} != {pfsConfig.proposalId[i]}')
        
        if target.targetType is None:
            target.targetType = pfsConfig.targetType[i]
        elif target.targetType != pfsConfig.targetType[i]:
            logger.warning(f'targetType mismatch for objId {objId}: {target.targetType} != {pfsConfig.targetType[i]}')

        if target.identity.catId != pfsConfig.catId[i]:
            logger.warning(f'catId mismatch for objId {objId}: {target.identity.catId} != {pfsConfig.catId[i]}')

        target.observations.arm[visit] = pfsConfig.arms             # TODO: Normalize order of arms?
        target.observations.spectrograph[visit] = pfsConfig.spectrograph[i]
        target.observations.pfsDesignId[visit] = pfsConfig.pfsDesignId
        target.observations.fiberId[visit] = pfsConfig.fiberId[i]
        target.observations.fiberStatus[visit] = pfsConfig.fiberStatus[i]
        target.observations.pfiNominal[visit] = pfsConfig.pfiNominal[i]
        target.observations.pfiCenter[visit] = pfsConfig.pfiCenter[i]
        
        # TODO: update this to get exact time, not just the date
        target.observations.obsTime[visit] = obsTime

        # TODO update this once exposure time appears in the pfsConfig file
        target.observations.expTime[visit] = expTime

    def __sort_target_observations_by_visit(self, target):
        def sort_by_visit(observations, values):
            return np.array([ values[v] for v in observations.visit ])

        observations = target.observations

        # Convert the dict to numpy arrays, and sort them by visit
        idx = np.argsort(observations.visit)
        observations.visit = np.array(observations.visit)[idx]

        observations.arm = sort_by_visit(observations, observations.arm)
        observations.spectrograph = sort_by_visit(observations, observations.spectrograph)
        observations.pfsDesignId = sort_by_visit(observations, observations.pfsDesignId)
        observations.fiberId = sort_by_visit(observations, observations.fiberId)
        observations.fiberStatus = sort_by_visit(observations, observations.fiberStatus)
        observations.pfiNominal = sort_by_visit(observations, observations.pfiNominal)
        observations.pfiCenter = sort_by_visit(observations, observations.pfiCenter)
        observations.obsTime = sort_by_visit(observations, observations.obsTime)
        observations.expTime = sort_by_visit(observations, observations.expTime)

    def __update_target_identity(self, target):
        # Update the identity
        target.identity.nVisit = wraparoundNVisit(len(target.observations.visit))
        target.identity.pfsVisitHash = calculatePfsVisitHash(target.observations.visit)
    
    def __generate_config_files(self, targets):
        """
        Generate a config file for each of the inputs.

        While the result of the final processing is a single FITS file, we need
        a separate work directory for each object to store the auxiliary files.
        """

        q = 0
        for objid in sorted(targets.keys()):
            # Generate the config
            config, filename = self.__create_config(targets[objid])

            # Save the config to a file
            if not self.__dry_run:
                logger.info(f'Saving configuration file `{filename}`.')
                os.makedirs(os.path.dirname(filename), exist_ok=True)
                self.__config.save(filename)
            else:
                logger.info(f'Skipped saving configuration file `{filename}`.')

            q += 1
            if self.__top is not None and q >= self.__top:
                logger.info(f'Stopping after {q} objects.')
                break

    def __create_config(self, target, ext='.yaml'):
        """
        Initialze a pipeline configuration object based on the template and the target.
        """

        config = self.__config      # TODO: should we make a deep copy here?

        # Compose the directory and file names for the identity of the object
        # The file should be written somewhere under the work directory
        dir = self.repo.format_dir(GAPipelineConfig, target.identity)
        config_file = self.repo.format_filename(GAPipelineConfig, target.identity)

        # Name of the output pipeline configuration
        filename = os.path.join(dir, config_file)

        # Update config with directory names

        # Input data directories
        config.datadir = self.repo.get_resolved_variable('datadir')
        config.rerundir = self.repo.get_resolved_variable('rerundir')

        logger.debug(f'Configured data directory for object {target.identity}: {config.datadir}')
        logger.debug(f'Configured rerun directory for object {target.identity}: {config.rerundir}')

        # Output
        config.workdir = self.__workdir
        config.outdir = self.__outdir

        logger.debug(f'Configured work directory for object {target.identity}: {config.workdir}')
        logger.debug(f'Configured output directory for object {target.identity}: {config.outdir}')

        # Update the config with the ids

        config.target = target

        return config, filename

def main():
    script = ConfigureScript()
    script.execute()

if __name__ == "__main__":
    main()