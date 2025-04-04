#!/usr/bin/env python3

import os
import re
from glob import glob
from types import SimpleNamespace
from datetime import datetime
import numpy as np

from pfs.datamodel import *
from pfs.datamodel.utils import calculatePfsVisitHash, wraparoundNVisit
from pfs.ga.pfsspec.survey.repo import FileSystemRepo

from ..common import Script, PipelineError
from ..gapipe.config import *
from ..repo import PfsFileSystemConfig

from ..setup_logger import logger

class Configure(Script):
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

        self.__repo = self.__create_data_repo()

    def _add_args(self):
        self.add_arg('--config', type=str, nargs='*', required=True, help='Configuration file')

        self.add_arg('--outdir', type=str, help='Output directory')

        self.add_arg('--dry-run', action='store_true', help='Dry run mode')
        self.add_arg('--top', type=int, help='Stop after this many objects')

        # Register the identity param filters
        self.__repo.add_args(self)

        super()._add_args()

    def _init_from_args(self, args):
        # Parse the identity param filters
        self.__repo.init_from_args(self)

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
        if self.is_arg('rerundir', args):
            self.__config.rerundir = self.get_arg('rerundir', args)

        # Override data store connector with configuration values
        # Also save workdir and outdir because these might be overwritten
        # in the configuration template
        if self.__config.workdir is not None:
            self.__repo.set_variable('workdir', self.__config.workdir)
            self.__workdir = self.__config.workdir
        if self.__config.outdir is not None:
            self.__repo.set_variable('outdir', self.__config.outdir)
            self.__outdir = self.__config.outdir
        if self.__config.datadir is not None:
            self.__repo.set_variable('datadir', self.__config.datadir)
        if self.__config.rerundir is not None:
            self.__repo.set_variable('rerundir', self.__config.rerundir)

        self.__dry_run = self.get_arg('dry_run', args, self.__dry_run)
        self.__top = self.get_arg('top', args, self.__top)

        super()._init_from_args(args)

    def __create_data_repo(self):
        """
        Create a repo connector to the file system.
        """

        return FileSystemRepo(config=PfsFileSystemConfig)

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

        # Find all the pfsSingle or pfsConfig files that match the filters
        # TODO: add option to use either pfsSingle or pfsConfig files
        # targets, _ = self.__find_targets_pfsSingle()
        targets = self.__find_targets_pfsConfig()

        if len(targets) == 0:
            logger.warning('No pfsSingle or Config files, nor object within them found matching the filters.')
            return
        
        # Generate the configuration file for each target
        self.__generate_config_files(targets)

    def __find_targets_pfsConfig(self):
        """
        Find all the pfsConfig files that match the filters and convert the object
        lists into a dictionary of targets.
        """

        # TODO: This function now finds observations by looking of PfsSIngle files.
        #       Use some database instead.

        logger.info(f'Finding pfsConfig files matching the following filters:')
        logger.info(f'    visit: {repr(self.__repo.filters.visit)}')

        filenames, identities = self.__repo.find_product(PfsConfig)

        logger.info(f'Found {len(filenames)} pfsConfig files matching the filters.')

        # Create a dict keyed by objId and load the pfsConfig files of each visit to get the fiberId etc.
        targets = {}
        for i, filename in enumerate(filenames):
            pfsConfig, config_identity, _ = self.__repo.load_product(PfsConfig, filename=filename)
            obsTime = datetime.combine(config_identity.date, datetime.min.time())
            expTime = np.nan

            for j, objId in enumerate(pfsConfig.objId):
                if objId != -1 \
                    and self.__repo.filters.objId.match(objId) \
                    and self.__repo.filters.catId.match(pfsConfig.catId[j]) \
                    and self.__repo.filters.tract.match(pfsConfig.tract[j]) \
                    and self.__repo.filters.patch.match(pfsConfig.patch[j]) \
                    and self.__repo.filters.spectrograph.match(pfsConfig.spectrograph[j]) \
                    and self.__repo.filters.arm.match(pfsConfig.arms):

                    if objId not in targets:
                        targets[objId] = GATargetConfig(
                            identity = GAObjectIdentityConfig(
                                catId = pfsConfig.catId[j],
                                tract = pfsConfig.tract[j],
                                patch = pfsConfig.patch[j],
                                objId = objId),
                            observations = GAObjectObservationsConfig(
                                visit = [],
                                arm = {},
                                spectrograph = {},
                                pfsDesignId = {},
                                fiberId = {},
                                fiberStatus = {},
                                pfiNominal = {},
                                pfiCenter = {},
                                obsTime = {},
                                expTime = {},
                            ))
                    
                    target = targets[objId]
                    target.observations.visit.append(identities.visit[i])

                    self.__load_target_from_pfsConfig(target, objId, 
                                                      pfsConfig.visit, pfsConfig, j,
                                                      obsTime, expTime)

        if len(targets) == 0:
            return targets, filenames

        # Report some statistics in the log
        unique_visits = np.unique(np.concatenate([ target.observations.visit for _, target in targets.items() ]))
        logger.info(f'Targets span {len(unique_visits)} unique visits.')

        # Update targets: sort observations and calculate nVisit and pfsVisitHash
        for _, target in targets.items():
            self.__sort_target_observations_by_visit(target)
            self.__update_target_identity(target)

        return targets

    def __find_targets_pfsSingle(self):
        """
        Find all the pfsSingle files that match the filters and convert the object
        lists into a dictionary of targets.
        """

        # TODO: This function now finds observations by looking of PfsSIngle files.
        #       Use some database instead.

        logger.info(f'Finding pfsSingle files matching the following filters:')
        logger.info(f'    catId: {repr(self.__repo.filters.catId)}')
        logger.info(f'    tract: {repr(self.__repo.filters.tract)}')
        logger.info(f'    patch: {repr(self.__repo.filters.patch)}')
        logger.info(f'    objId: {repr(self.__repo.filters.objId)}')
        logger.info(f'    visit: {repr(self.__repo.filters.visit)}')

        filenames, identities = self.__repo.find_product(PfsSingle)

        logger.info(f'Found {len(filenames)} pfsSingle files matching the filters.')

        # Create a dict keyed by objId
        targets = {}
        for i, filename in enumerate(filenames):
            objId = identities.objId[i]

            if objId not in targets:
                targets[objId] = GATargetConfig(
                    identity = GAObjectIdentityConfig(
                        catId = identities.catId[i],
                        tract = identities.tract[i],
                        patch = identities.patch[i],
                        objId = objId),
                    observations = GAObjectObservationsConfig(
                        visit = [],
                        arm = {},
                        spectrograph = {},
                        pfsDesignId = {},
                        fiberId = {},
                        fiberStatus = {},
                        pfiNominal = {},
                        pfiCenter = {},
                        obsTime = {},
                        expTime = {},
                    ))
                
            targets[objId].observations.visit.append(identities.visit[i])

        if len(targets) == 0:
            return targets, filenames
                
        # Report some statistics in the log
        unique_visits = np.unique(np.concatenate([ target.observations.visit for _, target in targets.items() ]))
        logger.info(f'Targets span {len(unique_visits)} unique visits.')

        # Load the pfsConfig files of each visit to get the fiberId etc.
        for visit in unique_visits:
            logger.info(f'Finding pfsConfig file matching the following filters:')
            logger.info(f'    visit: {visit}')

            try:
                filename, identity = self.__repo.locate_product(PfsConfig, visit=visit)
            except FileNotFoundError:
                raise PipelineError(f'No pfsConfig file found for visit {visit}.')

            pfsConfig, config_identity, _ = self.__repo.load_product(PfsConfig, filename=filename)
            obsTime = datetime.combine(config_identity.date, datetime.min.time())
            expTime = np.nan

            for i, objId in enumerate(pfsConfig.objId):
                if objId in targets:
                    target = targets[objId]
                    self.__load_target_from_pfsConfig(target, objId, visit, pfsConfig, i, obsTime, expTime)

        # Update targets: sort observations and calculate nVisit and pfsVisitHash
        for _, target in targets.items():
            self.__sort_target_observations_by_visit(target)
            self.__update_target_identity(target)

        return targets, filenames
    
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
        for objId in sorted(targets.keys()):
            # Generate the config
            config, filename = self.__create_config(targets[objId])

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
        dir = self.__repo.format_dir(GAPipelineConfig, target.identity)
        config_file = self.__repo.format_filename(GAPipelineConfig, target.identity)

        # Name of the output pipeline configuration
        filename = os.path.join(dir, config_file)

        # Update config with directory names

        # Input data directories
        config.datadir = self.__repo.get_resolved_variable('datadir')
        config.rerundir = self.__repo.get_resolved_variable('rerundir')

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
    script = Configure()
    script.execute()

if __name__ == "__main__":
    main()