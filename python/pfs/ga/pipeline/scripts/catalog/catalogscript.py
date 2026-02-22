#!/usr/bin/env python3

import os
import re
from copy import deepcopy
from glob import glob
from types import SimpleNamespace
from datetime import datetime
import numpy as np
import pandas as pd

from pfs.datamodel import *
from pfs.datamodel.utils import calculatePfsVisitHash, wraparoundNVisit
from pfs.ga.pfsspec.survey.pfs.utils import *

from ..pipelinescript import PipelineScript
from ...gapipe.config import *

from ...setup_logger import logger

class CatalogScript(PipelineScript):
    """
    Create a PfsStarCatalog from the GA pipeline results using the PfsStar files and
    other parameters.

    The script works by finding the relevant PfsStar files based on the specified
    filters and compiles a catalog from them.
    """

    def __init__(self):
        super().__init__()

        self.__obs_logs = None                      # Observation log files
        self.__top = None                           # Stop after this many objects
        self.__assignments = None                   # Fiber assignments file
        self.__include_missing_objects = False      # Include missing objects in the catalog

    def _add_args(self):
        self.add_arg('--top', type=int, help='Stop after this many objects')
        self.add_arg('--obs-logs', type=str, nargs='*', help='Observation log files')
        self.add_arg('--assignments', type=str, nargs='*', help='Fiber assignments file')
        self.add_arg('--include-missing-objects', action='store_true', help='Include missing objects in the catalog')

        super()._add_args()

    def _init_from_args(self, args):
        self.__top = self.get_arg('top', args, self.__top)
        self.__obs_logs = self.get_arg('obs_logs', args, self.__obs_logs)
        self.__assignments = self.get_arg('assignments', args, self.__assignments)
        self.__include_missing_objects = self.get_arg('include_missing_objects', args, False)

        super()._init_from_args(args)

    def prepare(self):
        super().prepare()

        # Override logging directory to use the same as the pipeline workdir
        self._set_log_file_to_workdir()

    def run(self):
        """
        Find all the pfsStar or pfsConfig files that match the filters, look
        up the corresponding PfsStar files and compile the final catalog.
        """

        # Load the observation logs
        if self.__obs_logs is not None:
            obs_log = self._load_obs_log_files(self.__obs_logs)
        else:
            logger.warning('No observation log files specified, skipping loading obslog.')
            obs_log = None

        # Load fiber assignments file
        if self.__assignments is not None:
            assignments = self.__load_assignment_files(self.__assignments)
        else:
            logger.warning('No fiber assignments file specified, skipping loading assignments.')
            assignments = None

        # Update the repo directories based on the config and the command-line arguments
        self._update_repo_directories(self.config)

        # Find the objects matching the command-line arguments. Arguments
        # are parsed by the repo object itself, so no need to pass them in here.
        logger.info('Finding objects matching the filters. This requires loading all PfsConfig files for the given visits and can take a while.')
        pfs_configs = self.input_repo.load_pfsConfigs()

        # Find the objects matching the command-line arguments. Arguments
        # are parsed by the repo object itself, so no need to pass them in here.
        identities = self.input_repo.find_objects(pfs_configs=pfs_configs, groupby='objid')

        if len(identities) == 0:
            logger.error('No objects found matching the filters.')
            return
        else:
            logger.info(f'Found {len(identities)} objects matching the filters.')

        catalog = self.__create_catalog(identities, pfs_configs, obs_log, assignments)
        logger.info(f'Created catalog with {len(catalog.catalog)} objects.')

        _, filename, _ = self.work_repo.get_data_path(catalog)
        logger.info(f'Saving catalog to `{filename}`...')
        self.work_repo.save_product(catalog, filename=filename, create_dir=True)

    def __load_assignment_files(self, assignment_files):
        assignments = None
        for path in assignment_files if isinstance(assignment_files, list) else [assignment_files]:
            for file in glob(path):
                if not os.path.exists(file):
                    logger.error(f'Fiber assignments file `{file}` does not exist.')
                    raise FileNotFoundError(f'Fiber assignments file `{file}` does not exist.')
                else:
                    logger.info(f'Loading assignments file `{file}`.')
        
                    df = pd.read_feather(file)

                    if assignments is None:
                        assignments = df
                    else:
                        assignments = pd.concat([assignments, df])

        # Remove all duplicate rows
        assignments = assignments.drop_duplicates(
            # subset=['__target_idx', 'stage', 'pointing_idx', 'visit_idx'],
            subset=['obcode'],
            keep='last')

        return assignments

    def __create_catalog_table(self):
        table = SimpleNamespace(
            catId = [],
            objId = [],
            ra = [],
            dec = [],
            epoch = [],
            pmRa = [],
            pmDec = [],
            parallax = [],
            targetType = [],
            targetPriority = [],
            proposalId = [],
            obCode = [],

            fiberId = [],
            nVisit = [],
            pfsVisitHash = [],

            nVisit_b = [],
            nVisit_m = [],
            nVisit_r = [],
            nVisit_n = [],

            expTimeEff_b = [],
            expTimeEff_m = [],
            expTimeEff_r = [],
            expTimeEff_n = [],

            snr_b = [],
            snr_m = [],
            snr_r = [],
            snr_n = [],

            v_los = [],
            v_losErr = [],
            v_losStatus = [],
            EBV = [],
            EBVErr = [],
            EBVStatus = [],
            T_eff = [], 
            T_effErr = [],
            T_effStatus = [],
            M_H = [],
            M_HErr = [],
            M_HStatus = [],
            a_M = [],
            a_MErr = [],
            a_MStatus = [],
            C = [],
            CErr = [],
            CStatus = [],
            log_g = [],
            log_gErr = [],
            log_gStatus = [],

            flag = [],
            status = [],
        )

        return table

    def __create_catalog(self, identities, pfs_configs, obs_log, assignments):

        # A unique catalog ID that all objects must match
        catid = None
    
        # Observations that are used to generate the pfsStar files
        visit = []
        arm = {}
        pfsDesignId = []
        obstime = []
        exptime = []

        # Final catalog table
        table = self.__create_catalog_table()

        q = 0
        for objid, id in identities.items():
            q += 1
            if self.__top is not None and q >= self.__top:
                logger.info(f'Stopping after {q} objects.')
                break

            obj, _, _ = self.__load_pfsStar(id)
            
            if obj is not None:
                self.__validate_pfsStar(obj)

                if catid is None:
                    catid = obj.target.identity['catId']
                elif catid != obj.target.identity['catId']:
                    raise ValueError("All catIDs must match in a catalog.")

                self.__append_pfsStar(
                    table, objid, obj, visit, arm, pfsDesignId, obstime, exptime,
                    pfs_configs, assignments, obs_log)
            elif self.__include_missing_objects:
                self.__append_missing_object(
                    table, objid, visit, arm, pfsDesignId, obstime, exptime,
                    pfs_configs, assignments, obs_log)
        
        # Format table
        table = { k: np.array(v) for k, v in table.__dict__.items()}
        table = StarCatalogTable(**table)

        # Sort observations and append to the catalog
        observations = Observations(
            visit = np.array(visit),
            pfsDesignId = np.array(pfsDesignId),
            arm = np.array([sort_arms(''.join(arm[v])) for v in visit]),
            spectrograph = np.full(len(visit), -1),
            fiberId = np.full(len(visit), -1),
            pfiNominal = np.full((len(visit), 2), np.nan),
            pfiCenter = np.full((len(visit), 2), np.nan),
            obsTime = np.array(obstime),
            expTime = np.array(exptime))
        
        catalog = PfsStarCatalog(catid, observations, table)

        return catalog

    def __load_pfsStar(self, identity):
        # Convert exposure identities into a single composite identity
        id = SimpleNamespace(
            catId = identity.catId[0],
            tract = identity.tract[0],
            patch = identity.patch[0],
            objId = identity.objId[0],

            # Skip these because list of visits might be different if we
            # throw away bad exposures during processing
            # nVisit = wraparoundNVisit(len(identity.visit)),
            # pfsVisitHash = calculatePfsVisitHash(identity.visit)
        )

        try:
            data, id, filename = self.work_repo.load_product(
                PfsStar,
                identity=id,
                variables = { 'datadir': self.config.outdir })
        except FileNotFoundError:
            logger.error(f'PfsStar file for 0x{id.objId:016x} is missing, will be ignored.')
            data, id, filename = None, None, None

        return data, id, filename

    def __validate_pfsStar(self, pfsStar):
        pass

        # TODO: make sure it doesn use any visits that are not listed
        #       by the filter

    def __find_matching_assignment(self, assignments, obcode):
        # Match with the assignments by obCode
        if assignments is not None:
            assignments_idx = assignments['obcode'] == obcode
            if np.sum(assignments_idx) == 0:
                logger.warning(f'No matching assignment found for obCode {obcode}.')
            elif np.sum(assignments_idx) > 1:
                logger.warning(f'Multiple matching assignments found for obCode {obcode}, taking the one with the highest stage.')
        else:
            assignments_idx = None

        return assignments_idx

    def __append_pfsStar(self, table, objid, obj, visit, arm, pfsDesignId, obstime, exptime,
                         pfs_configs, assignments, obs_log):

        # Find the object in the PfsConfig file to look up certain parameters that
        # are not available in the PfsStar file
        config = pfs_configs[obj.observations.visit[0]]
        config_idx = np.where(config.objId == objid)[0].item()

        assignments_idx = self.__find_matching_assignment(assignments, config.obCode[config_idx])

        # Keep track of visits, designs, arms and spectrographs, etc. used for this catalog
        for i, v in enumerate(obj.observations.visit):
            if v not in visit:
                visit.append(obj.observations.visit[i])
                pfsDesignId.append(obj.observations.pfsDesignId[i])
                obstime.append(obj.observations.obsTime[i])
                exptime.append(obj.observations.expTime[i])

                arm[v] = set()
            
            arm[v].add(obj.observations.arm[i])

        # Collect the fiberId from each visit. Only set these IDs
        # if they are the same across all visits, otherwise set to -1.
        fiberid = np.array(obj.observations.fiberId)                
        if len(np.unique(fiberid)) == 1:
            fiberid = fiberid[0]
        else:
            fiberid = -1

        # Calculate some metrics from the obs_log if available
        eet = { a: 0.0 for a in ['b', 'm', 'r', 'n'] }
        if obs_log is not None:
            for i, v in enumerate(obj.observations.visit):
                if v not in obs_log.index:
                    logger.error(f"Visit number {v} is not available in the observation log.")

                for a in eet:
                    eet_arm = obs_log.loc[v, f'eet_{a}']
                    if eet_arm is not None and np.isfinite(eet_arm):
                        eet[a] += eet_arm

        # Get the full identity
        id = obj.getIdentity()

        # Append the table columns
        table.catId.append(obj.target.identity['catId'])
        table.objId.append(obj.target.identity['objId'])

        table.ra.append(config.ra[config_idx])
        table.dec.append(config.dec[config_idx])
        table.epoch.append(config.epoch[config_idx])
        table.pmRa.append(config.pmRa[config_idx])
        table.pmDec.append(config.pmDec[config_idx])
        table.parallax.append(config.parallax[config_idx])
        table.targetType.append(config.targetType[config_idx])
        
        if assignments is not None and np.sum(assignments_idx) == 1:
            table.targetPriority.append(assignments.loc[assignments_idx, 'priority'].item())
        else:
            table.targetPriority.append(-1)

        table.proposalId.append(config.proposalId[config_idx])
        table.obCode.append(config.obCode[config_idx])

        table.fiberId.append(fiberid)

        # These are used to generate the file name for PfsStar, include these in the catalog
        # to allow finding the corresponding files more easily
        table.nVisit.append(id['nVisit'])
        table.pfsVisitHash.append(id['pfsVisitHash'])

        # Count how many times an arm's been used to process PfsStar
        def count_arms(a):
            return np.sum([a in arm for arm in obj.observations.arm])
        
        for a, nVisit in zip(
            ['b', 'm', 'r', 'n'],
            [table.nVisit_b, table.nVisit_m, table.nVisit_r, table.nVisit_n]
        ):
            nVisit.append(count_arms(a))

        for a, expTimeEff in zip(
            ['b', 'm', 'r', 'n'],
            [table.expTimeEff_b, table.expTimeEff_m, table.expTimeEff_r, table.expTimeEff_n]
        ):
            expTimeEff.append(eet[a])

        for a, snr_arm in zip(
            ['b', 'm', 'r', 'n'],
            [table.snr_b, table.snr_m, table.snr_r, table.snr_n]
        ):
            snr_idx = np.where(np.array(obj.stellarParams.param) == f'snr_{a}')[0]
            if snr_idx.size == 1:
                snr_arm.append(obj.stellarParams.value[snr_idx[0]])
            else:
                snr_arm.append(np.nan)

        for param, value, error, status in zip(
            ['v_los', 'ebv', 'T_eff', 'M_H', 'a_M', 'C', 'log_g'],
            [table.v_los, table.EBV, table.T_eff, table.M_H, table.a_M, table.C, table.log_g],
            [table.v_losErr, table.EBVErr, table.T_effErr, table.M_HErr, table.a_MErr, table.CErr, table.log_gErr],
            [table.v_losStatus, table.EBVStatus, table.T_effStatus, table.M_HStatus, table.a_MStatus, table.CStatus, table.log_gStatus],
        ):
            param_idx = np.where((np.array(obj.stellarParams.param) == param) &
                                    (np.array(obj.stellarParams.method) == 'tempfit'))[0]
            if param_idx.size == 1:
                value.append(obj.stellarParams.value[param_idx[0]])
                error.append(obj.stellarParams.valueErr[param_idx[0]])
                status.append(obj.stellarParams.status[param_idx[0]])
            else:
                value.append(np.nan)
                error.append(np.nan)
                status.append('')

        # Status value of tempfit
        # TODO: add different flags for tempfit, chemfit and coadd
        flags_index = np.where(obj.measurementFlags.method == 'tempfit')[0]
        if flags_index.size == 1:
            table.flag.append(obj.measurementFlags.flag[flags_index[0]])
            table.status.append(obj.measurementFlags.status[flags_index[0]])
        else:
            table.flag.append(False)
            table.status.append('')

    def __append_missing_object(self, table, objid, visit, arm, pfsDesignId, obstime, exptime,
                                pfs_configs, assignments, obs_log):
        # Collect missing object info from the PfsConfig files and obs_log if available,
        # and append to the catalog with NaN values for the parameters that would have been
        # derived from the PfsStar file.

        # Collect the fiberId from each visit. Only set these IDs
        # if they are the same across all visits, otherwise set to -1.
        fiberid = []
        last_visit = None
        for visit, config in pfs_configs.items():
            config_idx = np.where(config.objId == objid)[0]
            if len(config_idx) == 0:
                # objId not in the config file
                continue
            elif len(config_idx) == 1:
                # objId found in the config file, append the fiberId
                fiberid.append(config.fiberId[config_idx].item())
                last_visit = visit
            else:
                raise NotImplementedError()

        if last_visit is None:
            raise ValueError(f'Object with objId {objid} not found in any PfsConfig file, cannot append missing object.')
            
        if len(np.unique(np.array(fiberid))) == 1:
            fiberid = fiberid[0]
        else:
            fiberid = -1

        # Find the object in the PfsConfig file to look up certain parameters that
        # are not available in the PfsStar file
        config = pfs_configs[last_visit]
        config_idx = np.where(config.objId == objid)[0].item()

        assignments_idx = self.__find_matching_assignment(assignments, config.obCode[config_idx])

        # Append the table columns
        table.catId.append(config.catId[config_idx])
        table.objId.append(config.objId[config_idx])

        table.ra.append(config.ra[config_idx])
        table.dec.append(config.dec[config_idx])
        table.epoch.append(config.epoch[config_idx])
        table.pmRa.append(config.pmRa[config_idx])
        table.pmDec.append(config.pmDec[config_idx])
        table.parallax.append(config.parallax[config_idx])
        table.targetType.append(config.targetType[config_idx])

        if assignments is not None and np.sum(assignments_idx) == 1:
            table.targetPriority.append(assignments.loc[assignments_idx, 'priority'].item())
        else:
            table.targetPriority.append(-1)

        table.proposalId.append(config.proposalId[config_idx])
        table.obCode.append(config.obCode[config_idx])

        table.fiberId.append(fiberid)
        table.nVisit.append(-1)
        table.pfsVisitHash.append(-1)

        # Set the arm, nVisit and expTimeEff to 0 since we don't have a PfsStar file for this object
        for a, nVisit in zip(
            ['b', 'm', 'r', 'n'],
            [table.nVisit_b, table.nVisit_m, table.nVisit_r, table.nVisit_n]
        ):
            nVisit.append(0)

        for a, expTimeEff in zip(
            ['b', 'm', 'r', 'n'],
            [table.expTimeEff_b, table.expTimeEff_m, table.expTimeEff_r, table.expTimeEff_n]
        ):
            expTimeEff.append(0.0)

        for a, snr_arm in zip(
            ['b', 'm', 'r', 'n'],
            [table.snr_b, table.snr_m, table.snr_r, table.snr_n]
        ):
            snr_arm.append(np.nan)

        for param, value, error, status in zip(
            ['v_los', 'ebv', 'T_eff', 'M_H', 'a_M', 'C', 'log_g'],
            [table.v_los, table.EBV, table.T_eff, table.M_H, table.a_M, table.C, table.log_g],
            [table.v_losErr, table.EBVErr, table.T_effErr, table.M_HErr, table.a_MErr, table.CErr, table.log_gErr],
            [table.v_losStatus, table.EBVStatus, table.T_effStatus, table.M_HStatus, table.a_MStatus, table.CStatus, table.log_gStatus],
        ):
            value.append(np.nan)
            error.append(np.nan)
            status.append('')

        # Status value of tempfit
        table.flag.append(True)
        table.status.append(TempFitFlag.NODATA.name)

def main():
    script = CatalogScript()
    script.execute()

if __name__ == "__main__":
    main()