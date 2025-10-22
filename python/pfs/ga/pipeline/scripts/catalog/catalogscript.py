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
        self.__top = None                   # Stop after this many objects

    def _add_args(self):
        self.add_arg('--top', type=int, help='Stop after this many objects')
        self.add_arg('--obs-logs', type=str, nargs='*', help='Observation log files')

        super()._add_args()

    def _init_from_args(self, args):
        self.__top = self.get_arg('top', args, self.__top)
        self.__obs_logs = self.get_arg('obs_logs', args, self.__obs_logs)

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

        catalog = self.__create_catalog(identities, pfs_configs, obs_log)
        logger.info(f'Created catalog with {len(catalog.catalog)} objects.')

        _, filename, _ = self.work_repo.get_data_path(catalog)
        logger.info(f'Saving catalog to `{filename}`...')
        self.work_repo.save_product(catalog, filename=filename, create_dir=True)

    def __create_catalog(self, identities, pfs_configs, obs_log):

        # A unique catalog ID that all objects must match
        catid = None
    
        # Observations that are used to generate the pfsStar files
        visit = []
        arm = {}
        pfsDesignId = []
        obstime = []
        exptime = []

        table = SimpleNamespace(
            catId = [],
            objId = [],
            gaiaId = [],
            ps1Id = [],
            hscId = [],
            sdssId = [],
            miscId = [],
            ra = [],
            dec = [],
            epoch = [],
            pmRa = [],
            pmDec = [],
            parallax = [],
            targetType = [],
            proposalId = [],
            obCode = [],

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

        q = 0
        for objid, id in identities.items():
            obj, _, _ = self.__load_pfsStar(id)
            if obj is not None:
                self.__validate_pfsStar(obj)

                if catid is None:
                    catid = obj.target.identity['catId']
                elif catid != obj.target.identity['catId']:
                    raise ValueError("All catIDs must match in a catalog.")
                
                # Keep track of visits, designs, arms and spectrographs, etc. used for this catalog
                for i, v in enumerate(obj.observations.visit):
                    if v not in visit:
                        visit.append(obj.observations.visit[i])
                        pfsDesignId.append(obj.observations.pfsDesignId[i])
                        obstime.append(obj.observations.obsTime[i])
                        exptime.append(obj.observations.expTime[i])

                        arm[v] = set()
                    
                    arm[v].add(obj.observations.arm[i])

                # Calculate some metrics from the obs_log if available
                eet = { a: 0.0 for a in ['b', 'm', 'r', 'n'] }
                if obs_log is not None:
                    for i, v in enumerate(obj.observations.visit):
                        for a in eet:
                            eet_arm = obs_log.loc[v, f'eet_{a}']
                            if eet_arm is not None and np.isfinite(eet_arm):
                                eet[a] += eet_arm

                # Find the object in the PfsConfig file to look up certain parameters that
                # are not available in the PfsStar file
                config = pfs_configs[obj.observations.visit[0]]
                idx = np.where(config.objId == objid)[0].item()

                # Append the table columns
                table.catId.append(obj.target.identity['catId'])
                table.objId.append(obj.target.identity['objId'])

                # TODO: sort out these IDs, might need to load them from a params file
                table.gaiaId.append(-1)
                table.ps1Id.append(-1)
                table.hscId.append(-1)
                table.sdssId.append(-1)
                table.miscId.append(-1)

                table.ra.append(config.ra[idx])
                table.dec.append(config.dec[idx])
                table.epoch.append(config.epoch[idx])
                table.pmRa.append(config.pmRa[idx])
                table.pmDec.append(config.pmDec[idx])
                table.parallax.append(config.parallax[idx])
                table.targetType.append(config.targetType[idx])
                table.proposalId.append(config.proposalId[idx])
                table.obCode.append(config.obCode[idx])

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
                                         (np.array(obj.stellarParams.method) == 'gapipe'))[0]
                    if param_idx.size == 1:
                        value.append(obj.stellarParams.value[param_idx[0]])
                        error.append(obj.stellarParams.valueErr[param_idx[0]])
                        status.append(obj.stellarParams.status[param_idx[0]])
                    else:
                        value.append(np.nan)
                        error.append(np.nan)
                        status.append('')

                table.flag.append(False)
                table.status.append('')

            q += 1
            if self.__top is not None and q >= self.__top:
                logger.info(f'Stopping after {q} objects.')
                break
        
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

def main():
    script = CatalogScript()
    script.execute()

if __name__ == "__main__":
    main()