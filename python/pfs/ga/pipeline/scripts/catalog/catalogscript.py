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
    Create a PfsGACatalog from the GA pipeline results using the PfsGAObject files and
    other parameters.

    The script works by finding the relevant PfsGAObject files based on the specified
    filters and compiles a catalog from them.
    """

    def __init__(self):
        super().__init__()

        self.__params = None                # Params file with stellar parameters etc.
        self.__params_id = '__target_idx'   # ID column of the params file
        self.__top = None                   # Stop after this many objects

    def _add_args(self):
        self.add_arg('--params', type=str, help='Path to stellar parameters file')
        self.add_arg('--params-id', type=str, help='ID column of the stellar parameters to use')
        self.add_arg('--top', type=int, help='Stop after this many objects')

        super()._add_args()

    def _init_from_args(self, args):
        self.__params = self.get_arg('params', args, self.__params)
        self.__params_id = self.get_arg('params_id', args, self.__params_id)
        self.__top = self.get_arg('top', args, self.__top)

        super()._init_from_args(args)

    def prepare(self):
        super().prepare()

        # Override logging directory to use the same as the pipeline workdir
        self._set_log_file_to_workdir()

    def run(self):
        """
        Find all the pfsGAObject or pfsConfig files that match the filters, look
        up the corresponding PfsGAObject files and compile the final catalog.
        """

        # Load the stellar parameters
        params = self._load_stellar_params_file(self.__params, self.__params_id)

        # Find all config files matching the command-line arguments.
        configs = {}
        filenames, ids = self.input_repo.find_product(PfsConfig)
        for visit, fn in zip(ids.visit, filenames):
            config, id, fn = self.input_repo.load_product(PfsConfig, filename=fn)
            configs[id.visit] = config

        # Find the objects matching the command-line arguments. Arguments
        # are parsed by the repo object itself, so no need to pass them in here.
        identities = self.input_repo.find_objects(configs=configs, groupby='objid')

        if len(identities) == 0:
            logger.error('No objects found matching the filters.')
            return
        else:
            logger.info(f'Found {len(identities)} objects matching the filters.')

        catalog = self.__create_catalog(configs, identities)
        logger.info(f'Created catalog with {len(catalog.catalog)} objects.')

        _, filename, _ = self.input_repo.get_data_path(catalog)
        logger.info(f'Saving catalog to `{filename}`...')
        self.input_repo.save_product(catalog, filename=filename, create_dir=True)

        pass
        

    def __create_catalog(self, configs, identities):

        # A unique catalog ID that all objects must match
        catid = None
    
        # Observations that are used to generate the pfsGAObject files
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

            v_los = [],
            v_losErr = [],
            T_eff = [],
            T_effErr = [],
            M_H = [],
            M_HErr = [],
            log_g = [],
            log_gErr = [],

            flag = [],
            status = [],
        )

        q = 0
        for objid, id in identities.items():
            obj, _, _ = self.__load_pfsGAObject(id)
            if obj is not None:
                self.__validate_pfsGAObject(obj)

                if catid is None:
                    catid = obj.target.identity['catId']
                elif catid != obj.target.identity['catId']:
                    raise ValueError("All catIDs must match in a catalog.")
                
                # Keep track of visits, designs, arms and spectrographs, etc. used for each visit
                for i, v in enumerate(obj.observations.visit):
                    if v not in visit:
                        visit.append(obj.observations.visit[i])
                        pfsDesignId.append(obj.observations.pfsDesignId[i])
                        obstime.append(obj.observations.obsTime[i])
                        exptime.append(obj.observations.expTime[i])

                        arm[v] = set()
                    
                    arm[v].add(obj.observations.arm[i])

                # Find the object in the PfsConfig file to look up certain parameters that
                # are not available in the PfsGAObject file
                config = configs[obj.observations.visit[0]]
                idx = np.where(config.objId == objid)[0].item()

                # Append the table columns
                table.catId.append(obj.target.identity['catId'])
                table.objId.append(obj.target.identity['objId'])

                # TODO: sort out these IDs, might need to load them from a params file
                table.gaiaId.append(-1)
                table.ps1Id.append(-1)
                table.hscId.append(-1)
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

                # Count how many times an arm's been used to process PfsGAObject
                def count_arms(a):
                    return np.sum([a in arm for arm in obj.observations.arm])
                
                table.nVisit_b.append(count_arms('b'))
                table.nVisit_m.append(count_arms('m'))
                table.nVisit_r.append(count_arms('r'))
                table.nVisit_n.append(count_arms('n'))

                table.expTimeEff_b.append(-1)
                table.expTimeEff_m.append(-1)
                table.expTimeEff_r.append(-1)
                table.expTimeEff_n.append(-1)

                # TODO: add measured SNR of coadd

                for i in range(len(obj.stellarParams)):
                    method = obj.stellarParams.method[i]
                    param =  obj.stellarParams.param[i]
                    value = obj.stellarParams.value[i]
                    error = obj.stellarParams.valueErr[i]
                    
                    # Data model allows for different algorithms so use output of the default
                    if method == 'ga1dpipe':
                        if param == 'v_los':
                            table.v_los.append(value)
                            table.v_losErr.append(error)
                        elif param == 'T_eff':
                            table.T_eff.append(value)
                            table.T_effErr.append(error)
                        elif param == 'M_H':
                            table.M_H.append(value)
                            table.M_HErr.append(error)
                        elif param == 'log_g':
                            table.log_g.append(value)
                            table.log_gErr.append(error)

                table.flag.append(False)
                table.status.append('')

            q += 1
            if self.__top is not None and q >= self.__top:
                logger.info(f'Stopping after {q} objects.')
                break
        
        # Format table
        table = { k: np.array(v) for k, v in table.__dict__.items()}
        table = GACatalogTable(**table)

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
        
        catalog = PfsGACatalog(catid, observations, table)

        return catalog

    def __load_pfsGAObject(self, identity):
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
            data = self.input_repo.load_product(PfsGAObject, identity=id)
        except FileNotFoundError:
            logger.error(f'PfsGAObject file for 0x{id.objId:016x} is missing, will be ignored.')
            data = None, None, None

        return data

    def __validate_pfsGAObject(self, pfsGAObject):
        pass

        # TODO: make sure it doesn use any visits that are not listed
        #       by the filter

def main():
    script = CatalogScript()
    script.execute()

if __name__ == "__main__":
    main()