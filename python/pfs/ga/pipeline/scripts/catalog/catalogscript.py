#!/usr/bin/env python3

import os
import re
from copy import deepcopy
from glob import glob
from types import SimpleNamespace
from datetime import datetime
import numpy as np
import pandas as pd
import astropy.io.fits

from pfs.datamodel import *
from pfs.datamodel.utils import calculatePfsVisitHash, wraparoundNVisit
from pfs.ga.pfsspec.survey.pfs.utils import *
from pfs.ga.common.scripts import Progress

from ..pipelinescript import PipelineScript
from ...gapipe.config import *

from ...setup_logger import logger

class CatalogScript(PipelineScript, Progress):
    """
    Create a PfsStarCatalog from the GA pipeline results using the PfsStar files and
    other parameters.

    The script works by finding the relevant PfsStar files based on the specified
    filters and compiles a catalog from them.
    """

    def __init__(self):
        super().__init__()

        self.__obs_logs = None                      # Observation log files
        self.__target_lists = None                  # Target list files to look up the target properties from
        self.__assignments = None                   # Fiber assignments file
        self.__include_missing_objects = False      # Include missing objects in the catalog

    def _add_args(self):
        self.add_arg('--config', type=str, nargs='*', required=True, help='Configuration file')
        self.add_arg('--obs-logs', type=str, nargs='*', help='Observation log files')
        self.add_arg('--target-lists', type=str, nargs='*', help='Target list files to look up the target properties from')
        self.add_arg('--assignments', type=str, nargs='*', help='Fiber assignments file')
        self.add_arg('--include-missing-objects', action='store_true', help='Include missing objects in the catalog')

        super()._add_args()

    def _init_from_args(self, args):
        self.__obs_logs = self.get_arg('obs_logs', args, self.__obs_logs)
        self.__target_lists = self.get_arg('target_lists', args, self.__target_lists)
        self.__assignments = self.get_arg('assignments', args, self.__assignments)
        self.__include_missing_objects = self.get_arg('include_missing_objects', args, False)

        PipelineScript._init_from_args(self, args)
        Progress._init_from_args(self, args)

    def prepare(self):
        super().prepare()

        # Override logging directory to use the same as the pipeline workdir
        self._set_log_file_to_workdir()

    def run(self):
        """
        Find all the pfsStar or pfsConfig files that match the filters, look
        up the corresponding PfsStar files and compile the final catalog.
        """

        files = ' '.join(self.config.config_files)
        logger.info(f'Using configuration template file(s) {files}.')

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

        # Load target list files
        if self.__target_lists is not None:
            target_lists = self._load_target_list_files(self.__target_lists)
        else:
            logger.warning('No target list files specified, skipping loading target lists.')
            target_lists = None

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

        catalog = self.__create_catalog(identities, pfs_configs, obs_log, target_lists, assignments)
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

    def __init_catalog_table(self):
        catalog_table = SimpleNamespace(
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
            spectrograph = [],
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

            tempfitflag = [],
            tempfitstatus = [],
        )

        return catalog_table

    def __init_photometry_table(self):
        photometry_table = SimpleNamespace(
            catId = [],
            objId = [],
            filterName = [],
            flux = [],
            fluxErr = [],
            mag = [],
            magErr = [],
        )

        return photometry_table

    def __create_catalog(self, identities, pfs_configs, obs_log, target_lists, assignments):
        catalog_table, photometry_table, catid, visit, arm, pfsDesignId, obstime, exptime = \
            self.__create_catalog_table(identities, pfs_configs, obs_log, target_lists, assignments)

        # Format the tables into a dict of arrays
        catalog_table = { k: np.array(v) for k, v in catalog_table.__dict__.items()}
        photometry_table = { k: np.array(v) for k, v in photometry_table.__dict__.items()}
        
        # Sanitize a few columns that might have invalid values for FITS format
        catalog_table['targetPriority'][pd.isna(catalog_table['targetPriority'])] = -1
        catalog_table['targetPriority'] = np.astype(catalog_table['targetPriority'], np.int32)
        
        # Create the catalog table
        catalog_table = StarCatalogTable(**catalog_table)
        photometry_table = StarPhotometryTable(**photometry_table)

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
        
        catalog = PfsStarCatalog(catid, observations, catalog_table, photometry_table)

        self.__verify_catalog(catalog)

        return catalog

    def __create_catalog_table(self, identities, pfs_configs, obs_log, target_lists, assignments):
        # A unique catalog ID that all objects must match
        catid = None
    
        # Observations that are used to generate the pfsStar files
        visit = []
        arm = {}
        pfsDesignId = []
        obstime = []
        exptime = []

        # Initialize the empty catalog table and photometry table
        catalog_table = self.__init_catalog_table()
        photometry_table = self.__init_photometry_table()

        q = 0
        for objid, id in self._wrap_in_progressbar(identities.items(), total=len(identities), logger=logger):
            q += 1
            if self.top is not None and q >= self.top:
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
                    catalog_table, photometry_table,
                    objid, obj, visit, arm, pfsDesignId, obstime, exptime,
                    pfs_configs, target_lists, assignments, obs_log)
            elif self.__include_missing_objects:
                self.__append_missing_object(
                    catalog_table, photometry_table,
                    objid, visit, arm, pfsDesignId, obstime, exptime,
                    pfs_configs, target_lists, assignments, obs_log)
        
        return catalog_table, photometry_table, catid, visit, arm, pfsDesignId, obstime, exptime

    def __verify_catalog(self, catalog):
        """
        Verify that the catalog can be saved as a FITS file by checking that the data types of the columns
        are compatible with the FITS format.

        Code copied from PfsTable.writeHdu
        """

        format = {
            int: "K",
            float: "D",
            np.int32: "J",
            np.float32: "E",
            bool: "L",
            np.uint8: "B",
            np.int16: "I",
            np.int64: "K",
            np.float64: "D",
        }

        def getFormat(name: str, dtype: type) -> str:
            """Determine suitable FITS column format

            This is a simple mapping except for string types.

            Parameters
            ----------
            name : `str`
                Column name, so we can get the data if we need to inspect it.
            dtype : `type`
                Data type.

            Returns
            -------
            format : `str`
                FITS column format string.
            """
            if issubclass(dtype, str):
                array = getattr(catalog.catalog, name)
                unique = np.unique(array)
                if unique.size == 1:
                    size = len(unique[0])
                    if size == 0:
                        return "PA()"
                    return f"{size}A"
                return f"PA()"
            return format[dtype]

                # Verify the data types
        for col in catalog.catalog.schema:
            try:
                astropy.io.fits.Column(
                    name=col.name,
                    format=getFormat(col.name, col.dtype),
                    array=getattr(catalog.catalog, col.name),
                )
            except Exception as ex:
                raise Exception(f"Column {col.name} with dtype {col.dtype} is not compatible with FITS format: {ex}")

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

    def __find_matching_assignment(self, assignments, obcode, objid):
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

    def __get_unique_fiber_id(self, obj):
        # Collect the fiberId from each visit. Only set these IDs
        # if they are the same across all visits, otherwise set to -1.
        fiberid = np.array(obj.observations.fiberId)
        if len(np.unique(fiberid)) == 1:
            return fiberid[0]
        else:
            return -1

    def __get_unique_spectrograph(self, obj):
        spectrograph = np.array(obj.observations.spectrograph)
        if len(np.unique(spectrograph)) == 1:
            return spectrograph[0]
        else:
            return -1

    def __append_pfsStar(self, catalog_table, photometry_table, objid, obj, visit, arm, pfsDesignId, obstime, exptime,
                         pfs_configs, target_lists, assignments, obs_log):

        # Find the object in the PfsConfig file
        config = pfs_configs[obj.observations.visit[0]]
        config_idx = np.where(config.objId == objid)[0].item()

        # Look up the object in the target lists and assignments files to get the target properties
        obcode = config.obCode[config_idx]
        objid = config.objId[config_idx]
        catid = config.catId[config_idx]
        
        self.__append_magnitudes_from_target_lists(photometry_table, obj, target_lists, obcode, catid, objid)
        self.__append_parameters_from_assignments(catalog_table, obj, assignments, obcode, objid)

        # Keep track of visits, designs, arms and spectrographs, etc. used for this catalog
        for i, v in enumerate(obj.observations.visit):
            if v not in visit:
                visit.append(obj.observations.visit[i])
                pfsDesignId.append(obj.observations.pfsDesignId[i])
                obstime.append(obj.observations.obsTime[i])
                exptime.append(obj.observations.expTime[i])

                arm[v] = set()
            
            arm[v].add(obj.observations.arm[i])

        fiberid = self.__get_unique_fiber_id(obj)
        spectrograph = self.__get_unique_spectrograph(obj)

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
        catalog_table.catId.append(obj.target.identity['catId'])
        catalog_table.objId.append(obj.target.identity['objId'])

        catalog_table.ra.append(config.ra[config_idx])
        catalog_table.dec.append(config.dec[config_idx])
        catalog_table.epoch.append(config.epoch[config_idx])
        catalog_table.pmRa.append(config.pmRa[config_idx])
        catalog_table.pmDec.append(config.pmDec[config_idx])
        catalog_table.parallax.append(config.parallax[config_idx])
        catalog_table.targetType.append(config.targetType[config_idx])        
        catalog_table.proposalId.append(config.proposalId[config_idx])
        catalog_table.obCode.append(config.obCode[config_idx])

        catalog_table.fiberId.append(fiberid)
        catalog_table.spectrograph.append(spectrograph)

        # These are used to generate the file name for PfsStar, include these in the catalog
        # to allow finding the corresponding files more easily
        catalog_table.nVisit.append(id['nVisit'])
        catalog_table.pfsVisitHash.append(id['pfsVisitHash'])

        # Count how many times an arm's been used to process PfsStar
        def count_arms(a):
            return np.sum([a in arm for arm in obj.observations.arm])
        
        for a, nVisit in zip(
            ['b', 'm', 'r', 'n'],
            [catalog_table.nVisit_b, catalog_table.nVisit_m, catalog_table.nVisit_r, catalog_table.nVisit_n]
        ):
            nVisit.append(count_arms(a))

        for a, expTimeEff in zip(
            ['b', 'm', 'r', 'n'],
            [catalog_table.expTimeEff_b, catalog_table.expTimeEff_m, catalog_table.expTimeEff_r, catalog_table.expTimeEff_n]
        ):
            expTimeEff.append(eet[a])

        for a, snr_arm in zip(
            ['b', 'm', 'r', 'n'],
            [catalog_table.snr_b, catalog_table.snr_m, catalog_table.snr_r, catalog_table.snr_n]
        ):
            snr_idx = np.where(np.array(obj.stellarParams.param) == f'snr_{a}')[0]
            if snr_idx.size == 1:
                snr_arm.append(obj.stellarParams.value[snr_idx[0]])
            else:
                snr_arm.append(np.nan)

        for param, value, error, status in zip(
            ['v_los', 'ebv', 'T_eff', 'M_H', 'a_M', 'C', 'log_g'],
            [catalog_table.v_los, catalog_table.EBV, catalog_table.T_eff, catalog_table.M_H, catalog_table.a_M, catalog_table.C, catalog_table.log_g],
            [catalog_table.v_losErr, catalog_table.EBVErr, catalog_table.T_effErr, catalog_table.M_HErr, catalog_table.a_MErr, catalog_table.CErr, catalog_table.log_gErr],
            [catalog_table.v_losStatus, catalog_table.EBVStatus, catalog_table.T_effStatus, catalog_table.M_HStatus, catalog_table.a_MStatus, catalog_table.CStatus, catalog_table.log_gStatus],
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
            catalog_table.tempfitflag.append(obj.measurementFlags.flag[flags_index[0]])
            catalog_table.tempfitstatus.append(obj.measurementFlags.status[flags_index[0]])
        else:
            catalog_table.tempfitflag.append(False)
            catalog_table.tempfitstatus.append('')

    def __append_missing_object(self, catalog_table, photometry_table, objid, visit, arm, pfsDesignId, obstime, exptime,
                                pfs_configs, target_lists, assignments, obs_log):
        # Collect missing object info from the PfsConfig files and obs_log if available,
        # and append to the catalog with NaN values for the parameters that would have been
        # derived from the PfsStar file.

        # Collect the fiberId from each visit. Only set these IDs
        # if they are the same across all visits, otherwise set to -1.
        fiberid = []
        spectrograph = []
        last_visit = None
        for visit, config in pfs_configs.items():
            config_idx = np.where(config.objId == objid)[0]
            if len(config_idx) == 0:
                # objId not in the config file
                continue
            elif len(config_idx) == 1:
                # objId found in the config file, append the fiberId
                fiberid.append(config.fiberId[config_idx].item())
                spectrograph.append(config.spectrograph[config_idx].item())
                last_visit = visit
            else:
                raise NotImplementedError()

        if last_visit is None:
            raise ValueError(f'Object with objId {objid} not found in any PfsConfig file, cannot append missing object.')
            
        if len(np.unique(np.array(fiberid))) == 1:
            fiberid = fiberid[0]
        else:
            fiberid = -1

        if len(np.unique(np.array(spectrograph))) == 1:
            spectrograph = spectrograph[0]
        else:
            spectrograph = -1

        # Find the object in the PfsConfig file to look up certain parameters that
        # are not available in the PfsStar file
        config = pfs_configs[last_visit]
        config_idx = np.where(config.objId == objid)[0].item()

        obcode = config.obCode[config_idx]
        objid = config.objId[config_idx]
        catid = config.catId[config_idx]
        
        self.__append_magnitudes_from_target_lists(photometry_table, None, target_lists, obcode, catid, objid)
        self.__append_parameters_from_assignments(catalog_table, None, assignments, obcode, objid)

        # Append the table columns
        catalog_table.catId.append(config.catId[config_idx])
        catalog_table.objId.append(config.objId[config_idx])

        catalog_table.ra.append(config.ra[config_idx])
        catalog_table.dec.append(config.dec[config_idx])
        catalog_table.epoch.append(config.epoch[config_idx])
        catalog_table.pmRa.append(config.pmRa[config_idx])
        catalog_table.pmDec.append(config.pmDec[config_idx])
        catalog_table.parallax.append(config.parallax[config_idx])
        catalog_table.targetType.append(config.targetType[config_idx])
        catalog_table.proposalId.append(config.proposalId[config_idx])
        catalog_table.obCode.append(config.obCode[config_idx])

        catalog_table.fiberId.append(fiberid)
        catalog_table.spectrograph.append(spectrograph)
        catalog_table.nVisit.append(-1)
        catalog_table.pfsVisitHash.append(-1)

        # Set the arm, nVisit and expTimeEff to 0 since we don't have a PfsStar file for this object
        for a, nVisit in zip(
            ['b', 'm', 'r', 'n'],
            [catalog_table.nVisit_b, catalog_table.nVisit_m, catalog_table.nVisit_r, catalog_table.nVisit_n]
        ):
            nVisit.append(0)

        for a, expTimeEff in zip(
            ['b', 'm', 'r', 'n'],
            [catalog_table.expTimeEff_b, catalog_table.expTimeEff_m, catalog_table.expTimeEff_r, catalog_table.expTimeEff_n]
        ):
            expTimeEff.append(0.0)

        for a, snr_arm in zip(
            ['b', 'm', 'r', 'n'],
            [catalog_table.snr_b, catalog_table.snr_m, catalog_table.snr_r, catalog_table.snr_n]
        ):
            snr_arm.append(np.nan)

        for param, value, error, status in zip(
            ['v_los', 'ebv', 'T_eff', 'M_H', 'a_M', 'C', 'log_g'],
            [catalog_table.v_los, catalog_table.EBV, catalog_table.T_eff, catalog_table.M_H, catalog_table.a_M, catalog_table.C, catalog_table.log_g],
            [catalog_table.v_losErr, catalog_table.EBVErr, catalog_table.T_effErr, catalog_table.M_HErr, catalog_table.a_MErr, catalog_table.CErr, catalog_table.log_gErr],
            [catalog_table.v_losStatus, catalog_table.EBVStatus, catalog_table.T_effStatus, catalog_table.M_HStatus, catalog_table.a_MStatus, catalog_table.CStatus, catalog_table.log_gStatus],
        ):
            value.append(np.nan)
            error.append(np.nan)
            status.append('')

        # Status value of tempfit
        catalog_table.tempfitflag.append(True)
        catalog_table.tempfitstatus.append(TempFitFlag.NODATA.name)

    def __append_magnitudes_from_target_lists(self, photometry_table, obj, target_lists, obcode, catid, objid):
        if target_lists is not None:
            primary_target_idx, secondary_target_idx = self._find_matching_targets(target_lists, obcode, objid)

            # Look up the broad-band magnitudes in the original target lists
            magnitudes = self._find_magnitudes_in_target_list(
                self.config.tempfit.photometry,
                target_lists,
                primary_target_idx)

            for sidx in secondary_target_idx:
                magnitudes = self._find_magnitudes_in_target_list(
                    self.config.tempfit.photometry,
                    target_lists,
                    sidx,
                    magnitudes=magnitudes,
                    force_update=False)
                
            for mag in magnitudes:
                photometry_table.catId.append(catid)
                photometry_table.objId.append(objid)
                photometry_table.filterName.append(mag)
                photometry_table.flux.append(magnitudes[mag].flux)
                photometry_table.fluxErr.append(magnitudes[mag].flux_err)
                photometry_table.mag.append(magnitudes[mag].mag)
                photometry_table.magErr.append(magnitudes[mag].mag_err)
        else:
            pass

        

    def __append_parameters_from_assignments(self, table, obj, assignments, obcode, objid):
        if assignments is not None:
            assignments_idx = self.__find_matching_assignment(assignments, obcode, objid)

            if np.sum(assignments_idx) == 1:
                priority = assignments.loc[assignments_idx, 'priority'].item()
            else:
                priority = -1
        else:
            priority = -1    
            
        table.targetPriority.append(priority)

def main():
    script = CatalogScript()
    script.execute()

if __name__ == "__main__":
    main()