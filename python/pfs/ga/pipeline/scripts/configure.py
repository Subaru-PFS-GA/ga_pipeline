#!/bin/env python3

import os
import re
from glob import glob
from types import SimpleNamespace
import numpy as np

from pfs.datamodel.utils import calculatePfsVisitHash, wraparoundNVisit

from ..constants import Constants
from .script import Script
from ..util import IntIDFilter, HexIDFilter
from ..config import GA1DPipelineConfig

from ..setup_logger import logger

class Configure(Script):
    """
    Generate the job configuration file for a set of observations.

    The script works by finding all pfsSingle files that match the filters specified
    on the command-line and then reads an existing configuration file which is used
    as a template. The script then updates the configuration file with the IDs of the
    pfsSingle files and saves the updated configuration file to the output directory.
    """

    def __init__(self):
        super().__init__()

        self.__config_file = None       # Path of the configuration file
        self.__config = None            # Pipeline configuration

        self.__workdir = None           # Working directory for the pipeline job
        self.__datadir = None           # Data directory
        self.__rerundir = None          # Directory of rerun, relative to datadir
        self.__outdir = None            # 

        self.__catId = IntIDFilter('catid', format='{:05d}')
        self.__tract = IntIDFilter('tract', format='{:05d}')
        self.__patch = None
        self.__objId = HexIDFilter('objid', format='{:016x}')
        self.__visit = IntIDFilter('visit', format='{:06d}')

    def _add_args(self):
        super()._add_args()

        self._add_arg('--config', type=str, nargs='*', required=True, help='Configuration file')

        self._add_arg('--workdir', type=str, help='Working directory')
        self._add_arg('--datadir', type=str, help='Data directory')
        self._add_arg('--rerundir', type=str, help='Rerun directory')
        self._add_arg('--outdir', type=str, help='Output directory')

        self._add_arg('--catid', type=str, nargs='*', help='Filter on catId')
        self._add_arg('--tract', type=str, help='Filter on tract')
        self._add_arg('--patch', type=str, help='Patch string')
        self._add_arg('--objid', type=str, nargs='*', help='Filter on objid')
        self._add_arg('--visit', type=str, nargs='*', help='Filter on visit')

    def _init_from_args(self, args):
        super()._init_from_args(args)

        # Load the configuration file
        self.__config = self._get_arg('config', args)
        self.__config = GA1DPipelineConfig(self.__config)

        # Command-line arguments override the configuration file
        self.__workdir = self._get_arg('workdir', args, self.__config.workdir)
        self.__datadir = self._get_arg('datadir', args, self.__config.datadir)
        self.__rerundir = self._get_arg('rerundir', args, self.__config.rerundir)
        self.__outdir = self._get_arg('outdir', args, self.__config.outdir)

        # Parse the ID filters
        self.__catId.parse(self._get_arg('catid', args))
        self.__tract.parse(self._get_arg('tract', args))
        self.__patch = self._get_arg('patch', args, self.__patch)
        self.__objId.parse(self._get_arg('objid', args))
        self.__visit.parse(self._get_arg('visit', args))

    def prepare(self):
        super().prepare()

        # Override logging directory to use the same as the pipeline workdir
        logfile = os.path.basename(self.logfile)
        self.logfile = os.path.join(self.__workdir, 'log', logfile)

    def run(self):
        """
        Find all the pfsSingle files that match the filters and generate a config file for each.
        """

        logger.info('Using configuration template file(s) `{self.__config.config_files}`.')

        # Find all the pfsSingle files that match the filters
        targets = self.__get_pfsSingle_targets()

        if len(targets) == 0:
            return
        
        # Generate a config file for each of the inputs
        for objId in sorted(targets.keys()):

            identity = dict(
                catId = targets[objId].catId,
                tract = targets[objId].tract,
                patch = targets[objId].patch,
                objId = targets[objId].objId,
                nVisit = wraparoundNVisit(len(targets[objId].visits)),
                pfsVisitHash = calculatePfsVisitHash(targets[objId].visits),
            )

            # Compose the filename
            dir = Constants.PFSGACONFIG_DIR_FORMAT.format(**identity)
            filename = Constants.PFSGACONFIG_FILENAME_FORMAT.format(**identity)
            path = os.path.join(self.__outdir.format(**identity), dir, filename)

            logger.info(f'Generating configuration file `{path}`.')

            # Update the config with the ids
            self.__config.target.catId = targets[objId].catId
            self.__config.target.tract = targets[objId].tract
            self.__config.target.patch = targets[objId].patch
            self.__config.target.objId = targets[objId].objId

            # TODO: the empty dict here is a placeholder for per visit configuration
            self.__config.target.visits = { v: {} for v in targets[objId].visits }

            # TODO: update config with directory names?
            self.__config.workdir = self.__workdir.format(**identity)
            self.__config.datadir = self.__datadir.format(**identity)
            self.__config.rerundir = self.__rerundir.format(**identity)
            self.__config.outdir = self.__outdir.format(**identity)

            # Save the config to a file
            os.makedirs(os.path.dirname(path), exist_ok=True)
            self.__config.save(path)

    def __get_pfsSingle_glob_pattern(self):
        dir = Constants.PFSSINGLE_DIR_GLOB.format(
            catId = self.__catId.get_glob_pattern(),
            tract = self.__tract.get_glob_pattern(),
            patch = self.__patch if self.__patch is not None else '*',
            objId = self.__objId.get_glob_pattern(),
            visit = self.__visit.get_glob_pattern()
        )

        filename = Constants.PFSSINGLE_FILENAME_GLOB.format(
            catId = self.__catId.get_glob_pattern(),
            tract = self.__tract.get_glob_pattern(),
            patch = self.__patch if self.__patch is not None else '*',
            objId = self.__objId.get_glob_pattern(),
            visit = self.__visit.get_glob_pattern()
        )

        pattern = os.path.join(self.__datadir, self.__rerundir, dir, filename)

        return pattern
    
    def __get_pfsSingle_targets(self):
        """
        Find all the pfsSingle files that match the filters and parse the
        filenames into a dictionary of targets.
        """

        logger.info(f'Finding pfsSingle files matching the following filters:')
        logger.info(f'    catId: {str(self.__catId)}')
        logger.info(f'    tract: {str(self.__tract)}')
        logger.info(f'    patch: {self.__patch}')
        logger.info(f'    objId: {str(self.__objId)}')
        logger.info(f'    visit: {str(self.__visit)}')
        
        glob_pattern = self.__get_pfsSingle_glob_pattern()
        paths = glob(glob_pattern)

        logger.info(f'Found {len(paths)} pfsSingle files matching pattern `{glob_pattern}`.')

        targets = {}
        for path in paths:
            filename = os.path.basename(path)
            match = re.match(Constants.PFSSINGLE_FILENAME_REGEX, filename)

            catId = self.__catId.parse_value(match.group(1))
            tract = self.__tract.parse_value(match.group(2))
            patch = match.group(3)
            objId = self.__objId.parse_value(match.group(4))
            visit = self.__visit.parse_value(match.group(5))

            if match is not None and \
                self.__catId.match(catId) and \
                self.__tract.match(tract) and \
                (self.__patch is None or self.__patch == patch) and \
                self.__objId.match(objId) and \
                self.__visit.match(visit):
                    
                if objId not in targets:
                    targets[objId] = SimpleNamespace(
                        catId = catId,
                        tract = tract,
                        patch = patch,
                        objId = objId,
                        visits = [],
                        files = [],
                    )
                
                targets[objId].visits.append(visit)
                targets[objId].files.append(path)

        unique_visits = np.unique(np.concatenate([ target.visits for objid, target in targets.items() ]))
        logger.info(f'Number of targets filtered down to {len(targets)} spanning {len(unique_visits)} unique visits.')

        for objId, obj in targets.items():
            obj.visits.sort()
            obj.files.sort()

        return targets

def main():
    script = Configure()
    script.execute()

if __name__ == "__main__":
    main()