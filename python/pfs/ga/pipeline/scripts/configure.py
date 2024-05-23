#!/bin/env python3

import os
import re
from glob import glob
from types import SimpleNamespace

from pfs.datamodel.utils import calculatePfsVisitHash, wraparoundNVisit

from ..constants import Constants
from .script import Script
from ..util import IntIDFilter, HexIDFilter
from ..config import GA1DPipelineConfig

class Configure(Script):
    """
    Generate the job configuration file for a set of observations.
    """

    def __init__(self):
        super().__init__()

        self.__config = None

        self.__workdir = None
        self.__datadir = None
        self.__rerundir = None
        self.__outdir = None

        self.__catId = IntIDFilter('catid', format='{:05d}')
        self.__tract = IntIDFilter('tract', format='{:05d}')
        self.__patch = None
        self.__objId = HexIDFilter('objid', format='{:016x}')
        self.__visit = IntIDFilter('visit', format='{:06d}')

    def _add_args(self):
        super()._add_args()

        self._add_arg('--config', type=str, help='Configuration file')

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

        # Load the default configuration file into a dict
        config_file = self._get_arg('config', args)
        if config_file is not None:
            config = GA1DPipelineConfig.load_dict(config_file)
        else:
            config = None
        self.__config = GA1DPipelineConfig(config)
        
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

    def run(self):
        # Find all the pfsSingle files that match the filters
        targets = self.__get_pfsSingle_targets()

        # Generate a config file for each of the inputs
        for objId in sorted(targets.keys()):
            # Update the config with the ids
            self.__config.target.catId = targets[objId].catId
            self.__config.target.tract = targets[objId].tract
            self.__config.target.patch = targets[objId].patch
            self.__config.target.objId = targets[objId].objId

            # TODO: the empty dict here is a placeholder for per visit configuration
            self.__config.target.visits = { v: {} for v in targets[objId].visits }

            identity = dict(
                catId = targets[objId].catId,
                tract = targets[objId].tract,
                patch = targets[objId].patch,
                objId = targets[objId].objId,
                nVisit = wraparoundNVisit(len(targets[objId].visits)),
                pfsVisitHash = calculatePfsVisitHash(targets[objId].visits),
            )

            # TODO: update config with directory names?
            self.__config.workdir = self.__workdir.format(**identity)
            self.__config.datadir = self.__datadir.format(**identity)
            self.__config.rerundir = self.__rerundir.format(**identity)
            self.__config.outdir = self.__outdir.format(**identity)

            # Save the config to a file
            filename = Constants.PFSGACONFIG_FILENAME_FORMAT.format(
                catId = targets[objId].catId,
                tract = targets[objId].tract,
                patch = targets[objId].patch,
                objId = targets[objId].objId,
                nVisit = wraparoundNVisit(len(targets[objId].visits)),
                pfsVisitHash = calculatePfsVisitHash(targets[objId].visits),
            )

            path = os.path.join(self.__config.outdir, filename)
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
        Find all the pfsSingle files that match the filters
        """
        
        glob_pattern = self.__get_pfsSingle_glob_pattern()
        targets = {}
        for path in glob(glob_pattern):
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

        for objId, obj in targets.items():
            obj.visits.sort()
            obj.files.sort()

        return targets

def main():
    script = Configure()
    script.execute()

if __name__ == "__main__":
    main()