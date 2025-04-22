#!/usr/bin/env python3

import os
import numpy as np
import pandas as pd

from pfs.datamodel import *
from pfs.datamodel.utils import calculatePfsVisitHash, wraparoundNVisit

from ..pipelinescript import PipelineScript
from ...gapipe.config import *

from ...setup_logger import logger

class SubmitScript(PipelineScript):
    """
    Submit pipeline execution to slurm.
    """

    def __init__(self):
        super().__init__()

        self.__partition = 'default'
        self.__cpus = 4
        self.__memory = '4g'

        self.__dry_run = False              # Dry run mode
        self.__top = None                   # Stop after this many objects

    def _add_args(self):
        
        self.add_arg('--partition', type=str, help='Slurm partition')
        self.add_arg('--cpus', type=str, help='Number of CPUs per task')
        self.add_arg('--memory', type=str, help='Memory per task')
        
        self.add_arg('--dry-run', action='store_true', help='Dry run mode')
        self.add_arg('--top', type=int, help='Stop after this many objects')

        super()._add_args()

    def _init_from_args(self, args):
        
        self.__partition = self.get_arg('partition', args, self.__partition)
        self.__cpus = self.get_arg('cpus', args, self.__cpus)
        self.__memory = self.get_arg('memory', args, self.__memory)

        self.__dry_run = self.get_arg('dry_run', args, self.__dry_run)
        self.__top = self.get_arg('top', args, self.__top)

        super()._init_from_args(args)

    def prepare(self):
        super().prepare()

        # Override logging directory to use the same as the pipeline workdir
        logfile = os.path.basename(self.log_file)
        self.log_file = os.path.join(self.repo.get_resolved_variable('workdir'), logfile)

    def run(self):
        """
        Find all the pfsSingle or pfsConfig files that match the filters and
        generate an sbatch script for each, then submit to slurm.
        """

        # Find the objects matching the command-line arguments. Arguments
        # are parsed by the repo object itself, so no need to pass them in here.
        identities = self.repo.find_object(groupby='objid')

        if len(identities) == 0:
            logger.error('No objects found matching the filters.')
            return
        else:
            logger.info(f'Found {len(identities)} objects matching the filters.')

        # Create the target configuration objects
        job_files = self.__create_sbatch_files(identities)

        # Submit the slurm jobs
        self.__submit_sbatch_jobs(job_files)

    def __create_sbatch_files(self, identities):
        job_files = {}
        q = 0
        for objid, id in identities.items():
            identity = GAObjectIdentityConfig(
                catId = id.catId[0],
                tract = id.tract[0],
                patch = id.patch[0],
                objId = objid,
                nVisit = wraparoundNVisit(len(id.visit)),
                pfsVisitHash = calculatePfsVisitHash(id.visit),
            )

            job_file = self.__create_sbatch_job_file(objid, identity)
            job_files[objid] = job_file

            q += 1
            if self.__top is not None and q >= self.__top:
                logger.info(f'Stopping after {q} objects.')
                break

        return job_files
    
    def __create_sbatch_job_file(self, objid, identity):
        # Compose the directory and file names for the identity of the object
        # The file should be written somewhere under the work directory
        dir = self.repo.format_dir(GAPipelineConfig, identity)
        config_file = self.repo.format_filename(GAPipelineConfig, identity)
        config_file = os.path.join(dir, config_file)

        # Name of the output pipeline configuration
        job_file = os.path.join(dir, 'job.sh')

        bash = f"""#!/bin/bash
#SBATCH --partition {self.__partition}
#SBATCH --cpus-per-task {self.__cpus}
#SBATCH --mem {self.__memory}
#SBATCH --output={dir}/job.out

srun python -m pfs.ga.pipeline.scripts.run.runscript --config {config_file}
"""

        # Write into the file
        if not self.__dry_run:
            with open(job_file, 'w') as f:
                print(bash, file=f)

        return job_file
    
    def __submit_sbatch_jobs(self, job_files):
        for objid, job_file in job_files.items():
            os.system(f'sbatch {job_file}')

def main():
    script = SubmitScript()
    script.execute()

if __name__ == "__main__":
    main()