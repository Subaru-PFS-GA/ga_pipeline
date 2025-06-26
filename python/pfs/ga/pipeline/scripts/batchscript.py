import os
import subprocess

from ..setup_logger import logger

class BatchScript():
    """
    Mixin class to implement submitting jobs to slurm etc.
    """

    def __init__(self):
        self.__batch = None                 # Type of batch system to use, None for none
        self.__partition = 'default'
        self.__cpus = 4
        self.__memory = '4g'
        self.__time = '01:00:00'            # Time limit for the job

        self.__dry_run = False              # Dry run mode
        self.__top = None                   # Stop after this many objects

    def is_batch(self):
        return self.__batch is not None
    
    def __get_dry_run(self):
        return self.__dry_run
    
    dry_run = property(__get_dry_run)

    def __get_top(self):
        return self.__top
    
    top = property(__get_top)

    def _add_args(self):   
        self.add_arg('--batch', type=str, choices=['slurm'], help='Submit to batch system.') 
        self.add_arg('--partition', type=str, help='Slurm partition')
        self.add_arg('--cpus', type=str, help='Number of CPUs per task')
        self.add_arg('--memory', type=str, help='Memory per task')
        self.add_arg('--time', type=str, help='Time limit for the job')
        
        self.add_arg('--dry-run', action='store_true', help='Dry run mode')
        self.add_arg('--top', type=int, help='Stop after this many objects')

    def _init_from_args(self, args):
        self.__batch = self.get_arg('batch', args, self.__batch)
        self.__partition = self.get_arg('partition', args, self.__partition)
        self.__cpus = self.get_arg('cpus', args, self.__cpus)
        self.__memory = self.get_arg('memory', args, self.__memory)
        self.__time = self.get_arg('time', args, self.__time)

        self.__dry_run = self.get_arg('dry_run', args, self.__dry_run)
        self.__top = self.get_arg('top', args, self.__top)
    
    def _submit_job(self, command, item):
        sbatch_script = f"""#!/bin/env bash
#SBATCH --partition {self.__partition}
#SBATCH --cpus-per-task {self.__cpus}
#SBATCH --mem {self.__memory}
#SBATCH --time {self.__time}

echo $PYTHONPATH

set -e

out=slurm-\$SLURM_JOB_ID.out
srun {command}
outdir=\$(cat \$out | grep -Po 'Output directory is (\K.+)')
mv \$out \$outdir/slurm.out
"""

        # Submit the job to slurm
        if self.__dry_run:
            logger.info(f'Dry run: sbatch script for {item}.')
        else:
            logger.info(f'Submitting sbatch script for {item}.')

            # Execute the sbatch command and pass in sbatch_script via stdin
            process = subprocess.Popen(
                ['sbatch'],
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )
            stdout, stderr = process.communicate(sbatch_script)
            if process.returncode != 0:
                raise RuntimeError(f'Sbatch submission failed: {stderr.strip()}')
