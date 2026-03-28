from ...common import PipelineConfig

class RepoConfig(PipelineConfig):
    """
    Repository configuration.
    """

    def __init__(self):
        super().__init__()

        self.datadir = self._get_env('GAPIPE_DATADIR')        # PFS survey data directory root
        self.workdir = self._get_env('GAPIPE_WORKDIR')        # Working directory
        self.outdir = self._get_env('GAPIPE_OUTDIR')          # Pipeline output directory
        self.rundir = self._get_env('GAPIPE_RUNDIR')          # Directory for the run
        self.run = self._get_env('GAPIPE_RUN')                # Name of data processing run
        self.garundir = self._get_env('GAPIPE_GARUNDIR')      # Directory for the GA data processing run
        self.garun = self._get_env('GAPIPE_GARUN')            # Name of GA data processing run

        self.ignore_missing_files = False                     # Ignore missing data files
