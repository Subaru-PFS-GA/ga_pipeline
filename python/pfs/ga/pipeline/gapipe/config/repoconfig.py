from ...common import PipelineConfig

class RepoConfig(PipelineConfig):
    """
    Repository configuration.
    """

    def __init__(self):
        super().__init__()

        self.workdir = self._get_env('GAPIPE_WORKDIR')        # Working directory
        self.datadir = self._get_env('GAPIPE_DATADIR')        # PFS survey data directory root
        self.rerundir = self._get_env('GAPIPE_RERUNDIR')      # Path to rerun data, absolute or relative to `datadir`
        self.outdir = self._get_env('GAPIPE_OUTDIR')          # Pipeline output directory
        self.rerun = self._get_env('GAPIPE_RERUN')            # Rerun name, used in file names

        self.ignore_missing_files = False                     # Ignore missing data files
