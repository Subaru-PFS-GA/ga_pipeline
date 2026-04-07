from ...common import PipelineConfig

class RepoConfig(PipelineConfig):
    """
    Repository configuration.
    """

    def __init__(self):
        super().__init__()

        self.datadir = self._get_env('GAPIPE_DATADIR')              # PFS survey data directory root
        self.workdir = self._get_env('GAPIPE_WORKDIR')              # Working directory
        self.outdir = self._get_env('GAPIPE_OUTDIR')                # Pipeline output directory
        self.rundir = self._get_env('GAPIPE_RUNDIR')                # Directory of the input processing run
        self.run = self._get_env('GAPIPE_RUN')                      # Name of the input processing run
        self.configrundir = self._get_env('GAPIPE_CONFIGRUNDIR')    # Path to pfsConfig files
        self.configrun = self._get_env('GAPIPE_CONFIGRUN')          # Run name for pfsConfig files
        self.garundir = self._get_env('GAPIPE_GARUNDIR')            # Directory for the GA data processing run
        self.garun = self._get_env('GAPIPE_GARUN')                  # Name of GA data processing run

        self.ignore_missing_files = False                           # Ignore missing data files

    def init_from_args(self, args):
        def is_arg(name):
            return name in args and args[name] is not None

        if is_arg('datadir'):
            self.datadir = args['datadir']
        if is_arg('workdir'):
            self.workdir = args['workdir']
        if is_arg('outdir'):
            self.outdir = args['outdir']
        if is_arg('rundir'):
            self.rundir = args['rundir']
        if is_arg('run'):                          #### TODO: multi-repo
            self.run = args['run'][0]
        if is_arg('garundir'):
            self.garundir = args['garundir']
        if is_arg('garun'):
            self.garun = args['garun']
        if is_arg('configrundir'):
            self.configrundir = args['configrundir']
        if is_arg('configrun'):
            self.configrun = args['configrun']