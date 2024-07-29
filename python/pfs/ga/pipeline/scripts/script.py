import os
import sys
import logging
from datetime import datetime, timezone
from argparse import ArgumentParser
import commentjson as json
import yaml
import numpy as np

from ..setup_logger import logger

class Script():
    """
    Implements generic function for pipeline command-line scripts.
    """

    def __init__(self):
        self.__debug = False                        # If True, script is running in debug mode
        self.__profile = False                      # If True, the profiler is enabled
        
        self.__logLevel = logging.INFO              # Default log level
        self.__logFile = None                       # Log file name
        self.__logFormatter = None                  # Log formatter
        self.__logFileHandler = None                # Log file handler
        self.__logConsoleHandler = None             # Log console handler

        self.__parser = ArgumentParser()
        self.__profiler = None
        self.__timestamp = datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')

    def __get_debug(self):
        return self.__debug
    
    debug = property(__get_debug)

    def __get_profile(self):
        return self.__profile
    
    profile = property(__get_profile)

    def __get_loglevel(self):
        return self.__logLevel
    
    log_level = property(__get_loglevel)

    def __get_logfile(self):
        return self.__logFile
    
    def __set_logfile(self, value):
        self.__logFile = value
    
    logfile = property(__get_logfile, __set_logfile)

    def __parse_args(self):
        self.__args = self.__parser.parse_args().__dict__

    def _add_arg(self, *args, **kwargs):
        self.__parser.add_argument(*args, **kwargs)

    def _get_arg(self, name, args=None, default=None):
        args = args if args is not None else self.__args

        if name in args and args[name] is not None and args[name] != '':
            return args[name]
        else:
            return default
        
    def _add_args(self):
        self._add_arg('--debug', action='store_true', help='Enable debug mode')
        self._add_arg('--profile', action='store_true', help='Enable performance profiler')
        self._add_arg('--log-level', type=str, help='Set log level')

    def _init_from_args(self, args):
        self.__debug = self._get_arg('debug', args, self.__debug)
        self.__profile = self._get_arg('profile', args, self.__profile)
        
        self.__logLevel = self._get_arg('log_level', args, self.__logLevel)
        if isinstance(self.__logLevel, str) and hasattr(logging, self.__logLevel.upper()):
            self.__logLevel = getattr(logging, self.__logLevel.upper())
        else:
            self.__logLevel = logging.INFO

        if self.__debug and self.__logLevel > logging.DEBUG:
            self.__logLevel = logging.DEBUG
        

    def _create_dir(self, name, dir, logger=logger):
        dir = os.path.join(os.getcwd(), dir)
        if not os.path.isdir(dir):
            os.makedirs(dir, exist_ok=True)
            logger.debug(f'Created {name} directory `{dir}`.')
        else:
            logger.debug(f'Found existing {name} directory `{dir}`.')

    def __start_logging(self):

        logdir = os.path.dirname(self.__logFile)
        self._create_dir('log', logdir)

        self.__logFormatter = logging.Formatter("%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s", datefmt='%H:%M:%S')
        self.__logFileHandler = logging.FileHandler(self.__logFile)
        self.__logFileHandler.setFormatter(self.__logFormatter)
        self.__logConsoleHandler = logging.StreamHandler()
        self.__logConsoleHandler.setFormatter(self.__logFormatter)

        # Configure root logger
        root = logging.getLogger()
        root.handlers = []
        root.setLevel(self.__logLevel)
        root.addHandler(self.__logFileHandler)
        root.addHandler(self.__logConsoleHandler)

        # Filter out log messages from matplotlib
        logging.getLogger('matplotlib').setLevel(logging.WARNING)
 
        # Configure pipeline logger
        logger.propagate = True
        logger.setLevel(self.__logLevel)

        logger.info(f'Logging started to `{self.__logFile}`.')

    def __stop_logging(self):
        logger.info(f'Logging finished to `{self.__logFile}`.')

        # Disconnect file logger and re-assign stderr
        root = logging.getLogger()
        root.handlers = []
        root.addHandler(logging.StreamHandler())

        # Destroy logging objects (but keep last filename)
        self.__logFormatter = None
        self.__logFileHandler = None
        self.__logConsoleHandler = None

    def __start_profiler(self):
        """
        Start the profiler session
        """

        if self.__profile:
            import cProfile
            import pstats
            import io

            self.__profiler = cProfile.Profile()
            self.__profiler.enable()
        else:
            self.__profiler = None

    def __stop_profiler(self):
        """
        Stop the profiler session and save the results
        """

        if self.__profiler is not None:
            import pstats

            self.__profiler.disable()

            # Save profiler data to file
            with open(os.path.join('profile.cum.stats'), 'w') as f:
                ps = pstats.Stats(self.__profiler, stream=f).sort_stats('cumulative')
                ps.print_stats()

            with open(os.path.join('profile.tot.stats'), 'w') as f:
                ps = pstats.Stats(self.__profiler, stream=f).sort_stats('time')
                ps.print_stats()

            self.__profiler = None

    def __dump_env(self, filename):
        with open(filename, 'w') as f:
            for key, value in os.environ.items():
                f.write(f'{key}={value}\n')

        logger.debug(f'Environment variables saved to `{filename}`.')

    def __dump_args(self, filename):

        def default(obj):
            if isinstance(obj, float):
                return "%.5f" % obj
            if type(obj).__module__ == np.__name__:
                if isinstance(obj, np.ndarray):
                    if obj.size < 100:
                        return obj.tolist()
                    else:
                        return "(not serialized)"
                else:
                    return obj.item()
            return "(not serialized)"

        _, ext = os.path.splitext(filename)
        with open(filename, 'w') as f:
            if ext == '.json':
                json.dump(self.__args, f, default=default, indent=4)
            elif ext == '.yaml':
                yaml.dump(self.__args, f, indent=4)

        logger.debug(f'Arguments saved to `{filename}`.')
            
    def __dump_cmdline(self, filename):
        """
        Save to command-line into a file.
        """

        with open(filename, 'w') as f:
            f.write(f'./bin/{self.__class__.__name__.lower()} ')
            if len(sys.argv) > 1:
                f.write(' '.join(sys.argv[1:]))
            f.write('\n')

        logger.debug(f'Command-line saved to `{filename}`.')

    def _dump_settings(self):
        # Save environment, arguments and command-line to files next to the log file
        logdir = os.path.dirname(self.__logFile)
        self.__dump_env(os.path.join(logdir, f'env_{self.__timestamp}.sh'))
        self.__dump_args(os.path.join(logdir, f'args_{self.__timestamp}.json'))
        self.__dump_cmdline(os.path.join(logdir, f'command_{self.__timestamp}.sh'))

    def execute(self):
        self._add_args()
        self.__parse_args()
        self._init_from_args(self.__args)

        # NOTE: debugging is started from the wrapper shell script

        self.prepare()

        self.__start_logging()    
        self._dump_settings()
        self.__start_profiler()

        self.run()

        self.__stop_profiler()
        self.__stop_logging()

    def prepare(self):
        """
        Executed before the main run method. Do initializations here, such as setting
        up the logging level, directories, etc.
        """
        
        name = self.__class__.__name__.lower()
        time = self.__timestamp
        self.__logFile = f'{name}_{time}.log'

    def run(self):
        raise NotImplementedError()