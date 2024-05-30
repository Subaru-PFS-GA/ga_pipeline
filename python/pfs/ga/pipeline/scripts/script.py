import os
import logging
from argparse import ArgumentParser

class Script():
    """
    Implements generic function for pipeline command-line scripts.
    """

    def __init__(self):
        self.__debug = False
        self.__profile = False
        self.__log_level = logging.INFO

        self.__parser = ArgumentParser()

    def __get_debug(self):
        return self.__debug
    
    debug = property(__get_debug)

    def __get_profile(self):
        return self.__profile
    
    profile = property(__get_profile)

    def __get_log_level(self):
        return self.__log_level
    
    log_level = property(__get_log_level)

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
        self.__log_level = self._get_arg('log_level', args, self.__log_level)
        if isinstance(self.__log_level, str) and hasattr(logging, self.__log_level.upper()):
            self.__log_level = getattr(logging, self.__log_level.upper())
        else:
            self.__log_level = logging.INFO

    def execute(self):
        self._add_args()
        self.__parse_args()
        self._init_from_args(self.__args)

        # NOTE: debugging is started from the wrapper shell script

        if self.__profile:
            import cProfile
            import pstats
            import io

            pr = cProfile.Profile()
            pr.enable()
        else:
            pr = None

        self.run()

        if pr is not None:
            pr.disable()

            # Save profiler data to file
            with open(os.path.join('profile.cum.stats'), 'w') as f:
                ps = pstats.Stats(pr, stream=f).sort_stats('cumulative')
                ps.print_stats()

            with open(os.path.join('profile.tot.stats'), 'w') as f:
                ps = pstats.Stats(pr, stream=f).sort_stats('time')
                ps.print_stats()

    def run(self):
        raise NotImplementedError()