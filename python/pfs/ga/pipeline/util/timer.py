import logging
import time
from datetime import datetime

class Timer():
    def __init__(self, logger=None, log_level=None, message=None):
        if logger is not None:
            self.__logger = logger
        else:
            from pfs.ga.pipeline.setup_logger import logger
            self.__logger = logger

        self.__log_level = log_level if log_level is not None else logging.INFO
        self.__message = message if message is not None else "Elapsed time {:.3f} seconds."
        self.__start_time = None

    def get_logger(self):
        return self.__logger
    
    def set_logger(self, logger):
        self.__logger = logger

    logger = property(get_logger, set_logger)

    def __enter__(self):
        self.__start_time = time.perf_counter()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    def format_message(self, message):
        message = message if message is not None else self.__message
        elapsed_time = time.perf_counter() - self.__start_time
        return message.format(elapsed_time, elapsed_time=elapsed_time)

    def stamp(self, logger=None, log_level=None, message=None):
        logger = logger if logger is not None else self.__logger
        log_level = log_level if log_level is not None else self.__log_level
        message = self.format_message(message)
        logger.log(log_level, message)
