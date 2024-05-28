import logging
from unittest import TestCase

from pfs.ga.pipeline.util import Timer

from pfs.ga.pipeline.setup_logger import logger

class TestTime(TestCase):
    def test_init_global(self):
        with Timer(logger):
            pass


