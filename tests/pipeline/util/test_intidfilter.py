import os
from unittest import TestCase

from pfs.ga.pipeline.util import IntIDFilter

class TestIntIDFilter(TestCase):
    def test_init(self):
        pass

    def test_parse(self):
        filter = IntIDFilter()

        filter.parse(['12345'])
        self.assertEqual([12345], filter.values)

        filter.parse(['12345', '23456'])
        self.assertEqual([12345, 23456], filter.values)
        
        filter.parse(['12345-12348'])
        self.assertEqual([(12345, 12348)], filter.values)