import os
from unittest import TestCase

from pfs.ga.pipeline.util.idfilter import IDFilter

class TestIDFilter(TestCase):
    def test_init(self):
        filter = IDFilter()
        self.assertEqual(None, filter.name)
        self.assertEqual('{}', filter.format)
        self.assertEqual(None, filter.values)

        filter = IDFilter(name='test', format='{:05d}', orig=filter)
        self.assertEqual('test', filter.name)
        self.assertEqual('{:05d}', filter.format)
        self.assertEqual(None, filter.values)

        filter = IDFilter(1, 2, 3)
        self.assertEqual([1, 2, 3], filter.values)

        filter = IDFilter(filter)
        self.assertEqual(None, filter.name)
        self.assertEqual('{}', filter.format)
        self.assertEqual([1, 2, 3], filter.values)


    def test_normalize_values(self):
        filter = IDFilter()
        f2 = IDFilter(3, 4)
        
        self.assertEqual(None, filter._normalize_values(None))
        self.assertEqual([1], filter._normalize_values(1))
        self.assertEqual([1], filter._normalize_values([1]))
        self.assertEqual([1, 2], filter._normalize_values([1, 2]))
        self.assertEqual([1, 2], filter._normalize_values((1, 2)))
        self.assertEqual([3, 4], filter._normalize_values(f2))
        self.assertEqual([3, 4], filter._normalize_values([f2]))