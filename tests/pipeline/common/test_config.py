import os
import numpy as np
from datetime import datetime, date
from unittest import TestCase
from typing import List, Dict

from pfs.ga.pipeline.common import Config

class SubConfig(Config):
    def __init__(self):
        super().__init__()

        self.c = None
        self.d = None

class MainConfig(Config):
    def __init__(self,
                 sub: SubConfig = SubConfig(),
                 entries: List[SubConfig] = None,
                 dicts: Dict[str, SubConfig] = None):
        
        super().__init__()

        self.a = None
        self.b = None
        self.sub = sub
        self.entries = entries
        self.dicts = dicts


class TestPipeline(TestCase):
    def get_test_dict(self):
        return {
            'a': 1,
            'b': 2,
            'sub': { 'c': 3, 'd': 4 },
            'entries': [
                { 'c': 5, 'd': 6 },
                { 'c': 7, 'd': 8 }
            ],
            'dicts': {
                'one': { 'c': 9, 'd': 10 },
                'two': { 'c': 11, 'd': 12 }
            }
        }

    def get_test_config(self):
        c = MainConfig()
        c.a = 1
        c.b = 2
        c.sub.c = 3
        c.sub.d = 4
        c.entries = [ SubConfig(), SubConfig() ]
        c.entries[0].c = 5
        c.entries[0].d = 6
        c.entries[1].c = 7
        c.entries[1].d = 8
        c.dicts = { 'one': SubConfig(), 'two': SubConfig() }
        c.dicts['one'].c = 9
        c.dicts['one'].d = 10
        c.dicts['two'].c = 11
        c.dicts['two'].d = 12

        return c

    def test_init(self):
        c = self.get_test_config()

    def test_config_to_class(self):
        d = self.get_test_dict()
        c = Config._config_to_class(MainConfig, d)

        self.assertIsInstance(c, MainConfig)
        self.assertEqual(c.a, 1)
        self.assertEqual(c.b, 2)
        self.assertIsInstance(c.sub, SubConfig)
        self.assertEqual(c.sub.c, 3)
        self.assertEqual(c.sub.d, 4)
        self.assertIsInstance(c.entries, list)
        self.assertEqual(len(c.entries), 2)
        self.assertIsInstance(c.entries[0], SubConfig)
        self.assertEqual(c.entries[0].c, 5)
        self.assertEqual(c.entries[0].d, 6)
        self.assertIsInstance(c.dicts, dict)
        self.assertEqual(len(c.dicts), 2)
        self.assertIsInstance(c.dicts['one'], SubConfig)
        self.assertEqual(c.dicts['one'].c, 9)
        self.assertEqual(c.dicts['one'].d, 10)

    def test_config_to_dict(self):
        c = self.get_test_config()
        d = Config._save_config_to_dict(c)
        
        self.assertEqual(d, self.get_test_dict())

    def test_save(self):
        c = self.get_test_config()
        c.save('./tmp/test/config.yaml')
        c.save('./tmp/test/config.json')

    def test_load(self):
        c = MainConfig()
        c.load('./data/test/config_01.yaml')
        
        c = MainConfig()
        c.load('./data/test/config_01.json')

        c = MainConfig()
        c.load('./data/test/config_01.py')