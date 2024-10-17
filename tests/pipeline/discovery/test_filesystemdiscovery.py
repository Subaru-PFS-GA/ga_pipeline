import os
from datetime import date
from unittest import TestCase

from pfs.ga.pipeline.discovery import FileSystemDiscovery

class TestFileSystemDiscovery(TestCase):

    DATADIR = '/datascope/subaru/data/commissioning'
    RERUNDIR = 'rerun/run17/20240604'

    def get_test_discovery(self):
        return FileSystemDiscovery(
            datadir=TestFileSystemDiscovery.DATADIR,
            rerundir=TestFileSystemDiscovery.RERUNDIR,
        )

    def test_init(self):
        discovery = self.get_test_discovery()

    def test_find_datadir(self):
        discovery = self.get_test_discovery()

        datadir = discovery.find_datadir()
        self.assertEqual(TestFileSystemDiscovery.DATADIR, datadir)

        datadir = discovery.find_datadir(os.path.join(TestFileSystemDiscovery.DATADIR, TestFileSystemDiscovery.RERUNDIR))
        self.assertEqual(TestFileSystemDiscovery.DATADIR, datadir)

    def test_find_rerundir(self):
        discovery = self.get_test_discovery()

        rerundir = discovery.find_rerundir()
        self.assertTrue(rerundir.endswith(TestFileSystemDiscovery.RERUNDIR))

        rerundir = discovery.find_rerundir(os.path.join(TestFileSystemDiscovery.DATADIR, TestFileSystemDiscovery.RERUNDIR, 'pfsMerged'))
        self.assertTrue(rerundir.endswith(TestFileSystemDiscovery.RERUNDIR))

    def test_find_pfsDesign(self):
        discovery = self.get_test_discovery()

        files, ids = discovery.find_pfsDesign()
        self.assertTrue(len(files) > 0)
        self.assertEqual(len(files), len(ids.pfsDesignId))

    def test_get_pfsDesign(self):
        discovery = self.get_test_discovery()

        file, id = discovery.get_pfsDesign(0x6d832ca291636984)
        self.assertIsNotNone(file)
        self.assertEqual(0x6d832ca291636984, id.pfsDesignId)

    def test_find_pfsConfig(self):
        discovery = self.get_test_discovery()

        files, ids = discovery.find_pfsConfig()
        self.assertTrue(len(files) > 0)
        self.assertEqual(len(files), len(ids.pfsDesignId))
        self.assertEqual(len(files), len(ids.visit))
        self.assertEqual(len(files), len(ids.date))

        files, ids = discovery.find_pfsConfig(date=(date(2024, 6, 1), date(2024, 6, 4)))
        self.assertTrue(len(files) > 0)
        self.assertEqual(len(files), len(ids.pfsDesignId))
        self.assertEqual(len(files), len(ids.visit))
        self.assertEqual(len(files), len(ids.date))

        files, ids = discovery.find_pfsConfig(visit=111483)
        self.assertTrue(len(files) > 0)
        self.assertEqual(111483, ids.visit[0])

    def test_get_pfsConfig(self):
        discovery = self.get_test_discovery()

        files = discovery.get_pfsConfig(0x6d832ca291636984, 111636)