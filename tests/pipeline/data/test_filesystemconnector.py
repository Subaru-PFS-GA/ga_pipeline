import os
from datetime import date
from unittest import TestCase

from pfs.ga.pipeline.data import FileSystemConnector

class TestFileSystemConnector(TestCase):

    DATADIR = '/datascope/subaru/data/commissioning'
    RERUNDIR = 'rerun/run17/20240604'

    def get_test_connector(self):
        return FileSystemConnector(
            datadir=TestFileSystemConnector.DATADIR,
            rerundir=TestFileSystemConnector.RERUNDIR,
        )

    def test_init(self):
        connector = self.get_test_connector()

    def test_find_datadir(self):
        connector = self.get_test_connector()

        datadir = connector.find_datadir()
        self.assertEqual(TestFileSystemConnector.DATADIR, datadir)

        datadir = connector.find_datadir(os.path.join(TestFileSystemConnector.DATADIR, TestFileSystemConnector.RERUNDIR))
        self.assertEqual(TestFileSystemConnector.DATADIR, datadir)

    def test_find_rerundir(self):
        connector = self.get_test_connector()

        rerundir = connector.find_rerundir()
        self.assertTrue(rerundir.endswith(TestFileSystemConnector.RERUNDIR))

        rerundir = connector.find_rerundir(os.path.join(TestFileSystemConnector.DATADIR, TestFileSystemConnector.RERUNDIR, 'pfsMerged'))
        self.assertTrue(rerundir.endswith(TestFileSystemConnector.RERUNDIR))

    def test_find_pfsDesign(self):
        connector = self.get_test_connector()

        files, ids = connector.find_pfsDesign()
        self.assertTrue(len(files) > 0)
        self.assertEqual(len(files), len(ids.pfsDesignId))

    def test_get_pfsDesign(self):
        connector = self.get_test_connector()

        file, id = connector.get_pfsDesign(0x6d832ca291636984)
        self.assertIsNotNone(file)
        self.assertEqual(0x6d832ca291636984, id.pfsDesignId)

    def test_load_pfsDesign(self):
        connector = self.get_test_connector()

        filename, identity = connector.get_pfsDesign(0x6d832ca291636984)

        pfsDesign = connector.load_pfsDesign(path=filename)
        pfsDesign = connector.load_pfsDesign(identity=identity)

    def test_find_pfsConfig(self):
        connector = self.get_test_connector()

        files, ids = connector.find_pfsConfig()
        self.assertTrue(len(files) > 0)
        self.assertEqual(len(files), len(ids.pfsDesignId))
        self.assertEqual(len(files), len(ids.visit))
        self.assertEqual(len(files), len(ids.date))

        files, ids = connector.find_pfsConfig(date=(date(2024, 6, 1), date(2024, 6, 4)))
        self.assertTrue(len(files) > 0)
        self.assertEqual(len(files), len(ids.pfsDesignId))
        self.assertEqual(len(files), len(ids.visit))
        self.assertEqual(len(files), len(ids.date))

        files, ids = connector.find_pfsConfig(visit=111483)
        self.assertTrue(len(files) > 0)
        self.assertEqual(111483, ids.visit[0])

    def test_get_pfsConfig(self):
        connector = self.get_test_connector()

        files = connector.get_pfsConfig(0x6d832ca291636984, 111636)

    def test_load_pfsConfig(self):
        connector = self.get_test_connector()

        filename, identity = connector.get_pfsConfig(visit=111483)
        
        pfsConfig = connector.load_pfsConfig(path=filename)
        pfsConfig = connector.load_pfsConfig(identity=identity)