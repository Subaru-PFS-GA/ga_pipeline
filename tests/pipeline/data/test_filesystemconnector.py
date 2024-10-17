import os
from datetime import date
from unittest import TestCase

from pfs.ga.pipeline.data import FileSystemConnector

class TestFileSystemConnector(TestCase):

    DATADIR = os.environ['GAPIPE_DATADIR']
    RERUNDIR = os.environ['GAPIPE_RERUNDIR']

    def get_test_connector(self):
        return FileSystemConnector(
            datadir=TestFileSystemConnector.DATADIR,
            rerundir=TestFileSystemConnector.RERUNDIR,
        )

    def test_init(self):
        connector = self.get_test_connector()

    def test_get_datadir(self):
        connector = self.get_test_connector()

        datadir = connector.get_datadir()
        self.assertEqual(TestFileSystemConnector.DATADIR, datadir)

        datadir = connector.get_datadir(os.path.join(TestFileSystemConnector.DATADIR, TestFileSystemConnector.RERUNDIR))
        self.assertEqual(TestFileSystemConnector.DATADIR, datadir)

        datadir = connector.get_datadir(os.path.join(TestFileSystemConnector.DATADIR))
        self.assertEqual(TestFileSystemConnector.DATADIR, datadir)

        self.failureException(
            ValueError,
            lambda _: connector.get_datadir('pfsConfig-0x6d832ca291636984-111483.fits', required=True))

        datadir = connector.get_datadir(os.path.join(TestFileSystemConnector.DATADIR, 'pfsDesign'))
        self.assertEqual(TestFileSystemConnector.DATADIR, datadir)

        datadir = connector.get_datadir(os.path.join(TestFileSystemConnector.DATADIR, 'pfsConfig'))
        self.assertEqual(TestFileSystemConnector.DATADIR, datadir)

        datadir = connector.get_datadir(os.path.join(TestFileSystemConnector.DATADIR, 'pfsConfig'))
        self.assertEqual(TestFileSystemConnector.DATADIR, datadir)

    def test_get_rerundir(self):
        connector = self.get_test_connector()

        rerundir = connector.get_rerundir()
        self.assertTrue(rerundir.endswith(TestFileSystemConnector.RERUNDIR))

        rerundir = connector.get_rerundir(os.path.join(TestFileSystemConnector.DATADIR, TestFileSystemConnector.RERUNDIR))
        self.assertTrue(rerundir.endswith(TestFileSystemConnector.RERUNDIR))

        self.failureException(
            ValueError,
            lambda _: connector.get_rerundir(os.path.join(TestFileSystemConnector.DATADIR), required=True))

        rerundir = connector.get_rerundir(os.path.join(TestFileSystemConnector.DATADIR, TestFileSystemConnector.RERUNDIR, 'pfsMerged'))
        self.assertTrue(rerundir.endswith(TestFileSystemConnector.RERUNDIR))

    #region PfsDesign

    def test_find_pfsDesign(self):
        connector = self.get_test_connector()

        files, ids = connector.find_pfsDesign()
        self.assertTrue(len(files) > 0)
        self.assertEqual(len(files), len(ids.pfsDesignId))

    def test_get_pfsDesign(self):
        connector = self.get_test_connector()

        file, id = connector.locate_pfsDesign(0x6d832ca291636984)
        self.assertIsNotNone(file)
        self.assertEqual(0x6d832ca291636984, id.pfsDesignId)

    def test_load_pfsDesign(self):
        connector = self.get_test_connector()

        filename, identity = connector.locate_pfsDesign(0x6d832ca291636984)

        pfsDesign = connector.load_pfsDesign(path=filename)
        pfsDesign = connector.load_pfsDesign(identity=identity)

    #endregion
    #region PsfConfig

    def test_parse_pfsConfig(self):
        connector = self.get_test_connector()

        identity = connector.parse_pfsConfig('pfsConfig-0x6d832ca291636984-111483.fits')
        self.assertEqual(111483, identity.visit)
        self.assertEqual(0x6d832ca291636984, identity.pfsDesignId)
        self.assertFalse(hasattr(identity, 'date'))

        identity = connector.parse_pfsConfig('2024-06-01/pfsConfig-0x6d832ca291636984-111483.fits')
        self.assertEqual(111483, identity.visit)
        self.assertEqual(0x6d832ca291636984, identity.pfsDesignId)
        self.assertEqual(date(2024, 6, 1), identity.date)

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

        files, ids = connector.find_pfsConfig(pfsDesignId=0x6d832ca291636984)
        self.assertTrue(len(files) > 0)

    def test_locate_pfsConfig(self):
        connector = self.get_test_connector()

        files = connector.locate_pfsConfig(111636, 0x6d832ca291636984)
        files = connector.locate_pfsConfig(111636)

        # More than one file matching
        self.failureException(
            FileNotFoundError,
            lambda _: connector.locate_pfsConfig(None, 0x6d832ca291636984))

        # No file matching
        self.failureException(
            FileNotFoundError,
            lambda _: connector.locate_pfsConfig(111636, 0x6d832ca291636985))

    def test_load_pfsConfig(self):
        connector = self.get_test_connector()

        filename, identity = connector.locate_pfsConfig(visit=111483)
        
        pfsConfig, id = connector.load_pfsConfig(path=filename)
        pfsConfig, id = connector.load_pfsConfig(identity=identity)

    #endregion