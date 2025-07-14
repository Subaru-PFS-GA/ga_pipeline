# 2 Data

GAPIPE requires two kinds of data files: model data, such as stellar template librararies, and observation data. Paths to model data are defined in the configuration files, see Section 3, whereas observation data is accessed from the file system via a built-in file system crawler or Butler.

# 2.1 Model data

Model data should be stored in the `$GAPIPE_DIR/data` directory (`$PFSSPEC_DATA` should also point here), although any path can be used but it requires more manual configuration.

The suggested structure of the model data directory is as follows:

TODO: folder names below are preliminary and currently reflect the current development system.

```
$GAPIPE_DIR/data/
  + models/
    + stellar/
      + grid/
        + <grid_name>/
          + <model_name>/
            + spectra.fits
    + subaru/
      + hsc/
        + filters/
          - <filter_name>.txt
      + pfs/
        + psf/import
          - <psf_name>.fits
```

# 2.2 Observation data

# 2.2.1 Using GAPIPE with Butler

TBW

# 2.2.2 Using GAPIPE without Butler

When not using Butler, the observation data is accessed from the file system by sweeping the file system for files that match the expected naming convention. The following environment variables must be set to point to the directories where the observation data is stored:

* `GAPIPE_DATADIR`
* `GAPIPE_RERUN`
* `GAPIPE_RERUNDIR`
* `PFSSPEC_PFS_DATADIR`
* `PFSSPEC_PFS_RERUNDIR`
* `PFSSPEC_PFS_RERUN`
* `PFSSPEC_PFS_DESIGNDIR`
* `PFSSPEC_PFS_CONFIGDIR`

When not using Butler, objects are looked up from the `pfsConfig` files and then the file system is searched for the files that match the expected naming convention. File lookup can be tested by the `gapipe-repo` command-line tool, which is part of GAPIPE.