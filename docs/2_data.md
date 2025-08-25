# 2 Data

GAPIPE requires two kinds of data files: model data, such as stellar template librararies, and observation data. Paths to model data are defined in the configuration files, see Section 3, whereas observation data is accessed from the file system via a built-in file system crawler or Butler.

## 2.1 Model data

Model data should be stored in the `$GAPIPE_DIR/data` directory (`$PFSSPEC_DATA` should also point here), although any path can be used but it requires more manual configuration.

The suggested structure of the model data directory is as follows:

TODO: folder names below are preliminary and currently reflect the current development system.

```
$GAPIPE_DIR/data/
  + 2d/
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

### 2.1.1 Importing synthetic stellar grids

## 2.2 Observational data

PFS spectrum data files come with three different storage models:

* single spectrum files, such as PfsSingle, 
* multiple spectrum files, such as PfsArm and PfsMerged and 
* container files that contain multiple spectra, such as PfsCalibrated, which contain several PfsSingle spectra.

For flexibility, GAPIPE can handle any of these storage models, regardless of the level of processing of the data.

Container files are usually large and take significantly more time and I/O to load individual spectra from them, so we provide functionality to extract the individual spectra from the containers before processing. For example, the `gapipe-repo extract` command can be used to extract individual spectra from a PfsCalibrated container file into individual PfsSingle files. The files are extracted into the `$GAPIPE_WORKDIR` staging directory.

### 2.2.1 Using GAPIPE with Butler

TBW

### 2.2.2 Using GAPIPE without Butler

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

## 2.3 Obtaining data from the science platform

In order to process PFS stellar spectra on your own computer, install GAPIPE locally and download the observation data from the PFS Science Platform. This is done in several steps:

* Obtain the latest Butler database
* Configure GAPIPE to use the Butler database
* Generate a list of PfsConfig files and download them
* Download the observation data files

The PfsConfig files have to be downloaded first, as they contain the information about the individual objects. Once the PfsConfig files are available, the observation data can be filtered on a per-object basis.

Here we will assume that a directory named `$GAPIPE_DATADIR/pfs` exists and this is where the data will be downloaded to.

### 2.3.1 Obtaining the Butler database

After signing in to the Science Platform and use the direct file access link to download the Butler database and the yaml config file via a browser. They are located under the directory `/fileaccess/pfs/programs/<program_name>/2d/`. From the command-line the files can be downloaded using `curl` or `wget`. Command-line download tools require an Application Password that you can generate on the Science Platform main page.

    $ export PFS_PROGRAM="S25A-OT02"
    $ export PFSSP_TOKEN="<your_application_password>"
    $ mkdir -p $GAPIPE_DATADIR/2d && cd $GAPIPE_DATADIR/2d
    $ wget --header="Authorization: Bearer $PFSSP_TOKEN" "https://hscpfs.mtk.nao.ac.jp/fileaccess/pfs/programs/${PFS_PROGRAM}/2d/butler.yaml" -O butler.yaml
    $ wget --header="Authorization: Bearer $PFSSP_TOKEN" "https://hscpfs.mtk.nao.ac.jp/fileaccess/pfs/programs/${PFS_PROGRAM}/2d/gen3.sqlite3" -O gen3.sqlite3

### 2.3.2 Configuring GAPIPE to use the Butler database

Once the Butler database is downloaded, GAPIPE must be configured to use it. This is done by setting the `BUTLER_CONFIGDIR` environment variable to point to the directory where the Butler configuration file (`butler.yaml`) is located.

    $ export BUTLER_CONFIGDIR="$GAPIPE_DATADIR/2d"

You should also update the environment file `$GAPIPE_ROOT/configs/envs/default` to set this variable, so that it is set automatically when GAPIPE is initialized.

On the Science Platform, you can find the list of the directories, referring  to the observation runs such as `run21_June2025`, `run22_July2025` etc. These need to be added to the `$BUTLER_COLLECTIONS` environment variable, which is a colon-separated list of the paths relative to `$BUTLER_CONFIGDIR`. For example, if you want to use the aforementioned runs, you would set:

    $ export BUTLER_COLLECTIONS="run21_June2025"

Test the configuration by activating GAPIPE and running `gapipe-repo`:

    $ cd $GAPIPE_ROOT
    $ source ./bin/init
    $ gapipe-repo find-product PfsConfig --visit 123317 --format path

This should return the local path to the PfsConfig file for the given visit. To get the list of all PfsConfig files, you can run:

    $ gapipe-repo find-product PfsConfig --format path

### 2.3.3 Downloading the PfsConfig files

First, generate a list of PfsConfig files that you want to download. And save the list to a file. Since the paths returned by `gapipe-repo` are prefixed with the local path `$GAPIPE_DIR/data/2d`, you should use `sed` to remove the prefix and save the list of relative paths to a local file:

    $ gapipe-repo find-product PfsConfig --format path | sed "s|$GAPIPE_DATADIR/2d/||g" > run21_June2025_PfsConfig.txt

If you want to limit the visits to a particular sub-project, use the observation log files available in the `spt_ssp_observation` repository.

    $ VISITS="$(cat spt_ssp_observation/runs/2025-03/obslog/*.csv | grep SSP_GA | cut -d ',' -f 1)"
    $ echo $VISITS

Note the discrepancy here! We are getting the list of visits from run 2025-03 (which is run 21) but the data was reprocessed by the 2d pipeline in June, 2025 so it is `run21_June2025`.

Then run `gapipe-repo` to find the PfsConfig files for these visits and save the list to a file:

    $ gapipe-repo find-product PfsConfig --visit $VISITS --format path | sed "s|$GAPIPE_DATADIR/2d/||g" > run21_June2025_GA_PfsConfig.txt

Remember to use the full path to the repository in the above command.

Once the list is generated, you can download the PfsConfig files using `wget`:

    $ wget -i run21_June2025_GA_PfsConfig.txt --header="Authorization: Bearer $PFSSP_TOKEN" --base https://hscpfs.mtk.nao.ac.jp/fileaccess/pfs/programs/${PFS_PROGRAM}/2d/ -P $GAPIPE_DATADIR/2d/ --no-host-directories --cut-dirs=5 -x

### 2.3.4 Downloading the PfsCalibrated files

PfsCalibrated files can be downloaded similarly to PfsConfig files. First, generate a list of PfsCalibrated files for the visits you are interested in:

    $ gapipe-repo find-product PfsCalibrated --visit $VISITS --format path | sed "s|$GAPIPE_DATADIR/2d/||g" > run21_June2025_GA_PfsCalibrated.txt

Then download the files using `wget`:

    $ wget -i run21_June2025_GA_PfsCalibrated.txt --header="Authorization: Bearer $PFSSP_TOKEN" --base https://hscpfs.mtk.nao.ac.jp/fileaccess/pfs/programs/${PFS_PROGRAM}/2d/ -P $GAPIPE_DATADIR/2d/ --no-host-directories --cut-dirs=5 -x

Remember that these files are large and the data volume can easily be hundreds of gigabytes! You probably want to start the download in a screen session or submit it to a job queue.


### 2.3.5. Finding the data files for a specific object

If the PfsConfig files are available locally, we can use them to filter the observation data files for specific objects. Please note that the objIds usually differ from GAIA and HSC object ids to avoid conflicts.

TBW: Write about how the figure out the object IDs from the targeting feather files.

To get the list of visits that contain a specific object, run the following command:

    $ gapipe-repo find-object --visit 123424

To get the list of visits during which a particular object was observed, run:

    $ gapipe-repo find-object --objid 0x0000000600002fec

Note, that queries like these are relatively slow because they require reading the PfsConfig files and filtering the objects by their objId.