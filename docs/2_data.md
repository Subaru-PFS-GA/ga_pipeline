# 2 Data

GAPIPE requires two kinds of data files: model data, such as stellar template librararies, and observation data. Paths to model data are defined in the configuration files, see Section 3, whereas observation data is accessed from the file system via a built-in file system crawler or Butler.

## 2.1 Model data

Model data should be stored in the `$GAPIPE_DIR/data` directory (`$PFSSPEC_DATA` should also point here), although any path can be used but it requires more manual configuration.

The suggested structure of the model data directory is as follows:

TODO: folder names below are preliminary and currently reflect the current development system.

```
$GAPIPE_DIR/data/
  + instruments
    + hsc/
      + filters/
        - <filter_name>.txt
    + pfs/
      + arms/
      + noise/
      + psf/
        + import/
          + <psf_name>/
            - gauss.h5
            - pca.h5
  + pfsspec
    + models/
      + stellar/
        + grid/
          + <grid_name>/
            + <model_name>/
              - spectra.fits
```

## 2.2 Importing synthetic stellar grids

In order to use synthetic stellar spectra in GAPIPE, the grids must first be imported into the model data directory. This is done using the `pfsspec-import` command-line tool, which is part of PFSSPEC, the core library used by GAPIPE for stellar spectral analysis.

`pfsspec-import` currently supports importing the following synthetic stellar grids:

* BOSZ - https://archive.stsci.edu/hlsp/bosz
* PHOENIX - https://www2.astro.uni-jena.de/Users/theory/for2285-phoenix/grid.php
* GK2025 - not currently publicly available

The related references are:

* https://ui.adsabs.harvard.edu/abs/2017AJ....153..234B/abstract
* https://ui.adsabs.harvard.edu/abs/2024arXiv240710872M/abstract
* https://ui.adsabs.harvard.edu/abs/2013A&A...553A...6H/abstract

Each grid has its own set of atmospheric and abundance parameters that are hard-coded into the PFSSPEC library. The related classes can be found under the `pfs.ga.pfsspec.stellar.grid` namespace.

Downloading the synthetic stellar spectra from the distribution web sites is not covered here. Please refer to the respective web sites for instructions.

To import a synthetic stellar grid, run the following command from the root of the GAPIPE directory:

    $ cd $GAPIPE_ROOT
    $ pfsspec-import stellar-grid <model_name> basic --in <input_dir> --out <output-dir>

* `<model_name>` is the name of the synthetic stellar grid, e.g. `bosz`, `phoenix` or `gk2025`.

* The parameter `basic` specifies the type of preprocessing done on the spectra during import. Currently, only `basic` is supported, which means that the spectra are imported as-is without any additional processing.

* **--in <input_dir>** is the path to the directory where the downloaded synthetic stellar spectra are stored.

* **--out <output_dir>** is the path to the directory where the imported synthetic stellar grid will be stored.

Please note, that the output directory must not exist prior to running the import command. This is to prevent accidental overwriting of existing data.

The import script supports additional options, which can be listed by running:

    $ pfsspec-import stellar-grid <model_name> basic --help

Many of these options help fine-tuning the import by limiting the parameter ranges, the wavelength range, etc.

To limit the parameter ranges, use the following options. Note, that not all parameters are available in all grids. Some grids may have additional parameters that are not listed here. Set minimum and maximum to the same value to select a single value. This is very effective in reducing the size of the imported grid.

* **--M_H <min> <max>**
* **--T_eff <min> <max>**
* **--log_g <min> <max>**
* **--a_M <min> <max>**

Additionally, the following options are supported:

* **--resolution <number>** is the spectral resolution of the input synthetic spectra. Optional, not used for anything except for metadata.

* **--lambda <number> <number>** can be used to limit the wavelength range of the imported spectra. Provide the minimum and maximum wavelength in Angstroms.

* **--top <number>** can be used to limit the number of spectra imported. Useful for testing.

* **--threads <number>** can be used to specify the number of threads used during the import. Default is the number of CPU cores available but it might not perform well with a slow or network file system. Please set it to `--threads 1` when you experience poor performance.

* **--resume** can be used to resume an interrupted import. The import script will skip already imported spectra.

* **--compression <compression_type>** is used to specify the compression level of the HDF5 files. Use this option with large grids only, as it increases the import time significantly. Use `gzip` as the compression type.

* **--chunking spectrum** is used to specify how many spectra are stored in the HDF5 file. Use this option with large grids only, in conjunction with the **--compression** option.

### 2.2.1 Importing the BOSZ grid

To import the grid from FITS files, run the following command:

    $ pfsspec-import stellar-grid bosz basic --in ${PFSSPEC_DATA}/download/models/stellar/grid/bosz/bosz_50000_fits --out ${PFSSPEC_DATA}/models/stellar/grid/bosz/bosz_50000 --resolution 50000 --lambda 3000 15000 --format fits

### 2.2.2 Importing the PHOENIX grid

The following command imports the entire high-resolution PHOENIX grid into an uncompressed HDF5 format:

    $ pfsspec-import stellar-grid phoenix basic --in ${PFSSPEC_DATA}/download/models/stellar/grid/phoenix/phoenix_HiRes --out ${PFSSPEC_DATA}/models/stellar/grid/phoenix/phoenix_HiRes --resolution 50000 --lambda 3000 15000

### 2.2.3 Importing the GK2025 grid

This command imports the GK2025 grid into a compressed HDF5 format.

    $ pfsspec-import stellar-grid gk2025 basic --in ${PFSSPEC_DATA}/download/models/stellar/grid/gk2025/final/binned --out ${PFSSPEC_DATA}/models/stellar/grid/gk2025/gk2025_binned_compressed --resolution 30000 --chunking spectrum --compression gzip --threads 1

A smaller version of the same grid can be imported with this command:

    $ pfsspec-import stellar-grid gk2025 basic --in ${PFSSPEC_DATA}/download/models/stellar/grid/gk2025/final/binned --out ${PFSSPEC_DATA}/models/stellar/grid/gk2025/gk2025_small_binned_compressed --resolution 30000 --a_M 0.0 --C 0.0 --chunking spectrum --compression gzip --threads 1

## 2.3 Observational data

PFS spectrum data files come with three different storage models:

* single spectrum files, such as PfsSingle, 
* multiple spectrum files, such as PfsArm and PfsMerged and 
* container files that contain multiple spectra, such as PfsCalibrated, which contain several PfsSingle spectra.

For flexibility, GAPIPE can handle any of these storage models, regardless of the level of processing of the data.

Container files are usually large and take significantly more time and I/O to load individual spectra from them, so we provide functionality to extract the individual spectra from the containers before processing. For example, the `gapipe-repo extract` command can be used to extract individual spectra from a PfsCalibrated container file into individual PfsSingle files. The files are extracted into the `$GAPIPE_WORKDIR` staging directory.

### 2.3.1 Using GAPIPE with Butler

GAPIPE can use Butler to find input data products. For data staging and output data management, GAPIPE currently uses the file system. Data downloaded from the Science Platform will most likely come with a `butler.yaml` configuration file and a `gen3.sqlite` database file. To use GAPIPE with Butler the following environment variables must be set:

* `BUTLER_CONFIGDIR` should point to the directory where the `butler.yaml` file is located.
* `BUTLER_COLLECTIONS` should be set to a colon-separated list of the Butler collections

For example, in case of S25A-OT02, the directory structure of the data downloaded from the Science Platform looks like this:

pfs/programs/S25A-OT02/2d/
                          S25A_November2025/ 
                          run21_June2025/
                          run22_July2025/
                          run23_August2025/
                          butler.yaml
                          gen3.sqlite3

In this case, set the `BUTLER_CONFIGDIR` environment variable to point to the `2d` directory and set the `BUTLER_COLLECTIONS` environment variable to include the runs you want to use, for example:

    $ export BUTLER_CONFIGDIR="$GAPIPE_DATADIR/2d"
    $ export BUTLER_COLLECTIONS="run21_June2025:run22_July2025:run23_August2025"

You can test the configuration by running the `gapipe-repo` command-line tool, which is part of GAPIPE. In the current version, you explicitly need to set the `--butler` switch for the scripts to use Butler.

    $ cd $GAPIPE_ROOT
    $ source ./bin/init
    $ gapipe-repo info --butler
    $ gapipe-repo find-product PfsConfig --butler

The current version of GAPIPE can use a single Butler repository at a time, but multiple reduction runs can be included in the `BUTLER_COLLECTIONS` environment variable. The `gapipe-repo` command will search for the requested data products in the specified collections in the order they are listed in the `BUTLER_COLLECTIONS` variable.

### 2.3.2 Using GAPIPE without Butler

When not using Butler, the observation data is accessed from the file system by sweeping the file system for files that match the expected naming convention. The following environment variables must be set to point to the directories where the observation data is stored:

* `GAPIPE_DATADIR`: points to the root directory of the data repository
* `GAPIPE_RUN`: the name of the PIPE2D run, e.g. `run21_June2025`
* `GAPIPE_RUNDIR`: directory relative to `PFSSPEC_PFS_DATADIR` where the data for a specific PIPE2D run is stored, e.g. `run21_June2025`. Most of the time the directory name is the same as the run name but when the path is more than one directory deep, the run name cannot contain any slashed, hence it will be different. This is important when working with date from the Hilo servers.
* `GAPIPE_CONFIGRUN`: the name of the PIPE2D run that stored the pfsConfig files. It can be different for data repositories that are not from the Science Platform.
* `GAPIPE_CONFIGRUNDIR`: relative path to the directory where the PfsConfig files are stored. This is important when working with data from the Hilo servers, where the PfsConfig files are stored under a different processing run, e.g. `PFS_raw_pfsConfig`.

All settings above are unnecessary when using Butler.

In addition to the settings above, GAPIPE requires setting the following variables:

* `GAPIPE_WORKDIR`: full path to the directory where the temporary files will be stored during the processing.
* `GAPIPE_OUTDIR`: full path to the directory where the final results of the processing will be stored.
* `GAPIPE_GARUN`: name of the GAPIPE processing run. This use used to distinguish re-runs of the GA pipeline on the same data.
* `GAPIPE_GARUNDIR` relative path to the `GAPIPE_OUTDIR` where the results of a specific GA pipeline run will be stored; relative to `GAPIPE_OUTDIR` and `GAPIPE_OUTDIR`.

When not using Butler, objects are looked up from the `pfsConfig` files and then the file system is searched for the files that match the expected naming convention. File lookup can be tested by the `gapipe-repo` command-line tool, which is part of GAPIPE.

The environmental variables above can be overwritten on the command-line of most GAPIPE commands. The corresponding command-line arguments are

* `--datadir`
* `--run`
* `--rundir`
* `--configrun`
* `--workdir`
* `--outdir`
* `--garun`
* `--garundir`

Run the `gapipe-repo info` command to print the configuration. If using buler, test with `gapipe-repo info --butler` to verify that the Butler configuration is correct:

    $ gapipe-repo info

## 2.4 Obtaining data from the science platform

In order to process PFS stellar spectra on your own computer, install GAPIPE locally and download the observation data from the PFS Science Platform. This is done in several steps:

* Obtain the latest Butler database
* Configure GAPIPE to use the Butler database
* Generate a list of PfsConfig files and download them
* Download the observation data files

The PfsConfig files have to be downloaded first, as they contain the information about the individual objects. Once the PfsConfig files are available, the observation data can be filtered on a per-object basis.

Here we will assume that a directory named `$GAPIPE_DATADIR/pfs` exists and this is where the data will be downloaded to.

### 2.4.1 Obtaining the Butler database

After signing in to the Science Platform and use the direct file access link to download the Butler database and the yaml config file via a browser. They are located under the directory `/fileaccess/pfs/programs/<program_name>/2d/`. From the command-line the files can be downloaded using `curl` or `wget`. Command-line download tools require an Application Password that you can generate on the Science Platform main page.

    $ export PFS_PROGRAM="S25A-OT02"
    $ export PFSSP_TOKEN="<your_application_password>"
    $ mkdir -p $GAPIPE_DATADIR/2d && cd $GAPIPE_DATADIR/2d
    $ wget --header="Authorization: Bearer $PFSSP_TOKEN" "https://hscpfs.mtk.nao.ac.jp/fileaccess/pfs/programs/${PFS_PROGRAM}/2d/butler.yaml" -O butler.yaml
    $ wget --header="Authorization: Bearer $PFSSP_TOKEN" "https://hscpfs.mtk.nao.ac.jp/fileaccess/pfs/programs/${PFS_PROGRAM}/2d/gen3.sqlite3" -O gen3.sqlite3

### 2.4.2 Configuring GAPIPE to use the Butler database

Once the Butler database is downloaded, GAPIPE must be configured to use it. This is done by setting the `BUTLER_CONFIGDIR` environment variable to point to the directory where the Butler configuration file (`butler.yaml`) is located.

    $ export BUTLER_CONFIGDIR="$GAPIPE_DATADIR/2d"

You should also update the environment file `$GAPIPE_ROOT/configs/envs/default` to set this variable, so that it is set automatically when GAPIPE is initialized.

On the Science Platform, you can find the list of the directories, referring  to the observation runs such as `run21_June2025`, `run22_July2025` etc. These need to be added to the `$BUTLER_COLLECTIONS` environment variable, which is a colon-separated list of the paths relative to `$BUTLER_CONFIGDIR`. For example, if you want to use the aforementioned runs, you would set:

    $ export BUTLER_COLLECTIONS="run21_June2025"

Get the list of visits from the observation logs

    $ VISITS="$(cat spt_ssp_observation/runs/2025-03/obslog/*.csv | grep SSP_GA | cut -d ',' -f 1)"
    $ echo $VISITS

The SSP runs, with 2d processing versions are, so far:

```
<proposal>  <obs_date>    <2d_processing_version>         <spt_ssp_observation>             <comments>
S25A-OT02   2025-03       run21_June2025                  runs/2025-03/obslog/*.csv
            2025-05       run22_July2025                  runs/2025-05/obslog/*.csv
            2025-06       run23_July2025                                                    preview 
            2025-06       run23_August2025                runs/2025-06/obslog/*.csv
            2025-0[3-6]   S25A_November2025               runs/2025-0[3-6]/obslog/*.csv     all runs
S25B-OT02   2025-09       run24_November2025              runs/2025-09/obslog/*.csv
            2025-11       run25_November2025              runs/2025-11/obslog/*.csv
            2026-01
```

ObjID ranges for each run. These are defined in `ga_targeting`, in `python/pfs/ga/targeting/targets/ids.py`. You can also search for `ID_PREFIX` in all caps.

You can figure out the field observed during a run by running

    $ cd spt_ssp_observation
    $ ./scripts/sum_exp_time.py ./runs/2025-11/obslog/*.csv --name-pattern 'SSP_GA*'

```
<proposal> <obs_date>         <id_prefix>              <target>
S25A-OT02  2025-03     run21  0x0200000000             Draco dSph
                              0x0600000000             Ursa Minor dSph
           2025-05     run22  0x1000000000             MW Outer disk l=90 b=28
                              0x2000000000             MW Outer disk l=90 b=29
                                                       ** cross-calibration fields
           2025-06     run23  0x7000000000             MW Outer disk l=90 b=16
                              0x3000000000             MW Outer disk l=90 b=-28
                              0x0200000000             Draco dSph
                              0x0600000000             Ursa Minor dSph
 
S25B-OT02  2025-09     run24  0x100000100000000        M31 E0
                              0x100000200000000        M31 W0
                              0x100000300000000        M31 GSS0
                              0x100000400000000        M31 NWS0
           2025-11     run25  0x100000100000000        M31 E0
                              0x50000000000            MW outerdisk_l180_b16
                              0x51000000000            MW outerdisk_l180_b17
           2026-01     run26  0x0700000000             Sextans dSph
```

Test the configuration by activating GAPIPE and running `gapipe-repo`:

    $ cd $GAPIPE_ROOT
    $ source ./bin/init
    $ gapipe-repo find-product PfsConfig --visit $VISITS --format path

This should return the local path to the PfsConfig file for the given visit. To get the list of all PfsConfig files, you can run:

    $ gapipe-repo find-product PfsConfig --format path

### 2.4.3 Downloading the PfsConfig files

First, generate a list of PfsConfig files that you want to download. And save the list to a file. Since the paths returned by `gapipe-repo` are prefixed with the local path `$GAPIPE_DIR/data/2d`, you should use `sed` to remove the prefix and save the list of relative paths to a local file:

    $ gapipe-repo find-product PfsConfig --butler --format path | sed "s|$GAPIPE_DATADIR/2d/||g" > run21_June2025_PfsConfig.txt

If you want to limit the visits to a particular sub-project, use the observation log files available in the `spt_ssp_observation` repository.

    $ VISITS="$(cat spt_ssp_observation/runs/2025-03/obslog/*.csv | grep SSP_GA | cut -d ',' -f 1)"
    $ echo $VISITS

Note the convention here! We are getting the list of visits from run 2025-03, which is run 21, but the data was reprocessed by the 2d pipeline in June, 2025 so it is `run21_June2025`.

You can specify more than one collection separated by a colon in the `BUTLER_COLLECTIONS` environment variable:

    $ export BUTLER_COLLECTIONS="run21_June2025:run22_July2025"

Then run `gapipe-repo` to find the PfsConfig files for these visits and save the list to a file:

    $ gapipe-repo find-product PfsConfig --visit $VISITS --format path --butler | sed "s|$GAPIPE_DATADIR/||g" > run21_June2025_GA_PfsConfig.txt

Remember to use the full path to the repository in the above command.

Once the list is generated, you can download the PfsConfig files using `wget`:

    $ wget -i run21_June2025_GA_PfsConfig.txt --header="Authorization: Bearer $PFSSP_TOKEN" --base https://hscpfs.mtk.nao.ac.jp/fileaccess/pfs/programs/${PFS_PROGRAM}/2d/ -P $GAPIPE_DATADIR/ --no-host-directories --cut-dirs=5 -x

### 2.4.4 Find individual objects

If the PfsConfig files are available locally, we can use them to filter the observation data files for specific objects. Please note that the objIds usually differ from GAIA and HSC object ids to avoid conflicts.

TBW: Write about how the figure out the object IDs from the targeting feather files.

To get the list of objects that were observed during a specific visit, run the following command:

    $ gapipe-repo find-object --visit 123317 --butler

To get the list of visits that contain a specific object, run the following command:

    $ gapipe-repo find-object --visit $VISITS --objid 0x0000000600002fec --butler

In the last query, it is necessary to provide the list of visits to limit the search space to the filed that have been downloaded locally, otherwise the command will try to search all the PfsConfig files in the Butler collections defined in the `BUTLER_COLLECTIONS` environment variable.

Note, that queries like these are slow because they require reading the PfsConfig files and filtering the objects by their objId.

### 2.4.5 Downloading the PfsCalibrated files

PfsCalibrated files can be downloaded similarly to PfsConfig files. First, generate a list of PfsCalibrated files for the visits you are interested in:

    $ gapipe-repo find-product PfsCalibrated --visit $VISITS --format path --butler | sed "s|$GAPIPE_DATADIR/||g" > run21_June2025_GA_PfsCalibrated.txt

Then download the files using `wget`:

    $ wget -i run21_June2025_GA_PfsCalibrated.txt --header="Authorization: Bearer $PFSSP_TOKEN" --base https://hscpfs.mtk.nao.ac.jp/fileaccess/pfs/programs/${PFS_PROGRAM}/2d/ -P $GAPIPE_DATADIR/ --no-host-directories --cut-dirs=5 -x

Remember that these files are large and the data volume can easily be hundreds of gigabytes! You probably want to start the download in a screen session or submit it to a job queue.

## 2.5 Searching the data repository

GAPIPE provides the `gapipe-repo` command-line tool to search the data repository for objects and visits. This tool can either use Butler or search the file system directly, depending on the configuration. Before configuring and executing the pipeline, it is recommended to test the data repository access by running the `gapipe-repo` command first.

The following command locates the PfsCalibrated file for a given visit or list of visits:

    $ gapipe-repo find-product PfsCalibrated --visit $VISIT --format path --butler

where `$VISIT` is the visit ID or a list of visit IDs. You can get the list of visits from the `obslog` files, for example:

    $ VISITS="$(cat spt_ssp_observation/runs/2025-03/obslog/*.csv | grep SSP_GA | cut -d ',' -f 1)"

The `--format path` option specifies that the output should be the file path of the PfsCalibrated file.

To get the available information on a specific object, you can use the `find-object` command:

    $ gapipe-repo find-object --visit $VISIT --objid $OBJID --butler

Specify the `--format` option to get the output in a specific format, such as `table`, `json`, or `path`. The default format is `table`.

## 2.6 Workdir and output data organization

GAPIPE doesn't currently use Butler for output data management. Instead, it uses two directories on the file system to store the temporary and final results of the processing. The directory structure and file naming roughly follows what was used for the PFS data before upgrading to Butler gen3.

### 2.6.1 Output directories

TODO: review this part

GAPIPE uses two output directories (and many subdirectories withing them) to store the temporary and final results of the processing:

* **Work directory**: The `workdir` is used to store temporary files during the processing of each object. It is specified in the configuration file and can be overridden from the command-line. The default value is `$GAPIPE_WORKDIR` and it can be overriden from the configuration template (see below) or the `--workdir` command-line argument.

* **Output directory**: The `outdir` is used to store the final results of the processing, such as the processed spectra and the final catalog files. It is specified in the configuration file and can be overridden from the command-line. The default value is `$GAPIPE_OUTDIR` and it can be overridden from the configuration template (see below) or the `--outdir` command-line argument.

* **Run directory**: This directory refers to the pipe2d processing run. Since GAPIPE doesn't currently use Butler for output data, a directory is created for each 2dpipe run under `workdir` and `outdir`. Note, that the run directory is not currently figured out from the Butler collections, so it must be specified manually in the configuration file or from the command-line using the `--rundir` argument.

### 2.6.2 GAPIPE file names

Examples:

TODO: update

```
run21_June2025/pfsStarCatalog/10092/051-0x310d8290f6dc2641/pfsStarCatalog-10092-051-0x310d8290f6dc2641.fits
```

## 2.7 Extracting single-object data products from container files

GAPIPE processes spectra on a per-object basis whereas the 2D pipeline works on a per-exposure basis and stores the results in large container files. To speed up data access by the batch system, it might be useful to extract the single-object data products from the container files before running the pipeline. This can be done using the `gapipe-repo` command-line tool. Below are a few examples.

Note, that it is not necessary to extract the single-object data products from the container files before running the pipeline, but it can speed up the processing and reduce memory use.

Once the PfsCalibrated files are downloaded, you can extract the individual PfsSingle files using the `gapipe-repo extract-product` command. This should be done by 2dpipe processing runs, so let's define a few variables first:

    $ OBSRUN="2025-03"
    $ RUN="S25A_November2025"
    $ BUTLER_COLLECTIONS="$RUN"
    $ VISITS="$(cat spt_ssp_observation/runs/$OBSRUN/obslog/*.csv | grep SSP_GA | cut -d ',' -f 1)"

To extract all PfsSingle files for a specific visit and catId, run:

    $ gapipe-repo extract-product PfsCalibrated,PfsSingle --visit 122805 --catid 10092 --rundir $RUN --log-level DEBUG

Extract all the PfsSingle files from PfsCalibrated for a given list of visits and a catalog ID by running.

    $ gapipe-repo extract-product PfsCalibrated,PfsSingle --visit $VISITS --catid $CATID

This command will write the PfsSingle files to the `workdir/rundir` directory specified in the configuration file. The list of objects can further be filtered by target type, spectrograph arm, obcode, etc. For example, to extract only the PfsSingle files for science targets, you can run:

    $ gapipe-repo extract-product PfsCalibrated,PfsSingle --visit $VISITS --catid $CATID --targettype SCIENCE --rundir $RUN

Similarly, you can extract FLUXSTD targets by setting `--targettype FLUXSTD`.

PfsSingle files for all visits of a single objects can be extracted by running:

    $ gapipe-repo extract-product PfsCalibrated,PfsSingle --visit $VISITS --objid "$OBJID" --rundir $RUN

To get some progress information during the extraction, use the `--log-level DEBUG` option or try `--progress`.

Please note that extracting files for a large number of objects and visits can take a long time and consume a lot of disk space.

### 2.7.1 Batch processing

Extracting lots of PfsSingle files can take a long time, especially when the number of visits is large. To parallelize the process, you can schedule a batch job for each visit to extract the PfsSingle files from PfsCalibrated. This can be done by using the `--batch` option with the `gapipe-repo extract-product` command.

    $ gapipe-repo extract-product PfsCalibrated,PfsSingle --visit $VISITS --catid $CATID --rundir $RUN --batch slurm --partition v100 --cpus 2 --memory 8G

Here the `--batch slurm` option specifies that the extraction should be run as a batch job on the SLURM scheduler, and the `--partition` option specifies the SLURM partition to use. The `gapipe-repo` command will generate a batch script for each visit and submit it to the SLURM scheduler.

TBW: Other batch systems are to be implemented.









