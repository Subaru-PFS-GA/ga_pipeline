# Using the batch script

To automate processing of SSP and PI stellar spectra, we provide a batch script that uses configuration files to process entire observing runs, individual fields or individual objects.

The steps of processing are the following:

* Create a configuration for each output catalog you want to produce.
* Extract the pfsSingle files for the visits you want to process.
* Generate the GA pipeline config files for each object that matches the search filters.
* Run the pipeline on each object, either by executing the `gapipe-run` script on each config file or by submitting a batch job.
* Generate a catalog from the pfsStar files.

## Config file format

Config files are standard Python and bash scripts. The directory structure of a GAPIPE batch configuration is the following:

<ConfigRoot>/
    common.sh
    common.py
    <config1>.py
    <config2>.py
    garuns/
        <garun1>.sh
        <garun2>.sh
        ...

The directory `<ConfigRoot>` should match the name of the PIPE2D data release, e.g. `S25A_November2025`. This is also the name of what we call the PIPE2D data processing run.

The file `common.sh` contains the configuration variables that are common to all GA pipeline runs, such as the data locations and the observing run information.

The file `common.sh` should export at least the following variables that define the data locations and the observing run information:

* `GAPIPE_DATADIR`: The full absolute path to the directory under which the individual PIPE2D runs are stored. In case of data downloaded from the science platform, it should be something like `${GAPIPE_DATAROOT}/S25A-OT02/2d`, i.e. the same location where `butler.yaml˙ is.
* `GAPIPE_RUN`: The name of the PIPE2D data processing run, e.g. `S25A_November2025`. In case of data downloaded from the science platform, it should be the same as the name of the directory under which the individual PIPE2D runs are stored, e.g. `S25A_November2025`.
* `GAPIPE_RUNDIR`: The directory where the PIPE2D processing run is stored. In case of data downloaded from the science platform, it should be the same as `GAPIPE_RUN`, e.g. `S25A_November2025`.
* `GAPIPE_CONFIGRUN`: The name of the run where the `pfsConfig` files are stored. In case of data downloaded from the science platform, it should be the same as `GAPIPE_RUN`, e.g. `S25A_November2025`.
* `GAPIPE_CONFIGRUNDIR`: The directory of the run where the `pfsConfig` files are stored. In case of data downloaded from the science platform, it should be the same as `GAPIPE_RUN`, e.g. `S25A_November2025`.
* `GAPIPE_USE_BUTLER`: Whether to use the butler or direct file system sweeping to locate data products. If using the butler, the configuration should also export the following variables:
* `BUTLER_CONFIGDIR`: The directory where the butler configuration is stored, typically the same as `GAPIPE_DATADIR`.
* `BUTLER_COLLECTIONS`: The name of the butler collections to use, typically the same as `GAPIPE_RUN`.
* `GAPIPE_ALL_VISITS`: The script `common.sh` should also export the variable `GAPIPE_ALLVISITS` which will be used to extract the pfsSingle files for the visits to process. For example, you can find all relevant visits with:

```bash
OBSLOGS="${GAPIPE_OBSLOGDIR}/runs/2025-03/obslog/*.csv ${GAPIPE_OBSLOGDIR}/runs/2025-06/obslog/*.csv"
OBSPREFIX="SSP_GA_"
export GAPIPE_ALLVISITS=($(cat ${OBSLOGS} | grep "${OBSPREFIX}" | cut -d ',' -f 1))
```

The file `common.py` contains the configuration for the GA pipeline that is common to all runs, such as the parameters for tempfit, etc. The configuration in `common.py` can be further customized for each run by overriding certain sections of the configuration in files `<config1>.py`, `<config2>.py`, etc. Typically, you would have one large `common.py` file that contains the configuration for all runs and then have small config files that only override a few parameters for each run. For example, you can create a `b.py` to reduce the blue arm only and a `b_mr.py` to reduce the blue arm with the together with the medium-resolution arm. You can define configurations to use different stellar template libraries, fit different wavelength ranges, etc.

The files `<garun1>.sh`, `<garun2>.sh`, ... are the data configurations for each GA pipeline run. These files are sourced by the batch script and each configuration can define multiple data configurations. Use these files to define subsets of each PIPE2D run. For example, you can define one configuration for each target dSph, each field, or each observing run. Each configuration should define the list of visits to process, the target catalog and assignment files to use, and the search filters for selecting the objects to process.

The `<garun*>.sh` files should populate the following arrays for each configuration:

* `PROPOSAL`: The name of the proposal, e.g. `S25A-OT02`.
* `RUN`: The name of the PIPE2D data processing run, e.g. `S25A_November2025`.
* `RUNDIR`: For standard processing runs from the Science Platform, should be the same as `RUN`. For custom processing runs, should be the directory where the `pfsSingle` files are stored.
* `CONFIGRUN`: For standard processing runs from the Science Platform, should be the same as `RUN`. For custom processing runs, should be the name of the run where the `pfsConfig` files are stored.
* `CONFIGRUNDIR`: For standard processing runs from the Science Platform, should be the same as `RUN`. For custom processing runs, should be the directory where the `pfsConfig` files are stored.
* `GARUN`: The name of the GA pipeline run, e.g. `dSph_dra_2025-03_S25A_November2025_mr`.
* `GARUNDIR`: The directory where the GA pipeline output will be stored. By default, this should be the same as `GARUN`.
* `OBSLOGS`: The list of paths to the observation log files to use for selecting the visits to process. You can use a glob pattern to select multiple files, e.g. `${GAPIPE_OBSLOGDIR}/runs/${OBSDATE}/obslog/*.csv`.
* `TARGETLISTS`: To generate a final catalog, you can use the target lists from the netflow output. This is only relevant for the fields for which the fiber assignment was done with the GA version of netflow.
* `ASSIGNMENTS`: To generate a final catalog, you can use the assignment files from the netflow output. This is only relevant for the fields for which the fiber assignment was done with the GA version of netflow.
* `VISITS`: The list of visits to process. You can use a command like `$(cat ${OBSLOGS[0]} | grep ${OBSPREFIX} | cut -d ',' -f 1)` to select the visits from the observation log files that match a certain prefix.
* `CATID`: The catalog ID to use for the GA science targets. You can list more than one catalog ID separated by space or use ranges like `10092-10095`. In the latter case, there is no space between the start and end of the range, but there should be a space between different ranges or individual catalog IDs.
* `OBJID`: The object ID to use for the GA science targets. You can list more than one object ID separated by space or use ranges like `0x0200000000-0x02FFFFFFFF`. In the latter case, there is no space between the start and end of the range, but there should be a space between different ranges or individual object IDs.

The order in which the files are sourced by the batch script is the following:

1. The environment configuration is sources when you initialize the terminal sessions with `source bin/init`
2. `batch.sh` sources `common.sh` which should export the common configuration variables.
3. `batch.sh` sources `garun1.sh` which should populate the arrays for each GAPIPE processing run.

## Extracting the pfsSingle files

PIPE2D generate a pfsCalibrated file for each visit, which is just a large container of pfsSingle objects. pfsCalibrated files are substatial and since the GA pipeline processes the data on an object-by-object basis, it is more efficient to extract the pfsSingle files for the visits you want to process and store them in a separate directory. The batch script wraps the `gapipe-repo extract-prduct` command to load the pfsCalibrated files for a given list of visits and extract all, or a subset of the pfsSingle files and save them into a directory.

## Using Butler

## Using direct file access

## Some useful command-line arguments

--dry-run
--progress
--yes
--log-level DEBUG
--top 10
--debug
--profile


## Extract the pfsSingle files

    $ ./scripts/batch.sh extract S25A_November2025