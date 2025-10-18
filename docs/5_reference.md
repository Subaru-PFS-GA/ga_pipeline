# Directories

The pipeline uses various directories to locate input data, write configuration files, log files, and plots, as well as the final data products. The following directories are used:

* `datadir`: root directory of the PFS data repository
* `workdir`: working directory for gapipe
* `outdir`: output directory for the final data products

The `rerundir` directory is relative to the above directories and is used to store the configuration files and log files for each run of the pipeline.

# Environment variables

* `GAPIPE_DATADIR`: root directory of the PFS data repository
* `GAPIPE_RERUNDIR`: rerun directory, relative to the data directories
* `GAPIPE_WORKDIR`: working directory for gapipe
* `GAPIPE_OUTDIR`: output directory for the gapipe data products

The following variable are only relevant for development, when running from "source", i.e. not as an installed package:

* `GAPIPE_CONDAPATH`: root directory of the Anaconda installation
* `GAPIPE_CONDAENV`: name of the conda environment
* `GAPIPE_ROOT`: root directory of the gapipe git repository
* `GAPIPE_DEBUGPORT`: TCP port used for debugging with `debugpy`

# GAPIPE commands

## Generic options

The following options are available for many commands:

* `--config`: configuration file
* `--top`: number of top-level objects to process
* `--dry-run`: only run the script but do not write any output

* `--log-level`: set the log level, one of `DEBUG`, `INFO`, `WARNING`, `ERROR`, `CRITICAL`

When logging is enabled (default), a log file is written to the current directory (except when otherwise specified). The log file name is generated from the command name and the current date and time. In addition to the log file, the log messages are also written to the standard output. The scripts also save the command-line string to the output directory, as well as the effective configuration options.

The following options are only relant for developers:

* `--debug`: enable debugging using `debugpy`, the debugger will listen on the port specified by the `GAPIPE_DEBUGPORT` environment variable
* `--profile`: enable profiling, the profiler output will be written to the current directory into files named `profile.*`

## Data filtering options

The following options are available for all commands that process data:

* `--catid`: category ID
* `--visit`: visit ID

## ˙gapipe-data`

## `gapipe-configure`

Note that the configuration template file specified by the `--config` option has to uses when running the `gapipe-configure` command. The directories specified in the configuration file have precedence over the environment variables (but can be overridden by the command line options). The configuration file is also used as a template to generate the configuration files for the individual objects for which the pipeline is executed.

Instead of the current directory, the `gapipe-configure` writes its log and other output to `workdir` whereas the generated configuration files are written to `workdir/rerundir`.

* **--datadir** *datadir*: the root directory of PFS DRP output files.

* **--rerundir** *rerundir*: directory relative to `datadir` that contains the output files, such as `PfsSingle` of a specifc rerun.

* **--workdir** *workdir*: directory to write the config files and auxiliary output files to for each object, basically anything other than the pfsStar files. Log files and figures are written to the `workdir/{objId}/log` and `workdir/{objId}/fig`, respectively.

* **--outdir** *outdir*: directory to write the output data files to. Directory naming follows the DRP standard of `{outdir}/pfsStar/{catId}/{tract}/{patch}/`

Note that all directory names can contain Python-style format strings that are replaced by the catId, tract, patch, objId, nVisit or pfsVisitHash. For example, `--outdir /datascope/subaru/user/dobos/gapipe/rerun/run16/20240709/pfsStar/{catid:05d}/{tract:05d}/{patch}/` will create a directory structure that contains the catId, tract, and patch values.

The object ID filters, with the exception of `--patch` can be specified as a list of single values and ranges, the latter being two numerical values separated by a hyphen. Any of the object ID filters can be omitted.

* **--catid** *catid1 [catid2 ...]*: List of PFS Catalog ID filters

* **--tract** *tract1 [tract2 ...]*: List of PFS tract filters

* **--patch** *patch1*: A single value for patch. If set, the patch will be limited to that value, if omitted, objects with any patch value will be included.

* **--objid** *objid1 [objid2 ...]*: List of PFS Object ID filters. Hex notation of IDs must start with `0x`.

## `gapipe-run`

## `gapipe-submit`

# Configuration files

TBW: supported formats, configuration options, configuration file template etc.