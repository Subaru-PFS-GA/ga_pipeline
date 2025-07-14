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

* --log-level: set the log level, one of `DEBUG`, `INFO`, `WARNING`, `ERROR`, `CRITICAL`

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

## `gapipe-run`

## `gapipe-submit`

# Configuration files

TBW: supported formats, configuration options, configuration file template etc.