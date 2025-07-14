# 1 Installation and configuration

GAPIPE can be installed on a vanila Anaconda (Miniconda) installation or on top of the Rubin (LSST) software stack. In the former case, LSST components such as Butler data access, won't be available, others, such as the PFS `datamodel` package will be installed directly from the github source.

GAPIPE depends on the PFSSPEC Python packages as well as on a set of data files that includes configuration files and sythetic stellar libraries. The data files are distributed separately.

The install script `./setup/install.sh` provides a convenient way to install the GAPIPE software stack.

The current version of GAPIPE is available in the form of Python source code only and the install script installs it that way, directly cloning it from github. Later, we will provide versioned packages for both Anaconda and EUPS (LSST's package manager).

## 1.1 Installing GAPIPE

### 1.1.1 General installation procedure

In order to check out the source code of GAPIPE and the dependent libraries from github, you must have a valid SSH key that's configured under your github profile. Please refer to [GitHub's SSH key setup documentation](https://docs.github.com/en/authentication/connecting-to-github-with-ssh/about-ssh) for details.

To install GAPIPE in a fresh environment, clone the github repository from `https://github.com/Subaru-PFS-GA/ga_pipeline` and run the script `./setup/install.sh`.

    $ mkdir gapipe && cd gapipe
    $ git clone git@github.com:Subaru-PFS-GA/ga_pipeline.git
    $ cd ga_pipeline
    $ export GAPIPE_DIR=<path_to_gapipe_installation>
    $ ./setup/install.sh -d $GAPIPE_DIR --lsst

This will install the complete LSST stack, PFS pipe2d stack and GAPIPE under the selected directory. Tha installation is fully automated and will ask no questions. The full process will take approximately five minutes without displaying any status information while installing Miniconda, so be patient.

## 1.1.2 Configuring the GAPIPE installation

Please note, that this configuration step is independent from the configuration step described in Section 3. Here we set some data paths only but not the settings of the actual GAPIPE spectrum processing steps.

GAPIPE stores a handful of settings in environmental variables, such as the location of data files. While the installer script generates the default configuration file `$GAPIPE_DIR/src/ga_pipeline/configs/default`, files paths in it need to be manually configured after installation. The following environmental variable must be set. The value of some of these variables can be overridden from the command-line when running GAPIPE.

Environment variables not listed here are already configure by their correct value by the install script and must not be changed.

* `GAPIPE_DATADIR` when not using the LSST stack with Butler, this path must point to the directory with observation data. Please refer to Section 2 for details.
* `GAPIPE_RERUN` must be the name of the rerun to be processed. This is just a default value that can be overriden from the command-line when GAPIPE is executed. This variable is not used when GAPIPE is installed on top of the LSST stack and Butler is available.
* `GAPIPE_RERUNDIR` must be path to the rerun files to be processed. This is just a default value that can be overriden from the command-line when GAPIPE is executed. This variable is not used when GAPIPE is installed on top of the LSST stack and Butler is available.
* `GAPIPE_WORKDIR` must point to an existing directory which will be used for data staging during GAPIPE execution.
* `GAPIPE_OUTDIR` must point to an existing directory which will be used to write the GAPIPE output files such as the results of processing of the spectrum of each object and the final catalog files.
* `PFSSPEC_DATA`  must point to the data directory where model files such as synthetic stellar grids are stored. Please refer to Section 2 for details.

When running on top of the LSST stack and Butler is available, the following variables must also be set.

* `BUTLER_CONFIGDIR` must point to the directory where the Butler configuration file is located at.
* `BUTLER_COLLECTIONS` this variable must be a colon-separated list of the paths, relative to `$BUTLER_CONFIGDIR`, that contain the Butler collections to be served while running GAPIPE.

When not using Butler, the following variables must be set to find data products by sweeping the file system.

* `PFSSPEC_PFS_DATADIR` must point to the root of the observation data directory (datastore).
* `PFSSPEC_PFS_RERUNDIR` must be the path to the directory, relative to `$PFSSPEC_PFS_DATADIR`, that contains the rerun we want to process
* `PFSSPEC_PFS_RERUN` must be the name of the rerun as it appears in the name of the data files.
* `PFSSPEC_PFS_DESIGNDIR` must be the path to the directory, relative to `$PFSSPEC_PFS_DATADIR`, that contains the PfsDesign files.
* `PFSSPEC_PFS_CONFIGDIR` must be the path to the directory, relative to `$PFSSPEC_PFS_DATADIR`, that contains the PfsConfig files.

### 1.1.3 Activating the GAPIPE environment

Once GAPIPE is installed, you have to initialize in every new terminal session by entering the GAPIPE installation directory and sourcing the `init` script. Please note that initialization is not a one-time thing. Since GAPIPE is installed as Python source, currently every new terminal session must be initialized this way. This process will likely be simplified once pre-packaged version of GAPIPE become available.

    $ cd $GAPIPE_DIR/src/ga_pipeline
    $ source ./bin/init

Please note, that `./bin/init` must be sourced directly from the `src/ga_pipeline` directory under installation directory. Do not try to source it from the directory you originally clone `ga_pipeline` to as the script will fail.

### 1.1.4 Testing the installation

Test the installation by running the command `gapipe-repo`.

    $ gapipe-repo find-product PfsConfig --visit 123317 --format path

This should report no errors and print the path to the PfsConfig file for the specified visit.

### 1.1.5 Install script options

The `install.sh` accepts the following command-line arguments:

* `-d`, `--dir`: Target directory. Defaults to `$HOME/gapipe`
* `-f`, `--force`: Force installation on an existing directory.
* `--conda-dir`: Use a different directory to automatically install Miniconda. Use the existing conda installation if one is already available at this path. Also applies to the LSST stack, please refer to the documentation of `lsstinstall`. The default is to install a new conda stack. If a conda installation is already activated, the GAPIPE installer will deactivate it.
* `-e`, `--env`: Name of the conda environment. If an environment of the same name is found, GAPIPE dependencied will be installed and GAPIPE will be configured to use it, otherwise a new environment with the specified name will be installed. Also applied to the LSST stack, please refer to the documentation of `lsstinstall`. The default environment name is `gapipe` when installing on top of the vanila Anaconda stack and `lsst-scipipe-<version>` when installing on the LSST stack.
* `--lsst`: Install on top of the LSST stack (default).
* `--no-lsst`: Install on top of a vanila Anaconda stack.
* `--source`: Install GAPIPE and its dependencies as Python source code, for development, as opposed to installation from packages.
* `--conda`: Reserved for future use.
* `--eups`: Reserved for future use.
* `--debug`: Turn on more detailed logging.

All other arguments are ignored.

## 1.2 Customizing the installation

### 1.2.1 Installing on top of the LSST stack

TBW

- install from scratch
- install on top of existing stack

### 1.2.2 Installing on top of the vanila Anaconda stack

TBW

### 1.2.3 Installing in Docker

TBW

### 1.2.4 Installing for development

TBW

## 1.3 Updating GAPIPE

The installation script currently does not support version updates. For now, please reinstall the entire stack from scratch if a new version is needed.

## 1.4 Installation directory structure

The install script will create the following directory structure, when executed with default settings:

```
$GAPIPE_DIR/
  + bin/
  + data/
    + pfsspec/
  + out/
  + src/
    + datamodel/
    + ga_chemfit/
    + ga_pfsspec_all/
    + ga_pipeline/
  + stack/
    + conda/
      + env/
        + lsst-scipipe-<version>
  + work/
```

The directories are the following.

* `./bin` is reserved for future use
* `./data` is to store GAPIPE data files, except observation data
* `./out` is to store the output files of GAPIPE processing.
* `./src` is to store the source code of GAPIPE and its dependencies
* `./stack` is to store the installed software stack, such as the LSST stack
* `./work` is to store temporary files during GAPIPE processing