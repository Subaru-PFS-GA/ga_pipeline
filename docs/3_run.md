# 3 Running the GA Pipeline

Executing the pipeline consists of multiple steps:

* First, the individual spectra are extracted from the container files, such as PfsSingle files from PfsCalibrated files. This step is optional but can speed up the processing and reduce memory use. Running `gapipe-repo extract` extracts the individual spectra into the `$GAPIPE_WORKDIR` staging directory.

* Second, a configuration file is created for every object, which lists all visits (exposures) that are to be processed in a single GAPIPE run. The `./bin/configure` script automates creating the configuration files in batch mode.

* A batch job for each of the configurations (objects) is submitted. Alternatively, individual and batch jobs can be manually executed from the command-line. The command-line script `./bin/run` executes the pipeline on a single object, using the configuration file as input.

3. Once all objects of a given run are processed, a final command is executed to collect (a subset) of the results into a single catalog file.

## 3.1 Initializing the environment

Please note that the following is preliminary and assumes that GAPIPE is installed from "source" and not as a pre-packaged Python package, either Acanconda or EUPS.

In development mode, a prerequisite to initializing the environment is a proper configuration. Please refer to Section 1 on how to configure the GAPIPE environment. When properly configured, the environment can be initialized from the root directory, source the init script:

    $ cd $GAPIPE_DIR/src/ga_pipeline
    $ source bin/init

where `$GAPIPE_DIR` is the root directory of the GAPIPE installation.

This should activate the python environment defined in the default environment file and set the `PYTHONPATH` variable for all dependencies that are available as source and not as pre-installed packages.

Once the environment is initialized, a set of commands starting with `gapipe-` are available in the command-line.

For the sake of simplicity, we define a few environmental varibles that are used in the examples of the following sections. Some of these read the observation logs available in the `spt_ssp_observation` repository. The IDs refer to valid data in the March, 2025 observing run.

    $ CATID="10092"
    $ OBJID="0x6000030bf"
    $ OBSLOGS="spt_ssp_observation/runs/2025-03/obslog/*.csv"
    $ VISIT="123317"
    $ VISITS="$(cat $OBSLOGS | grep SSP_GA | cut -d ',' -f 1)"
    

## 3.2 Searching the data repository

GAPIPE provides the `gapipe-repo` command-line tool to search the data repository for objects and visits. This tool can either use Butler or search the file system directly, depending on the configuration. Before configuring and executing the pipeline, it is recommended to test the data repository access by running the `gapipe-repo` command first.

The following command locates the PfsCalibrated file for a given visit or list of visits:

    $ gapipe-repo find-product PfsCalibrated --visit $VISIT --format path

where `$VISIT` is the visit ID or a list of visit IDs. You can get the list of visits from the `obslog` files, for example:

    $ VISITS="$(cat spt_ssp_observation/runs/2025-03/obslog/*.csv | grep SSP_GA | cut -d ',' -f 1)"

The `--format path` option specifies that the output should be the file path of the PfsCalibrated file.

To get the available information on a specific object, you can use the `find-object` command:

    $ gapipe-repo find-object --visit $VISIT --objid $OBJID

Specify the `--format` option to get the output in a specific format, such as `table`, `json`, or `path`. The default format is `table`.

## 3.3 Output directories

GAPIPE uses two output directories (and many subdirectories withing them) to store the temporary and final results of the processing:

* **Work directory**: The `workdir` is used to store temporary files during the processing of each object. It is specified in the configuration file and can be overridden from the command-line. The default value is `$GAPIPE_WORKDIR` and it can be overriden from the configuration template (see below) or the `--workdir` command-line argument.

* **Output directory**: The `outdir` is used to store the final results of the processing, such as the processed spectra and the final catalog files. It is specified in the configuration file and can be overridden from the command-line. The default value is `$GAPIPE_OUTDIR` and it can be overridden from the configuration template (see below) or the `--outdir` command-line argument.

## 3.4 Extracting single-object data products from container files

GAPIPE processes spectra on a per-object basis whereas the 2D pipeline works on a per-exposure basis and stores the results in large container files. To speed up data access by the batch system, it might be useful to extract the single-object data products from the container files before running the pipeline. This can be done using the `gapipe-repo` command-line tool. Below are a few examples.

Note, that it is not necessary to extract the single-object data products from the container files before running the pipeline, but it can speed up the processing and reduce memory use.

Extract all the PfsSingle files from PfsCalibrated for a given list of visits and a catalog ID by running.

    $ gapipe-repo extract-product PfsCalibrated,PfsSingle --visit $VISITS --catid $CATID

This command will write the PfsSingle files to the `workdir` directory specified in the configuration file. The list of objects can further be filtered by target type, spectrograph arm, obcode, etc. For example, to extract only the PfsSingle files for science targets, you can run:

    $ gapipe-repo extract-product PfsCalibrated,PfsSingle --visit $VISITS --catid $CATID --targettype SCIENCE

Similarly, PfsSingle files for all visits of a single objects can be extracted by running:

    $ gapipe-repo extract-product PfsCalibrated,PfsSingle --visit $VISITS --objid "$OBJID"

Use the `--progress` option to see the progress of processing and the files being extracted.

Please note that extracting files for a large number of objects and visits can take a long time and consume a lot of disk space.

### 3.4.1 Batch processing

Extracting lots of PfsSingle files can take a long time, especially when the number of visits is large. To parallelize the process, you can schedule a batch job for each visit to extract the PfsSingle files from PfsCalibrated. This can be done by using the `--batch` option with the `gapipe-repo extract-product` command.

    $ gapipe-repo extract-product PfsCalibrated,PfsSingle --visit $VISITS --catid $CATID --batch slurm --partition v100

Here the `--batch slurm` option specifies that the extraction should be run as a batch job on the SLURM scheduler, and the `--partition` option specifies the SLURM partition to use. The `gapipe-repo` command will generate a batch script for each visit and submit it to the SLURM scheduler.

TBW: Other batch systems are to be implemented.

## 3.5 Configure GAPIPE to process a batch of objects

Before executing the pipeline, a configuration file must be created for each object that is to be processed. The configuration file contains the list of visits (exposures) that are to be processed in a single GAPIPE run and all necessary settings for the pipeline spectrum processing steps. Since many of the configuration options are common for all objects, the configuration file is generated from a template configuration file. Some templates are available in the `configs` directory of the GAPIPE source code.

To run the configuration script, all data files must be available, even when using Butler to access the data, because the `PfsConfig` files are necessary to generate the configuration files. In addition, files containing the photometric stellar parameters (to define the priors), as well as observational parameters, 

The `gapipe-configure` script  generates the pipeline config files for each object that matches the search filters. For example:

    $ gapipe-configure --config ./configs/gapipe/run21_June2025/single.py --objid $OBJID --visit $VISITS --obs-logs $OBSLOGS

* The `--config` parameter sets the file that serves as the template of the config files generated by the `configure` script. Config files can have the extension `.yaml`, `.json` or `.py`. In case of a Python file, the file is loaded and executed as a Python script that must create the global variable `config`.

To generate the configuration files for all objects that match the search filters, you should remove the `--objid` argument. For example:

    $ gapipe-configure --config ./configs/gapipe/run21_June2025/single.py --visit $VISITS --obs-logs $OBSLOGS --catid $CATID --targettype SCIENCE

Specify the `--progress` option to see a progress bar and the number of objects processed so far. You can use the `--top` option to limit the number of objects to process. This is useful for testing the configuration script and the pipeline processing steps.

See the reference section for a complete list of command-line arguments.

## 3.5.1 Specifying the stellar parameters to define the priors

In order to generate config files with parameter priors that depend on the photometric stellar parameters, known velocities, etc, use the `--stellar-params` and `--obs-params` options to specify the files containing the stellar parameters and observational parameters, respectively. For example:

    $ gapipe-configure --config ./configs/gapipe/run21_June2025/single.py --objid $OBJID --visit $VISITS --stellar-params stellar_params.csv --obs-params obs_params.csv

TBW: write about the format of the stellar parameters and observational parameters files.

## 3.4 Run processing object by object

The pipeline can be executed on individual object or submitted as a batch job. To see how batch jobs are submitted, please refer to Section 4.4.

To execute the pipeline on a single object, use the `run` script and pass the path to the configuration file as an argument:

    $ gapipe-run --config $GAPIPE_WORKDIR$/run21_June2025/pfsStar/10092/000000020000607f-028-0x53aa1227c7ab7463/pfsStar-10092-000000020000607f-028-0x53aa1227c7ab7463.yaml

The `run` script will execute the pipeline on the object specified in the configuration file. The output will be written to the `outdir` directory specified in the configuration file.

It is possible to override the input and output directories defined in the configuration file by passing additional command-line arguments. The following arguments are available:

* **--datadir** *datadir*

* **--rerundir** *rerundir*

* **--workdir** *workdir*

* **--outdir** *outdir*

See Section 4.2 for a description of these arguments.

# 3.5 Submitting a batch job

    $ gapipe-run --catid $CATID --visit $VISITS --batch slurm --partition v100 --cpus 4 --mem 16G --top 100 --progress

# 3.6 Collecting the results

# 3.7 Additional command-line arguments

* **--debug**: Enable debug mode. The scripts are executed within a `debugpy` context.

* **--profile**: Enable profiling. The scripts are executed within a `cProfile` context.

* **--log-level** *level*: Set the log level. The default is `INFO` but can be set to `TRACE`, `DEBUG`, `WARNING`, `ERROR`, or `CRITICAL`.