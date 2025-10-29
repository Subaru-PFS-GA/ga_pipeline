# 4 Development

## 4.1 Development Environment

## 4.2 Dependencies

## 4.3 Project structure

The project folders are as follows

* *bin*: command-line scripts to be used in development mode
* *docs*: documentation
* *python*: source code
* *tests*: unit tests

## 4.4 Command-line scripts

How command-line arguments are registered

### 4.4.1 Additional command-line arguments

* **--debug**: Enable debug mode. The scripts are executed within a `debugpy` context.

* **--profile**: Enable profiling. The scripts are executed within a `cProfile` context.

* **--log-level** *level*: Set the log level. The default is `INFO` but can be set to `TRACE`, `DEBUG`, `WARNING`, `ERROR`, or `CRITICAL`.

## 4.5 Debugging in vscode

## 4.6 Unit tests

From VSCode
