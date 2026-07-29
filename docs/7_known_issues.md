## 7 Known Issues

### 7.1 Installation

**LSST stack installation sometimes gets stuck at solving environment:**

You may have a local (/home/$USER/.conda) or global setting enabled that forces classic anaconda package resolution which is notoriously slow. Try to disable this setting and retry the installation.

Files and directories to verify

* ~/.cache/conda
* ~/.cache/conda-anaconda-tos
* ~/.conda
* ~/.condarc
