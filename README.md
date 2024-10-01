Common Libraries (common-libs)
======================

[![pre-commit.ci status](https://results.pre-commit.ci/badge/github/yugokato/common-libs/main.svg)](https://results.pre-commit.ci/latest/github/yugokato/common-libs/main)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

This repository contains some common Python libraries shared among my other private/public repositories.


## Installation

To utilize these libraries in a downsteram project, add the following line to your dependencies:
```
common-libs@git+https://github.com/yugokato/common-libs
```

> [!NOTE]
> See [pyproject.toml](pyproject.toml) for supported optional dependencies


## Notes

The version of the libraries in this repository is currently not incremented with each update.  
When you need to reflect the latest changes from this repository in your downstream project, make sure to reinstall the library using the `--force-reinstall` option with `pip`. 

Example:

  ```sh
  pip install --force-reinstall <package-name>
  ```
