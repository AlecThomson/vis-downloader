# Vis Downloader

<!-- [![PyPI - Version](https://img.shields.io/pypi/v/vis-downloader.svg)](https://pypi.org/project/vis-downloader)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/vis-downloader.svg)](https://pypi.org/project/vis-downloader)

----- -->


Download visibilties from CASDA

## Table of Contents

- [Vis Downloader](#vis-downloader)
  - [Table of Contents](#table-of-contents)
  - [Installation](#installation)
  - [Usage](#usage)
  - [License](#license)

## Installation
I haven't published on PyPI yet so run:
```console
pip install git+https://github.com/AlecThomson/vis-downloader
```

## Usage
To make sure you don't DDoS CASDA, please make use of the `--max-workers` option.

```console
get_vis -h
usage: vis_download [-h] [--output-dir OUTPUT_DIR] [--username USERNAME] [--store-password] [--reenter-password] [--max-workers MAX_WORKERS] sbids [sbids ...]

Download visibilities from CASDA for a given SBID

positional arguments:
  sbids                 SBID to download

options:
  -h, --help            show this help message and exit
  --output-dir OUTPUT_DIR
                        Output directory
  --username USERNAME   CASDA username
  --store-password      Store password in keyring
  --reenter-password    Reenter password
  --max-workers MAX_WORKERS
                        Number of workers
```

To cache your CASDA credentials run:
```console
casda_login -h
usage: casda_login [-h] username

Login to CASDA and save credentials

positional arguments:
  username    Username for CASDA

options:
  -h, --help  show this help message and exit
```

Note that you will also want to set the `CASDA_USERNAME` environment variable for non-iteractive use.

## License

`vis-downloader` is distributed under the terms of the [MIT](https://spdx.org/licenses/MIT.html) license.
