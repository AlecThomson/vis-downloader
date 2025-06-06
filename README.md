# Vis Downloader

<!-- [![PyPI - Version](https://img.shields.io/pypi/v/vis-downloader.svg)](https://pypi.org/project/vis-downloader)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/vis-downloader.svg)](https://pypi.org/project/vis-downloader)

----- -->
Download visibilties from CASDA.

## Installation

I haven't published on PyPI yet so run:

```console
pip install git+https://github.com/AlecThomson/vis-downloader
```

## Usage

To make sure you don't DDoS CASDA, please make use of the `--max-workers` option.

```bash
vis_download -h
# usage: vis_download [-h] [--output-dir OUTPUT_DIR] [--username USERNAME] [--store-password] [--reenter-password] [--max-workers MAX_WORKERS] [--extract-tar] [--download-holography] [--log-only] sbids [sbids ...]
#
# Download visibilities from CASDA for a given SBID
#
# positional arguments:
#   sbids                 SBID to download
#
# options:
#   -h, --help            show this help message and exit
#   --output-dir OUTPUT_DIR
#                         Output directory. If unset a directory for each SBID will be created.
#   --username USERNAME   CASDA username
#   --store-password      Store password in keyring
#   --reenter-password    Reenter password
#   --max-workers MAX_WORKERS
#                         Number of workers
#   --extract-tar         If a file is a tarball attempt to extract it. This removes the original tar file if successful.
#   --download-holography
#                         Download the evaluation files that contain the holography data
#   --log-only
```

To cache your CASDA credentials run:

```bash
casda_login -h
# usage: casda_login [-h] username
#
# Login to CASDA and save credentials
#
# positional arguments:
#   username    Username for CASDA
#
# options:
#   -h, --help  show this help message and exit
```

Note that you will also want to set the `CASDA_USERNAME` environment variable for non-interactive use.

## License

`vis-downloader` is distributed under the terms of the [MIT](https://spdx.org/licenses/MIT.html) license.
