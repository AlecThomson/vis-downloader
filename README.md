# Vis Downloader

<!-- [![PyPI - Version](https://img.shields.io/pypi/v/vis-downloader.svg)](https://pypi.org/project/vis-downloader)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/vis-downloader.svg)](https://pypi.org/project/vis-downloader)

----- -->
Download visibilties from CASDA.

## Installation

Use `pip`:

```sh
# From PyPI
pip install vis-downloader
# - OR -
# Latest git version
pip install git+https://github.com/AlecThomson/vis-downloader
```

## Usage

To make sure you don't DDoS CASDA, please make use of the `--max-workers` option.

```bash
usage: vis_download [-h] [--beam BEAM] [--scan-id SCAN_ID] [--vis-type {craco,science}] [--output-dir OUTPUT_DIR]
                    [--username USERNAME] [--store-password] [--reenter-password] [--max-workers MAX_WORKERS] [--extract-tar]
                    [--download-holography] [--log-only] [--disable-progress] [--quiet] [--max-retries MAX_RETRIES] [--resume]
                    sbids [sbids ...]

Download visibilities from CASDA for a given SBID

positional arguments:
  sbids                 SBID to download

options:
  -h, --help            show this help message and exit
  --beam BEAM           Beam to download. Defaults to all.
  --scan-id SCAN_ID     Scan ID to download. Defaults to all.
  --vis-type {craco,science}
                        Filter visibilities by product type: 'craco' (cracoData / uvfits) or 'science' (scienceData / ms).
                        Defaults to None (all).
  --output-dir OUTPUT_DIR
                        Output directory. If unset a directory for each SBID will be created.
  --username USERNAME   CASDA username
  --store-password      Store password in keyring
  --reenter-password    Reenter password
  --max-workers MAX_WORKERS
                        Number of workers
  --extract-tar         If a file is a tarball attempt to extract it. This removes the original tar file if successful.
  --download-holography
                        Download the evaluation files that contain the holography data
  --log-only
  --disable-progress    Disable the progress bars produced by `tqdm`.
  --quiet               Silence logged output and progress bar updates
  --max-retries MAX_RETRIES
                        The maximum number of retries allowed for each file when downloading.
  --resume              Resume partially downloaded files and skip already downloaded or extracted files.
```

### Resuming an interrupted download

Interrupted downloads can be continued by re-running the same command with `--resume` e.g.:

```bash
vis_download 12345 --output-dir /path/to/data --max-workers 4 --extract-tar --resume
```

A file that was partway through continues from the byte it stopped at, and files that already finished are skipped without re-querying CASDA. With `--extract-tar`, an archive that was already unpacked is skipped as well; if an extraction was itself interrupted, the tarball is still on disk and the data is extracted again rather than left incomplete.

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
