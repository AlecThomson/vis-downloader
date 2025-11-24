"""Get all the data off CASDA."""

from __future__ import annotations

import argparse
import asyncio
import logging
import tarfile
from collections.abc import Awaitable
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Literal, TypeVar, cast

import aiohttp
import aiohttp.client_exceptions
import requests
from astropy import log as logger
from astropy.table import Row, Table, vstack
from astroquery.casda import CasdaClass, conf
from astroquery.utils.tap.core import TapPlus
from tqdm.asyncio import tqdm

from vis_downloader.casda_login import login as casda_login

if TYPE_CHECKING:
    from collections.abc import Awaitable

T = TypeVar("T")
R = TypeVar("R")

logger.setLevel(logging.INFO)

CASDATAP: TapPlus = TapPlus(url="https://casda.csiro.au/casda_vo_tools/tap")
SEMAPHORES: dict[str, asyncio.Semaphore] = {}

conf.timeout = 120  # Overwrite the default 20 seconds


@dataclass
class DownloadOptions:
    """options to use for downloading of CASDA SBID data."""

    output_dir: Path | None = None
    """Output directory to write files to. If None output directory is
    built from the current working directory and SBID befing downloaded.
    Defaults to None."""
    extract_tar: bool = False
    """Extract tarballs at the end of downloading"""
    download_holography: bool = False
    """Download the evaluation file that contains the holography"""
    max_workers: int = 1
    """The maximum number of download workers to use"""
    log_only: bool = False
    """Simply log the URLs to download. Don't download."""
    disable_progress: bool = False
    """Disable the progress bars produced by tqdm.
    Useful when running in a non-TTY setting."""
    max_tries: int = 3
    """The maximum number of retries to allow when downloading a file."""


def retry_download(func: Awaitable[T, R]) -> Awaitable[T, R]:
    """Add retry loop around a wrapped function to re-run the function
    should it fail, e.g. network outage issues.

    The returned function will have a `max_retries` keyword added to denotes how many
    retries are allowed before a `ValueError` is raised.

    Args:
        func (Awaitable[T]): The function to retry on failure

    Returns:
        Awaitable: The wrapped function that will be restarted on failure

    """

    async def _wrapper(*args: T, max_retries: int = 3, **kwargs: T) -> R:  # qa: ignore
        if max_retries <= 0:
            msg = f"{max_retries=}, but should be larger than 0"
            raise ValueError(msg)

        count = 0
        while count < max_retries:
            try:
                return await func(*args, **kwargs)
            except aiohttp.client_exceptions.ClientPayloadError:
                logger.critical("Failed to run. Retrying. ")
                asyncio.sleep(4)

            count += 1

        raise ValueError("Too many retries")

    return _wrapper


# Stolen from https://stackoverflow.com/a/61478547
async def gather_with_limit(
    limit: int | None,
    *coros: Awaitable[T],
    desc: str | None = None,
) -> list[T]:
    """Gather with a limit on the number of coroutines running at once.

    Args:
        limit (int): The number of coroutines to run at once
        coros (Awaitable): The coroutines to run
        desc (str | None, optional): Description to show in the progress bar.
            Defaults to None.

    Returns:
        Awaitable: The result of the coroutines

    """
    if limit is None:
        return cast(
            "list[T]",
            await tqdm.gather(*coros, maxinterval=100000000, desc=desc),
        )

    semaphore = asyncio.Semaphore(limit)

    async def sem_coro(coro: Awaitable[T]) -> T:
        async with semaphore:
            return await coro

    return cast(
        "list[T]",
        await tqdm.gather(
            *(sem_coro(c) for c in coros),
            maxinterval=100000000,
            desc=desc,
        ),
    )


async def _get_holography_url(
    sbid: int,
    mode: Literal["vis", "holography"] = "vis",
) -> Table:
    """Generate and execute a ADQL query.

    Args:
        sbid (int): The SBID we want files for
        mode (Literal["vis, "holography"], optional): Whether visibilities or holography
            will be downloaded. Defaults to "vis".

    Raises:
        ValueError: Raised if `mode` is not known
        ValueError: Raised if the remote request returns failed

    Returns:
        Table: Matching results of the ADQL request

    """
    if mode == "vis":
        query_str = (
            f"SELECT * FROM ivoa.obscore "  # noqa: S608
            f"where obs_id='ASKAP-{sbid}' "
            f"AND dataproduct_type='visibility'"
        )
    elif mode == "holography":
        query_str = (
            f"SELECT * FROM casda.observation_evaluation_file "  # noqa: S608
            f"where sbid='{sbid}' and format='calibration'"
        )
    else:
        msg = f"Unknown {mode=}"
        raise ValueError(msg)

    logger.info(f"Querying CASDA for {sbid=} {mode=}")

    job = await asyncio.to_thread(CASDATAP.launch_job_async, query_str)
    results = job.get_results()

    if results is None:
        msg = f"Failed to find holography for {sbid=}"
        raise ValueError(msg)

    return results


async def get_files_to_download(
    sbid: int,
    *,
    download_holography: bool = False,
) -> Table:
    """Lookup in CASDA files to download for a specified SBID.

    Args:
        sbid (int): The SBID to download
        download_holography (bool, optional): Whether holography data needs to be
            downloaded. Defaults to False.

    Returns:
        Table: Result set of matching files. Should multiple requests be made the
            intersection of columns between tables is returned.

    """
    tables: list[Table] = []
    results = await _get_holography_url(sbid=sbid)
    tables.append(results)

    if download_holography:
        results = await _get_holography_url(sbid=sbid, mode="holography")
        tables.append(results)

    return vstack(tables, join_type="inner")


def get_download_url(result_row: Row, casda: CasdaClass) -> str:
    """Get the download URL for a file on CASDA.

    Args:
        result_row (Row): Result row
        casda (CasdaClass): CASDA class

    Raises:
        ValueError: If no results are found
        ValueError: If multiple results are found

    Returns:
        str: Download URL

    """
    logger.info("Staging data on CASDA...")
    max_retry = 3
    while max_retry > 0:
        try:
            url_list: list[str] = casda.stage_data(Table(result_row))
            break
        except (
            ValueError,
            requests.exceptions.ConnectionError,
            requests.exceptions.ReadTimeout,
            requests.exceptions.HTTPError,
        ):
            logger.warning("Failed to stage. retrying.")
            max_retry -= 1
    else:
        raise ValueError("Failed to stage data too many times.")

    good_url_list = []
    for url in url_list:
        if url.endswith("checksum"):
            continue
        good_url_list.append(url)

    if len(good_url_list) == 0:
        msg = "No file found!"
        raise ValueError(msg)
    if len(good_url_list) > 1:
        msg = "Multiple files found!"
        raise ValueError(msg)

    url = good_url_list[0]
    msg = f"Staged data at {url}"
    logger.info(msg)
    return url


@retry_download
async def download_file(  # noqa: PLR0913
    url: str,
    output_file: Path,
    connect_timeout_seconds: int = 120,
    download_timeout_seconds: int = 60 * 60 * 12,
    chunk_size: int = 1000000,
    *,
    disable_progress: bool = False,
) -> Path:
    """Download a file from CASDA, streaming it to its final location.

    Args:
        url (str): The URL describing the remote resources to download
        output_file (Path): The location to write the file to.
        connect_timeout_seconds (int, optional): The acceptable amount of time to
            establish a connection to server. Defaults to 43200.
        download_timeout_seconds (int, optional): The acceptable amount of time to wait
            for the download to finish. Defaults to 60*60*12.
        chunk_size (int, optional): Size of data blocks to store in memory before
            flushing to disk. Defaults to 1000000.
        disable_progress (bool, optional): Disable the progress bars produced by tqdm.
            Useful when running in a non-TTY setting. Defaults to False.

    Raises:
        ValueError: A status code other than 200 is returned when accessing the server

    Returns:
        Path: Location of the file that was written to

    """
    msg = f"Using aiohttp, Downloading from {url}"
    logger.info(msg)

    timeout = aiohttp.ClientTimeout(
        total=download_timeout_seconds,
        connect=connect_timeout_seconds,
    )
    ok_status = 200
    async with (
        aiohttp.ClientSession(timeout=timeout) as session,
        session.get(url) as response,
    ):
        if response.status != ok_status:
            msg = f"{response.status=}, indicating the request was not successful."
            raise ValueError(
                msg,
            )

        total_size = int(response.headers.get("content-length", 0))

        with (
            output_file.open("wb") as file_desc,
            tqdm(
                total=total_size,
                unit="B",
                unit_scale=True,
                unit_divisor=1024,
                desc=output_file.name,
                disable=disable_progress,
            ) as pbar,
        ):
            async for chunk in response.content.iter_chunked(chunk_size):
                pbar.update(len(chunk))

                file_desc.write(chunk)

    msg = f"Downloaded to {output_file}"
    logger.info(msg)
    return output_file


async def stage_and_download(  # noqa: RUF100 PLR0913
    sbid: int,
    result_row: Row,
    casda: CasdaClass,
    output_dir: Path | None = None,
    *,
    disable_progress: bool = False,
    max_retries: int = 3,
) -> Path:
    """Trigger CASDA to stage the data and then download it once it has been staged.

    The `result_table` is generated via the ADQL query.

    Args:
        sbid (int): The SBID of the data being downloaded
        result_row (Row): A data row to download, including its url and file name
        casda (CasdaClass): An active CASDA session that has passed user
            authentication
        output_dir (Path | None, optional): The location to write the data to.
            If None data will be downloaded into a folder for the SBID. Defaults to None
        disable_progress (bool, optional): Disable the progress bars produced
            by `tqdm`. Useful when running in a non-TTY setting.. Defaults to False.
        max_retries (int, optional): The maximum number of retries allowed before a
            file is deemed unsuccessful. Defaults to 3.

    Returns:
        Path: Path to the file that has been downloaded

    """
    if output_dir is None:
        output_dir = Path.cwd() / str(sbid)
        output_dir.mkdir(parents=True, exist_ok=True)

    url = await asyncio.to_thread(get_download_url, result_row, casda)
    output_file = output_dir / result_row["filename"]

    return await download_file(
        url, output_file, disable_progress=disable_progress, max_retries=max_retries
    )


def extract_tarball(in_path: Path) -> Path:
    """Extract the contents of a tarball, and delete it once extracted.

    Files are extracted alongside the tarball.

    Args:
        in_path (Path): Location of the tarball.

    Returns:
        Path: Directory containing the extracted files

    """
    if not tarfile.is_tarfile(in_path):
        return in_path

    logger.info(f"Extracting {in_path=}")

    with tarfile.open(name=in_path, mode="r") as tar:
        for member in tar.getmembers():
            # Some tarballs have symlinks that point to absolute paths
            # extractall() falls over on these
            if not member.isfile():
                continue

            tar.extract(member, in_path.parent, filter="data")

    in_path.unlink()

    return in_path.parent


def coros_with_limits(
    coros: Awaitable[T], max_limit: int, key: str | None = None
) -> Awaitable[T]:
    """Place a limiter on a set of co-routines via an asynio Semaphore. The `key`
    is used to denote different semaphores from one another, or use a previously
    created semaphore.

    Args:
        coros (Awaitable[T]): The co-routines that will have some limiter placed
        max_limit (int): The maximum limit of workers
        key (str | None, optional): The semaphore to use for this limiter. If None or
          the `key` has not been used one is created. Defaults to None.

    Returns:
        Awaitable[T]: New routines with a collective semaphore context applied

    """
    semaphore = SEMAPHORES.get(key)
    if semaphore is None:
        semaphore = asyncio.Semaphore(max_limit)
        SEMAPHORES[key] = semaphore

    async def _limit(_coro: Awaitable[T]) -> T:
        async with semaphore:
            return await _coro

    return [_limit(coro) for coro in coros]


async def get_cutouts_from_casda(
    sbid_list: list[int],
    username: str | None = None,
    *,
    store_password: bool = False,
    reenter_password: bool = False,
    download_options: DownloadOptions | None = None,
) -> list[Path]:
    """Download visibilities and other products for a nominated set of SBIDs from CASDA.

    Args:
        sbid_list (list[int]): Set of SBIDs to download data for
        username (str | None, optional): The username to use to authenticate with.
            Defaults to None.
        store_password (bool, optional): Whether the password should be stored in a
            keyring. Defaults to False.
        reenter_password (bool, optional): Force the password to be entered.
            Defaults to False.
        download_options (DownloadOptions | None, optional): Settings to use while
            downloading. Defaults to None.

    Returns:
        list[Path]: A list of files downloaded

    """
    if download_options is None:
        download_options = DownloadOptions()

    casda = casda_login(
        username=username,
        store_password=store_password,
        reenter_password=reenter_password,
    )

    sbids_coros = []

    for sbid in sbid_list:
        result_table: Table = await get_files_to_download(
            sbid, download_holography=download_options.download_holography
        )

        if download_options.log_only:
            logger.info(result_table)
            continue

        sbids_coros.extend(
            [
                stage_and_download(
                    sbid=sbid,
                    result_row=row,
                    output_dir=download_options.output_dir,
                    casda=casda,
                    disable_progress=download_options.disable_progress,
                    max_retries=download_options.max_workers,
                )
                for row in result_table
            ]
        )

    paths = []

    coros = coros_with_limits(
        sbids_coros, max_limit=download_options.max_workers, key="sbid"
    )
    for item in asyncio.as_completed(coros):
        path = await item

        if download_options.extract_tar:
            path = await asyncio.to_thread(extract_tarball, in_path=path)

        paths.append(path)

    return paths


def main() -> None:
    """Run the main CLI."""
    parser = argparse.ArgumentParser(
        description="Download visibilities from CASDA for a given SBID",
    )
    parser.add_argument("sbids", nargs="+", type=int, help="SBID to download")
    parser.add_argument(
        "--output-dir",
        type=Path,
        help="Output directory. If unset a directory for each SBID will be created.",
        default=None,
    )
    parser.add_argument("--username", type=str, help="CASDA username", default=None)
    parser.add_argument(
        "--store-password",
        action="store_true",
        help="Store password in keyring",
    )
    parser.add_argument(
        "--reenter-password",
        action="store_true",
        help="Reenter password",
    )
    parser.add_argument(
        "--max-workers",
        type=int,
        help="Number of workers",
        default=None,
    )
    parser.add_argument(
        "--extract-tar",
        action="store_true",
        help="If a file is a tarball attempt to extract it. This removes the original "
        "tar file if successful.",
    )
    parser.add_argument(
        "--download-holography",
        action="store_true",
        help="Download the evaluation files that contain the holography data",
    )
    parser.add_argument("--log-only", action="store_true")
    parser.add_argument(
        "--disable-progress",
        action="store_true",
        help=(
            "Disable the progress bars produced by `tqdm` ",
            "Useful when running in a non-TTY setting.",
        ),
    )
    parser.add_argument(
        "--quiet",
        action="store_true",
        help="Silence logged output and progress bar updates",
    )
    parser.add_argument(
        "--max-retries",
        type=int,
        default=3,
        help="The maximum number of retries allowed for each file when downloading.",
    )

    args = parser.parse_args()

    disable_progress = args.quiet or args.disable_progress

    download_options = DownloadOptions(
        output_dir=args.output_dir,
        extract_tar=args.extract_tar,
        download_holography=args.download_holography,
        max_workers=args.max_workers,
        log_only=args.log_only,
        disable_progress=disable_progress,
        max_tries=args.max_retries,
    )

    # Set the logging to a higher level
    if args.quiet:
        logger.setLevel(logging.CRITICAL)

    asyncio.run(
        get_cutouts_from_casda(
            sbid_list=args.sbids,
            username=args.username,
            store_password=args.store_password,
            reenter_password=args.reenter_password,
            download_options=download_options,
        ),
    )


if __name__ == "__main__":
    main()
