"""Get all the data off CASDA."""

from __future__ import annotations

import argparse
import asyncio
import logging
import tarfile
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Literal, NamedTuple, TypeVar, cast

import aiohttp
import aiohttp.client_exceptions
import requests
import yarl
from astropy import log as logger
from astropy.table import Row, Table, vstack
from astroquery.casda import CasdaClass, conf
from astroquery.utils.tap.core import TapPlus
from tqdm.asyncio import tqdm

from vis_downloader.casda_login import login as casda_login

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Iterable

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
    max_retries: int = 3
    """The maximum number of retries to allow when downloading a file."""
    resume: bool = False
    """Resume partially downloaded files and skip already completed files."""
    vis_type: Literal["craco", "science"] | None = None
    """Visibility data product type filter setting: 'craco' (cracoData / uvfits)
    or 'science' (scienceData / ms). Defaults to None (no filter)."""
    scan_id: int | None = None
    """Scan ID filter setting, relevant for CRACO data only.
    Scan ID is yyyymmddhhmmss format. Defaults to None (no filter)."""


def retry_download(func: Callable[..., Awaitable[R]]) -> Callable[..., Awaitable[R]]:
    """Add retry loop around a wrapped function to re-run the function
    should it fail, e.g. network outage issues.

    The returned function will have a `max_retries` keyword added to denotes how many
    retries are allowed before a `ValueError` is raised.

    Args:
        func (Callable[..., Awaitable[R]]): The function to retry on failure

    Returns:
        Callable[..., Awaitable[R]]: The wrapped function that will be restarted
            on failure

    """

    async def _wrapper(*args: object, max_retries: int = 3, **kwargs: object) -> R:
        if max_retries <= 0:
            msg = f"{max_retries=}, but should be larger than 0"
            raise ValueError(msg)

        count = 0
        while count < max_retries:
            try:
                return await func(*args, **kwargs)
            except aiohttp.client_exceptions.ClientPayloadError:
                logger.critical("Failed to run. Retrying. ")
                await asyncio.sleep(4)
            except (
                aiohttp.client_exceptions.ClientError,
                asyncio.TimeoutError,
            ) as err:
                logger.critical(
                    f"Network error: {err}. Retrying ({count + 1}/{max_retries})... "
                )
                await asyncio.sleep(4)

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


def _build_query(
    sbid: int,
    mode: Literal["vis", "holography"] = "vis",
    vis_type: Literal["craco", "science"] | None = None,
    beam: int | None = None,
    scan_id: int | None = None,
) -> str:
    """Build the ADQL query for a CASDA lookup.

    Args:
        sbid (int): The SBID we want files for
        mode (Literal["vis", "holography"], optional): Whether visibilities or
            holography will be downloaded. Defaults to "vis".
        vis_type (Literal["craco", "science"] | None, optional):
            Filter visibilities by product type. Defaults to None.
        beam (int | None, optional): Restrict results to a single beam.
            Defaults to None.
        scan_id (int | None, optional): Restrict results to a single scan -
            relevant for CRACO data only. Format is yyyymmddhhmmss.
            Defaults to None.

    Returns:
        str: The ADQL query

    Raises:
        ValueError: Raised if `mode` is not known

    """
    if mode == "holography":
        return (
            f"SELECT * FROM casda.observation_evaluation_file "  # ruff: ignore[hardcoded-sql-expression]
            f"where sbid='{sbid}' and format='calibration'"
        )

    if mode != "vis":
        msg = f"Unknown {mode=}"
        raise ValueError(msg)

    query_str = (
        f"SELECT * FROM ivoa.obscore "  # ruff: ignore[hardcoded-sql-expression]
        f"where obs_id='ASKAP-{sbid}' "
        f"AND dataproduct_type='visibility'"
    )
    prefixes = {"craco": "cracoData", "science": "scienceData"}
    if vis_type is not None:
        query_str += f" AND filename LIKE '{prefixes[vis_type]}%'"

    if scan_id is not None:
        query_str += f" AND filename LIKE '%{scan_id}%'"

    if beam is not None:
        query_str += rf" AND filename LIKE '%beam{beam:01d}%'"

    return query_str


async def _get_holography_url(
    sbid: int,
    mode: Literal["vis", "holography"] = "vis",
    vis_type: Literal["craco", "science"] | None = None,
    beam: int | None = None,
    scan_id: int | None = None,
) -> Table:
    """Generate and execute a ADQL query.

    Args:
        sbid (int): The SBID we want files for
        mode (Literal["vis, "holography"], optional): Whether visibilities or holography
            will be downloaded. Defaults to "vis".
        vis_type (Literal["craco", "science"] | None, optional):
            Filter visibilities by product type. Defaults to None.
        beam (int | None, optional): Restrict results to a single beam.
            Defaults to None.
        scan_id (int | None, optional): Restrict results to a single scan -
            relevant for CRACO data only. Format is yyyymmddhhmmss.
            Defaults to None.

    Returns:
        Table: Matching results of the ADQL request

    Raises:
        ValueError: Raised if `mode` is not known
        ValueError: Raised if the remote request returns failed

    """
    query_str = _build_query(
        sbid=sbid, mode=mode, vis_type=vis_type, beam=beam, scan_id=scan_id
    )

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
    vis_type: Literal["craco", "science"] | None = None,
    beam: int | None = None,
    scan_id: int | None = None,
) -> Table:
    """Lookup in CASDA files to download for a specified SBID.

    Args:
        sbid (int): The SBID to download
        download_holography (bool, optional): Whether holography data needs to be
            downloaded. Defaults to False.
        vis_type (Literal["craco", "science"] | None, optional):
            Filter visibilities by product type. Defaults to None.
        beam (int | None, optional): Restrict results to a single beam.
            Defaults to None.
        scan_id (int | None, optional): Restrict results to a single scan -
            relevant for CRACO data only. Format is yyyymmddhhmmss. Defaults to None.

    Returns:
        Table: Result set of matching files. Should multiple requests be made the
            intersection of columns between tables is returned.

    """
    tables: list[Table] = []
    results = await _get_holography_url(
        sbid=sbid,
        vis_type=vis_type,
        beam=beam,
        scan_id=scan_id,
    )
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

    Returns:
        str: Download URL

    Raises:
        ValueError: If no results are found
        ValueError: If multiple results are found

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


def _get_extracted_path(output_dir: Path, filename: str) -> Path | None:
    """Get candidate extracted path for a tarball if it was previously extracted.

    Args:
        output_dir (Path): Directory where extracted files are placed.
        filename (str): Name of the tarball file.

    Returns:
        Path | None: Path to the extracted folder or file if it exists, otherwise None.

    """
    for ext in (".tar.gz", ".tar.bz2", ".tar.xz", ".tar", ".tgz"):
        if filename.endswith(ext):
            extracted_name = filename[: -len(ext)]
            candidate = output_dir / extracted_name
            if candidate.exists():
                return candidate
    return None


def _content_range_total(content_range: str | None) -> int | None:
    """Parse the total resource size out of a ``Content-Range`` header.

    Args:
        content_range (str | None): Raw header value, e.g. ``bytes */12345``.

    Returns:
        int | None: The total size in bytes, or None if it could not be parsed.

    """
    if not content_range or "/" not in content_range:
        return None
    total = content_range.rsplit("/", 1)[-1].strip()
    try:
        return int(total)
    except ValueError:
        return None


class StreamPlan(NamedTuple):
    """How a response body should be written into a partial file."""

    total_size: int
    """Expected size of the complete resource, in bytes."""
    file_mode: str
    """Mode to open the partial file with; appends when resuming."""
    initial_bytes: int
    """Bytes already on disk, used to seed the progress bar."""


def plan_stream(
    response: aiohttp.ClientResponse, output_filename: str, curr_bytes: int
) -> StreamPlan:
    """Decide how to write a response body, based on how the server answered.

    Args:
        response (aiohttp.ClientResponse): The HTTP response to read from.
        output_filename (str): Name of the target file, for logging.
        curr_bytes (int): Number of bytes previously downloaded.

    Returns:
        StreamPlan: The size, file mode and progress offset to stream with.

    Raises:
        RuntimeError: If the response status code is not 200 or 206.

    """
    ok_status = 200
    partial_content_status = 206

    if response.status == partial_content_status:
        total_size = _content_range_total(response.headers.get("Content-Range"))
        if total_size is None:
            total_size = curr_bytes + int(response.headers.get("content-length", 0))
        logger.info(
            f"Resuming {output_filename}: "
            f"{curr_bytes}/{total_size} bytes already downloaded."
        )
        return StreamPlan(
            total_size=total_size, file_mode="ab", initial_bytes=curr_bytes
        )

    if response.status == ok_status:
        if curr_bytes > 0:
            logger.warning(
                f"Server returned 200 OK for {output_filename}; resuming "
                "not supported or range ignored. Restarting from byte 0."
            )
        return StreamPlan(
            total_size=int(response.headers.get("content-length", 0)),
            file_mode="wb",
            initial_bytes=0,
        )

    msg = f"{response.status=}, indicating the request was not successful."
    raise RuntimeError(msg)


async def _stream_response_to_file(  # ruff: ignore[too-many-arguments]
    response: aiohttp.ClientResponse,
    part_file: Path,
    output_filename: str,
    curr_bytes: int,
    chunk_size: int,
    *,
    disable_progress: bool,
) -> None:
    """Stream chunks from an aiohttp response to a destination partial file.

    Args:
        response (aiohttp.ClientResponse): The HTTP response to read from.
        part_file (Path): Temporary file path to write data into.
        output_filename (str): Name of the target file for progress bar display.
        curr_bytes (int): Number of bytes previously downloaded.
        chunk_size (int): Size of chunks to read from the stream.
        disable_progress (bool): Whether to disable the tqdm progress bar.

    """
    plan = plan_stream(response, output_filename=output_filename, curr_bytes=curr_bytes)

    with (
        part_file.open(plan.file_mode) as file_desc,
        tqdm(
            total=plan.total_size,
            initial=plan.initial_bytes,
            unit="B",
            unit_scale=True,
            unit_divisor=1024,
            desc=output_filename,
            disable=disable_progress,
        ) as pbar,
    ):
        async for chunk in response.content.iter_chunked(chunk_size):
            pbar.update(len(chunk))
            file_desc.write(chunk)


def _escaped_url(url: str) -> yarl.URL:
    """Encode a CASDA URL so aiohttp sends it verbatim.

    Args:
        url (str): The raw URL to encode.

    Returns:
        yarl.URL: The encoded URL.

    """
    escaped_url_str = (
        url.replace("+", "%2B")  # for the S3 signature verification)
        .replace(
            " ",
            "%20",  # prevent the Squid 400 Bad Request proxy error
        )
        .replace('"', "%22")  # standard quote encoding
    )
    msg = f"Using aiohttp, Downloading from '{escaped_url_str}'"
    logger.info(msg)
    # Force yarl/aiohttp to use this exact string without auto-decoding it
    return yarl.URL(escaped_url_str, encoded=True)


def _file_has_content(path: Path) -> bool:
    """Check whether a path exists and is non-empty.

    Args:
        path (Path): The path to check.

    Returns:
        bool: True if the path exists and holds at least one byte.

    """
    return path.exists() and path.stat().st_size > 0


def _partial_byte_count(part_file: Path, *, resume: bool) -> int:
    """Count the bytes already written to a partial file.

    Args:
        part_file (Path): The partial file to measure.
        resume (bool): Whether resuming is enabled.

    Returns:
        int: Bytes already downloaded, or 0 when not resuming.

    """
    if not resume or not part_file.exists():
        return 0
    return part_file.stat().st_size


async def fetch_to_part_file(  # ruff: ignore[too-many-arguments]
    session: aiohttp.ClientSession,
    url: yarl.URL,
    part_file: Path,
    *,
    output_filename: str,
    curr_bytes: int,
    chunk_size: int,
    disable_progress: bool,
) -> None:
    """Fill ``part_file`` with the complete remote object.

    Resumes from ``curr_bytes`` when the server supports it. A 416 response
    means the requested range is past the end of the resource, which is only
    safe to treat as "already complete" when the partial file is exactly the
    size the server reports; otherwise the partial file is stale and the whole
    object is fetched again.

    Args:
        session (aiohttp.ClientSession): Session used to issue the requests.
        url (yarl.URL): The encoded URL to download.
        part_file (Path): Temporary file to write data into.
        output_filename (str): Name of the target file, for logging.
        curr_bytes (int): Number of bytes already in ``part_file``.
        chunk_size (int): Size of chunks to read from the stream.
        disable_progress (bool): Whether to disable the tqdm progress bar.

    """
    range_not_satisfiable_status = 416

    headers = {}
    if curr_bytes > 0:
        headers["Range"] = f"bytes={curr_bytes}-"
        logger.info(
            f"Attempting to resume download for {output_filename} "
            f"from byte {curr_bytes}"
        )

    async with session.get(url, headers=headers) as response:
        if response.status != range_not_satisfiable_status or curr_bytes == 0:
            await _stream_response_to_file(
                response=response,
                part_file=part_file,
                output_filename=output_filename,
                curr_bytes=curr_bytes,
                chunk_size=chunk_size,
                disable_progress=disable_progress,
            )
            return

        remote_size = _content_range_total(response.headers.get("Content-Range"))
        if remote_size == curr_bytes:
            logger.info(
                f"Range not satisfiable (status 416) for {output_filename} and the "
                f"partial file matches the remote size ({remote_size} bytes). "
                "Finalizing existing download."
            )
            return

        logger.warning(
            f"Range not satisfiable (status 416) for {output_filename}, but the "
            f"partial file ({curr_bytes} bytes) does not match the remote size "
            f"({remote_size}). Discarding it and downloading the whole file again."
        )

    # A plain request answers 200, which truncates the stale partial file.
    async with session.get(url) as response:
        await _stream_response_to_file(
            response=response,
            part_file=part_file,
            output_filename=output_filename,
            curr_bytes=0,
            chunk_size=chunk_size,
            disable_progress=disable_progress,
        )


@retry_download
async def download_file(  # ruff: ignore[too-many-arguments]
    url: str,
    output_file: Path,
    connect_timeout_seconds: int = 120,
    download_timeout_seconds: int = 60 * 60 * 12,
    chunk_size: int = 1000000,
    *,
    disable_progress: bool = False,
    resume: bool = False,
) -> Path:
    """Download a file from CASDA, streaming it to its final location.

    Args:
        url (str): The URL describing the remote resources to download
        output_file (Path): The location to write the file to.
        connect_timeout_seconds (int, optional): The acceptable amount of time to
            establish a connection to server. Defaults to 120.
        download_timeout_seconds (int, optional): The acceptable amount of time to wait
            for the download to finish. Defaults to 60*60*12.
        chunk_size (int, optional): Size of data blocks to store in memory before
            flushing to disk. Defaults to 1000000.
        disable_progress (bool, optional): Disable the progress bars produced by tqdm.
            Useful when running in a non-TTY setting. Defaults to False.
        resume (bool, optional): Resume partially downloaded files and skip already
            downloaded files. Defaults to False.

    Returns:
        Path: Location of the file that was written to

    """
    if resume and _file_has_content(output_file):
        logger.info(f"File {output_file} already exists. Skipping download.")
        return output_file

    part_file = output_file.with_name(f"{output_file.name}.part")
    encoded_url = _escaped_url(url)
    curr_bytes = _partial_byte_count(part_file, resume=resume)

    timeout = aiohttp.ClientTimeout(
        total=download_timeout_seconds,
        connect=connect_timeout_seconds,
    )

    async with aiohttp.ClientSession(timeout=timeout) as session:
        await fetch_to_part_file(
            session=session,
            url=encoded_url,
            part_file=part_file,
            output_filename=output_file.name,
            curr_bytes=curr_bytes,
            chunk_size=chunk_size,
            disable_progress=disable_progress,
        )

    part_file.replace(output_file)
    msg = f"Downloaded to {output_file}"
    logger.info(msg)
    return output_file


def resolve_resume_target(
    output_dir: Path, filename: str, output_file: Path, *, extract_tar: bool
) -> Path | None:
    """Find an already-downloaded path that makes this download unnecessary.

    Args:
        output_dir (Path): Directory the data is written to.
        filename (str): Name of the file being downloaded.
        output_file (Path): Location the download would be written to.
        extract_tar (bool): Whether tarballs are extracted after downloading,
            in which case a previously extracted directory also counts.

    Returns:
        Path | None: An existing complete path, or None if work is still needed.

    """
    extracted_path = _get_extracted_path(output_dir, filename) if extract_tar else None
    if extracted_path is not None:
        # extract_tarball() removes the tarball only after every member has been
        # written, so a surviving tarball means the previous extraction was cut
        # short and the directory is incomplete.
        if not output_file.exists():
            logger.info(
                f"Extracted data for {filename} already exists at "
                f"{extracted_path}. Skipping."
            )
            return extracted_path

        logger.warning(
            f"Found extracted data at {extracted_path}, but {output_file} is still "
            "present, so the previous extraction did not finish. Re-extracting."
        )

    if _file_has_content(output_file):
        logger.info(f"File {output_file} already exists. Skipping download.")
        return output_file

    return None


async def stage_and_download(  # ruff: ignore[too-many-arguments]
    sbid: int,
    result_row: Row,
    casda: CasdaClass,
    output_dir: Path | None = None,
    *,
    disable_progress: bool = False,
    max_retries: int = 3,
    resume: bool = False,
    extract_tar: bool = False,
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
        resume (bool, optional): Resume partially downloaded files and skip already
            downloaded or extracted files. Defaults to False.
        extract_tar (bool, optional): Whether tarballs are to be extracted after
            downloading. Used to check for existing extracted directories when
            resuming. Defaults to False.

    Returns:
        Path: Path to the file that has been downloaded

    """
    if output_dir is None:
        output_dir = Path.cwd() / str(sbid)
        output_dir.mkdir(parents=True, exist_ok=True)

    filename = str(result_row["filename"])
    output_file = output_dir / filename

    if resume:
        existing = resolve_resume_target(
            output_dir, filename, output_file, extract_tar=extract_tar
        )
        if existing is not None:
            return existing

    url = await asyncio.to_thread(get_download_url, result_row, casda)

    return await download_file(
        url,
        output_file,
        disable_progress=disable_progress,
        max_retries=max_retries,
        resume=resume,
    )


def extract_tarball(in_path: Path) -> Path:
    """Extract the contents of a tarball, and delete it once extracted.

    Files are extracted alongside the tarball.

    Args:
        in_path (Path): Location of the tarball.

    Returns:
        Path: Directory containing the extracted files

    """
    if not in_path.is_file() or not tarfile.is_tarfile(in_path):
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
    coros: Iterable[Awaitable[T]], max_limit: int, key: str = "default"
) -> list[Awaitable[T]]:
    """Place a limiter on a set of co-routines via an asynio Semaphore. The `key`
    is used to denote different semaphores from one another, or use a previously
    created semaphore.

    Args:
        coros (Iterable[Awaitable[T]]): The co-routines that will have some limiter
          placed
        max_limit (int): The maximum limit of workers
        key (str, optional): The semaphore to use for this limiter. If the `key` has
          not been used one is created. Defaults to "default".

    Returns:
        list[Awaitable[T]]: New routines with a collective semaphore context applied

    """
    semaphore = SEMAPHORES.get(key)
    if semaphore is None:
        semaphore = asyncio.Semaphore(max_limit)
        SEMAPHORES[key] = semaphore

    async def _limit(_coro: Awaitable[T]) -> T:
        async with semaphore:
            return await _coro

    return [_limit(coro) for coro in coros]


async def get_cutouts_from_casda(  # ruff: ignore[too-many-arguments]
    sbid_list: list[int],
    username: str | None = None,
    *,
    store_password: bool = False,
    reenter_password: bool = False,
    download_options: DownloadOptions | None = None,
    beam: int | None = None,
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
        beam (int | None, optional): Restrict results to a single beam.
            Defaults to None.

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

    sbids_coros: list[Awaitable[Path]] = []

    for sbid in sbid_list:
        result_table: Table = await get_files_to_download(
            sbid,
            download_holography=download_options.download_holography,
            beam=beam,
            vis_type=download_options.vis_type,
            scan_id=download_options.scan_id,
        )

        if len(result_table) == 0:
            logger.warning(f"No files found for {sbid=} with the given filters.")

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
                    max_retries=download_options.max_retries,
                    resume=download_options.resume,
                    extract_tar=download_options.extract_tar,
                )
                for row in result_table
            ]
        )

    paths: list[Path] = []

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
        "--beam",
        type=int,
        help="Beam to download. Defaults to all.",
        default=None,
    )
    parser.add_argument(
        "--scan-id",
        type=int,
        help="Scan ID to download. Defaults to all.",
        default=None,
    )
    parser.add_argument(
        "--vis-type",
        type=str,
        default=None,
        choices=["craco", "science"],
        help="Filter visibilities by product type: 'craco' (cracoData / uvfits) "
        "or 'science' (scienceData / ms). Defaults to None (all).",
    )
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
        default=1,
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
        help="Disable the progress bars produced by `tqdm`.",
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
    parser.add_argument(
        "--resume",
        action="store_true",
        help=(
            "Resume partially downloaded files and skip already downloaded "
            "or extracted files."
        ),
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
        max_retries=args.max_retries,
        resume=args.resume,
        vis_type=args.vis_type,
        scan_id=args.scan_id,
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
            beam=args.beam,
        ),
    )


if __name__ == "__main__":
    main()
