"""Get all the data off CASDA"""
import argparse
import asyncio
import logging
from pathlib import Path
from typing import Awaitable, TypeVar, cast
from dataclasses import dataclass

import os
import aiohttp
from astropy import log as logger
from astropy.table import Row, Table
from astroquery.casda import CasdaClass
from astroquery.utils.tap.core import TapPlus
from tqdm.asyncio import tqdm

from vis_downloader.casda_login import login as casda_login

T = TypeVar("T")

logger.setLevel(logging.INFO)

CASDATAP: TapPlus = TapPlus(url="https://casda.csiro.au/casda_vo_tools/tap")
            

@dataclass
class DownloadOptions:
    """options to use for downloading of CASDA SBID data"""
    output_dir: Path | None = None
    """Output directory to write files to. If None output directory is built from the current working directory and SBID befing downloaded. Defaults to None."""
    extract_tar: bool = False
    """Extract tarballs at the end of downloading"""
    download_holography: bool = False
    """Download the evaluation file that contains the holography"""
    max_workers: int = 1
    """The maximum number of download workers to use"""
    log_only: bool = False
    """Simply log the URLs to download. Don't download."""


# Stolen from https://stackoverflow.com/a/61478547
async def gather_with_limit(
    limit: int | None, *coros: Awaitable[T], desc: str | None = None, as_completed: bool = False
) -> list[T]:
    """Gather with a limit on the number of coroutines running at once.

    Args:
        limit (int): The number of coroutines to run at once
        coros (Awaitable): The coroutines to run

    Returns:
        Awaitable: The result of the coroutines
    """
    tqdm_func = tqdm.as_completed if as_completed else tqdm.gather
    
    if limit is None:
        return cast(list[T], await tqdm_func(*coros, maxinterval=100000000,  desc=desc))

    semaphore = asyncio.Semaphore(limit)

    async def sem_coro(coro: Awaitable[T]) -> T:
        async with semaphore:
            return await coro

    return cast(
        list[T],
        await tqdm_func(*(sem_coro(c) for c in coros), maxinterval=100000000, desc=desc),
    )



from typing import Literal
async def _get_holography_url(sbid: int, mode: Literal["vis", "holography"] ="vis") -> Table:
    """Internal function to generate and execute a TAQL query.

    Args:
        sbid (int): The SBID we want files for
        mode (Literal[&quot;vis&quot;, &quot;holography&quot;], optional): Wheter visibilities or holography will be downloaded. Defaults to "vis".

    Raises:
        ValueError: Raised if `mode` is not known
        ValueError: Raised if the remote request returns failed

    Returns:
        Table: Matching results of the TAQL request
    """
    
        
    if mode == "vis":
        query_str = f"SELECT TOP 10000 * FROM ivoa.obscore where obs_id='ASKAP-{sbid}' AND dataproduct_type='visibility'"
    elif mode == "holography":
        query_str = f"SELECT TOP 10000 * FROM casda.observation_evaluation_file where sbid='{sbid}' and format='calibration'"
    else:
        raise ValueError(f"Unknown {mode=}")
    
    logger.info(f"Querying CASDA for {sbid=} {mode=}")
    
    job = await asyncio.to_thread(CASDATAP.launch_job_async, query_str)
    results = job.get_results()

    if results is None:
        raise ValueError(f"Failed to find holography for {sbid=}")
    
    return results

async def get_files_to_download(
    sbid: int,
    download_holography: bool = False
) -> Table:
    """Lookup in CASDA files to download for a specified SBID.

    Args:
        sbid (int): The SBID to download
        download_holography (bool, optional): Whether holography data needs to be downloaded. Defaults to False.

    Returns:
        Table: Result set of matching files. Should multuple requests be made the intersection of columns between tables is returned.
    """
    from astropy.table import vstack
    
    tables: list[Table] = []
    results = await _get_holography_url(sbid=sbid)
    tables.append(results)
    
    if download_holography:
        results = await _get_holography_url(sbid=sbid, mode="holography")
        tables.append(results)
    
    results = vstack(tables, join_type="inner")
    return results

def get_download_url(result_row: Row, casda: CasdaClass) -> str:
    """Get the download URL for a file on CASDA.

    Args:
        result_table (Table): Table of results
        casda (CasdaClass): CASDA class

    Raises:
        ValueError: If no results are found
        ValueError: If multiple results are found

    Returns:
        str: Download URL
    """
    logger.info("Staging data on CASDA...")
    url_list: list[str] = casda.stage_data(Table(result_row))
    

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

async def download_file(
    url: str,
    output_file: Path,
    connect_timeout_seconds: int = 120, 
    download_timeout_seconds: int = 60*60*12, 
    chunk_size: int = 1000000,
) -> Path:
    """Download a file from CASDA, streaming it to its final location.

    Args:
        url (str): The URL describing the remote resources to download
        output_file (Path): The location to write the file to.
        connect_timeout_seconds (int, optional): The acceptable amount of time to establish a connection to server. Defaults to 60.
        download_timeout_seconds (int, optional): The acceptable amoutn of time to wait for the download to finish. Defaults to 60*60*12.
        chunk_size (int, optional): Size of data blocks to store in memory before flushing to disk. Defaults to 1000000.

    Raises:
        ValueError: A status code other than 200 is returned when accessing the server

    Returns:
        Path: Location of the file that was written to
    """
    msg = f"Using aiohttp, Downloading from {url}"
    logger.info(msg)
    
    timeout = aiohttp.ClientTimeout(total=download_timeout_seconds, connect=connect_timeout_seconds)
    async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(url) as response:
                if response.status != 200:
                    raise ValueError(f"{response.status=}, indicating the request was no successful.")
                
                total_size = int(response.headers.get("content-length", 0))
                
                with output_file.open("wb") as file_desc, tqdm(
                    total=total_size, unit="B", unit_scale=True, unit_divisor=1024, desc=output_file.name
                ) as pbar:
                    async for chunk in response.content.iter_chunked(chunk_size):
                        pbar.update(len(chunk))
            
                        file_desc.write(chunk)
                    
    msg = f"Downloaded to {output_file}"
    logger.info(msg)
    return output_file


async def stage_and_download(
    sbid: int,
    result_row: Row,
    casda: CasdaClass,
    output_dir: Path | None = None,      
) -> Path:
    """Trigger CASDA to stage the data, and then download it once
    it has been staged. The `result_table` is generated via the TAQL
    query.

    Args:
        sbid (int): The SBID of the data being downloaded
        result_row (Row): A data row to download, including its url and file name
        casda (CasdaClass): An activate CASDA session that has passed user authentication
        output_dir (Path | None, optional): The location to write the data to. If None data will be downloaded into a folder for the SBID. Defaults to None.

    Returns:
        Path: Path to the file that has been downloaded
    """
    if output_dir is None:
        output_dir = Path(os.getcwd()) / str(sbid)
        output_dir.mkdir(parents=True, exist_ok=True)
    
    
    url = await asyncio.to_thread(get_download_url, result_row, casda)
    output_file = output_dir / result_row["filename"]
    
    return await download_file(url, output_file)


def extract_tarball(in_path: Path) -> Path:
    """Extract the contents of a tarball, and delete it once extracted. 
    Files are extracted alongside the tarball.

    Args:
        in_path (Path): Location of the tarball. 

    Returns:
        Path: Directory containing the extracted files
    """
    import tarfile

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

async def iterator(items: list[T]) -> T:
    for item in items:
        yield item

async def get_cutouts_from_casda(
    sbid_list: list[int],
    username: str | None = None,
    store_password: bool = False,
    reenter_password: bool = False,
    download_options: DownloadOptions | None = None
) -> list[Path]:
    """Download visibilities and other products for a nominated set of SBIDs
    from CASDA.

    Args:
        sbid_list (list[int]): Set of SBIDs to download data for
        username (str | None, optional): The username to use to authenticate with. Defaults to None.
        store_password (bool, optional): Whether the password should be stored in a keyring. Defaults to False.
        reenter_password (bool, optional): Force the password to be entered. Defaults to False.
        download_options (DownloadOptions | None, optional): Settings to use while downloading. Defaults to None.

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

    coros = []
    for sbid in sbid_list:
        result_table: Table = await get_files_to_download(sbid, download_holography=download_options.download_holography)
        
        if download_options.log_only:
            logger.info(result_table)
            continue
        
        paths = []
        outer_semaphore = asyncio.Semaphore(download_options.max_workers)
        inner_semaphore = asyncio.Semaphore(12)
        
        async for row in iterator(result_table):
            async with outer_semaphore:
                path = await stage_and_download(
                    sbid=sbid, result_row=row, output_dir=download_options.output_dir, casda=casda
                )
            async with inner_semaphore:
                if download_options.extract_tar:
                    path = await asyncio.to_thread(extract_tarball, in_path=path)
    
                paths.append(path)
                
    return paths

def main() -> None:
    parser = argparse.ArgumentParser(description="Download visibilities from CASDA for a given SBID")
    parser.add_argument("sbids", nargs="+", type=int, help="SBID to download")
    parser.add_argument("--output-dir", type=Path, help="Output directory. If unset a directory for each SBID will be created.", default=None)
    parser.add_argument("--username", type=str, help="CASDA username", default=None)
    parser.add_argument("--store-password", action="store_true", help="Store password in keyring")
    parser.add_argument("--reenter-password", action="store_true", help="Reenter password")
    parser.add_argument("--max-workers", type=int, help="Number of workers", default=None)
    parser.add_argument("--extract-tar", action="store_true", help="If a file is a tarball attempt to extract it. This removes the original tar file if successful.")
    parser.add_argument("--download-holography", action="store_true", help="Download the evaluation files that contain the holography data")
    parser.add_argument("--log-only", action="store_true")    
    
    args = parser.parse_args()
    
    download_options = DownloadOptions(
        output_dir=args.output_dir,
        extract_tar=args.extract_tar,
        download_holography=args.download_holography,
        max_workers=args.max_workers,
        log_only=args.log_only,
    )
    
    asyncio.run(
        get_cutouts_from_casda(
            sbid_list=args.sbids,
            username=args.username,
            store_password=args.store_password,
            reenter_password=args.reenter_password,
            download_options=download_options
        )
    )


if __name__ == "__main__":
    main()