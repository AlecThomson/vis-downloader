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
from tqdm import tqdm as sync_tqdm

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
    limit: int | None, *coros: Awaitable[T], desc: str | None = None
) -> list[T]:
    """Gather with a limit on the number of coroutines running at once.

    Args:
        limit (int): The number of coroutines to run at once
        coros (Awaitable): The coroutines to run

    Returns:
        Awaitable: The result of the coroutines
    """
    if limit is None:
        return cast(list[T], await tqdm.gather(*coros, maxinterval=100000000,  desc=desc))

    semaphore = asyncio.Semaphore(limit)

    async def sem_coro(coro: Awaitable[T]) -> T:
        async with semaphore:
            return await coro

    return cast(
        list[T],
        await tqdm.gather(*(sem_coro(c) for c in coros), maxinterval=100000000, desc=desc),
    )

from typing import Literal
async def _get_holography_url(sbid: int, mode: Literal["vis", "holography"] ="vis") -> Table:
    
    if mode == "vis":
        query_str = f"SELECT TOP 10000 * FROM casda.observation_evaluation_file where sbid='{sbid}'"
    elif mode == "holography":
        query_str = f"SELECT TOP 10000 * FROM casda.observation_evaluation_file where sbid='{sbid}'"
    else:
        raise ValueError(f"Unknown {mode=}")
    
    logger.info(f"Querying CASDA for {sbid=} {mode=}")
    
    job = await asyncio.to_thread(CASDATAP.launch_job_async, query_str)
    results = job.get_results()

    if results is None:
        raise ValueError(f"Failed to find holography for {sbid=}")
    
    return results

async def get_staging_url(
    sbid: int,
    download_holography: bool = False
) -> Table:
    
    results = await _get_holography_url(sbid=sbid)
    logger.info(results)
    logger.info(type(results))
    
    if download_holography:
        results = await _get_holography_url(sbid=sbid, mode="holography")
        logger.info(results)
        logger.info(type(results)) 
    
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
    url_list: list[str] = casda.stage_data(Table(result_row)
    )
    

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
    connect_timeout_seconds: int = 60, 
    download_timeout_seconds: int = 60*60*12, 
    chunk_size: int = 1000000,
) -> Path:
    """Download a file from a given URL using asyncio.

    Parameters
    ----------
    url : str
        URL to download.
    output_file : Path
        Output file path.
    connect_timeout_seconds : int, optional
        Number of seconds to wait to establish connection. Defaults to 30.
    download_timeout_seconds : int, optional
        Allowed length of time to dowload a file, in seconds. Defults to 12 hours.
    chunk_size : int, optional
        Chunks of data to download, by default 1000
    
    Raises
    ------
    IonexError
        If the download times out.
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
        result_table: Row,
        output_dir: Path,
        casda: CasdaClass,
) -> Path:
    url = await asyncio.to_thread(get_download_url, result_table, casda)
    output_file = output_dir / result_table["filename"]
    
    return await download_file(url, output_file)

async def download_sbid_from_casda(
        sbid: int,
        row,
        output_dir: Path,
        casda: CasdaClass,
) -> list[Awaitable[Path]]:
    
    if output_dir is None:
        output_dir = Path(os.getcwd()) / str(sbid)
        output_dir.mkdir(parents=True, exist_ok=True)
    
    path = await stage_and_download(row, output_dir, casda)

    return path

def extract_tarball(in_path: Path) -> Path:

    import tarfile

    if not tarfile.is_tarfile(in_path):
        return in_path

    logger.info(f"Extracting {in_path=}")

    with tarfile.open(name=in_path) as open_tarfile:
        open_tarfile.extractall(path=in_path.parent, filter="data")

    in_path.unlink()
    
    return in_path.parent


async def get_cutouts_from_casda(
    sbid_list: list[int],
    username: str | None = None,
    store_password: bool = False,
    reenter_password: bool = False,
    download_options: DownloadOptions | None = None
) -> list[Path]:
    if download_options is None:
        download_options = DownloadOptions()
    
    casda = casda_login(
        username=username,
        store_password=store_password,
        reenter_password=reenter_password,
    )

    coros = []
    for sbid in sbid_list:
        result_table: Table = await get_staging_url(sbid, )
        
        if download_options.log_only:
            logger.info(result_table)
            continue
        
        for row in result_table:
            coros.append(
                download_sbid_from_casda(
                    sbid=sbid, row=row, output_dir=download_options.output_dir, casda=casda
                )
            )
    

    logger.info(f"{coros=}")
    logger.info(f"{len(coros)=}")
    
    paths = await gather_with_limit(download_options.max_workers, *coros, desc="Download")
    
    if download_options.extract_tar:
        coros = [asyncio.to_thread(extract_tarball, in_path=path) for path in paths]
        paths = await gather_with_limit(download_options.max_workers, *coros, desc="Extracting tarballs")
    
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