"""Get all the data off CASDA"""
import argparse
import asyncio
import logging
from pathlib import Path
from typing import Awaitable, TypeVar, cast

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

async def get_staging_url(sbid: int) -> Table:
    tap = TapPlus(url="https://casda.csiro.au/casda_vo_tools/tap")
    query_str = f"SELECT * FROM ivoa.obscore where obs_id='ASKAP-{sbid}' AND dataproduct_type='visibility'"
    msg = f"Querying CASDA for {sbid}"
    logger.info(msg)
    msg = f"Query: {query_str}"
    logger.debug(msg)
    job = await asyncio.to_thread(tap.launch_job_async, query_str)
    results = job.get_results()

    if results is None:
        msg = f"Query was {query_str}"
        logger.error(msg)
        msg = "No results found!"
        raise ValueError(msg)
    
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
    connect_timeout_seconds: int = 30, 
    download_timeout_seconds: int = 60*60*12, 
    chunk_size: int = 100000,
    max_retries: int = 3
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
    max_retries: int, optional
       Maximum number of retries should the download fail. Defaults to 3.

    Raises
    ------
    IonexError
        If the download times out.
    """
    msg = f"Using aiohttp, Downloading from {url}"
    logger.info(msg)
    
    timeout = aiohttp.ClientTimeout(total=download_timeout_seconds, connect=connect_timeout_seconds)
    import requests
    async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(url) as response:
                assert response.status == 200, f"{response.status=}, not successful"
                
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
):
    url = await asyncio.to_thread(get_download_url, result_table, casda)
    output_file = output_dir / result_table["filename"]
    return await download_file(url, output_file)

async def download_sbid_from_casda(
        sbid: int,
        output_dir: Path,
        casda: CasdaClass,
        max_workers: int | None = None,
) -> list[Path]:
    result_table: Table = await get_staging_url(sbid)
    
    if output_dir is None:
        output_dir = Path(os.getcwd()) / str(sbid)
        output_dir.mkdir(parents=True, exist_ok=True)
    
    coros = []
    for row in result_table:
        coros.append(stage_and_download(row, output_dir, casda))

    return await gather_with_limit(max_workers, *coros, desc="Download")



async def get_cutouts_from_casda(
    sbid_list: list[int],
    output_dir: Path | None = None,
    username: str | None = None,
    store_password: bool = False,
    reenter_password: bool = False,
    max_workers: int | None = None,
) -> list[Path]:
    casda = casda_login(
        username=username,
        store_password=store_password,
        reenter_password=reenter_password,
    )

    coros = []
    for sbid in sbid_list:
        coros.append(await download_sbid_from_casda(sbid, output_dir, casda, max_workers=max_workers))
    
    return coros

def main() -> None:
    parser = argparse.ArgumentParser(description="Download visibilities from CASDA for a given SBID")
    parser.add_argument("sbids", nargs="+", type=int, help="SBID to download")
    parser.add_argument("--output-dir", type=Path, help="Output directory. If unset a directory for each SBID will be created.", default=None)
    parser.add_argument("--username", type=str, help="CASDA username", default=None)
    parser.add_argument("--store-password", action="store_true", help="Store password in keyring")
    parser.add_argument("--reenter-password", action="store_true", help="Reenter password")
    parser.add_argument("--max-workers", type=int, help="Number of workers", default=None)
    args = parser.parse_args()

    asyncio.run(
        get_cutouts_from_casda(
            sbid_list=args.sbids,
            output_dir=args.output_dir,
            username=args.username,
            store_password=args.store_password,
            reenter_password=args.reenter_password,
            max_workers=args.max_workers,
        )
    )


if __name__ == "__main__":
    main()