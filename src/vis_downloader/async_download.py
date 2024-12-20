#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Get all the data off CASDA"""
import asyncio
import logging
import os
from pathlib import Path
import argparse
from typing import Awaitable, TypeVar, cast


from astropy import log as logger
from astropy.table import Table, Row
from astroquery.casda import Casda, CasdaClass
import requests
from tqdm.asyncio import tqdm

from astroquery.utils.tap.core import TapPlus

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
        return cast(list[T], await tqdm.gather(*coros, desc=desc))

    semaphore = asyncio.Semaphore(limit)

    async def sem_coro(coro: Awaitable[T]) -> T:
        async with semaphore:
            return await coro

    return cast(
        list[T],
        await tqdm.gather(*(sem_coro(c) for c in coros), desc=desc),
    )

def casda_login(
    username: str | None = None,
    store_password: bool = False,
    reenter_password: bool = False,
) -> CasdaClass:
    """Login to CASDA.

    Args:
        username (str | None, optional): CASDA username. Defaults to None.
        store_password (bool, optional): Stores the password securely in your keyring. Defaults to False.
        reenter_password (bool, optional): Asks for the password even if it is already stored in the keyring. This is the way to overwrite an already stored passwork on the keyring. Defaults to False.

    Returns:
        CasdaClass: CASDA class
    """
    casda: CasdaClass = Casda()
    if username is None:
        username = os.environ.get("CASDA_USERNAME")
    if username is None:
        username = input("Please enter your CASDA username: ")

    casda.login(
        username=username,
        store_password=store_password,
        reenter_password=reenter_password,
    )

    return casda

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

async def get_download_url(result_row: Row, casda: CasdaClass) -> str:
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
    url_list: list[str] = await asyncio.to_thread(casda.stage_data, Table(result_row))

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
    timeout_seconds: int = 30,
    chunk_size: int = 1000,
) -> Path:
    """Download a file from a given URL using asyncio.

    Parameters
    ----------
    url : str
        URL to download.
    output_file : Path
        Output file path.
    timeout_seconds : int, optional
        Seconds to wait for request timeout, by default 30
    chunk_size : int, optional
        Chunks of data to download, by default 1000

    Raises
    ------
    IonexError
        If the download times out.
    """
    msg = f"Downloading from {url}"
    logger.info(msg)
    try:
        response = await asyncio.to_thread(requests.get, url, timeout=timeout_seconds)
    except requests.exceptions.Timeout as e:
        msg = "Timed out connecting to server"
        logger.error(msg)
        raise Exception(msg) from e

    response.raise_for_status()

    logger.info(f"Saving to {output_file}")
    total_size = int(response.headers.get("content-length", 0))
    total_size / chunk_size

    with output_file.open("wb") as file_desc, tqdm(
        total=total_size, unit="B", unit_scale=True, unit_divisor=chunk_size, desc=output_file.name
    ) as pbar:
        for chunk in response.iter_content(chunk_size=chunk_size):
            pbar.update(len(chunk))
            await asyncio.to_thread(file_desc.write, chunk)

    msg = f"Downloaded to {output_file}"
    logger.info(msg)
    return output_file

async def stage_and_download(
        result_table: Row,
        output_dir: Path,
        casda: CasdaClass,
):
    url = await get_download_url(result_table, casda)
    output_file = output_dir / result_table["filename"]
    return await download_file(url, output_file)

async def download_sbid_from_casda(
        sbid: int,
        output_dir: Path,
        casda: CasdaClass,
        max_workers: int | None = None,
) -> list[Path]:
    result_table: Table = await get_staging_url(sbid)
    coros = []
    for row in result_table:
        coros.append(stage_and_download(row, output_dir, casda))

    return await gather_with_limit(max_workers, *coros, desc=f"MSs for SBID {sbid}")



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

    if output_dir is None:
        output_dir = Path.cwd()

    coros = []
    for sbid in sbid_list:
        coros.append(download_sbid_from_casda(sbid, output_dir, casda, max_workers=max_workers))
    
    return await gather_with_limit(max_workers, *coros, desc="SBIDs")

def main() -> None:
    parser = argparse.ArgumentParser(description="Download visibilities from CASDA for a given SBID")
    parser.add_argument("sbids", nargs="+", type=int, help="SBID to download")
    parser.add_argument("--output-dir", type=Path, help="Output directory", default=None)
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