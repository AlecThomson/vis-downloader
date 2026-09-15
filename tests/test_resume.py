from __future__ import annotations

import asyncio
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from astropy.table import Row, Table

from vis_downloader.async_download import (
    DownloadOptions,
    _get_extracted_path,
    download_file,
    extract_tarball,
    stage_and_download,
)


def test_get_extracted_path(tmp_path: Path):
    out_dir = tmp_path / "out"
    out_dir.mkdir()

    extracted_dir = out_dir / "scienceData_SB123_beam00.ms"
    extracted_dir.mkdir()

    tar_name = "scienceData_SB123_beam00.ms.tar"
    match = _get_extracted_path(out_dir, tar_name)
    assert match == extracted_dir

    no_match = _get_extracted_path(out_dir, "scienceData_SB123_beam01.ms.tar")
    assert no_match is None


def test_download_options_resume_field():
    opts_default = DownloadOptions()
    assert opts_default.resume is False

    opts_resume = DownloadOptions(resume=True)
    assert opts_resume.resume is True


@pytest.mark.asyncio
async def test_stage_and_download_skip_extracted_dir(tmp_path: Path):
    out_dir = tmp_path / "123"
    out_dir.mkdir()

    extracted_dir = out_dir / "scienceData_SB123_beam00.ms"
    extracted_dir.mkdir()

    row = {"filename": "scienceData_SB123_beam00.ms.tar"}
    mock_casda = MagicMock()

    with patch("vis_downloader.async_download.get_download_url") as mock_url:
        result = await stage_and_download(
            sbid=123,
            result_row=row,
            casda=mock_casda,
            output_dir=out_dir,
            resume=True,
            extract_tar=True,
        )
        assert result == extracted_dir
        mock_url.assert_not_called()


@pytest.mark.asyncio
async def test_stage_and_download_skip_existing_output_file(tmp_path: Path):
    out_dir = tmp_path / "123"
    out_dir.mkdir()

    existing_file = out_dir / "scienceData_SB123_beam00.ms.tar"
    existing_file.write_bytes(b"existing content")

    row = {"filename": "scienceData_SB123_beam00.ms.tar"}
    mock_casda = MagicMock()

    with patch("vis_downloader.async_download.get_download_url") as mock_url:
        result = await stage_and_download(
            sbid=123,
            result_row=row,
            casda=mock_casda,
            output_dir=out_dir,
            resume=True,
            extract_tar=False,
        )
        assert result == existing_file
        mock_url.assert_not_called()


@pytest.mark.asyncio
async def test_download_file_skip_if_complete(tmp_path: Path):
    output_file = tmp_path / "test.tar"
    output_file.write_bytes(b"completed data")

    # Should return early without performing any network calls
    result = await download_file(
        url="http://example.com/test.tar",
        output_file=output_file,
        resume=True,
        disable_progress=True,
    )
    assert result == output_file
    assert output_file.read_bytes() == b"completed data"


class MockAsyncIterator:
    def __init__(self, chunks: list[bytes]):
        self._chunks = list(chunks)

    def __aiter__(self):
        return self

    async def __anext__(self):
        if not self._chunks:
            raise StopAsyncIteration
        return self._chunks.pop(0)


class MockContent:
    def __init__(self, chunks: list[bytes]):
        self._chunks = chunks

    def iter_chunked(self, chunk_size: int):
        return MockAsyncIterator(self._chunks)


class MockResponse:
    def __init__(self, status: int, headers: dict[str, str], chunks: list[bytes]):
        self.status = status
        self.headers = headers
        self.content = MockContent(chunks)

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        pass


class MockSession:
    def __init__(self, response: MockResponse):
        self._response = response
        self.recorded_headers = None

    def get(self, url, headers=None):
        self.recorded_headers = headers
        return self._response

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        pass


@pytest.mark.asyncio
async def test_download_file_resume_partial(tmp_path: Path):
    output_file = tmp_path / "partial.dat"
    part_file = tmp_path / "partial.dat.part"

    initial_data = b"Hello, "
    part_file.write_bytes(initial_data)

    resumed_data = b"World!"
    mock_resp = MockResponse(
        status=206,
        headers={
            "content-length": str(len(resumed_data)),
            "Content-Range": f"bytes 7-12/{len(initial_data) + len(resumed_data)}",
        },
        chunks=[resumed_data],
    )
    mock_session = MockSession(mock_resp)

    with patch("aiohttp.ClientSession", return_value=mock_session):
        result = await download_file(
            url="http://example.com/partial.dat",
            output_file=output_file,
            resume=True,
            disable_progress=True,
        )

    assert result == output_file
    assert not part_file.exists()
    assert output_file.read_bytes() == b"Hello, World!"
    assert mock_session.recorded_headers == {"Range": "bytes=7-"}


@pytest.mark.asyncio
async def test_download_file_resume_server_returns_200(tmp_path: Path):
    output_file = tmp_path / "reset.dat"
    part_file = tmp_path / "reset.dat.part"

    stale_data = b"old partial"
    part_file.write_bytes(stale_data)

    full_data = b"complete new content"
    mock_resp = MockResponse(
        status=200,
        headers={"content-length": str(len(full_data))},
        chunks=[full_data],
    )
    mock_session = MockSession(mock_resp)

    with patch("aiohttp.ClientSession", return_value=mock_session):
        result = await download_file(
            url="http://example.com/reset.dat",
            output_file=output_file,
            resume=True,
            disable_progress=True,
        )

    assert result == output_file
    assert not part_file.exists()
    assert output_file.read_bytes() == full_data


def test_extract_tarball_directory(tmp_path: Path):
    dir_path = tmp_path / "already_extracted_folder"
    dir_path.mkdir()

    # extract_tarball should return dir_path as-is without raising
    assert extract_tarball(dir_path) == dir_path
