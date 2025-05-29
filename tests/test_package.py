from __future__ import annotations

import importlib.metadata

import vis_downloader as m


def test_version():
    assert importlib.metadata.version("vis_downloader") == m.__version__
