from __future__ import annotations

import pytest

from vis_downloader.async_download import _build_query


def test_no_filters_by_default():
    query = _build_query(sbid=12345)
    assert "obs_id='ASKAP-12345'" in query
    assert "filename LIKE" not in query


@pytest.mark.parametrize(
    ("vis_type", "prefix"),
    [("craco", "cracoData"), ("science", "scienceData")],
)
def test_vis_type_filters_on_prefix(vis_type, prefix):
    assert f"filename LIKE '{prefix}%'" in _build_query(sbid=1, vis_type=vis_type)


def test_scan_id_is_not_tied_to_an_extension():
    # Regression: the scan filter hardcoded .uvfits, so pairing it with
    # --vis-type science built a query that could never match.
    query = _build_query(sbid=1, vis_type="science", scan_id=20260915123000)
    assert "filename LIKE 'scienceData%'" in query
    assert "filename LIKE '%20260915123000%'" in query
    assert "uvfits" not in query


def test_filters_combine():
    query = _build_query(sbid=1, vis_type="craco", beam=3, scan_id=20260915123000)
    assert query.count(" AND ") == 4


def test_holography_ignores_visibility_filters():
    query = _build_query(sbid=1, mode="holography", vis_type="craco", beam=3)
    assert "observation_evaluation_file" in query
    assert "filename" not in query


def test_unknown_mode_raises():
    with pytest.raises(ValueError, match="Unknown"):
        _build_query(sbid=1, mode="nope")
