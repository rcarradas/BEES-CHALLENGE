"""
test_open_brewery_hook.py
==========================
Unit tests for OpenBreweryHook.
All HTTP calls are mocked — no real network requests are made.
"""

import pytest
import requests
from unittest.mock import MagicMock, patch

import plugins.hooks.open_brewery_hook as hook_mod
from plugins.hooks.open_brewery_hook import OpenBreweryHook


def _make_hook():
    with patch.object(hook_mod.BaseHook, "__init__", return_value=None):
        hook = OpenBreweryHook.__new__(OpenBreweryHook)
        hook.base_url = "https://api.openbrewerydb.org/v1/breweries"
        hook.max_registries_per_page = 3
        hook.max_retries = 3
        hook.exponential_backoff_seconds = 0
        hook.api_timeout = 10
        return hook


def _mock_response(data, status_code=200):
    resp = MagicMock()
    resp.status_code = status_code
    resp.json.return_value = data
    if status_code >= 400:
        resp.raise_for_status.side_effect = requests.exceptions.HTTPError(response=resp)
    else:
        resp.raise_for_status.return_value = None
    return resp


class TestGetBreweries:

    def test_single_full_page_then_empty_stops(self):
        hook = _make_hook()
        page1 = [{"id": f"b{i}"} for i in range(3)]
        with patch.object(hook, "_get_breweries_with_retry_and_backoff",
                          side_effect=[page1, []]) as mock_fetch:
            result = hook.get_breweries()
        assert len(result) == 3
        assert mock_fetch.call_count == 2

    def test_partial_last_page_stops_without_extra_request(self):
        hook = _make_hook()
        page1 = [{"id": "b0"}, {"id": "b1"}]
        with patch.object(hook, "_get_breweries_with_retry_and_backoff",
                          return_value=page1) as mock_fetch:
            result = hook.get_breweries()
        assert len(result) == 2
        assert mock_fetch.call_count == 1

    def test_multiple_full_pages_then_partial(self):
        hook = _make_hook()
        full = [{"id": f"b{i}"} for i in range(3)]
        last = [{"id": "b9"}]
        with patch.object(hook, "_get_breweries_with_retry_and_backoff",
                          side_effect=[full, full, full, last]):
            result = hook.get_breweries()
        assert len(result) == 10

    def test_first_page_empty_returns_empty_list(self):
        hook = _make_hook()
        with patch.object(hook, "_get_breweries_with_retry_and_backoff",
                          return_value=[]):
            result = hook.get_breweries()
        assert result == []


class TestRetryAndBackoff:

    def test_successful_request_returns_data(self):
        hook = _make_hook()
        session = MagicMock()
        session.get.return_value = _mock_response([{"id": "b1"}])
        result = hook._get_breweries_with_retry_and_backoff(
            session=session, params={"page": 1, "per_page": 3}, base_url=hook.base_url)
        assert result == [{"id": "b1"}]
        assert session.get.call_count == 1

    def test_4xx_raises_immediately_without_retry(self):
        hook = _make_hook()
        session = MagicMock()
        session.get.return_value = _mock_response([], status_code=404)
        with pytest.raises(requests.exceptions.HTTPError):
            hook._get_breweries_with_retry_and_backoff(
                session=session, params={"page": 1, "per_page": 3}, base_url=hook.base_url)
        assert session.get.call_count == 1

    def test_connection_error_retries_then_raises(self):
        hook = _make_hook()
        session = MagicMock()
        session.get.side_effect = requests.exceptions.ConnectionError("refused")
        with patch("time.sleep"):
            with pytest.raises(requests.exceptions.ConnectionError):
                hook._get_breweries_with_retry_and_backoff(
                    session=session, params={"page": 1, "per_page": 3}, base_url=hook.base_url)
        assert session.get.call_count == hook.max_retries

    def test_succeeds_on_second_attempt_after_transient_error(self):
        hook = _make_hook()
        session = MagicMock()
        session.get.side_effect = [
            requests.exceptions.ConnectionError("timeout"),
            _mock_response([{"id": "b1"}]),
        ]
        with patch("time.sleep"):
            result = hook._get_breweries_with_retry_and_backoff(
                session=session, params={"page": 1, "per_page": 3}, base_url=hook.base_url)
        assert result == [{"id": "b1"}]
        assert session.get.call_count == 2

    def test_timeout_error_retries(self):
        hook = _make_hook()
        session = MagicMock()
        session.get.side_effect = [
            requests.exceptions.Timeout("timed out"),
            _mock_response([{"id": "b1"}]),
        ]
        with patch("time.sleep"):
            result = hook._get_breweries_with_retry_and_backoff(
                session=session, params={"page": 1, "per_page": 3}, base_url=hook.base_url)
        assert result == [{"id": "b1"}]