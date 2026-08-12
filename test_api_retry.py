"""Tests for surviving Wikidata API rate limits.

WikibaseIntegrator retries 500/502/503/504 on the MediaWiki API but lets a 429
fall through to raise_for_status(), so the waiting has to happen on our side.
"""
import pytest
import requests
from wikibaseintegrator.wbi_exceptions import MissingEntityException, NonExistentEntityError

import tools


def http_error(status, retry_after=None):
    """Build the exception requests raises for a failed API response."""
    response = requests.Response()
    response.status_code = status
    if retry_after is not None:
        response.headers['Retry-After'] = str(retry_after)
    return requests.exceptions.HTTPError(f'{status} Client Error', response=response)


def test_rate_limit_wait_reads_retry_after_header():
    assert tools._rate_limit_wait(http_error(429, retry_after=42)) == 42


def test_rate_limit_wait_ignores_non_rate_limit_errors():
    assert tools._rate_limit_wait(http_error(404, retry_after=42)) is None
    assert tools._rate_limit_wait(http_error(429)) is None, 'no header means no hint'
    assert tools._rate_limit_wait(ValueError('unrelated')) is None


def test_retry_waits_exactly_as_long_as_the_server_asked(monkeypatch):
    waits = []
    monkeypatch.setattr(tools.time, 'sleep', lambda s: waits.append(s))
    attempts = {'n': 0}

    def rate_limited_once():
        attempts['n'] += 1
        if attempts['n'] == 1:
            raise http_error(429, retry_after=90)
        return 'ok'

    assert tools.retry_with_backoff(rate_limited_once, 'test', initial_wait=15) == 'ok'
    assert waits == [90], 'Retry-After must win over the exponential backoff'


def test_retry_falls_back_to_backoff_without_a_hint(monkeypatch):
    waits = []
    monkeypatch.setattr(tools.time, 'sleep', lambda s: waits.append(s))
    attempts = {'n': 0}

    def rate_limited_without_header():
        attempts['n'] += 1
        if attempts['n'] < 3:
            raise http_error(429)
        return 'ok'

    assert tools.retry_with_backoff(rate_limited_without_header, 'test',
                                    initial_wait=10, max_wait=100) == 'ok'
    assert waits == [10, 20], 'backoff still grows when the server gives no hint'


def test_server_hint_does_not_inflate_the_backoff(monkeypatch):
    """A rate limit must not double the wait used for later, unrelated errors."""
    waits = []
    monkeypatch.setattr(tools.time, 'sleep', lambda s: waits.append(s))
    attempts = {'n': 0}

    def flaky():
        attempts['n'] += 1
        if attempts['n'] == 1:
            raise http_error(429, retry_after=5)
        if attempts['n'] < 4:
            raise RuntimeError('502 Bad Gateway')
        return 'ok'

    tools.retry_with_backoff(flaky, 'test', initial_wait=10, max_wait=100)
    assert waits == [5, 10, 20]


def test_missing_entity_is_not_retried(monkeypatch):
    """A deleted item is missing on every attempt - waiting out the backoff is wasted."""
    waits = []
    monkeypatch.setattr(tools.time, 'sleep', lambda s: waits.append(s))
    attempts = {'n': 0}

    def entity_is_gone():
        attempts['n'] += 1
        raise MissingEntityException('The MW API returned that the entity was missing.')

    with pytest.raises(MissingEntityException):
        tools.retry_with_backoff(entity_is_gone, 'test')

    assert attempts['n'] == 1, 'a missing entity must fail on the first attempt'
    assert waits == [], 'nothing to wait for'


def test_non_existent_entity_is_not_retried(monkeypatch):
    monkeypatch.setattr(tools.time, 'sleep', lambda s: pytest.fail('must not sleep'))
    attempts = {'n': 0}

    def no_such_entity():
        attempts['n'] += 1
        raise NonExistentEntityError({'code': 'no-such-entity', 'info': 'Could not find such an entity.'})

    with pytest.raises(NonExistentEntityError):
        tools.retry_with_backoff(no_such_entity, 'test')

    assert attempts['n'] == 1


def test_get_item_with_retry_survives_a_rate_limit(monkeypatch):
    monkeypatch.setattr(tools.time, 'sleep', lambda s: None)
    calls = {'n': 0}

    class FakeItem:
        def get(self, qid, **kwargs):
            calls['n'] += 1
            calls['kwargs'] = kwargs
            if calls['n'] == 1:
                raise http_error(429, retry_after=1)
            return f'item:{qid}'

    class FakeWbi:
        item = FakeItem()

    assert tools.get_item_with_retry(FakeWbi(), 'Q42') == 'item:Q42'
    assert calls['n'] == 2
    assert calls['kwargs'].get('maxlag') == tools.API_MAXLAG, 'maxlag must be sent explicitly'


def test_get_item_with_retry_gives_up_eventually(monkeypatch):
    monkeypatch.setattr(tools.time, 'sleep', lambda s: None)

    class FakeItem:
        def get(self, qid, **kwargs):
            raise http_error(429, retry_after=1)

    class FakeWbi:
        item = FakeItem()

    with pytest.raises(requests.exceptions.HTTPError):
        tools.get_item_with_retry(FakeWbi(), 'Q42')
