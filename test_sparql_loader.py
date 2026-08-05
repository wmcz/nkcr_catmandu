"""Tests for the VALUES-based NK ČR dataset loader in tools.py.

These run offline: the SPARQL call is monkeypatched with recorded response
shapes captured from query-main.wikidata.org.
"""
import pytest

import tools

ENTITY = 'http://www.wikidata.org/entity/'


def binding(nkcr, qid, **values):
    """Build one result row in the shape the endpoint returns."""
    row = {
        'item': {'type': 'uri', 'value': ENTITY + qid},
        'nkcr': {'type': 'literal', 'value': nkcr},
    }
    for key in ('isnis', 'orcids', 'births', 'deaths', 'fields', 'occups',
                'bplaces', 'dplaces', 'wplaces', 'languages'):
        row[key] = {'type': 'literal', 'value': values.get(key, '')}
    return row


def response(*bindings):
    return {'head': {'vars': []}, 'results': {'bindings': list(bindings)}}


def test_parse_builds_all_four_datasets_with_legacy_keys():
    datasets = tools.new_nkcr_datasets()
    tools.parse_nkcr_bindings([binding(
        'xx0338321', 'Q137018639',
        isnis='0000000505997791',
        births='1990-01-01T00:00:00Z',
        occups=ENTITY + 'Q688576|' + ENTITY + 'Q56224063',
        bplaces=ENTITY + 'Q1085',
        languages=ENTITY + 'Q1860',
    )], datasets)

    items = datasets['non_deprecated_items']['xx0338321']
    assert items == {'qid': 'Q137018639', 'isni': ['0000000505997791'],
                     'orcid': [], 'birth': ['1990-01-01T00:00:00Z'], 'death': []}

    # occupation QIDs must have the entity prefix stripped, like the old loader
    assert datasets['field_of_work_and_occupation']['xx0338321'] == {
        'qid': 'Q137018639', 'field': [], 'occup': ['Q688576', 'Q56224063']}

    # 'birth' means a place here and a date above - separate dicts, same key name
    assert datasets['places']['xx0338321'] == {
        'qid': 'Q137018639', 'birth': ['Q1085'], 'death': [], 'work': []}

    assert datasets['languages']['xx0338321'] == {
        'qid': 'Q137018639', 'language': ['Q1860']}


def test_empty_group_concat_becomes_empty_list():
    datasets = tools.new_nkcr_datasets()
    tools.parse_nkcr_bindings([binding('js20251279673', 'Q137018575')], datasets)

    entry = datasets['non_deprecated_items']['js20251279673']
    assert entry['isni'] == [] and entry['birth'] == []
    assert datasets['places']['js20251279673']['work'] == []


def test_two_items_sharing_one_authority_merge_without_duplicates():
    datasets = tools.new_nkcr_datasets()
    tools.parse_nkcr_bindings([
        binding('xx0000001', 'Q1', occups=ENTITY + 'Q100'),
        binding('xx0000001', 'Q2', occups=ENTITY + 'Q100|' + ENTITY + 'Q200'),
    ], datasets)

    entry = datasets['field_of_work_and_occupation']['xx0000001']
    assert entry['qid'] == 'Q1', 'first item wins, as in the previous loader'
    assert entry['occup'] == ['Q100', 'Q200']


def test_every_batch_is_queried_and_nothing_is_silently_dropped(monkeypatch):
    """An empty batch must not be read as 'end of data' - that was the old bug."""
    asked = []

    def fake_post(query, timeout=None):
        ids = [part.strip('"') for part in query.split('VALUES ?nkcr {')[1]
               .split('}')[0].split()]
        asked.extend(ids)
        # middle batch legitimately matches nothing on Wikidata
        if 'a10' in ids:
            return response()
        return response(*[binding(i, 'Q' + i[1:]) for i in ids])

    monkeypatch.setattr(tools, 'execute_sparql_post', fake_post)
    monkeypatch.setattr(tools.time, 'sleep', lambda s: None)

    ids = ['a%d' % n for n in range(25)]
    datasets = tools.load_non_deprecated_items_by_nkcr(ids, batch_size=10, pause_seconds=0)

    assert asked == ids, 'all ids must be queried even after an empty batch'
    # batches are a0-a9 (found), a10-a19 (empty), a20-a24 (found)
    assert len(datasets['non_deprecated_items']) == 15


def test_retry_recovers_from_transient_endpoint_failure(monkeypatch):
    waits = []
    monkeypatch.setattr(tools.time, 'sleep', lambda s: waits.append(s))
    attempts = {'n': 0}

    def flaky():
        attempts['n'] += 1
        if attempts['n'] < 4:
            raise RuntimeError('502 Server Error: Bad Gateway')
        return 'ok'

    assert tools.retry_with_backoff(flaky, 'test', initial_wait=1, max_wait=8) == 'ok'
    assert attempts['n'] == 4
    assert waits == [1, 2, 4], 'wait must grow exponentially, not stay flat'


def test_retry_gives_up_only_after_many_attempts(monkeypatch):
    """The worst burst observed in production was 10 failures in a row."""
    monkeypatch.setattr(tools.time, 'sleep', lambda s: None)
    attempts = {'n': 0}

    def always_fails():
        attempts['n'] += 1
        raise RuntimeError('Server query-main.wikidata.org timed out')

    with pytest.raises(RuntimeError):
        tools.retry_with_backoff(always_fails, 'test')

    assert attempts['n'] > 10, 'must survive a 10-failure burst like the one logged'
