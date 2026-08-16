"""Tests for splitting NK ČR field values and looking their terms up on Wikidata.

The CSV joins repeated MARC fields with '|' and repeated subfields with '$',
so both separators can appear in one value - splitting on only one of them
produced lookup keys like 'cze|cze' that never match anything.
"""
import cleaners
import pytest
import tools
from context import PipelineContext


@pytest.mark.parametrize(
    "value,expected",
    [
        ('malíři|sochaři', ['malíři', 'sochaři']),
        ('malíři,', ['malíři']),
        ('', []),
        ('malíři$sochaři', ['malíři', 'sochaři']),
    ],
)
def test_split_occupation_names(value, expected):
    assert cleaners.split_occupation_names(value) == expected


@pytest.mark.parametrize(
    "value,expected",
    [
        ('cze', ['cze']),
        ('cze|cze', ['cze']),
        ('eng$cze', ['eng', 'cze']),
        ('eng|cze', ['eng', 'cze']),
        ('', []),
    ],
)
def test_split_language_codes(value, expected):
    assert cleaners.split_language_codes(value) == expected


@pytest.mark.parametrize(
    "value,expected",
    [
        ('Praha (Česko)', ['Praha (Česko)']),
        ('Louny, Česko', ['Louny (Česko)']),
        ('Praha (Česko)$Brno (Česko)', ['Praha (Česko)', 'Brno (Česko)']),
        ('Praha (Česko)|Brno (Česko)', ['Praha (Česko)', 'Brno (Česko)']),
        # both separators in one value - the old code split on '$' only and
        # kept 'Brno (Česko)|Plzeň (Česko)' as a single unusable key
        ('Praha (Česko)$Brno (Česko)|Plzeň (Česko)',
         ['Praha (Česko)', 'Brno (Česko)', 'Plzeň (Česko)']),
        ('', []),
    ],
)
def test_split_place_names(value, expected):
    assert cleaners.split_place_names(value) == expected


def test_language_lookup_handles_repeated_fields():
    context = PipelineContext()
    context.language_dict = {'cze': 'Q9056', 'eng': 'Q1860'}

    assert cleaners.prepare_language_from_nkcr('cze|cze', '377a', context) == ['Q9056']
    assert cleaners.prepare_language_from_nkcr('eng$cze', '377a', context) == ['Q1860', 'Q9056']


def test_missing_language_is_counted_like_the_other_terms():
    context = PipelineContext()
    context.language_dict = {'cze': 'Q9056'}

    cleaners.prepare_language_from_nkcr('cze|xxx', '377a', context)
    cleaners.prepare_language_from_nkcr('xxx', '377a', context)

    assert context.not_found_languages == {'xxx': 2}


def test_names_from_csv_collect_every_looked_up_column(tmp_path):
    """The loader must ask about exactly the terms the processing will look up."""
    csv_file = tmp_path / 'nkcr.csv'
    csv_file.write_text(
        '_id,374a,372a,370a,370b,370f,377a\n'
        'jk01,malíři|sochaři,dějiny umění,"Louny, Česko",Praha (Česko),'
        'Brno (Česko)$Plzeň (Česko),cze\n'
        'jk02,malíři,,,,,\n',
        encoding='utf-8')

    names = tools.load_names_from_csv(str(csv_file))

    assert sorted(names) == sorted(['malíři', 'sochaři', 'dějiny umění', 'Louny (Česko)',
                                    'Praha (Česko)', 'Brno (Česko)', 'Plzeň (Česko)'])


def test_name_lookup_queries_every_batch(monkeypatch):
    """Every name must be asked about - the old LIMIT/OFFSET paging dropped rows."""
    asked = []

    def fake_query(query):
        values = query.split('VALUES ?string {')[1].split('}')[0]
        names = values.split('"')[1::2]
        asked.extend(names)
        # only the first name of each batch is known on Wikidata
        return {'results': {'bindings': [
            {'item': {'value': 'http://www.wikidata.org/entity/Q1'},
             'string': {'value': names[0]}},
        ]}}

    monkeypatch.setattr(tools, 'execute_sparql_post', fake_query)
    monkeypatch.setattr(tools.time, 'sleep', lambda s: None)

    names = [f'name{number}' for number in range(25)]
    result = tools.load_names_to_qid(names, batch_size=10, pause_seconds=0)

    assert asked == names, 'no name may be skipped'
    assert result == {'name0': 'Q1', 'name10': 'Q1', 'name20': 'Q1'}


def test_name_lookup_returns_empty_for_no_names():
    assert tools.load_names_to_qid([], pause_seconds=0) == {}


@pytest.mark.parametrize(
    "value,expected",
    [
        ('malíři', '"malíři"'),
        ('sochaři "kamenní"', '"sochaři \\"kamenní\\""'),
        ('zpětné\\lomítko', '"zpětné\\\\lomítko"'),
    ],
)
def test_sparql_literal_escapes_quotes(value, expected):
    """A term with a quote must stay in the batch, not break the query or vanish."""
    assert tools.sparql_literal(value) == expected


def test_sparql_post_states_the_charset(monkeypatch):
    """Without it the endpoint reads the body as Latin-1 and accented names stop matching."""
    sent = {}

    class FakeResponse:
        content = b'{"results": {"bindings": []}}'

        def raise_for_status(self):
            pass

    def fake_post(url, data=None, headers=None, timeout=None):
        sent['data'] = data
        sent['headers'] = headers
        return FakeResponse()

    monkeypatch.setattr(tools.requests, 'post', fake_post)
    tools.execute_sparql_post('select * where { ?s ?p "Příbram (Česko)" }')

    assert 'charset=utf-8' in sent['headers']['Content-Type']
    assert 'Příbram'.encode('utf-8') in sent['data']


def test_not_found_report_is_sorted_by_count(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    context = PipelineContext()
    context.not_found_occupations = {'vzácný obor': 1, 'častý obor': 9, 'střední obor': 4}
    context.not_found_languages = {'mul': 3}

    tools.write_not_found_report(context)

    assert (tmp_path / 'not_found_occupations.csv').read_text(encoding='utf-8').splitlines() == [
        'term,count', 'častý obor,9', 'střední obor,4', 'vzácný obor,1']
    assert (tmp_path / 'not_found_languages.csv').read_text(encoding='utf-8').splitlines() == [
        'term,count', 'mul,3']
    # an empty run still leaves a file, so a stale one is never mistaken for fresh
    assert (tmp_path / 'not_found_places.csv').read_text(encoding='utf-8').splitlines() == ['term,count']
