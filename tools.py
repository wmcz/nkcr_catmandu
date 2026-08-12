import csv
import gc
import logging
import os
import re
import time
from datetime import datetime
from json import JSONDecodeError
from typing import Union, Any, TYPE_CHECKING

import pandas
import pandas as pd
import pywikibot
import rapidjson
import requests
import simplejson.errors
from pywikibot.data import sparql
from wikibaseintegrator import wbi_helpers
from wikibaseintegrator.datatypes import Item, ExternalID, Time, String
from wikibaseintegrator.entities import ItemEntity
from wikibaseintegrator.models import References, Snaks, Claim, Snak, Reference
from wikibaseintegrator.wbi_enums import WikibaseTimePrecision, WikibaseDatatype, ActionIfExists
from wikibaseintegrator.wbi_exceptions import MissingEntityException, NonExistentEntityError

import mySparql
import pywikibot_extension
from cleaners import clean_last_comma
from config import Config
from memory_profiler import get_tracemalloc_usage_mb

if TYPE_CHECKING:
    from context import PipelineContext
log = logging.getLogger(__name__)


def write_log(fields, create_file=False):
    """
    Writes a log entry to a CSV file and a logger. The function either appends to or creates a
    new CSV file depending on the specified flag.

    :param fields: The dictionary containing field names as keys and corresponding values to
        be written to the log file.
    :type fields: dict
    :param create_file: Optional boolean flag that determines whether to create a new CSV file
        or append to an existing file. Defaults to False (append mode).
    :type create_file: bool
    :return: None
    """
    mode = 'w' if create_file else 'a'
    with open('debug.csv', mode) as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=['item', 'prop', 'value'])
        writer.writerow(fields)
    log.info(fields)


def reset_debug_file():
    """
    Resets the content of the debug file by creating or overwriting it.

    This function opens a file named 'debug.csv' in write mode, effectively clearing
    any existing content or creating the file if it does not already exist.
    The file is then closed immediately, ensuring no further action is performed.

    :return: None
    """
    with open('debug.csv', 'w'):
        pass


def read_log() -> csv.DictReader:
    """
    Reads data from a CSV file and returns a CSV dictionary reader object. This function opens a file
    named 'debug.csv' in read mode and uses the `csv.DictReader` class to parse the file's content.
    The expected fields in the CSV file are 'item', 'prop', and 'value'.

    :return: A CSV dictionary reader to iterate over rows of the input file, where each row is
             represented as a dictionary with keys corresponding to 'item', 'prop', and 'value'.
    :rtype: csv.DictReader
    """
    with open('debug.csv', 'r') as csvfile:
        reader = csv.DictReader(csvfile, fieldnames=['item', 'prop', 'value'])
        return list(reader)


def print_info(debug: bool):
    """
    Prints information and logs messages with a timestamp. The behavior differs
    based on the `debug` flag.

    :param debug: A boolean flag indicating whether to include additional debugging
        information in the log.
    :return: None
    """
    log_with_date_time('Catmandu processor for NKČR')
    if debug:
        log_with_date_time('DEBUG!!!')


def add_new_field_to_item_wbi(
        item_new_field: ItemEntity,
        property_new_field: str, value: Union[str, int],
        nkcr_aut_new_field: str):
    """
    Adds a new claim to an item in the Wikibase system.

    This function attempts to add a new field to an item. It checks the current
    claims of the item to ensure that deprecated claims matching the provided
    `nkcr_aut_new_field` do not result in duplicate or conflicting data being
    added. The function also handles different types of property values, such
    as strings, external IDs, or time-based properties. Appropriate references
    are created for the newly added claims.

    :param item_new_field: The Wikibase item to which the new claim will be added.
    :param property_new_field: The property for the new claim, represented as its
        string identifier (e.g., 'P213', 'P496').
    :param value: The value for the new claim. For string or integer properties,
        this is a simple value, while for time properties, this is a dictionary
        containing `property`, `time`, and `precision` keys.
    :param nkcr_aut_new_field: The external identifier or reference to be checked
        against existing claims for potential deprecation.
    :return: The modified Wikibase item containing the newly added claim.
    :rtype: ItemEntity
    """
    try:
        claims_by_property = item_new_field.claims.get('P691')
        for claim in claims_by_property:
            if claim.rank.value == 'deprecated' and nkcr_aut_new_field == claim.mainsnak.datavalue['value']:
                # deprecated so not add
                return item_new_field
    except KeyError:
        return item_new_field

    now = datetime.now()

    references = [
        [
            Item(value='Q13550863', prop_nr='P248'),
            ExternalID(value=nkcr_aut_new_field, prop_nr='P691'),
            Time(time=now.strftime('+%Y-%m-%dT00:00:00Z'), prop_nr='P813', precision=WikibaseTimePrecision.DAY),
        ]
    ]

    if property_new_field in ['P213', 'P496']:
        # external string
        final = {'item': item_new_field.id, 'prop': property_new_field, 'value': value}
        new_claim = ExternalID(value=value, prop_nr=property_new_field, references=references)
    elif property_new_field in ['P569', 'P570'] or property_new_field == ['P569', 'P570']:
        #Time
        final = {'item': item_new_field.id, 'prop': value.get('property'), 'value': value.get('time')}
        new_claim = Time(time=value.get('time'), prop_nr=value.get('property'), precision=value.get('precision'), references=references)
    else:
        final = {'item': item_new_field.id, 'prop': property_new_field, 'value': value}
        new_claim = Item(value=value, prop_nr=property_new_field, references=references)
    write_log(final)
    item_new_field.claims.add(new_claim, action_if_exists=ActionIfExists.APPEND_OR_REPLACE)

    return item_new_field

def add_nkcr_aut_to_item_wbi(
        item_to_add: ItemEntity,
        nkcr_aut_to_add: str,
        name_to_add: str):
    """
    Adds an NKCR AUT identifier to a Wikibase item along with references and qualifiers.

    This function updates the provided `Wikibase item` with the given NKCR AUT (National Library
    of the Czech Republic Authority) identifier. It also attaches relevant metadata, including the
    source reference ('P248'), the identifier value ('P691'), and a date stamp ('P813'), as well
    as an optional qualifier ('P1810') for the name.

    :param item_to_add: The Wikibase item to which the NKCR AUT identifier will be added.
    :type item_to_add: ItemEntity
    :param nkcr_aut_to_add: The NKCR AUT identifier string to be added to the item.
    :type nkcr_aut_to_add: str
    :param name_to_add: The formatted name string to be included as a qualifier.
    :type name_to_add: str
    :return: The updated Wikibase item containing the new claim.
    :rtype: ItemEntity
    """
    now = datetime.now()

    references = [
        [
            Item(value='Q13550863', prop_nr='P248'),
            ExternalID(value=nkcr_aut_to_add, prop_nr='P691'),
            Time(time=now.strftime('+%Y-%m-%dT00:00:00Z'), prop_nr='P813', precision=WikibaseTimePrecision.DAY),
        ]
    ]

    qualifier = [
        String(value=clean_last_comma(name_to_add), prop_nr='P1810'),
    ]

    final = {'item': item_to_add.id, 'prop': 'P691', 'value': nkcr_aut_to_add}
    write_log(final)
    new_claim = ExternalID(value=nkcr_aut_to_add, prop_nr='P691', references=references, qualifiers=qualifier)
    item_to_add.claims.add(new_claim, action_if_exists=ActionIfExists.APPEND_OR_REPLACE)
    return item_to_add


def get_nkcr_auts_from_item_wbi(datas) -> list:
    """
    Extracts NKCR AUT IDs from a given Wikibase item.

    This function processes the claims associated with the input data to retrieve
    NKCR AUT identifiers (external IDs) from the specified property `P691`. These
    identifiers are either extracted directly or from a nested literal value.

    :param datas: The input data representing a Wikibase item. It contains claims
        and associated metadata.
    :type datas: Any

    :return: A list of NKCR AUT IDs extracted from the provided data. If no IDs
        are found, an empty list is returned.
    :rtype: list
    """
    nkcr_auts_from_data = []
    try:
        claims_from_wd = datas.claims.get('P691')
    except KeyError:
        return []
    for claim in claims_from_wd:
        if claim.mainsnak.datatype == WikibaseDatatype.EXTERNALID.value:
            nkcr_auts_from_data.append(claim.mainsnak.datavalue['value'])
        else:
            nkcr_auts_from_data.append(claim.mainsnak.datavalue['value']['literal'])

    return nkcr_auts_from_data


def make_qid_database(items: dict) -> dict[str, list[str]]:
    """
    Creates a database mapping QIDs to lists of NKCR identifiers. This function takes
    a dictionary where each key is an NKCR identifier, and each value is a dictionary
    containing at least a 'qid' key. It processes these entries to create a new
    dictionary where each QID is a key mapped to a list of NKCR identifiers associated
    with that QID.

    :param items: A dictionary mapping NKCR identifiers to dictionaries. Each
        dictionary value must contain a 'qid' key which provides the QID
        associated with the NKCR identifier.
    :type items: dict

    :return: A dictionary where each QID is mapped to a list of NKCR identifiers
        that share the same QID.
    :rtype: dict[str, list[str]]
    """
    return_qids: dict[str, list[str]] = {}
    for nkcr, nkcr_line in items.items():
        if return_qids.get(nkcr_line['qid']):
            return_qids[nkcr_line['qid']].append(nkcr)
        else:
            return_qids[nkcr_line['qid']] = [nkcr]

    return return_qids


def get_occupations(limit: Union[int, None] = None, offset: Union[int, None] = None) -> dict[str, str]:
    """
    Retrieves a dictionary of unique occupations from a SPARQL query by filtering specific
    prefixes and ensuring only non-deprecated rankings are included. Occupation data includes
    an identifier value and a string representation for display.

    :param limit: The maximum number of occupations to retrieve. If None, no limit is applied.
    :param offset: The number of occupations to skip from the start of the query result. If None, no offset is applied.
    :return: A dictionary where keys are strings (occupation names) and values are occupation item identifiers.
    """
    query = """
    select distinct ?item ?value ?string where {

        ?item p:P691 ?s .
        ?s wikibase:rank ?rank filter(?rank != wikibase:DeprecatedRank) .
        ?s ps:P691 ?value filter(strstarts(str(?value),"ph") || strstarts(str(?value),"fd") || strstarts(str(?value),"ge") || strstarts(str(?value),"xx") ) .
        ?s pq:P1810 ?string .
    } LIMIT """ + str(limit) + """ OFFSET """ + str(offset) + """
    """

    occupation_dictionary: dict[str, str] = {}

    try:
        # data_occupation_wbi = wbi_helpers.execute_sparql_query(query=query, endpoint="https://try.orbopengraph.com/proxy/wdqs/bigdata/namespace/wdq/sparql")
        data_occupation_wbi = wbi_helpers.execute_sparql_query(query=query, endpoint="https://query-main.wikidata.org/sparql")

    except simplejson.errors.JSONDecodeError as e:
        log_with_date_time('get occupations JSONDecodeError: ' + str(e))
        raise Exception(str(e))
        return occupation_dictionary
    except requests.exceptions.ConnectionError as e:
        log_with_date_time('get occupations ConnectionError: ' + str(e))
        raise Exception(str(e))
        return occupation_dictionary
    except Exception as e:
        log_with_date_time('get occupations Exception: ' + str(e))
        raise Exception(str(e))
        return occupation_dictionary

    for item_occupation in data_occupation_wbi['results']['bindings']:
        if item_occupation['string'] is not None:
            name = item_occupation['string']['value']
        else:
            name = None

        if item_occupation['item']['value']:
            occupation_dictionary[name] = item_occupation['item']['value'].replace('http://www.wikidata.org/entity/', '')

    return occupation_dictionary


SPARQL_ENDPOINT = 'https://query-main.wikidata.org/sparql'
SPARQL_USER_AGENT = 'Frettiebot/1.0 (https://www.wikidata.org/wiki/User:Frettiebot)'
ENTITY_PREFIX = 'http://www.wikidata.org/entity/'

# Every property the pipeline needs, in one query per batch of authority ids.
# GROUP_CONCAT keeps it at one row per item; without it the OPTIONALs multiply
# out (a person with 3 occupations and 2 languages produced 6 rows).
NKCR_COMBINED_QUERY = """
select ?item ?nkcr
       (GROUP_CONCAT(DISTINCT ?isni; separator="|") as ?isnis)
       (GROUP_CONCAT(DISTINCT ?orcid; separator="|") as ?orcids)
       (GROUP_CONCAT(DISTINCT ?birth; separator="|") as ?births)
       (GROUP_CONCAT(DISTINCT ?death; separator="|") as ?deaths)
       (GROUP_CONCAT(DISTINCT ?field; separator="|") as ?fields)
       (GROUP_CONCAT(DISTINCT ?occup; separator="|") as ?occups)
       (GROUP_CONCAT(DISTINCT ?bplace; separator="|") as ?bplaces)
       (GROUP_CONCAT(DISTINCT ?dplace; separator="|") as ?dplaces)
       (GROUP_CONCAT(DISTINCT ?wplace; separator="|") as ?wplaces)
       (GROUP_CONCAT(DISTINCT ?language; separator="|") as ?languages)
where {
    VALUES ?nkcr { %s }
    ?item p:P691 [ps:P691 ?nkcr ; wikibase:rank ?rank ] filter(?rank != wikibase:DeprecatedRank) .
    OPTIONAL{?item wdt:P213 ?isni}.
    OPTIONAL{?item wdt:P496 ?orcid}.
    OPTIONAL{?item wdt:P569 ?birth}.
    OPTIONAL{?item wdt:P570 ?death}.
    OPTIONAL{?item wdt:P101 ?field}.
    OPTIONAL{?item wdt:P106 ?occup}.
    OPTIONAL{?item wdt:P19 ?bplace}.
    OPTIONAL{?item wdt:P20 ?dplace}.
    OPTIONAL{?item wdt:P937 ?wplace}.
    OPTIONAL{?item wdt:P1412 ?language}.
} GROUP BY ?item ?nkcr
"""

# dataset name -> {key in the dataset entry: (variable in the response, is it an entity URI)}
# The key names are the ones resolve_exist_claims() maps the MARC columns to.
# Note 'birth'/'death' mean dates in non_deprecated_items but places in places -
# that is why these stay four separate dictionaries.
NKCR_DATASET_SPEC: dict[str, dict[str, tuple[str, bool]]] = {
    'non_deprecated_items': {
        'isni': ('isnis', False),
        'orcid': ('orcids', False),
        'birth': ('births', False),
        'death': ('deaths', False),
    },
    'field_of_work_and_occupation': {
        'field': ('fields', True),
        'occup': ('occups', True),
    },
    'places': {
        'birth': ('bplaces', True),
        'death': ('dplaces', True),
        'work': ('wplaces', True),
    },
    'languages': {
        'language': ('languages', True),
    },
}


def execute_sparql_post(query: str, timeout: int = 180) -> dict:
    """
    Run a SPARQL query over POST and return the parsed JSON response.

    POST rather than GET because a VALUES block holding a thousand authority
    ids does not fit in a URL - the endpoint answers HTTP 414.

    :param query: The SPARQL query to run.
    :param timeout: Socket timeout in seconds.
    :return: Parsed SPARQL JSON results.
    :raises requests.HTTPError: On any non-2xx response (429/502/504 included).
    """
    response = requests.post(
        SPARQL_ENDPOINT,
        data=query.encode('utf-8'),
        headers={
            'User-Agent': SPARQL_USER_AGENT,
            'Accept': 'application/sparql-results+json',
            'Content-Type': 'application/sparql-query',
        },
        timeout=timeout,
    )
    response.raise_for_status()
    return rapidjson.loads(response.content)


def _rate_limit_wait(error) -> Union[int, None]:
    """
    Seconds the server asked us to wait, if it answered with a rate limit.

    WikibaseIntegrator retries 500/502/503/504 on the MediaWiki API but lets a
    429 fall straight through to raise_for_status(), so the Retry-After hint
    has to be read here. Its SPARQL path does handle 429; only the API path
    does not.

    :param error: The exception raised by the failed call.
    :return: Seconds to wait, or None when this was not a rate limit.
    """
    response = getattr(error, 'response', None)
    if response is None or getattr(response, 'status_code', None) != 429:
        return None
    try:
        return max(1, int(response.headers.get('Retry-After')))
    except (AttributeError, TypeError, ValueError):
        return None


# Failures that say "this will never work", not "try again later". A deleted or
# merged item is missing on every attempt, so retrying it only burns the whole
# backoff - up to an hour per record - before failing anyway.
PERMANENT_ERRORS = (MissingEntityException, NonExistentEntityError)


def retry_with_backoff(operation, description: str, max_attempts: int = 15,
                       initial_wait: int = 15, max_wait: int = 300):
    """
    Run an operation, retrying endpoint failures with exponential backoff.

    WDQS answers a busy or over-long query with 429/502/504, or cuts the
    response mid-stream once it hits its own 60s limit - which surfaces as a
    JSON parse error. All of those are transient, so they are retried. A
    production run was observed failing 10 times in a row on one request,
    hence the generous attempt count. PERMANENT_ERRORS are raised straight
    away - a missing item stays missing however long we wait.

    When the server states how long to wait, that wins over the computed
    backoff, and the backoff is left untouched so one rate limit does not
    inflate the waits used for later failures.

    :param operation: Zero-argument callable to run.
    :param description: Label used in log messages.
    :param max_attempts: How many times to try before giving up.
    :param initial_wait: Seconds to wait after the first failure.
    :param max_wait: Upper bound for the growing wait.
    :return: Whatever operation() returns.
    :raises Exception: The last failure, once max_attempts is exhausted.
    """
    wait = initial_wait
    for attempt in range(1, max_attempts + 1):
        try:
            return operation()
        except PERMANENT_ERRORS as e:
            log_with_date_time(f'{description} failed permanently, not retrying: {e}')
            raise
        except Exception as e:
            if attempt == max_attempts:
                log_with_date_time(f'{description} failed permanently after {max_attempts} attempts: {e}')
                raise
            requested_wait = _rate_limit_wait(e)
            sleep_seconds = requested_wait if requested_wait is not None else wait
            log_with_date_time(f'{description} failed (attempt {attempt}/{max_attempts}): {e}'
                               f' - retrying in {sleep_seconds}s')
            gc.collect()
            time.sleep(sleep_seconds)
            if requested_wait is None:
                wait = min(wait * 2, max_wait)


# Replication lag the API may have before it tells us to back off. WikibaseIntegrator
# already defaults to this value; it is passed explicitly so it is visible and tunable
# from here rather than depending on a library default.
API_MAXLAG = 5


def get_item_with_retry(wbi, qid: str, description: str = None):
    """
    Fetch an item, waiting out API rate limits instead of dying on them.

    A single 429 used to end a whole run: WikibaseIntegrator raises it from
    raise_for_status() without retrying, and requests.HTTPError was not among
    the exceptions the main loop caught.

    :param wbi: WikibaseIntegrator instance to fetch with.
    :param qid: Item id to fetch.
    :param description: Label used in log messages.
    :return: The fetched item.
    """
    return retry_with_backoff(lambda: wbi.item.get(qid, maxlag=API_MAXLAG),
                              description or f'item.get({qid})')


def new_nkcr_datasets() -> dict[str, dict]:
    """Create the empty dataset dictionaries described by NKCR_DATASET_SPEC."""
    return {name: {} for name in NKCR_DATASET_SPEC}


def _split_group_concat(binding: dict, variable: str, strip_entity: bool) -> list:
    """Split one GROUP_CONCAT cell into values; an empty cell means no values."""
    raw = binding.get(variable, {}).get('value', '')
    if not raw:
        return []
    values = raw.split('|')
    if strip_entity:
        values = [value.replace(ENTITY_PREFIX, '') for value in values]
    return values


def parse_nkcr_bindings(bindings: list, datasets: dict[str, dict]) -> None:
    """
    Fold SPARQL result rows into the four dataset dictionaries, in place.

    Where two items carry the same authority id, the first item supplies the
    qid and the later ones only contribute additional values - matching how
    the previous loader behaved.

    :param bindings: Result rows from the combined query.
    :param datasets: Dictionaries to extend, as built by new_nkcr_datasets().
    """
    for binding in bindings:
        nkcr = binding['nkcr']['value']
        qid = binding['item']['value'].replace(ENTITY_PREFIX, '')

        for name, spec in NKCR_DATASET_SPEC.items():
            entry = datasets[name].get(nkcr)
            if entry is None:
                entry = {'qid': qid}
                for key in spec:
                    entry[key] = []
                datasets[name][nkcr] = entry

            for key, (variable, strip_entity) in spec.items():
                for value in _split_group_concat(binding, variable, strip_entity):
                    if value not in entry[key]:
                        entry[key].append(value)


def load_non_deprecated_items_by_nkcr(nkcr_ids: list, batch_size: int = 1000,
                                      pause_seconds: float = 2) -> dict[str, dict]:
    """
    Fetch the Wikidata claims the pipeline needs for the given authority ids.

    Replaces four full-graph queries that paged with LIMIT/OFFSET. Deep OFFSET
    made the endpoint rescan and discard every skipped row, so past roughly
    offset 500000 each request ran into the 60s WDQS timeout - measured: the
    same query answers in 0.7s at offset 0 and returns HTTP 504 at offset
    500000. A bound VALUES list is served from the index instead and stays
    around 2-4s regardless of position in the file.

    Batches are kept near a thousand ids because the GROUP BY cost grows
    faster than linearly: 1100 ids answered in 4.2s, 3000 in 41.6s and 6000
    timed out.

    :param nkcr_ids: Authority ids to ask about.
    :param batch_size: Ids per query.
    :param pause_seconds: Delay between batches, to stay under the rate limit.
    :return: Dict of the four datasets, keyed by the names in NKCR_DATASET_SPEC.
    """
    datasets = new_nkcr_datasets()
    if not nkcr_ids:
        return datasets

    batches = [nkcr_ids[start:start + batch_size]
               for start in range(0, len(nkcr_ids), batch_size)]
    log_with_date_time(f'nkcr datasets: {len(nkcr_ids)} ids in {len(batches)} batches')

    for number, batch in enumerate(batches, 1):
        values = ' '.join('"%s"' % nkcr.replace('"', '') for nkcr in batch)
        query = NKCR_COMBINED_QUERY % values

        data = retry_with_backoff(lambda: execute_sparql_post(query),
                                  f'nkcr batch {number}/{len(batches)}')
        parse_nkcr_bindings(data['results']['bindings'], datasets)

        if number % 25 == 0 or number == len(batches):
            log_with_date_time(f'nkcr batch {number}/{len(batches)}, '
                               f'{len(datasets["non_deprecated_items"])} items found')
        if pause_seconds and number < len(batches):
            time.sleep(pause_seconds)

    return datasets


def load_nkcr_ids(file_name) -> list:
    """
    Read only the authority ids from the NK ČR CSV, keeping order and dropping duplicates.

    :param file_name: Path to the CSV produced by Catmandu.
    :return: List of authority ids.
    """
    ids: list = []
    for chunk in pd.read_csv(file_name, usecols=['_id'], dtype={'_id': str}, chunksize=100000):
        ids.extend(chunk['_id'].dropna().tolist())
    return list(dict.fromkeys(ids))


NKCR_DATASET_CACHE_FILES = {
    'non_deprecated_items': 'non_deprecated_items.json',
    'field_of_work_and_occupation': 'field_of_work_and_occupation.json',
    'places': 'places.json',
    'languages': 'languages.json',
}


def load_nkcr_datasets(nkcr_ids: list, batch_size: int = 1000,
                       pause_seconds: float = 2) -> dict[str, dict]:
    """
    Load the four datasets, reusing the JSON cache when use_json_database is set.

    :param nkcr_ids: Authority ids to ask about.
    :param batch_size: Ids per query.
    :param pause_seconds: Delay between batches.
    :return: Dict of the four datasets, keyed as in NKCR_DATASET_SPEC.
    """
    if Config.use_json_database and all(os.path.isfile(name)
                                        for name in NKCR_DATASET_CACHE_FILES.values()):
        datasets = {}
        for name, file_name in NKCR_DATASET_CACHE_FILES.items():
            with open(file_name) as infile:
                datasets[name] = simplejson.load(infile)
        log_with_date_time('nkcr datasets read from json cache')
        return datasets

    datasets = load_non_deprecated_items_by_nkcr(nkcr_ids, batch_size, pause_seconds)

    for name, file_name in NKCR_DATASET_CACHE_FILES.items():
        with open(file_name, 'w') as outfile:
            outfile.write(simplejson.dumps(datasets[name]))
    return datasets


def load_nkcr_items(file_name) -> pandas.DataFrame:
    """
    Reads a CSV file containing NKCR item data and returns it as a pandas DataFrame.

    This function is used to load large datasets containing bibliographic or authority
    record information into chunks with specified data types for each column. It uses
    pandas' `read_csv` method for efficient data loading and supports chunks of data
    for processing large files that may not fit entirely into memory.

    :param file_name: The path to the CSV file containing NKCR item data.
    :type file_name: str

    :return: A generator yielding pandas DataFrame chunks of the loaded data.
    :rtype: pandas.DataFrame
    """
    data_csv = pd.read_csv(file_name, dtype={
        '_id': 'S',
        '100a': 'S',
        '100b': 'S',
        '100d': 'S',
        '100q': 'S',
        '046f': 'S',
        '046g': 'S',
        '370a': 'S',
        '370b': 'S',
        '370f': 'S',
        '372a': 'S',
        '374a': 'S',
        '375a': 'S',
        '377a': 'S',
        '400ia': 'S',
        '500ia7': 'S',
        '678a': 'S',
        '0247a-isni': 'S',
        '0247a-wikidata': 'S',
        '0247a': 'S',
        '0247a-orcid': 'S'
    }, chunksize=10000)
    return data_csv

def get_claim_from_item_by_property_wbi(datas: ItemEntity, property_of_item: Any) -> list:
    """
    Extracts claims from a given ItemEntity based on provided property or list of properties. The claims are filtered
    and processed based on the datatype of each claim's mainsnak.

    :param datas: The ItemEntity object containing the claims to process.
    :type datas: ItemEntity
    :param property_of_item: The property or list of properties to filter claims within the ItemEntity. Can be a single
        property or a list of properties.
    :type property_of_item: Any
    :return: A list of extracted claim values from the ItemEntity, processed based on their datatype. For EXTERNALID
        and TIME datatypes, specific processing is applied. For TIME datatype, when a single property is provided, the
        property is included in the returned result.
    :rtype: list
    """
    if type(property_of_item) is list:
        claims_from_data = []
        for prop in property_of_item:
            try:
                claims_by_property = datas.claims.get(prop)
            except KeyError:
                pass
            for claim in claims_by_property:
                if claim.mainsnak.datatype == WikibaseDatatype.EXTERNALID.value:
                    claims_from_data.append(claim.mainsnak.datavalue['value'])
                elif claim.mainsnak.datatype == WikibaseDatatype.TIME.value:
                    value: dict = claim.mainsnak.datavalue['value']
                    value.update({'property': prop})
                    claims_from_data.append(value)
                else:
                    claims_from_data.append(claim.mainsnak.datavalue['value']['id'])

        return claims_from_data
    else:
        claims_from_data = []
        try:
            claims_by_property = datas.claims.get(property_of_item)
        except KeyError:
            return []
        for claim in claims_by_property:
            if claim.mainsnak.datatype == WikibaseDatatype.EXTERNALID.value:
                claims_from_data.append(claim.mainsnak.datavalue['value'])
            elif claim.mainsnak.datatype == WikibaseDatatype.TIME.value:
                value: dict = claim.mainsnak.datavalue['value']
                value.update({'property': property_of_item})
                claims_from_data.append(value)
            else:
                claims_from_data.append(claim.mainsnak.datavalue['value']['id'])

        return claims_from_data

def is_item_subclass_of_wbi(item_qid: str, subclass_qid: str, context: 'PipelineContext'):
    """
    Determines whether a given Wikidata item is a subclass of another specified item.
    The function checks if the given `item_qid` is either a direct or indirect subclass
    of the given `subclass_qid`. The relationship can be through multiple levels within
    the Wikidata property hierarchy. A cache mechanism is utilized to enhance performance
    by storing previous results in the context.

    :param item_qid: The QID of the item being checked (e.g., "Q42" for Douglas Adams).
    :type item_qid: str
    :param subclass_qid: The QID of the potential superclass item (e.g., "Q36180" for human).
    :type subclass_qid: str
    :param context: Pipeline context containing the subclass cache.
    :return: `True` if `item_qid` is a subclass of `subclass_qid`, otherwise `False`.
    :rtype: bool
    """
    cached = context.get_cached_subclass_result(subclass_qid, item_qid)
    if cached is not None:
        return cached

    query_first = """
        select distinct ?item where  {
            values ?item {wd:""" + item_qid + """}

            {?item wdt:P279 wd:""" + subclass_qid + """ .} union
            {?item wdt:P279/wdt:P279 wd:""" + subclass_qid + """ .} union
            {?item wdt:P279/wdt:P279/wdt:P279 wd:""" + subclass_qid + """ .} union
            {?item wdt:P279/wdt:P279/wdt:P279/wdt:P279 wd:""" + subclass_qid + """ .} union
            {?item wdt:P279/wdt:P279/wdt:P279/wdt:P279/wdt:P279 wd:""" + subclass_qid + """ .} union
            {?item wdt:P279/wdt:P279/wdt:P279/wdt:P279/wdt:P279/wdt:P279 wd:""" + subclass_qid + """ .} union

            {?item wdt:P31/wdt:P279 wd:""" + subclass_qid + """ .} union
            {?item wdt:P31/wdt:P279/wdt:P279 wd:""" + subclass_qid + """ .} union
            {?item wdt:P31/wdt:P279/wdt:P279/wdt:P279 wd:""" + subclass_qid + """ .} union
            {?item wdt:P31/wdt:P279/wdt:P279/wdt:P279/wdt:P279 wd:""" + subclass_qid + """ .} union
            {?item wdt:P31/wdt:P279/wdt:P279/wdt:P279/wdt:P279/wdt:P279 wd:""" + subclass_qid + """ .} union
            {?item wdt:P31/wdt:P279/wdt:P279/wdt:P279/wdt:P279/wdt:P279/wdt:P279 wd:""" + subclass_qid + """ .}
        }
    """

    data_first = wbi_helpers.execute_sparql_query(query=query_first)

    is_subclass = len(data_first['results']['bindings']) > 0
    context.cache_subclass_result(subclass_qid, item_qid, is_subclass)
    return is_subclass


def log_with_date_time(message: str = ''):
    """
    Logs a message with the current date and time stamp.

    This function uses the logging module to log a given message along with
    the current date and time. If no message is provided, an empty string
    will be logged.

    :param message: The message to log. Defaults to an empty string.
    :type message: str
    :return: None
    """
    log.info(message)


def load_sparql_query_by_chunks(limit: int, get_method, name: str):
    """
    Loads SPARQL query results in chunks, processes them in a paginated manner, and stores the results
    either in memory or as a JSON file. If the results are already cached in a JSON file and caching
    is enabled, the function retrieves them directly from the file.

    :param limit: The maximum number of records to be fetched in each chunk.
    :type limit: int
    :param get_method: The method used to retrieve the data in chunks. It must take `limit`
                       and `offset` as arguments and return a chunk of data.
    :type get_method: Callable
    :param name: The base name of the JSON file used for caching the query results. The `.json`
                 extension will be appended automatically.
    :type name: str
    :return: A dictionary containing the aggregated SPARQL query results.
    :rtype: dict
    """
    if Config.use_json_database:
        if os.path.isfile(name + '.json'):
            with open(name + '.json') as infile:
                data = simplejson.load(infile)
            return data

    i = 0
    run = True
    final_data = {}
    while run:
        offset = i * limit
        if i % 3 == 0:
            log_with_date_time(get_method.__name__ + ": " + str(offset)
                               + ", memory: " + str(get_tracemalloc_usage_mb()[0]))
        # An exhausted retry raises rather than returning nothing, so an empty
        # result here really does mean the end of the data - it used to be
        # indistinguishable from a failed request, which silently truncated
        # the dataset and cached it.
        data = retry_with_backoff(lambda: get_method(limit, offset),
                                  get_method.__name__ + ' offset ' + str(offset))
        gc.collect()
        final_data.update(data)
        if len(data) == 0:
            run = False
        i = i + 1

    json_object = simplejson.dumps(final_data)

    with open(name + '.json', "w") as outfile:
        outfile.write(json_object)
    return final_data


def load_language_dict_csv() -> dict:
    """
    Loads a language dictionary from a CSV file.

    This function reads a CSV file containing language codes and their
    corresponding items. It constructs and returns a dictionary where keys
    are language codes and values are the corresponding items. The CSV file
    is expected to have columns 'item' and 'kod'.

    :return: A dictionary mapping language codes (``kod``) to items (``item``).
    :rtype: dict
    """
    filename = download_language_dict_csv()
    data_csv = pd.read_csv(filename, dtype={
        'item': 'S',
        'kod': 'S',
    })

    language_dict = {}
    for line in data_csv.to_dict('records'):
        language_dict[line['kod']] = line['item']
    return language_dict


def download_language_dict_csv() -> str:
    """
    Downloads a language dictionary CSV file from a remote URL.

    This function retrieves a CSV file containing language data from the specified
    remote URL and saves it locally with a predefined filename. If the download
    fails due to a connection error, it returns the name of a default file instead.

    :raises requests.ConnectionError: If there's an issue with the network connection
        preventing the file download.
    :return: The filename of the downloaded CSV file or the default filename
        in case of connection failure.
    :rtype: str
    """
    url = "https://raw.githubusercontent.com/wmcz/WMCZ-scripts/main/jazyky.csv"
    filename = 'jazyky.csv'

    try:
    # creating HTTP response object from given url
        resp = requests.get(url)

        # saving the xml file
        with open(filename, 'wb') as f:
            f.write(resp.content)
    except requests.ConnectionError:
        filename = "jazyky_default.csv"

    return filename


def get_bot_password(filename):
    """
    Reads the first line of a given file to obtain the bot password.

    The function opens the specified file, reads its first line, and
    returns the content of that line. It assumes the file exists and
    is readable. This is useful for retrieving sensitive data such
    as passwords from files in a consistent manner.

    :param filename: The path to the file containing the bot password.
    :type filename: str
    :return: The first line of the file, typically the bot password.
    :rtype: str
    """
    with open(filename, 'r') as file:
        first_line = file.readline()
    return first_line

def first_name(name):
    """
    Extracts and returns the first name from a given string. The function parses the input string
    to determine the first name using various string manipulations and regex. If the input is not
    a valid string or cannot be parsed, it returns None.

    :param name: The full name input string to be parsed.
    :type name: str
    :return: The parsed first name from the input string or None if input is invalid or
        parsing fails.
    :rtype: Optional[str]
    """
    try:
        assert isinstance(name, str)
        splits = name.replace(',','').split(' ')
        length = len(splits)
        ret = splits[len(splits)-1]

        regex = r"(.*),\W+([\w‘ \.]*)(,*)"
        matches = re.search(regex, name, re.IGNORECASE)
        try:
            groups = matches.groups()
            name = groups[1]
        except AttributeError as e:
            name = ret
        except ValueError as e:
            name = ret
    except TypeError:
        name = None
    except KeyError:
        name = None

    return name

def last_name(name):
    """
    Extracts the last name from a given name string.

    This function attempts to identify and return the last name from a provided
    string, handling cases where names may include commas, extra spaces, or
    special characters. It uses regular expressions to parse names into their
    components and returns the portion corresponding to the last name.

    :param name: The full name string to process.
    :type name: str
    :return: The last name extracted from the input string, or None if extraction fails.
    :rtype: str or None
    """
    try:
        assert isinstance(name, str)
        splits = name.replace(',', '').split(' ')
        length = len(splits)
        ret = splits[0]

        regex = r"(.*),\W+([\w‘ \.]*)(,*)"
        matches = re.search(regex, name, re.IGNORECASE)
        try:
            groups = matches.groups()
            name = groups[0]
        except AttributeError as e:
            name = ret
        except ValueError as e:
            name = ret
    except TypeError:
        name = None
    except KeyError:
        name = None

    return name