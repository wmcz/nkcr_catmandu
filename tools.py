import csv
import gc
import logging
import os
import re
from datetime import datetime
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

import mySparql
import pywikibot_extension
from cleaners import clean_last_comma
from config import Config

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
        data_occupation_wbi = wbi_helpers.execute_sparql_query(query=query)
    except simplejson.errors.JSONDecodeError as e:
        log_with_date_time('get occupations JSONDecodeError: ' + str(e))
        return occupation_dictionary
    except requests.exceptions.ConnectionError as e:
        log_with_date_time('get occupations ConnectionError: ' + str(e))
        return occupation_dictionary
    except Exception as e:
        log_with_date_time('get occupations Exception: ' + str(e))
        return occupation_dictionary

    for item_occupation in data_occupation_wbi['results']['bindings']:
        if item_occupation['string'] is not None:
            name = item_occupation['string']['value']
        else:
            name = None

        if item_occupation['item']['value']:
            occupation_dictionary[name] = item_occupation['item']['value'].replace('http://www.wikidata.org/entity/', '')

    return occupation_dictionary


def _fetch_non_deprecated_sparql(query: str, optional_fields: list[str],
                                 entity_fields: list[str], log_label: str) -> dict:
    """
    Execute a SPARQL query via mySparql and build a dictionary keyed by 'nkcr'.

    Each result row must have 'item' and 'nkcr'. Optional fields are collected
    into lists per nkcr entry. Fields listed in entity_fields have the Wikidata
    entity URI prefix stripped.

    :param query: SPARQL query string (with LIMIT/OFFSET already included).
    :param optional_fields: Field names to collect from results.
    :param entity_fields: Subset of optional_fields that are entity URIs
                          (need 'http://www.wikidata.org/entity/' stripped).
    :param log_label: Label for error log messages.
    :return: Dict keyed by nkcr, values are dicts with 'qid' + list fields.
    """
    result: dict = {}
    entity_prefix = 'http://www.wikidata.org/entity/'
    entity_fields_set = set(entity_fields)

    query_object = mySparql.MySparqlQuery(endpoint="https://query-main.wikidata.org/sparql",
                                          entity_url=entity_prefix)
    try:
        data = query_object.select(query=query, full_data=False)
    except simplejson.errors.JSONDecodeError as e:
        log_with_date_time(f'{log_label} JSONDecodeError: {e}')
        return result
    except rapidjson.JSONDecodeError as e:
        log_with_date_time(f'{log_label} JSONDecodeError: {e}')
        return result
    except pywikibot.exceptions.ServerError as e:
        log_with_date_time(f'{log_label} ServerError: {e}')
        return result
    except requests.exceptions.ConnectionError as e:
        log_with_date_time(f'{log_label} ConnectionError: {e}')
        return result

    if data is None:
        return result

    for row in data:
        nkcr = row['nkcr']

        parsed = {}
        for field in optional_fields:
            val = row[field]
            if val is not None and field in entity_fields_set:
                val = val.replace(entity_prefix, '')
            parsed[field] = val

        if result.get(nkcr):
            for field in optional_fields:
                if parsed[field] is not None:
                    result[nkcr][field].append(parsed[field])
        else:
            entry = {'qid': row['item'].replace(entity_prefix, '')}
            for field in optional_fields:
                entry[field] = [parsed[field]] if parsed[field] is not None else []
            result[nkcr] = entry

    del data
    return result


def get_all_non_deprecated_items(limit: Union[int, None] = None, offset: Union[int, None] = None) -> dict:
    query = """
    select ?item ?nkcr ?isni ?orcid ?birth ?death where {
        ?item p:P691 [ps:P691 ?nkcr ; wikibase:rank ?rank ] filter(?rank != wikibase:DeprecatedRank) .
        OPTIONAL{?item wdt:P213 ?isni}.
        OPTIONAL{?item wdt:P496 ?orcid}.
        OPTIONAL{?item wdt:P569 ?birth}.
        OPTIONAL{?item wdt:P570 ?death}.
    } LIMIT """ + str(limit) + " OFFSET " + str(offset)
    return _fetch_non_deprecated_sparql(
        query, ['isni', 'orcid', 'birth', 'death'], [],
        'get non deprecated items')


def get_all_non_deprecated_items_field_of_work_and_occupation(limit: Union[int, None] = None, offset: Union[int, None] = None) -> dict:
    query = """
    select ?item ?nkcr ?field ?occup where {
        ?item p:P691 [ps:P691 ?nkcr ; wikibase:rank ?rank ] filter(?rank != wikibase:DeprecatedRank) .
        OPTIONAL{?item wdt:P101 ?field}.
        OPTIONAL{?item wdt:P106 ?occup}.
    } LIMIT """ + str(limit) + " OFFSET " + str(offset)
    return _fetch_non_deprecated_sparql(
        query, ['field', 'occup'], ['field', 'occup'],
        'get non deprecated items field of work and occupation')


def get_all_non_deprecated_items_places(limit: Union[int, None] = None, offset: Union[int, None] = None) -> dict:
    query = """
    select ?item ?nkcr ?birth ?death ?work where {
        ?item p:P691 [ps:P691 ?nkcr ; wikibase:rank ?rank ] filter(?rank != wikibase:DeprecatedRank) .
        OPTIONAL{?item wdt:P19 ?birth}.
        OPTIONAL{?item wdt:P20 ?death}.
        OPTIONAL{?item wdt:P937 ?work}.
    } LIMIT """ + str(limit) + " OFFSET " + str(offset)
    return _fetch_non_deprecated_sparql(
        query, ['birth', 'death', 'work'], ['birth', 'death', 'work'],
        'get non deprecated items places')


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
    if not os.path.isfile(name + '.json') or Config.debug == False:
        i = 0
        run = True
        final_data = {}
        while run:
            lim = limit

            offset = (i * limit)-1
            if (offset < 0):
                offset = 0
            if i % 3 == 0:
                log_with_date_time(get_method.__name__ + ": " + str(offset))
            data = get_method(lim, offset)
            gc.collect()
            if len(final_data) == 0:
                final_data = data
            else:
                final_data.update(data)
            if len(data) == 0:
                run = False
            i = i + 1

        data = final_data

        json_object = simplejson.dumps(data)

        with open(name + '.json', "w") as outfile:
            outfile.write(json_object)
        return data


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


def get_all_non_deprecated_items_languages(limit: Union[int, None] = None, offset: Union[int, None] = None) -> dict:
    query = """
    select ?item ?nkcr ?language where {
        ?item p:P691 [ps:P691 ?nkcr ; wikibase:rank ?rank ] filter(?rank != wikibase:DeprecatedRank) .
        OPTIONAL{?item wdt:P1412 ?language}.
    } LIMIT """ + str(limit) + " OFFSET " + str(offset)
    return _fetch_non_deprecated_sparql(
        query, ['language'], ['language'],
        'get non deprecated items languages')

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