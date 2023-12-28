import csv
import gc
from datetime import datetime

import pandas
import pandas as pd
import pywikibot
import rapidjson
import requests
import simplejson.errors
from pywikibot.data import sparql

import mySparql
import pywikibot_extension
from cleaners import clean_last_comma
from property_processor import *
from os.path import exists

def write_log(fields, create_file=False):
    if create_file:
        csvfile = open('debug.csv', 'w')
    else:
        csvfile = open('debug.csv', 'a')
    writer = csv.DictWriter(csvfile, fieldnames=['item', 'prop', 'value'])
    writer.writerow(fields)
    csvfile.close()


def reset_debug_file():
    csvfile = open('debug.csv', 'w')
    csvfile.close()


def read_log() -> csv.DictReader:
    csvfile = open('debug.csv', 'r')
    reader = csv.DictReader(csvfile, fieldnames=['item', 'prop', 'value'])
    return reader


def print_info(debug):
    log_with_date_time('Catmandu processor for NKČR')
    if debug:
        log_with_date_time('DEBUG!!!')


def add_new_field_to_item(debug: bool, repo: pywikibot_extension.MyDataSite, item_new_field: pywikibot.ItemPage,
                          property_new_field: str, value: object,
                          nkcr_aut_new_field: str):
    sources = []

    source_nkcr = pywikibot.Claim(repo, 'P248')
    source_nkcr.setTarget(pywikibot.ItemPage(repo, 'Q13550863'))

    source_nkcr_aut = pywikibot.Claim(repo, 'P691')
    source_nkcr_aut.setTarget(nkcr_aut_new_field)

    now = datetime.now()
    source_date = pywikibot.Claim(repo, 'P813')
    source_date.setTarget(pywikibot.WbTime(year=now.year, month=now.month, day=now.day))

    new_claim = pywikibot.Claim(repo, property_new_field)
    new_claim.setTarget(value)
    if debug:
        if type(value) is pywikibot.ItemPage:
            value = value.getID()
        final = {'item': item_new_field.getID(), 'prop': property_new_field, 'value': value}
        write_log(final)
    else:
        sources.append(source_nkcr)
        sources.append(source_nkcr_aut)
        sources.append(source_date)
        new_claim.addSources(sources)
        item_new_field.addClaim(new_claim, tags=['Czech-Authorities-Sync'])


def add_nkcr_aut_to_item(debug: bool, repo: pywikibot_extension.MyDataSite, item_to_add: pywikibot.ItemPage,
                         nkcr_aut_to_add: str, name_to_add: str):
    sources = []

    source_nkcr = pywikibot.Claim(repo, 'P248')
    source_nkcr.setTarget(pywikibot.ItemPage(repo, 'Q13550863'))

    source_nkcr_aut = pywikibot.Claim(repo, 'P691')
    source_nkcr_aut.setTarget(nkcr_aut_to_add)

    now = datetime.now()
    source_date = pywikibot.Claim(repo, 'P813')
    source_date.setTarget(pywikibot.WbTime(year=now.year, month=now.month, day=now.day))

    new_claim = pywikibot.Claim(repo, 'P691')
    new_claim.setTarget(nkcr_aut_to_add)

    qualifier = pywikibot.Claim(repo, 'P1810')
    qualifier.setTarget(clean_last_comma(name_to_add))
    if debug:
        final = {'item': item_to_add.getID(), 'prop': 'P691', 'value': nkcr_aut_to_add}
        write_log(final)
        # print(final)
    else:
        sources.append(source_nkcr)
        sources.append(source_nkcr_aut)
        sources.append(source_date)
        new_claim.addSources(sources)
        new_claim.addQualifier(qualifier)
        item_to_add.addClaim(new_claim, tags=['Czech-Authorities-Sync'])


def get_nkcr_auts_from_item(datas) -> list:
    nkcr_auts_from_data = []
    claims_from_wd = datas['claims'].get('P691', [])
    for claim in claims_from_wd:
        nkcr_auts_from_data.append(claim.getTarget())

    return nkcr_auts_from_data


def make_qid_database(items: dict) -> dict[str, list[str]]:
    return_qids: dict[str, list[str]] = {}
    for nkcr, nkcr_line in items.items():
        if return_qids.get(nkcr_line['qid']):
            return_qids[nkcr_line['qid']].append(nkcr)
        else:
            return_qids[nkcr_line['qid']] = [nkcr]

    return return_qids


def get_occupations() -> dict[dict[str, list, list]]:
    query = """
    select distinct ?item ?value ?string where {

        ?item p:P691 ?s .
        ?s wikibase:rank ?rank filter(?rank != wikibase:DeprecatedRank) .
        ?s ps:P691 ?value filter(strstarts(str(?value),"ph") || strstarts(str(?value),"fd") || strstarts(str(?value),"ge") || strstarts(str(?value),"xx") ) .
        ?s pq:P1810 ?string .
    }
    """

    # query = """
    # select distinct ?item ?value ?string where {
    #
    #     ?item p:P691 ?s .
    #     ?s wikibase:rank ?rank filter(?rank != wikibase:DeprecatedRank) .
    #     ?s ps:P691 ?value .
    #     ?s pq:P1810 ?string .
    #     VALUES ?value {'ph121664' 'ph126519' 'ph114952'}
    #
    # }
    # """
    occupation_dictionary: dict[dict[str, list, list]] = {}
    query_object = mySparql.MySparqlQuery()
    # query_object = sparql.SparqlQuery()

    try:
        data_occupation = query_object.select(query=query, full_data=True)
    except simplejson.errors.JSONDecodeError:
        return occupation_dictionary
    except rapidjson.JSONDecodeError:
        return occupation_dictionary

    for item_occupation in data_occupation:
        if item_occupation['string'] is not None:
            name = item_occupation['string'].value
        else:
            name = None

        if item_occupation['item'].getID() is not None:
            occupation_dictionary[name] = item_occupation['item'].getID()

    return occupation_dictionary


def get_all_non_deprecated_items(limit: Union[int, None] = None, offset: Union[int, None] = None) -> dict[
    dict[str, list, list]]:
    non_deprecated_dictionary: dict[dict[str, list, list]] = {}

    query = """
    select ?item ?nkcr ?isni ?orcid where {
        ?item p:P691 [ps:P691 ?nkcr ; wikibase:rank ?rank ] filter(?rank != wikibase:DeprecatedRank) .
        OPTIONAL{?item wdt:P213 ?isni}.
        OPTIONAL{?item wdt:P496 ?orcid}.
        # VALUES ?nkcr {'xx0226992' 'xx0137101' 'xx0136031' 'xx0277028'}
    } LIMIT """ + str(limit) + """ OFFSET """ + str(offset) + """
    """

    # query_object = sparql.SparqlQuery()
    query_object = mySparql.MySparqlQuery()
    try:
        data_non_deprecated = query_object.select(query=query, full_data=True)
    except simplejson.errors.JSONDecodeError:
        return non_deprecated_dictionary
    except rapidjson.JSONDecodeError:
        return non_deprecated_dictionary

    # non_deprecated_dictionary_cache = []
    item_non_deprecated: dict[str, Union[
        pywikibot.data.sparql.URI, pywikibot.data.sparql.Literal, Union[pywikibot.data.sparql.Literal, None], Union[
            pywikibot.data.sparql.Literal, None]]]
    for item_non_deprecated in data_non_deprecated:
        if item_non_deprecated['isni'] is not None:
            isni = item_non_deprecated['isni'].value
        else:
            isni = None

        if item_non_deprecated['orcid'] is not None:
            orcid = item_non_deprecated['orcid'].value
        else:
            orcid = None

        if non_deprecated_dictionary.get(item_non_deprecated['nkcr'].value, None):
            if isni is not None:
                non_deprecated_dictionary[item_non_deprecated['nkcr'].value]['isni'].append(isni)

            if orcid is not None:
                non_deprecated_dictionary[item_non_deprecated['nkcr'].value]['orcid'].append(orcid)
        else:
            if isni is not None:
                isni_add = [isni]
            else:
                isni_add = []

            if orcid is not None:
                orcid_add = [orcid]
            else:
                orcid_add = []

            non_deprecated_dictionary[item_non_deprecated['nkcr'].value] = {
                'qid': item_non_deprecated['item'].getID(),
                'isni': isni_add,
                'orcid': orcid_add,
            }
    del data_non_deprecated
    return non_deprecated_dictionary


def get_all_non_deprecated_items_occupation(limit: Union[int, None] = None, offset: Union[int, None] = None) -> dict[
    dict[str, list, list]]:
    non_deprecated_dictionary: dict[dict[str, list, list]] = {}

    query = """
    select ?item ?nkcr ?occup where {
        ?item p:P691 [ps:P691 ?nkcr ; wikibase:rank ?rank ] filter(?rank != wikibase:DeprecatedRank) .
        OPTIONAL{?item wdt:P106 ?occup}.
        # VALUES ?nkcr {'xx0226992' 'xx0137101' 'xx0136031' 'xx0277028'}
        
    } LIMIT """ + str(limit) + """ OFFSET """ + str(offset) + """
    """
    # if (limit is not None):
    #     query = query + ' LIMIT ' + str(limit)

    # query_object = mySparql.MySparqlQuery()
    query_object = mySparql.MySparqlQuery()

    try:
        data_non_deprecated = query_object.select(query=query, full_data=True)
    except simplejson.errors.JSONDecodeError:
        return non_deprecated_dictionary
    except rapidjson.JSONDecodeError:
        return non_deprecated_dictionary

    if type(data_non_deprecated) is None:
        return non_deprecated_dictionary

    # non_deprecated_dictionary_cache = []
    item_non_deprecated: dict[str, Union[
        pywikibot.data.sparql.URI, pywikibot.data.sparql.Literal, Union[pywikibot.data.sparql.Literal, None], Union[
            pywikibot.data.sparql.Literal, None]]]
    for item_non_deprecated in data_non_deprecated:
        if item_non_deprecated['occup'] is not None:
            occupation = item_non_deprecated['occup'].getID()
        else:
            occupation = None

        if non_deprecated_dictionary.get(item_non_deprecated['nkcr'].value, None):
            if occupation is not None:
                non_deprecated_dictionary[item_non_deprecated['nkcr'].value]['occup'].append(occupation)
        else:
            if occupation is not None:
                occupation_add = [occupation]
            else:
                occupation_add = []

            non_deprecated_dictionary[item_non_deprecated['nkcr'].value] = {
                'qid': item_non_deprecated['item'].getID(),
                'occup': occupation_add,
            }
    del data_non_deprecated
    del query_object
    return non_deprecated_dictionary


def get_all_non_deprecated_items_field_of_work_and_occupation(limit: Union[int, None] = None, offset: Union[int, None] = None) -> dict[
    dict[str, list, list]]:
    non_deprecated_dictionary: dict[dict[str, list, list]] = {}

    query = """
    select ?item ?nkcr ?field ?occup where {
        ?item p:P691 [ps:P691 ?nkcr ; wikibase:rank ?rank ] filter(?rank != wikibase:DeprecatedRank) .
        OPTIONAL{?item wdt:P101 ?field}.
        OPTIONAL{?item wdt:P106 ?occup}.
       #  VALUES ?nkcr {'xx0226992' 'xx0137101' 'xx0136031' 'xx0277028'}

    } LIMIT """ + str(limit) + """ OFFSET """ + str(offset) + """
    """
    # if (limit is not None):
    #     query = query + ' LIMIT ' + str(limit)

    # query_object = sparql.SparqlQuery()
    query_object = mySparql.MySparqlQuery()
    try:
        data_non_deprecated = query_object.select(query=query, full_data=True)
    except simplejson.errors.JSONDecodeError:
        return non_deprecated_dictionary
    except rapidjson.JSONDecodeError:
        return non_deprecated_dictionary

    if type(data_non_deprecated) is None:
        return non_deprecated_dictionary

    # non_deprecated_dictionary_cache = []
    item_non_deprecated: dict[str, Union[
        pywikibot.data.sparql.URI, pywikibot.data.sparql.Literal, Union[pywikibot.data.sparql.Literal, None], Union[
            pywikibot.data.sparql.Literal, None]]]
    for item_non_deprecated in data_non_deprecated:
        if item_non_deprecated['field'] is not None:
            field_of_work = item_non_deprecated['field'].getID()
        else:
            field_of_work = None

        if item_non_deprecated['occup'] is not None:
            occupation = item_non_deprecated['occup'].getID()
        else:
            occupation = None

        if non_deprecated_dictionary.get(item_non_deprecated['nkcr'].value, None):
            if field_of_work is not None:
                non_deprecated_dictionary[item_non_deprecated['nkcr'].value]['field'].append(field_of_work)

            if occupation is not None:
                non_deprecated_dictionary[item_non_deprecated['nkcr'].value]['occup'].append(occupation)
        else:
            if field_of_work is not None:
                field_of_work_add = [field_of_work]
            else:
                field_of_work_add = []

            if occupation is not None:
                occupation_add = [occupation]
            else:
                occupation_add = []

            non_deprecated_dictionary[item_non_deprecated['nkcr'].value] = {
                'qid': item_non_deprecated['item'].getID(),
                'field': field_of_work_add,
                'occup': occupation_add,
            }
    del data_non_deprecated
    return non_deprecated_dictionary

def get_all_non_deprecated_items_places(limit: Union[int, None] = None, offset: Union[int, None] = None) -> dict[
    dict[str, list, list]]:
    non_deprecated_dictionary: dict[dict[str, list, list]] = {}

    query = """
    select  ?item ?nkcr ?birth ?death ?work where {
        ?item p:P691 [ps:P691 ?nkcr ; wikibase:rank ?rank ] filter(?rank != wikibase:DeprecatedRank) .
        OPTIONAL{?item wdt:P19 ?birth}.
        OPTIONAL{?item wdt:P20 ?death}.
        OPTIONAL{?item wdt:P937 ?work}.
       #  VALUES ?nkcr {'xx0226992' 'xx0137101' 'xx0136031' 'xx0277028'}
    }  LIMIT """ + str(limit) + """ OFFSET """ + str(offset) + """
    """
    # if (limit is not None):
    #     query = query + ' LIMIT ' + str(limit)

    # query_object = sparql.SparqlQuery()
    query_object = mySparql.MySparqlQuery()
    try:
        data_non_deprecated = query_object.select(query=query, full_data=True)
    except simplejson.errors.JSONDecodeError:
        return non_deprecated_dictionary
    except rapidjson.JSONDecodeError:
        return non_deprecated_dictionary

    if type(data_non_deprecated) is None:
        return non_deprecated_dictionary

    # non_deprecated_dictionary_cache = []
    item_non_deprecated: dict[str, Union[
        pywikibot.data.sparql.URI, pywikibot.data.sparql.Literal, Union[pywikibot.data.sparql.Literal, None], Union[
            pywikibot.data.sparql.Literal, None]]]
    for item_non_deprecated in data_non_deprecated:
        if item_non_deprecated['birth'] is not None:
            birth = item_non_deprecated['birth'].getID()
        else:
            birth = None

        if item_non_deprecated['death'] is not None:
            death = item_non_deprecated['death'].getID()
        else:
            death = None

        if item_non_deprecated['work'] is not None:
            work = item_non_deprecated['work'].getID()
        else:
            work = None

        if non_deprecated_dictionary.get(item_non_deprecated['nkcr'].value, None):
            if birth is not None:
                non_deprecated_dictionary[item_non_deprecated['nkcr'].value]['birth'].append(birth)
            if death is not None:
                non_deprecated_dictionary[item_non_deprecated['nkcr'].value]['death'].append(death)
            if work is not None:
                non_deprecated_dictionary[item_non_deprecated['nkcr'].value]['work'].append(work)
        else:
            if birth is not None:
                birth_add = [birth]
            else:
                birth_add = []

            if death is not None:
                death_add = [death]
            else:
                death_add = []

            if work is not None:
                work_add = [work]
            else:
                work_add = []

            non_deprecated_dictionary[item_non_deprecated['nkcr'].value] = {
                'qid': item_non_deprecated['item'].getID(),
                'birth': birth_add,
                'death': death_add,
                'work': work_add,
            }
    del data_non_deprecated
    return non_deprecated_dictionary


def load_nkcr_items(file_name) -> pandas.DataFrame:
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
    # data_csv.fillna('', inplace=True)
    return data_csv


def get_claim_from_item_by_property(datas: dict[str, Any], property_of_item: str) -> list:
    claims_from_data = []
    claims_by_property = datas['claims'].get(property_of_item, [])
    for claim in claims_by_property:
        claims_from_data.append(claim.getTarget())

    return claims_from_data


def is_item_subclass_of(item: pywikibot.ItemPage, subclass: pywikibot.ItemPage):
    query = """
    select ?item where  {
        values ?item {wd:""" + item.getID() + """}
        ?item wdt:P31/wdt:P279* wd:""" + subclass.getID() + """ .
    }
    """

    query_object = mySparql.MySparqlQuery()
    data_is_subclass = query_object.select(query=query, full_data=False)
    if len(data_is_subclass) == 0:
        # not subclass of
        return False
    else:
        return True


def log_with_date_time(message: str = ''):
    datetime_object = datetime.now()
    formatted_time = datetime_object.strftime("%H:%M:%S")
    print(formatted_time + ": " + message)


def load_sparql_query_by_chunks(limit: int, get_method):
    i = 0
    run = True
    final_data = {}
    while run:
        lim = limit

        offset = i * limit
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
    return data

def load_language_dict_csv() -> dict:
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
    url = "https://raw.githubusercontent.com/wmcz/WMCZ-scripts/main/jazyky.csv"
    # print(url)
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

def get_all_non_deprecated_items_languages(limit: Union[int, None] = None, offset: Union[int, None] = None) -> dict[
    dict[str, list, list]]:
    non_deprecated_dictionary: dict[dict[str, list, list]] = {}

    query = """
    select  ?item ?nkcr ?language where {
        ?item p:P691 [ps:P691 ?nkcr ; wikibase:rank ?rank ] filter(?rank != wikibase:DeprecatedRank) .
        OPTIONAL{?item wdt:P1412 ?language}.
        # VALUES ?nkcr {'xx0226992' 'xx0137101' 'xx0136031' 'xx0277028'}
    }  LIMIT """ + str(limit) + """ OFFSET """ + str(offset) + """
    """
    query_object = mySparql.MySparqlQuery()
    try:
        data_non_deprecated = query_object.select(query=query, full_data=True)
    except simplejson.errors.JSONDecodeError:
        return non_deprecated_dictionary
    except rapidjson.JSONDecodeError:
        return non_deprecated_dictionary

    if type(data_non_deprecated) is None:
        return non_deprecated_dictionary

    # non_deprecated_dictionary_cache = []
    item_non_deprecated: dict[str, Union[
        pywikibot.data.sparql.URI, pywikibot.data.sparql.Literal, Union[pywikibot.data.sparql.Literal, None], Union[
            pywikibot.data.sparql.Literal, None]]]
    for item_non_deprecated in data_non_deprecated:
        if item_non_deprecated['language'] is not None:
            language = item_non_deprecated['language'].getID()
        else:
            language = None

        if non_deprecated_dictionary.get(item_non_deprecated['nkcr'].value, None):
            if language is not None:
                non_deprecated_dictionary[item_non_deprecated['nkcr'].value]['language'].append(language)
        else:
            if language is not None:
                language_add = [language]
            else:
                language_add = []

            non_deprecated_dictionary[item_non_deprecated['nkcr'].value] = {
                'qid': item_non_deprecated['item'].getID(),
                'language': language_add,
            }
    del data_non_deprecated
    return non_deprecated_dictionary
