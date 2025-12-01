import csv
import gc
import logging
import os
from datetime import datetime
from typing import Union, Any

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
from wikibaseintegrator.models import References
from wikibaseintegrator.wbi_enums import WikibaseTimePrecision, WikibaseDatatype, ActionIfExists

import cleaners
import mySparql
import pywikibot_extension
from cleaners import clean_last_comma
from config import Config
log = logging.getLogger(__name__)


def write_log(fields, create_file=False):
    if create_file:
        csvfile = open('debug.csv', 'w')
    else:
        csvfile = open('debug.csv', 'a')
    writer = csv.DictWriter(csvfile, fieldnames=['item', 'prop', 'value'])
    writer.writerow(fields)
    log.info(fields)
    csvfile.close()


def reset_debug_file():
    csvfile = open('debug.csv', 'w')
    csvfile.close()


def read_log() -> csv.DictReader:
    csvfile = open('debug.csv', 'r')
    reader = csv.DictReader(csvfile, fieldnames=['item', 'prop', 'value'])
    return reader


def print_info(debug: bool):
    log_with_date_time('Catmandu processor for NKČR')
    if debug:
        log_with_date_time('DEBUG!!!')


def add_new_field_to_item_wbi(
        item_new_field: ItemEntity,
        property_new_field: str, value: Union[str, int],
        nkcr_aut_new_field: str):

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
        # string
        final = {'item': item_new_field.id, 'prop': property_new_field, 'value': value}
        new_claim = ExternalID(value=value, prop_nr=property_new_field, references=references)
    elif property_new_field in ['P569', 'P570'] or property_new_field == ['P569', 'P570']:
        final = {'item': item_new_field.id, 'prop': property_new_field, 'value': value}
        value: Time
        new_refs = References()
        for ref in references:
            for ref_claim in ref:
                new_refs.add(ref_claim)
        value.references = new_refs
        new_claim = value
    else:
        final = {'item': item_new_field.id, 'prop': property_new_field, 'value': value}
        new_claim = Item(value=value, prop_nr=property_new_field, references=references)
    write_log(final)
    item_new_field.claims.add(new_claim, action_if_exists=ActionIfExists.APPEND_OR_REPLACE)

    return item_new_field


def add_nkcr_aut_to_item(debug: bool, repo: pywikibot_extension.MyDataSite, item_to_add: pywikibot.ItemPage,
                         nkcr_aut_to_add: str, name_to_add: str):
    now = datetime.now()

    sources = []

    source_date = pywikibot.Claim(repo, 'P813')
    source_date.setTarget(pywikibot.WbTime(year=now.year, month=now.month, day=now.day))

    source_nkcr = pywikibot.Claim(repo, 'P248')
    source_nkcr.setTarget(pywikibot.ItemPage(repo, 'Q13550863'))

    source_nkcr_aut = pywikibot.Claim(repo, 'P691')
    source_nkcr_aut.setTarget(nkcr_aut_to_add)

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


def add_nkcr_aut_to_item_wbi(
        item_to_add: ItemEntity,
        nkcr_aut_to_add: str,
        name_to_add: str):

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
    # print(final)
    new_claim = ExternalID(value=nkcr_aut_to_add, prop_nr='P691', references=references, qualifiers=qualifier)
    item_to_add.claims.add(new_claim, action_if_exists=ActionIfExists.APPEND_OR_REPLACE)
    return item_to_add


def get_nkcr_auts_from_item(datas) -> list:
    nkcr_auts_from_data = []
    claims_from_wd = datas['claims'].get('P691', [])
    for claim in claims_from_wd:
        nkcr_auts_from_data.append(claim.getTarget())

    return nkcr_auts_from_data


def get_nkcr_auts_from_item_wbi(datas) -> list:
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
    return_qids: dict[str, list[str]] = {}
    for nkcr, nkcr_line in items.items():
        if return_qids.get(nkcr_line['qid']):
            return_qids[nkcr_line['qid']].append(nkcr)
        else:
            return_qids[nkcr_line['qid']] = [nkcr]

    return return_qids


def get_occupations(limit: Union[int, None] = None, offset: Union[int, None] = None) -> dict[str, str]:
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
    except Exception as e:
        log_with_date_time('get occupations Exception: ' + str(e))
        return occupation_dictionary
    except requests.exceptions.ConnectionError as e:
        log_with_date_time('get occupations ConnectionError: ' + str(e))
        return occupation_dictionary

    for item_occupation in data_occupation_wbi['results']['bindings']:
        if item_occupation['string'] is not None:
            name = item_occupation['string']['value']
        else:
            name = None

        if item_occupation['item']['value']:
            occupation_dictionary[name] = item_occupation['item']['value'].replace('http://www.wikidata.org/entity/', '')

    return occupation_dictionary


def get_all_non_deprecated_items(limit: Union[int, None] = None, offset: Union[int, None] = None) -> dict[
    dict[str, list, list]]:
    non_deprecated_dictionary: dict[dict[str, list, list]] = {}

    query = """
    select ?item ?nkcr ?isni ?orcid ?birth ?death where {
        ?item p:P691 [ps:P691 ?nkcr ; wikibase:rank ?rank ] filter(?rank != wikibase:DeprecatedRank) .
        OPTIONAL{?item wdt:P213 ?isni}.
        OPTIONAL{?item wdt:P496 ?orcid}.
        OPTIONAL{?item wdt:P569 ?birth}.
        OPTIONAL{?item wdt:P570 ?death}.
        # VALUES ?nkcr {'mub2013789925' 'xx0270669' 'xx0279468' 'uk20241216330'}
    } LIMIT """ + str(limit) + """ OFFSET """ + str(offset) + """
    """

    # query_object = sparql.SparqlQuery()
    # query_object = mySparql.MySparqlQuery()
    query_object = mySparql.MySparqlQuery(endpoint="https://query-main.wikidata.org/sparql",
                                          entity_url='http://www.wikidata.org/entity/')
    try:
        data_non_deprecated = query_object.select(query=query, full_data=False)
    except simplejson.errors.JSONDecodeError as e:
        log_with_date_time('get non deprecated items JSONDecodeError: ' + str(e))
        return non_deprecated_dictionary
    except rapidjson.JSONDecodeError as e:
        log_with_date_time('get non deprecated items JSONDecodeError: ' + str(e))
        return non_deprecated_dictionary
    except pywikibot.exceptions.ServerError as e:
        log_with_date_time('get non deprecated items ServerError: ' + str(e))
        return non_deprecated_dictionary
    except requests.exceptions.ConnectionError as e:
        log_with_date_time('get non deprecated items ConnectionError: ' + str(e))
        return non_deprecated_dictionary

    # non_deprecated_dictionary_cache = []
    item_non_deprecated: dict[str, Union[
        pywikibot.data.sparql.URI, pywikibot.data.sparql.Literal, Union[pywikibot.data.sparql.Literal, None], Union[
            pywikibot.data.sparql.Literal, None]]]
    for item_non_deprecated in data_non_deprecated:
        if item_non_deprecated['isni'] is not None:
            isni = item_non_deprecated['isni']
        else:
            isni = None

        if item_non_deprecated['orcid'] is not None:
            orcid = item_non_deprecated['orcid']
        else:
            orcid = None

        if item_non_deprecated['birth'] is not None:
            birth = item_non_deprecated['birth']
        else:
            birth = None

        if item_non_deprecated['death'] is not None:
            death = item_non_deprecated['death']
        else:
            death = None

        if non_deprecated_dictionary.get(item_non_deprecated['nkcr'], None):
            if isni is not None:
                non_deprecated_dictionary[item_non_deprecated['nkcr']]['isni'].append(isni)

            if orcid is not None:
                non_deprecated_dictionary[item_non_deprecated['nkcr']]['orcid'].append(orcid)

            if birth is not None:
                non_deprecated_dictionary[item_non_deprecated['nkcr']]['birth'].append(birth)

            if death is not None:
                non_deprecated_dictionary[item_non_deprecated['nkcr']]['death'].append(death)
        else:
            if isni is not None:
                isni_add = [isni]
            else:
                isni_add = []

            if orcid is not None:
                orcid_add = [orcid]
            else:
                orcid_add = []

            if birth is not None:
                birth_add = [birth]
            else:
                birth_add = []

            if death is not None:
                death_add = [death]
            else:
                death_add = []

            non_deprecated_dictionary[item_non_deprecated['nkcr']] = {
                'qid': item_non_deprecated['item'].replace('http://www.wikidata.org/entity/', ''),
                'isni': isni_add,
                'orcid': orcid_add,
                'birth': birth_add,
                'death': death_add,
            }
    del data_non_deprecated
    return non_deprecated_dictionary


def get_all_non_deprecated_items_occupation(limit: Union[int, None] = None, offset: Union[int, None] = None) -> dict[dict[str, list, list]]:
    non_deprecated_dictionary: dict[dict[str, list, list]] = {}

    query = """
    select ?item ?nkcr ?occup where {
        ?item p:P691 [ps:P691 ?nkcr ; wikibase:rank ?rank ] filter(?rank != wikibase:DeprecatedRank) .
        OPTIONAL{?item wdt:P106 ?occup}.
        # VALUES ?nkcr {'test123' 'xx0313436' 'xx0313312' 'uk20241216330'}
        
    } LIMIT """ + str(limit) + """ OFFSET """ + str(offset) + """
    """
    # if (limit is not None):
    #     query = query + ' LIMIT ' + str(limit)

    # query_object = mySparql.MySparqlQuery()
    # query_object = mySparql.MySparqlQuery()
    query_object = mySparql.MySparqlQuery(endpoint="https://query-main.wikidata.org/sparql",
                                          entity_url='http://www.wikidata.org/entity/')

    try:
        data_non_deprecated = query_object.select(query=query, full_data=False)
    except simplejson.errors.JSONDecodeError as e:
        log_with_date_time('get non deprecated items occupation JSONDecodeError: ' + str(e))
        return non_deprecated_dictionary
    except rapidjson.JSONDecodeError as e:
        log_with_date_time('get non deprecated items occupation JSONDecodeError: ' + str(e))
        return non_deprecated_dictionary
    except pywikibot.exceptions.ServerError as e:
        log_with_date_time('get non deprecated items occupation ServerError: ' + str(e))
        return non_deprecated_dictionary
    except requests.exceptions.ConnectionError as e:
        log_with_date_time('get non deprecated items occupation ConnectionError: ' + str(e))
        return non_deprecated_dictionary

    if type(data_non_deprecated) is None:
        return non_deprecated_dictionary

    # non_deprecated_dictionary_cache = []
    item_non_deprecated: dict[str, Union[
        str, pywikibot.data.sparql.Literal, Union[pywikibot.data.sparql.Literal, None], Union[
            pywikibot.data.sparql.Literal, None]]]
    for item_non_deprecated in data_non_deprecated:
        if item_non_deprecated['occup'] is not None:
            occupation = item_non_deprecated['occup'].replace('http://www.wikidata.org/entity/', '')
        else:
            occupation = None

        if non_deprecated_dictionary.get(item_non_deprecated['nkcr'], None):
            if occupation is not None:
                non_deprecated_dictionary[item_non_deprecated['nkcr']]['occup'].append(occupation)
        else:
            if occupation is not None:
                occupation_add = [occupation]
            else:
                occupation_add = []

            non_deprecated_dictionary[item_non_deprecated['nkcr']] = {
                'qid': item_non_deprecated['item'].replace('http://www.wikidata.org/entity/', ''),
                'occup': occupation_add,
            }
    del data_non_deprecated
    del query_object
    return non_deprecated_dictionary


def get_all_non_deprecated_items_field_of_work_and_occupation(limit: Union[int, None] = None, offset: Union[int, None] = None) -> dict[dict[str, list, list]]:
    non_deprecated_dictionary: dict[dict[str, list, list]] = {}

    query = """
    select ?item ?nkcr ?field ?occup where {
        ?item p:P691 [ps:P691 ?nkcr ; wikibase:rank ?rank ] filter(?rank != wikibase:DeprecatedRank) .
        OPTIONAL{?item wdt:P101 ?field}.
        OPTIONAL{?item wdt:P106 ?occup}.
       # VALUES ?nkcr {'test123' 'xx0313436' 'xx0313312' 'uk20241216330'}

    } LIMIT """ + str(limit) + """ OFFSET """ + str(offset) + """
    """
    # if (limit is not None):
    #     query = query + ' LIMIT ' + str(limit)

    # query_object = sparql.SparqlQuery()
    query_object = mySparql.MySparqlQuery(endpoint="https://query-main.wikidata.org/sparql", entity_url='http://www.wikidata.org/entity/')
    # query_object = mySparql.MySparqlQuery()
    try:
        data_non_deprecated = query_object.select(query=query, full_data=False)
    except simplejson.errors.JSONDecodeError as e:
        log_with_date_time('get non deprecated items field of work and occupation JSONDecodeError: ' + str(e))
        return non_deprecated_dictionary
    except rapidjson.JSONDecodeError as e:
        log_with_date_time('get non deprecated items field of work and occupation JSONDecodeError: ' + str(e))
        return non_deprecated_dictionary
    except pywikibot.exceptions.ServerError as e:
        log_with_date_time('get non deprecated items field of work and occupation ServerError: ' + str(e))
        return non_deprecated_dictionary
    except requests.exceptions.ConnectionError as e:
        log_with_date_time('get non deprecated items field of work and occupation ConnectionError: ' + str(e))
        return non_deprecated_dictionary

    if type(data_non_deprecated) is None:
        return non_deprecated_dictionary

    # non_deprecated_dictionary_cache = []
    item_non_deprecated: dict[str, Union[
        pywikibot.data.sparql.URI, pywikibot.data.sparql.Literal, Union[pywikibot.data.sparql.Literal, None], Union[
            pywikibot.data.sparql.Literal, None]]]
    for item_non_deprecated in data_non_deprecated:
        if item_non_deprecated['field'] is not None:
            field_of_work = item_non_deprecated['field'].replace('http://www.wikidata.org/entity/', '')
        else:
            field_of_work = None

        if item_non_deprecated['occup'] is not None:
            occupation = item_non_deprecated['occup'].replace('http://www.wikidata.org/entity/', '')
        else:
            occupation = None

        if non_deprecated_dictionary.get(item_non_deprecated['nkcr'], None):
            if field_of_work is not None:
                non_deprecated_dictionary[item_non_deprecated['nkcr']]['field'].append(field_of_work)

            if occupation is not None:
                non_deprecated_dictionary[item_non_deprecated['nkcr']]['occup'].append(occupation)
        else:
            if field_of_work is not None:
                field_of_work_add = [field_of_work]
            else:
                field_of_work_add = []

            if occupation is not None:
                occupation_add = [occupation]
            else:
                occupation_add = []

            non_deprecated_dictionary[item_non_deprecated['nkcr']] = {
                'qid': item_non_deprecated['item'].replace('http://www.wikidata.org/entity/', ''),
                'field': field_of_work_add,
                'occup': occupation_add,
            }
    del data_non_deprecated
    return non_deprecated_dictionary


def get_all_non_deprecated_items_places(limit: Union[int, None] = None, offset: Union[int, None] = None) -> dict[dict[str, list, list]]:
    non_deprecated_dictionary: dict[dict[str, list, list]] = {}

    query = """
    select  ?item ?nkcr ?birth ?death ?work where {
        ?item p:P691 [ps:P691 ?nkcr ; wikibase:rank ?rank ] filter(?rank != wikibase:DeprecatedRank) .
        OPTIONAL{?item wdt:P19 ?birth}.
        OPTIONAL{?item wdt:P20 ?death}.
        OPTIONAL{?item wdt:P937 ?work}.
       # VALUES ?nkcr {'test123' 'xx0313436' 'xx0313312' 'uk20241216330'}
    }  LIMIT """ + str(limit) + """ OFFSET """ + str(offset) + """
    """
    # if (limit is not None):
    #     query = query + ' LIMIT ' + str(limit)

    # query_object = sparql.SparqlQuery()
    # query_object = mySparql.MySparqlQuery()
    query_object = mySparql.MySparqlQuery(endpoint="https://query-main.wikidata.org/sparql",
                                          entity_url='http://www.wikidata.org/entity/')
    try:
        data_non_deprecated = query_object.select(query=query, full_data=False)
    except simplejson.errors.JSONDecodeError as e:
        log_with_date_time('get non deprecated items places JSONDecodeError: ' + str(e))
        return non_deprecated_dictionary
    except rapidjson.JSONDecodeError as e:
        log_with_date_time('get non deprecated items places JSONDecodeError: ' + str(e))
        return non_deprecated_dictionary
    except pywikibot.exceptions.ServerError as e:
        log_with_date_time('get non deprecated items places ServerError: ' + str(e))
        return non_deprecated_dictionary
    except requests.exceptions.ConnectionError as e:
        log_with_date_time('get non deprecated items places ConnectionError: ' + str(e))
        return non_deprecated_dictionary

    if type(data_non_deprecated) is None:
        return non_deprecated_dictionary

    # non_deprecated_dictionary_cache = []
    item_non_deprecated: dict[str, Union[
        str, str, Union[pywikibot.data.sparql.Literal, None], Union[
            pywikibot.data.sparql.Literal, None]]]
    for item_non_deprecated in data_non_deprecated:
        if item_non_deprecated['birth'] is not None:
            birth = item_non_deprecated['birth'].replace('http://www.wikidata.org/entity/', '')
        else:
            birth = None

        if item_non_deprecated['death'] is not None:
            death = item_non_deprecated['death'].replace('http://www.wikidata.org/entity/', '')
        else:
            death = None

        if item_non_deprecated['work'] is not None:
            work = item_non_deprecated['work'].replace('http://www.wikidata.org/entity/', '')
        else:
            work = None

        if non_deprecated_dictionary.get(item_non_deprecated['nkcr'], None):
            if birth is not None:
                non_deprecated_dictionary[item_non_deprecated['nkcr']]['birth'].append(birth)
            if death is not None:
                non_deprecated_dictionary[item_non_deprecated['nkcr']]['death'].append(death)
            if work is not None:
                non_deprecated_dictionary[item_non_deprecated['nkcr']]['work'].append(work)
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

            non_deprecated_dictionary[item_non_deprecated['nkcr']] = {
                'qid': item_non_deprecated['item'].replace('http://www.wikidata.org/entity/', ''),
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


def get_claim_from_item_by_property_wbi(datas: ItemEntity, property_of_item: list) -> list:
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
                    claims_from_data.append(claim.mainsnak.datavalue['value'])
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
                claims_from_data.append(claim.mainsnak.datavalue['value'])
            else:
                claims_from_data.append(claim.mainsnak.datavalue['value']['id'])

        return claims_from_data


def is_item_subclass_of(item: pywikibot.ItemPage, subclass: pywikibot.ItemPage):
    query = """
    select ?item where  {
        values ?item {wd:""" + item.getID() + """}
        ?item wdt:P31/wdt:P279* wd:""" + subclass.getID() + """ .
    }
    """

    # query_object = mySparql.MySparqlQuery()
    query_object = mySparql.MySparqlQuery(endpoint="https://query-main.wikidata.org/sparql",
                                          entity_url='http://www.wikidata.org/entity/')
    data_is_subclass = query_object.select(query=query, full_data=False)
    if len(data_is_subclass) == 0:
        # not subclass of
        return False
    else:
        return True


def is_item_subclass_of_wbi(item_qid: str, subclass_qid: str):
    try:
        cached = cleaners.cachedData[subclass_qid][item_qid]
        return cached
    except KeyError as e:
        pass
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

    if len(data_first['results']['bindings']) == 0:
        # not subclass of
        if cleaners.cachedData.get(subclass_qid, False):
            cleaners.cachedData[subclass_qid][item_qid] = False
        else:
            cleaners.cachedData[subclass_qid] = {}
            cleaners.cachedData[subclass_qid][item_qid] = False
        return False
    else:
        if cleaners.cachedData.get(subclass_qid, False):
            cleaners.cachedData[subclass_qid][item_qid] = True
        else:
            cleaners.cachedData[subclass_qid] = {}

        return True

    # if len(data_first['results']['bindings']) == 0:
    #     # not subclass of - maybe
    #     query = """
    #         select ?item where  {
    #             values ?item {wd:""" + item_qid + """}
    #             ?item wdt:P279*/wdt:P31 wd:""" + subclass_qid + """ .
    #         }
    #         """
    #
    #     data_is_subclass = wbi_helpers.execute_sparql_query(query=query)
    #     # data_is_subclass = query_object.select(query=query, full_data=False)
    #     if len(data_is_subclass['results']['bindings']) == 0:
    #         # not subclass of
    #         return False
    #     else:
    #         return True
    # else:
    #     return True


def log_with_date_time(message: str = ''):
    log.info(message)


def load_sparql_query_by_chunks(limit: int, get_method, name: str):
    if Config.use_json_database:
        if os.path.isfile(name + '.json'):
            infile = open(name + '.json')
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
        # VALUES ?nkcr {'test123' 'xx0313436' 'xx0313312' 'uk20241216330'}
    }  LIMIT """ + str(limit) + """ OFFSET """ + str(offset) + """
    """
    # query_object = mySparql.MySparqlQuery()
    query_object = mySparql.MySparqlQuery(endpoint="https://query-main.wikidata.org/sparql",
                                          entity_url='http://www.wikidata.org/entity/')
    try:
        data_non_deprecated = query_object.select(query=query, full_data=False)
    except simplejson.errors.JSONDecodeError:
        return non_deprecated_dictionary
    except rapidjson.JSONDecodeError:
        return non_deprecated_dictionary
    except pywikibot.exceptions.ServerError:
        return non_deprecated_dictionary

    if type(data_non_deprecated) is None:
        return non_deprecated_dictionary

    # non_deprecated_dictionary_cache = []
    item_non_deprecated: dict[str, Union[
        pywikibot.data.sparql.URI, pywikibot.data.sparql.Literal, Union[pywikibot.data.sparql.Literal, None], Union[
            pywikibot.data.sparql.Literal, None]]]
    for item_non_deprecated in data_non_deprecated:
        if item_non_deprecated['language'] is not None:
            language = item_non_deprecated['language'].replace('http://www.wikidata.org/entity/', '')
        else:
            language = None

        if non_deprecated_dictionary.get(item_non_deprecated['nkcr'], None):
            if language is not None:
                non_deprecated_dictionary[item_non_deprecated['nkcr']]['language'].append(language)
        else:
            if language is not None:
                language_add = [language]
            else:
                language_add = []

            non_deprecated_dictionary[item_non_deprecated['nkcr']] = {
                'qid': item_non_deprecated['item'].replace('http://www.wikidata.org/entity/', ''),
                'language': language_add,
            }
    del data_non_deprecated
    return non_deprecated_dictionary

def get_bot_password(filename):
    with open(filename, 'r') as file:
        first_line = file.readline()
    return first_line

def first_name(name):
    """
            Returns the title of the record (245 $a an $b).
            """
    try:
        assert isinstance(name, str)
        splits = name.replace(',','').split(' ')
        length = len(splits)
        ret = splits[len(splits)-1]

        import re
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
            Returns the title of the record (245 $a an $b).
            """
    try:
        assert isinstance(name, str)
        splits = name.replace(',', '').split(' ')
        length = len(splits)
        ret = splits[0]

        import re
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