import csv
from typing import Union, Any

import pandas
import pandas as pd
import pywikibot
from datetime import datetime

import simplejson.errors
from pywikibot.data import sparql

import pywikibot_extension
from cleaners import clean_last_comma


def write_log(fields, create_file=False):
    if create_file:
        csvfile = open('debug.csv', 'w')
    else:
        csvfile = open('debug.csv', 'a')
    writer = csv.DictWriter(csvfile, fieldnames=['item', 'prop', 'value'])
    writer.writerow(fields)
    csvfile.close()

def print_info(debug):
    log_with_date_time('Catmandu processor for NKČR')
    if debug:
        log_with_date_time('DEBUG!!!')

def add_new_field_to_item(debug: bool, repo: pywikibot_extension.MyDataSite, item_new_field: pywikibot.ItemPage, property_new_field: str, value: object,
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
        if (type(value) is pywikibot.ItemPage):
            value = value.getID()
        final = {'item': item_new_field.getID(), 'prop': property_new_field, 'value': value}
        write_log(final)
    else:
        item_new_field.addClaim(new_claim, tags=['Czech-Authorities-Sync'])
        sources.append(source_nkcr)
        sources.append(source_nkcr_aut)
        sources.append(source_date)
        new_claim.addSources(sources, tags=['Czech-Authorities-Sync'])

def add_nkcr_aut_to_item(debug: bool, repo: pywikibot_extension.MyDataSite, item_to_add: pywikibot.ItemPage, nkcr_aut_to_add: str, name_to_add: str):
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
        item_to_add.addClaim(new_claim, tags=['Czech-Authorities-Sync'])
        sources.append(source_nkcr)
        sources.append(source_nkcr_aut)
        sources.append(source_date)
        new_claim.addSources(sources, tags=['Czech-Authorities-Sync'])
        new_claim.addQualifier(qualifier, tags=['Czech-Authorities-Sync'])



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

def get_occupations():
    query = """
    select distinct ?item ?value ?string where {

        ?item p:P691 ?s .
        ?s wikibase:rank ?rank filter(?rank != wikibase:DeprecatedRank) .
        ?s ps:P691 ?value filter(strstarts(str(?value),"ph")) .
        ?s pq:P1810 ?string .
    }
    """

    query_object = sparql.SparqlQuery()
    data_occupation = query_object.select(query=query, full_data=True)

    occupation_dictionary: dict[dict[str, list, list]] = {}

    for item_occupation in data_occupation:
        if item_occupation['string'] is not None:
            name = item_occupation['string'].value
        else:
            name = None

        if item_occupation['item'].getID() is not None:
            occupation_dictionary[name] = item_occupation['item'].getID()

    return occupation_dictionary

def get_all_non_deprecated_items(limit:Union[int,None] = None) -> dict[dict[str, list, list]]:
    non_deprecated_dictionary: dict[dict[str, list, list]] = {}

    query = """
    select ?item ?nkcr ?isni ?orcid where {
        ?item p:P691 [ps:P691 ?nkcr ; wikibase:rank ?rank ] filter(?rank != wikibase:DeprecatedRank) .
        OPTIONAL{?item wdt:P213 ?isni}.
        OPTIONAL{?item wdt:P496 ?orcid}.
        # VALUES ?nkcr {'jk01010030' 'mzk2004248910' 'xx0278251'}
    }
    """

    if (limit is not None):
        query = query + ' LIMIT ' + str(limit)

    query_object = sparql.SparqlQuery()
    try:
        data_non_deprecated = query_object.select(query=query, full_data=True)
    except simplejson.errors.JSONDecodeError as e:
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

    return non_deprecated_dictionary

def get_all_non_deprecated_items_occupation(limit:Union[int,None] = None, offset:Union[int,None] = None) -> dict[dict[str, list, list]]:
    non_deprecated_dictionary: dict[dict[str, list, list]] = {}

    query = """
    select ?item ?nkcr ?occup where {
        ?item p:P691 [ps:P691 ?nkcr ; wikibase:rank ?rank ] filter(?rank != wikibase:DeprecatedRank) .
        OPTIONAL{?item wdt:P106 ?occup}.
          # VALUES ?nkcr {'jk01010030' 'mzk2004248910' 'xx0278251'} 
        
    } LIMIT """ + str(limit) + """ OFFSET """ + str(offset) + """
    """
    # if (limit is not None):
    #     query = query + ' LIMIT ' + str(limit)

    query_object = sparql.SparqlQuery()
    data_non_deprecated = query_object.select(query=query, full_data=True)

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

def get_claim_from_item_by_property(datas: dict[str, Any], property: str) -> list:
    claims_from_data = []
    claims_by_property = datas['claims'].get(property, [])
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

    query_object = sparql.SparqlQuery()
    data_is_subclass = query_object.select(query=query, full_data=False)
    if (len(data_is_subclass) == 0):
        # not subclass of
        return False
    else:
        return True

def log_with_date_time(message:str = ''):
    datetime_object = datetime.now()
    formatted_time = datetime_object.strftime("%H:%M:%S")
    print(formatted_time + ": " + message)