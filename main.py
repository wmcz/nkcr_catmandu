import csv
from typing import Union
from datetime import datetime
import pandas
import pywikibot.data.sparql
from pywikibot.data import sparql
import pandas as pd
from os.path import exists

debug = True
file_name = 'output.csv'

if debug:
    csvfile = open('debug.csv', 'w')
    writer = csv.DictWriter(csvfile, fieldnames=['item', 'prop', 'value'])

def print_info():
    print('Catmandu processor for NKČR')

    print('TEST deploy')
    if debug:
        print('DEBUG!!!')


def clean_last_comma(string: str) -> str:
    if string.endswith(','):
        return string[:-1]
    return string

def clean_qid(string: str) -> str:
    string = string.replace(')', '').replace('(', '')
    return string

def get_all_non_deprecated_items() -> dict:
    non_deprecated_dictionary: dict[str, str] = {}
    if exists('cache.csv'):
        with open('cache.csv') as csvfile:
            lines = csv.DictReader(csvfile)
            for line in lines:
                non_deprecated_dictionary[line['nkcr']] = line['qid']
        return non_deprecated_dictionary
    query = """
    select ?item ?nkcr where {
        ?item p:P691 [ps:P691 ?nkcr ; wikibase:rank ?rank ] filter(?rank != wikibase:DeprecatedRank) .
    }
    
    """
    query_object = sparql.SparqlQuery()
    data_non_deprecated = query_object.select(query=query, full_data=True)

    non_deprecated_dictionary_cache = []
    item_non_deprecated: dict[str, Union[pywikibot.data.sparql.URI, pywikibot.data.sparql.Literal]]
    for item_non_deprecated in data_non_deprecated:
        non_deprecated_dictionary[item_non_deprecated['nkcr'].value] = item_non_deprecated['item'].getID()
        non_deprecated_dictionary_cache.append(
            {'nkcr': item_non_deprecated['nkcr'], 'qid': item_non_deprecated['item'].getID()})

    with open('cache.csv', 'w') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=['nkcr', 'qid'])
        writer.writeheader()
        writer.writerows(non_deprecated_dictionary_cache)

    return non_deprecated_dictionary


def load_nkcr_items() -> pandas.DataFrame:
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
    })
    data_csv.fillna('', inplace=True)
    return data_csv


def add_nkcr_aut_to_item(item_to_add: pywikibot.ItemPage, nkcr_aut_to_add: str, name_to_add: str):
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
        writer.writerow(final)
    else:
        item_to_add.addClaim(new_claim)
        sources.append(source_nkcr)
        sources.append(source_nkcr_aut)
        sources.append(source_date)
        new_claim.addSources(sources)
        new_claim.addQualifier(qualifier)


def add_new_field_to_item(item_new_field: pywikibot.ItemPage, property_new_field: str, value: object, nkcr_aut_new_field: str):
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
        final = {'item': item_new_field.getID(), 'prop': property_new_field, 'value': value}
        writer.writerow(final)
    else:
        item_new_field.addClaim(new_claim)
        sources.append(source_nkcr)
        sources.append(source_nkcr_aut)
        sources.append(source_date)
        new_claim.addSources(sources)


def prepare_isni_from_nkcr(isni:str) -> str:
    # https://pythonexamples.org/python-split-string-into-specific-length-chunks/
    n = 4
    chunks = [isni[i:i + n] for i in range(0, len(isni), n)]
    return ' '.join(chunks)


def process_new_fields(qid_new_fields: str, row_new_fields: object):
    # print('process')
    item_new_field = pywikibot.ItemPage(repo, qid_new_fields)
    datas_new_field = item_new_field.get(get_redirect=True)
    # isni = P213
    # orcid = P496
    properties = {'0247a-isni': 'P213', '0247a-orcid': 'P496'}
    for column, property_for_new_field in properties.items():
        try:
            claims_in_new_item = datas_new_field['claims'][property_for_new_field]
            if column == '0247a-isni':
                row_new_fields[column] = prepare_isni_from_nkcr(row_new_fields[column])
            for claim_in_new_item in claims_in_new_item:
                if row_new_fields[column] != claim_in_new_item.getTarget() and row_new_fields[column] != '':
                    # insert
                    add_new_field_to_item(item_new_field, property_for_new_field, row_new_fields[column], row_new_fields['_id'])

        except ValueError as e:
            print(e)
            pass
        except pywikibot.exceptions.OtherPageSaveError as e:
            print(e)
            pass
        except KeyError as e:
            # insert
            try:
                if column == '0247a-isni':
                    row_new_fields[column] = prepare_isni_from_nkcr(row_new_fields[column])
                if row_new_fields[column] != '':
                    add_new_field_to_item(item_new_field, property_for_new_field, row_new_fields[column], row_new_fields['_id'])
            except KeyError as e:
                print(e)
                pass
            except pywikibot.exceptions.OtherPageSaveError as e:
                print(e)
                pass


def get_nkcr_auts_from_item(datas) -> list:
    nkcr_auts = []
    claims = datas['claims']['P691']
    for claim in claims:
        nkcr_auts.append(claim.getTarget())

    return nkcr_auts

if __name__ == '__main__':
    print_info()
    site = pywikibot.Site('wikidata', 'wikidata')
    repo = site.data_repository()

    non_deprecated_items = get_all_non_deprecated_items()
    data = load_nkcr_items()


    for index, row in data.iterrows():
        nkcr_aut = row['_id']
        print(nkcr_aut)
        qid = row['0247a-wikidata']
        if qid != '':  # raději bych none, ale to tady nejde ... pandas, no
            name = row['100a']
            qid = clean_qid(qid)
            item = pywikibot.ItemPage(repo, qid)
            datas = item.get(get_redirect=True)
            try:
                nkcr_auts = get_nkcr_auts_from_item(datas)
                if nkcr_aut not in nkcr_auts:
                    try:
                        add_nkcr_aut_to_item(item, nkcr_aut, name)
                    except pywikibot.exceptions.OtherPageSaveError as e:
                        print(e)
                    except ValueError as e:
                        print(e)
            except KeyError as e:
                print('key err')
        if nkcr_aut in non_deprecated_items.keys():
            exist_qid = non_deprecated_items[nkcr_aut]
            if qid != exist_qid and qid != '':
                process_new_fields(exist_qid, row)

    csvfile.close()