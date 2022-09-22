import csv
import sys
from typing import Union
from datetime import datetime
import pandas
import pywikibot.data.sparql
from pywikibot.data import sparql
import pandas as pd
from os.path import exists

debug = True

def print_info():
    print('Catmandu processor for NKČR')
    if (debug):
        print('DEBUG!!!')


def get_all_non_deprecated_items() -> dict:
    non_deprecated_dictionary: dict[str, str] = {}
    if(exists('cache.csv')):
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
    data = query_object.select(query=query, full_data=True)

    non_deprecated_dictionary_cache = []
    item: dict[str, Union[pywikibot.data.sparql.URI, pywikibot.data.sparql.Literal]]
    for item in data:
        non_deprecated_dictionary[item['nkcr'].value] = item['item'].getID()
        non_deprecated_dictionary_cache.append({'nkcr' : item['nkcr'], 'qid' : item['item'].getID()})

    with open('cache.csv', 'w') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=['nkcr', 'qid'])
        writer.writeheader()
        writer.writerows(non_deprecated_dictionary_cache)

    return non_deprecated_dictionary


def load_nkcr_items() -> pandas.DataFrame:
    data = pd.read_csv("testovaci_soubor.csv", dtype={
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
    # data['100b'] = data['100b'].fillna('')
    data.fillna('', inplace=True)
    return data

def add_nkcr_aut_to_item(item, nkcr_aut, name):
    sources = []

    source_nkcr = pywikibot.Claim(repo, 'P248')
    source_nkcr.setTarget(pywikibot.ItemPage(repo, 'Q13550863'))

    source_nkcr_aut = pywikibot.Claim(repo, 'P691')
    source_nkcr_aut.setTarget(nkcr_aut)

    now = datetime.now()
    source_date = pywikibot.Claim(repo, 'P813')
    source_date.setTarget(pywikibot.WbTime(year=now.year, month=now.month, day=now.day))

    new_claim = pywikibot.Claim(repo, 'P691')
    new_claim.setTarget(nkcr_aut)

    qualifier = pywikibot.Claim(repo, 'P1810')
    qualifier.setTarget(name)

    item.addClaim(new_claim)
    sources.append(source_nkcr)
    sources.append(source_nkcr_aut)
    sources.append(source_date)
    new_claim.addSources(sources)
    new_claim.addQualifier(qualifier)

def add_new_field_to_item(item, property, value, nkcr_aut, name):
    sources = []

    source_nkcr = pywikibot.Claim(repo, 'P248')
    source_nkcr.setTarget(pywikibot.ItemPage(repo, 'Q13550863'))

    source_nkcr_aut = pywikibot.Claim(repo, 'P691')
    source_nkcr_aut.setTarget(nkcr_aut)

    now = datetime.now()
    source_date = pywikibot.Claim(repo, 'P813')
    source_date.setTarget(pywikibot.WbTime(year=now.year, month=now.month, day=now.day))

    new_claim = pywikibot.Claim(repo, property)
    new_claim.setTarget(value)

    item.addClaim(new_claim)
    sources.append(source_nkcr)
    sources.append(source_nkcr_aut)
    sources.append(source_date)
    new_claim.addSources(sources)


def prepare_isni_from_nkcr(isni):
    #https://pythonexamples.org/python-split-string-into-specific-length-chunks/
    n = 4
    chunks = [isni[i:i + n] for i in range(0, len(isni), n)]
    return ' '.join(chunks)

def process_new_fields(qid, row):
    print('process')
    item = pywikibot.ItemPage(repo, qid)
    datas = item.get()
    # isni = P213
    # orcid = P496
    properties = {'0247a-isni' : 'P213', '0247a-orcid' : 'P496'}
    for column, property in properties.items():
        try:
            vals = []
            claims = datas['claims'][property]
            if (column == '0247a-isni'):
                row[column] = prepare_isni_from_nkcr(row[column])
            for claim in claims:
                if (row[column] != claim.getTarget() and row[column] != ''):
                    #insert
                    # print(row[column])
                    add_new_field_to_item(item, property, row[column], row['_id'], row['100a'])
                    pass

        except ValueError as e:
            pass
            # print(e)
        except pywikibot.exceptions.OtherPageSaveError as e:
            pass
            # print(e)
        except KeyError as e:
            # insert
            try:
                if (column == '0247a-isni'):
                    row[column] = prepare_isni_from_nkcr(row[column])
                # print(row[column])
                add_new_field_to_item(item, property, row[column], row['_id'], row['100a'])
                # print(e)
            except KeyError as e:
                pass
                # print(e)
            except pywikibot.exceptions.OtherPageSaveError as e:
                pass
                # print(e)
    #Skript 3 (jen pokud byl nalezen match ve skriptu 2) →
    # načti si z matchované položky hodnoty vlastností ISNI, ORCID (bez ohledu na rank - klidně i zavržený) → pokud tyto hodnoty jsou odlišné od hodnoty v záznamu NK, tak vložit do položky příslušné ISNI a ORCID z pole 024 – Pokud je nkcr aut deprecated, přeskočit a nepokračovat do 3 (dle CSV).

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    print_info()
    site = pywikibot.Site('wikidata', 'wikidata')
    repo = site.data_repository()
    non_deprecated_items = get_all_non_deprecated_items()
    data = load_nkcr_items()
    for index, row in data.iterrows():
        # print(row['0247a-wikidata'], row['_id'])
        nkcr_aut = row['_id']
        qid = row['0247a-wikidata']
        if (qid != ''): #raději bych none, ale to tady nejde ... pandas, no
            name = row['100a']
            item = pywikibot.ItemPage(repo, qid)
            datas = item.get()
            try:
                nkcr_auts = []
                claims = datas['claims']['P691']
                for claim in claims:
                    nkcr_auts.append(claim.getTarget())
                print(nkcr_auts)
                if (nkcr_aut not in nkcr_auts):
                    try:
                        add_nkcr_aut_to_item(item, nkcr_aut, name)
                    except pywikibot.exceptions.OtherPageSaveError as e:
                        print(e)
                        print('Not save now, test only')
                    except ValueError as e:
                        print(e)
            except KeyError as e:
                print('key err')
        if nkcr_aut in non_deprecated_items.keys():
            exist_qid = non_deprecated_items[nkcr_aut]
            if (qid != exist_qid):
                process_new_fields(exist_qid, row)


#Skript 1: Pokud je v poli 024 vloženo QID → podívat se do této položky,
# je tam dané autoritní ID? (bez ohledu na rank - klidně i zavržený)
# → pokud ne, tak vložit do položky příslušné ID autorit z pole 024

#Skript 2 (probíhá bez ohledu na výsledek skriptu 1):
# Je ID daného záznamu v nějaké položce na Wikidatech?
# Nejprve se podívej do Query 1
# → pokud najdeš match, tak pokračuj do Skriptu 3.

#Skript 3 (jen pokud byl nalezen match ve skriptu 2)
# → načti si z matchované položky hodnoty vlastností ISNI, ORCID
# (bez ohledu na rank - klidně i zavržený)
# → pokud tyto hodnoty jsou odlišné od hodnoty v záznamu NK,
# tak vložit do položky příslušné ISNI a ORCID z pole 024



# See PyCharm help at https://www.jetbrains.com/help/pycharm/
