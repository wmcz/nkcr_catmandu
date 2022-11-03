import argparse
import csv
import json
import re
import uuid
from typing import Union
from datetime import datetime
import pandas
import pywikibot.data.sparql
import requests
from pywikibot.data import sparql
import pandas as pd
from os.path import exists
import time
from pywikibot.site._decorators import need_right, need_version

from pywikibot.page._collections import (
    ClaimCollection,
)


class BadItemException(Exception):
    pass


class MyItemPage(pywikibot.ItemPage):
    DATA_ATTRIBUTES = {
        'claims': ClaimCollection,
    }

class MyDataSite(pywikibot.DataSite):
    @need_right('edit')
    def addClaim(self, entity, claim, bot=True, summary=None, tags=[]):
        """
        Add a claim.

        :param entity: Entity to modify
        :type entity: WikibaseEntity
        :param claim: Claim to be added
        :type claim: pywikibot.Claim
        :param bot: Whether to mark the edit as a bot edit
        :type bot: bool
        :param summary: Edit summary
        :type summary: str
        """
        claim.snak = entity.getID() + '$' + str(uuid.uuid4())
        params = {'action': 'wbsetclaim',
                  'claim': json.dumps(claim.toJSON()),
                  'baserevid': entity.latest_revision_id,
                  'summary': summary,
                  'token': self.tokens['edit'],
                  'bot': bot,
                  'tags': tags
                  }
        req = self._simple_request(**params)
        jsonvys = claim.toJSON()
        data = req.submit()
        # Update the item
        if claim.getID() in entity.claims:
            entity.claims[claim.getID()].append(claim)
        else:
            entity.claims[claim.getID()] = [claim]
        entity.latest_revision_id = data['pageinfo']['lastrevid']

    @need_right('edit')
    def editSource(self, claim, source, new=False,
                   bot=True, summary=None, baserevid=None, tags=[]):
        """
        Create/Edit a source.

        :param claim: A Claim object to add the source to
        :type claim: pywikibot.Claim
        :param source: A Claim object to be used as a source
        :type source: pywikibot.Claim
        :param new: Whether to create a new one if the "source" already exists
        :type new: bool
        :param bot: Whether to mark the edit as a bot edit
        :type bot: bool
        :param summary: Edit summary
        :type summary: str
        :param baserevid: Base revision id override, used to detect conflicts.
            When omitted, revision of claim.on_item is used. DEPRECATED.
        :type baserevid: long
        """
        if claim.isReference or claim.isQualifier:
            raise ValueError('The claim cannot have a source.')
        params = {'action': 'wbsetreference', 'statement': claim.snak,
                  'baserevid': claim.on_item.latest_revision_id,
                  'summary': summary, 'bot': bot, 'token': self.tokens['edit'], 'tags': tags}

        # build up the snak
        if isinstance(source, list):
            sources = source
        else:
            sources = [source]

        snak = {}
        for sourceclaim in sources:
            datavalue = sourceclaim._formatDataValue()
            valuesnaks = []
            if sourceclaim.getID() in snak:
                valuesnaks = snak[sourceclaim.getID()]
            valuesnaks.append({'snaktype': 'value',
                               'property': sourceclaim.getID(),
                               'datavalue': datavalue,
                               },
                              )

            snak[sourceclaim.getID()] = valuesnaks
            # set the hash if the source should be changed.
            # if present, all claims of one source have the same hash
            if not new and hasattr(sourceclaim, 'hash'):
                params['reference'] = sourceclaim.hash
        params['snaks'] = json.dumps(snak)

        req = self._simple_request(**params)
        return req.submit()

    @need_right('edit')
    def editQualifier(self, claim, qualifier, new=False, bot=True,
                      summary=None, baserevid=None, tags=[]):
        """
        Create/Edit a qualifier.

        :param claim: A Claim object to add the qualifier to
        :type claim: pywikibot.Claim
        :param qualifier: A Claim object to be used as a qualifier
        :type qualifier: pywikibot.Claim
        :param bot: Whether to mark the edit as a bot edit
        :type bot: bool
        :param summary: Edit summary
        :type summary: str
        :param baserevid: Base revision id override, used to detect conflicts.
            When omitted, revision of claim.on_item is used. DEPRECATED.
        :type baserevid: long
        """
        if claim.isReference or claim.isQualifier:
            raise ValueError('The claim cannot have a qualifier.')
        params = {'action': 'wbsetqualifier', 'claim': claim.snak,
                  'baserevid': claim.on_item.latest_revision_id,
                  'summary': summary, 'bot': bot, 'tags': tags}

        if (not new and hasattr(qualifier, 'hash')
                and qualifier.hash is not None):
            params['snakhash'] = qualifier.hash
        params['token'] = self.tokens['edit']
        # build up the snak
        if qualifier.getSnakType() == 'value':
            params['value'] = json.dumps(qualifier._formatValue())
        params['snaktype'] = qualifier.getSnakType()
        params['property'] = qualifier.getID()

        req = self._simple_request(**params)
        return req.submit()

user_name = 'Frettiebot'
debug = False
count_first_step = 0
count_second_step = 0
parser = argparse.ArgumentParser(description='NKČR catmandu pipeline.')
parser.add_argument('-i', '--input', help='NKČR CSV file name', required=True)
args = parser.parse_args()
print("Input file: %s" % args.input)
file_name = args.input


def write_log(fields, create_file=False):
    if create_file:
        csvfile = open('debug.csv', 'w')
    else:
        csvfile = open('debug.csv', 'a')
    writer = csv.DictWriter(csvfile, fieldnames=['item', 'prop', 'value'])
    writer.writerow(fields)
    csvfile.close()


def print_info():
    print('Catmandu processor for NKČR')
    if debug:
        print('DEBUG!!!')


def clean_last_comma(string: str) -> str:
    if string.endswith(','):
        return string[:-1]
    return string


def clean_qid(string: str) -> str:
    string = string.replace(')', '').replace('(', '')
    first_letter = string[0]
    if first_letter.upper() != 'Q':
        raise BadItemException(string)

    return string


def get_all_non_deprecated_items() -> dict[dict[str, list, list]]:
    non_deprecated_dictionary: dict[dict[str, list, list]] = {}

    query = """
    select ?item ?nkcr ?isni ?orcid where {
        ?item p:P691 [ps:P691 ?nkcr ; wikibase:rank ?rank ] filter(?rank != wikibase:DeprecatedRank) .
        OPTIONAL{?item wdt:P213 ?isni}.
        OPTIONAL{?item wdt:P496 ?orcid}.
    }
    """
    query_object = sparql.SparqlQuery()
    data_non_deprecated = query_object.select(query=query, full_data=True)

    # non_deprecated_dictionary_cache = []
    item_non_deprecated: dict[str, Union[pywikibot.data.sparql.URI, pywikibot.data.sparql.Literal, Union[pywikibot.data.sparql.Literal,None], Union[pywikibot.data.sparql.Literal, None]]]
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
    }, chunksize=10000)
    # data_csv.fillna('', inplace=True)
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
        write_log(final)
        # print(final)
    else:
        item_to_add.addClaim(new_claim, tags=['Czech-Authorities-Sync'])
        sources.append(source_nkcr)
        sources.append(source_nkcr_aut)
        sources.append(source_date)
        new_claim.addSources(sources, tags=['Czech-Authorities-Sync'])
        new_claim.addQualifier(qualifier, tags=['Czech-Authorities-Sync'])


def add_new_field_to_item(item_new_field: pywikibot.ItemPage, property_new_field: str, value: object,
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
        final = {'item': item_new_field.getID(), 'prop': property_new_field, 'value': value}
        write_log(final)
    else:
        item_new_field.addClaim(new_claim, tags=['Czech-Authorities-Sync'])
        sources.append(source_nkcr)
        sources.append(source_nkcr_aut)
        sources.append(source_date)
        new_claim.addSources(sources, tags=['Czech-Authorities-Sync'])


def prepare_isni_from_nkcr(isni: str) -> str:
    # https://pythonexamples.org/python-split-string-into-specific-length-chunks/
    isni = isni.replace(' ', '')
    regex = u"^(\d{16})$"

    match = re.search(regex, isni, re.IGNORECASE)
    if not match:
        return ''
    else:
        n = 4
        str_chunks = [isni[i:i + n] for i in range(0, len(isni), n)]
        return ' '.join(str_chunks)

def prepare_orcid_from_nkcr(orcid: str) -> str:
    # https://pythonexamples.org/python-split-string-into-specific-length-chunks/
    orcid = orcid.replace(' ', '')
    regex = u"^(\d{4}-){3}\d{3}(\d|X)$"

    match = re.search(regex, orcid, re.IGNORECASE)
    if not match:
        return ''
    else:
        return orcid


def process_new_fields(qid_new_fields: Union[str, None], wd_data: dict, row_new_fields: object,
                       wd_item: Union[pywikibot.ItemPage, None] = None):
    # print('process')
    if wd_item is None:
        item_new_field = pywikibot.ItemPage(repo, qid_new_fields)
        # if (item_new_field.isRedirectPage()):
        #     item_new_field = item_new_field.getRedirectTarget()
        #     item_new_field.get(get_redirect=True)
        # datas_new_field = item_new_field.get(get_redirect=True)
    else:
        item_new_field = wd_item
        # datas_new_field = item.get(get_redirect=True)
    # isni = P213
    # orcid = P496
    # properties = {'0247a-isni': 'P213', '0247a-orcid': 'P496'}
    properties = {'0247a-isni': 'P213', '0247a-orcid': 'P496'}
    for column, property_for_new_field in properties.items():
        try:
            # claims_in_new_item = datas_new_field['claims'].get(property_for_new_field, [])
            claims = []
            if column == '0247a-isni':
                claims = wd_data['isni']
            if column == '0247a-orcid':
                claims = wd_data['orcid']

            if column == '0247a-isni':
                row_new_fields[column] = prepare_isni_from_nkcr(row_new_fields[column])

            if column == '0247a-orcid':
                row_new_fields[column] = prepare_orcid_from_nkcr(row_new_fields[column])

            # if item_new_field.isRedirectPage():
            #     #redirect
            #     claims = item_new_field.get(get_redirect=True)

            if row_new_fields[column] not in claims and row_new_fields[column] != '':
                # insert
                if (item_new_field.isRedirectPage()):
                    item_new_field = item_new_field.getRedirectTarget()
                    # item_new_field.get(get_redirect=True)
                datas_from_wd = item_new_field.get(get_redirect=True)
                claim_direct_from_wd = get_claim_from_item_by_property(datas_from_wd, property_for_new_field) ##pro kontrolu
                if row_new_fields[column] not in claim_direct_from_wd:
                    add_new_field_to_item(item_new_field, property_for_new_field, row_new_fields[column],
                                          row_new_fields['_id'])


        except ValueError as ve:
            print(ve)
            pass
        except pywikibot.exceptions.OtherPageSaveError as opse:
            print(opse)
            pass
        except KeyError as ke:
            print(ke)
            pass
            # try:
            #     if column == '0247a-isni':
            #         row_new_fields[column] = prepare_isni_from_nkcr(row_new_fields[column])
            #     if row_new_fields[column] != '':
            #         add_new_field_to_item(item_new_field, property_for_new_field, row_new_fields[column], row_new_fields['_id'])
            # except KeyError as e:
            #     print(e)
            #     pass
            # except pywikibot.exceptions.OtherPageSaveError as e:
            #     print(e)
            #     pass


def get_nkcr_auts_from_item(datas) -> list:
    nkcr_auts_from_data = []
    claims_from_wd = datas['claims'].get('P691', [])
    for claim in claims_from_wd:
        nkcr_auts_from_data.append(claim.getTarget())

    return nkcr_auts_from_data

def get_claim_from_item_by_property(datas, property) -> list:
    claims_from_data = []
    claims_by_property = datas['claims'].get(property, [])
    for claim in claims_by_property:
        claims_from_data.append(claim.getTarget())

    return claims_from_data


def make_qid_database(items: dict) -> dict[str, list[str]]:
    return_qids: dict[str, list[str]] = {}
    for nkcr, nkcr_line in items.items():
        if return_qids.get(nkcr_line['qid']):
            return_qids[nkcr_line['qid']].append(nkcr)
        else:
            return_qids[nkcr_line['qid']] = [nkcr]

    return return_qids

if __name__ == '__main__':
    print_info()

    repo = MyDataSite('wikidata', 'wikidata', user=user_name)

    non_deprecated_items = get_all_non_deprecated_items()

    qid_to_nkcr = make_qid_database(non_deprecated_items)

    chunks = load_nkcr_items()

    head = {'item': 'item', 'prop': 'property', 'value': 'value'}
    write_log(head, True)

    for chunk in chunks:
        chunk.fillna('', inplace=True)
        chunk = chunk[chunk['100a'] != '']
        for row in chunk.to_dict('records'):
            nkcr_aut = row['_id']
            print(nkcr_aut)
            try:
                qid = row['0247a-wikidata']
                if qid != '':  # raději bych none, ale to tady nejde ... pandas, no
                    name = row['100a']

                    qid = clean_qid(qid)
                    item = pywikibot.ItemPage(repo, qid)

                    try:
                        nkcr_auts = qid_to_nkcr.get(qid, [])
                        if nkcr_aut not in nkcr_auts:
                            datas = item.get(get_redirect=True)
                            nkcr_auts_from_wd = get_nkcr_auts_from_item(datas)
                            if nkcr_aut not in nkcr_auts_from_wd:
                                try:
                                    if (item.isRedirectPage()):
                                        item = item.getRedirectTarget()
                                        item.get(get_redirect=True)
                                    add_nkcr_aut_to_item(item, nkcr_aut, name)
                                    non_deprecated_items[nkcr_aut] = {
                                        'qid': qid,
                                        'isni': [],
                                        'orcid': []
                                    }
                                except pywikibot.exceptions.OtherPageSaveError as e:
                                    print(e)
                                except ValueError as e:
                                    print(e)
                    except KeyError as e:
                        print('key err')
                ms = time.time()
                # print(ms)
                if nkcr_aut in non_deprecated_items:

                    exist_qid = non_deprecated_items[nkcr_aut]['qid']
                    if exist_qid != '':
                        exist_qid = clean_qid(exist_qid)
                        process_new_fields(exist_qid, non_deprecated_items[nkcr_aut], row)
                    if qid != '' and exist_qid != qid:
                        if (item.isRedirectPage()):
                            item = item.getRedirectTarget()
                            item.get(get_redirect=True)
                        process_new_fields(None, non_deprecated_items[nkcr_aut], row, item)
            except BadItemException as e:
                print(e)
            except requests.exceptions.ConnectionError as e:
                print(e)


