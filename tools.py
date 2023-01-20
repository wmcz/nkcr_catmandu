import csv
import gc
from typing import Union, Any

import pandas
import pandas as pd
import pywikibot
from datetime import datetime

import rapidjson

import mySparql

import simplejson.errors
from pywikibot.data import sparql

import pywikibot_extension
from cleaners import clean_last_comma, clean_qid, resolve_exist_claims, prepare_column_of_content
from config import properties, occupations_not_used_in_occupation_because_is_in_function, \
    fields_of_work_not_used_in_field_of_work_because_is_not_ok


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
        ?s ps:P691 ?value filter(strstarts(str(?value),"ph") || strstarts(str(?value),"fd") ) .
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
    except simplejson.errors.JSONDecodeError as e:
        return occupation_dictionary
    except rapidjson.JSONDecodeError as e:
        return occupation_dictionary

    for item_occupation in data_occupation:
        if item_occupation['string'] is not None:
            name = item_occupation['string'].value
        else:
            name = None

        if item_occupation['item'].getID() is not None:
            occupation_dictionary[name] = item_occupation['item'].getID()

    return occupation_dictionary

def get_all_non_deprecated_items(limit:Union[int,None] = None, offset:Union[int,None] = None) -> dict[dict[str, list, list]]:
    non_deprecated_dictionary: dict[dict[str, list, list]] = {}

    query = """
    select ?item ?nkcr ?isni ?orcid where {
        ?item p:P691 [ps:P691 ?nkcr ; wikibase:rank ?rank ] filter(?rank != wikibase:DeprecatedRank) .
        OPTIONAL{?item wdt:P213 ?isni}.
        OPTIONAL{?item wdt:P496 ?orcid}.
        # VALUES ?nkcr {'mzk20221172051' 'xx0279013' 'jo20231173439'}
    } LIMIT """ + str(limit) + """ OFFSET """ + str(offset) + """
    """

    # query_object = sparql.SparqlQuery()
    query_object = mySparql.MySparqlQuery()
    try:
        data_non_deprecated = query_object.select(query=query, full_data=True)
    except simplejson.errors.JSONDecodeError as e:
        return non_deprecated_dictionary
    except rapidjson.JSONDecodeError as e:
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
    del (data_non_deprecated)
    return non_deprecated_dictionary

def get_all_non_deprecated_items_occupation(limit:Union[int,None] = None, offset:Union[int,None] = None) -> dict[dict[str, list, list]]:
    non_deprecated_dictionary: dict[dict[str, list, list]] = {}

    query = """
    select ?item ?nkcr ?occup where {
        ?item p:P691 [ps:P691 ?nkcr ; wikibase:rank ?rank ] filter(?rank != wikibase:DeprecatedRank) .
        OPTIONAL{?item wdt:P106 ?occup}.
        # VALUES ?nkcr {'mzk20221172051' 'xx0279013' 'jo20231173439'} 
        
    } LIMIT """ + str(limit) + """ OFFSET """ + str(offset) + """
    """
    # if (limit is not None):
    #     query = query + ' LIMIT ' + str(limit)

    # query_object = mySparql.MySparqlQuery()
    query_object = mySparql.MySparqlQuery()

    try:
        data_non_deprecated = query_object.select(query=query, full_data=True)
    except simplejson.errors.JSONDecodeError as e:
        return non_deprecated_dictionary
    except rapidjson.JSONDecodeError as e:
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
    del(data_non_deprecated)
    del(query_object)
    return non_deprecated_dictionary


def get_all_non_deprecated_items_field_of_work(limit: Union[int, None] = None, offset: Union[int, None] = None) -> dict[
    dict[str, list, list]]:
    non_deprecated_dictionary: dict[dict[str, list, list]] = {}

    query = """
    select ?item ?nkcr ?field where {
        ?item p:P691 [ps:P691 ?nkcr ; wikibase:rank ?rank ] filter(?rank != wikibase:DeprecatedRank) .
        OPTIONAL{?item wdt:P101 ?field}.
        # VALUES ?nkcr {'mzk20221172051' 'xx0279013' 'jo20231173439'} 

    } LIMIT """ + str(limit) + """ OFFSET """ + str(offset) + """
    """
    # if (limit is not None):
    #     query = query + ' LIMIT ' + str(limit)

    # query_object = sparql.SparqlQuery()
    query_object = mySparql.MySparqlQuery()
    try:
        data_non_deprecated = query_object.select(query=query, full_data=True)
    except simplejson.errors.JSONDecodeError as e:
        return non_deprecated_dictionary
    except rapidjson.JSONDecodeError as e:
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

        if non_deprecated_dictionary.get(item_non_deprecated['nkcr'].value, None):
            if field_of_work is not None:
                non_deprecated_dictionary[item_non_deprecated['nkcr'].value]['field'].append(field_of_work)
        else:
            if field_of_work is not None:
                field_of_work_add = [field_of_work]
            else:
                field_of_work_add = []

            non_deprecated_dictionary[item_non_deprecated['nkcr'].value] = {
                'qid': item_non_deprecated['item'].getID(),
                'field': field_of_work_add,
            }
    del (data_non_deprecated)
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

    # query_object = sparql.SparqlQuery()
    query_object = mySparql.MySparqlQuery()
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

def load_sparql_query_by_chunks(limit, get_method):
    i = 0
    run = True
    final_data = {}
    while run:
        lim = limit

        offset = i * limit
        if (i % 3 == 0):
            log_with_date_time(get_method.__name__ + ": " + str(offset))
        data = get_method(lim, offset)
        gc.collect()
        if (len(final_data) == 0):
            final_data = data
        else:
            final_data.update(data)
        if (len(data) == 0):
            run = False
        i = i + 1

    data = final_data
    return data


class Processor():

    debug = False

    def set_repo(self, repo):
        self.repo = repo

    def set_debug(self, debug):
        self.debug = debug
    def process_new_fields(self, qid_new_fields: Union[str, None], wd_data: dict, row_new_fields: dict,
                           wd_item: Union[pywikibot.ItemPage, None] = None):
        # print('process')
        if wd_item is None:
            item_new_field = pywikibot.ItemPage(self.repo, qid_new_fields)
        else:
            item_new_field = wd_item

        for column, property_for_new_field in properties.items():
            try:
                # claims_in_new_item = datas_new_field['claims'].get(property_for_new_field, [])
                claims = resolve_exist_claims(column, wd_data)

                row_new_fields[column] = prepare_column_of_content(column, row_new_fields)
                array_diff = []
                if (type(row_new_fields[column]) is list):
                    array_diff = set(row_new_fields[column]) - set(claims)
                if (type(row_new_fields[column]) == str and row_new_fields[column] not in claims and len(
                        row_new_fields[column]) > 0) or (type(row_new_fields[column] == list) and len(array_diff) > 0):
                    # insert
                    if (item_new_field.isRedirectPage()):
                        item_new_field = item_new_field.getRedirectTarget()

                    datas_from_wd = item_new_field.get(get_redirect=True)
                    claim_direct_from_wd = get_claim_from_item_by_property(datas_from_wd,
                                                                           property_for_new_field)  # pro kontrolu

                    if type(row_new_fields[column]) is list:
                        if column == '374a':
                            propertyProcessor = PropertyProcessor374a()
                            propertyProcessor.set_repo(self.repo)
                            propertyProcessor.set_debug(self.debug)
                            propertyProcessor.set_claim_direct_from_wd(claim_direct_from_wd)
                            propertyProcessor.set_property_for_new_field(property_for_new_field)
                            propertyProcessor.set_column(column)
                            propertyProcessor.set_item_new_field(item_new_field)
                            propertyProcessor.set_row_new_fields(row_new_fields)
                            propertyProcessor.process()
                        elif (column == '372a'):
                            propertyProcessor = PropertyProcessor372a()
                            propertyProcessor.set_repo(self.repo)
                            propertyProcessor.set_debug(self.debug)
                            propertyProcessor.set_claim_direct_from_wd(claim_direct_from_wd)
                            propertyProcessor.set_property_for_new_field(property_for_new_field)
                            propertyProcessor.set_column(column)
                            propertyProcessor.set_item_new_field(item_new_field)
                            propertyProcessor.set_row_new_fields(row_new_fields)

                            propertyProcessor.set_datas_from_wd(datas_from_wd)

                            propertyProcessor.process()
                    else:
                        propertyProcessor = PropertyProcessorOne()
                        propertyProcessor.set_repo(self.repo)
                        propertyProcessor.set_debug(self.debug)
                        propertyProcessor.set_claim_direct_from_wd(claim_direct_from_wd)
                        propertyProcessor.set_property_for_new_field(property_for_new_field)
                        propertyProcessor.set_column(column)
                        propertyProcessor.set_item_new_field(item_new_field)
                        propertyProcessor.set_row_new_fields(row_new_fields)
                        propertyProcessor.process()


            except ValueError as ve:
                log_with_date_time(str(ve))
                pass
            except pywikibot.exceptions.OtherPageSaveError as opse:
                log_with_date_time(str(opse))
                pass
            except KeyError as ke:
                # log_with_date_time(str(ke))
                pass

    def set_nkcr_aut(self, nkcr_aut):
        self.nkcr_aut = nkcr_aut

    def set_qid(self, qid):
        self.qid = qid

    def set_item(self, item):
        self.item = item

    def set_row(self, row):
        self.row = row
    def process_occupation_type(self, non_deprecated_items):

        nkcr_aut = self.nkcr_aut
        qid = self.qid
        item = self.item
        row = self.row

        if nkcr_aut in non_deprecated_items:
            exist_qid = non_deprecated_items[nkcr_aut]['qid']
            if exist_qid != '':
                exist_qid = clean_qid(exist_qid)
                self.process_new_fields(exist_qid, non_deprecated_items[nkcr_aut], row)
            if qid != '' and exist_qid != qid:
                if (item.isRedirectPage()):
                    item = item.getRedirectTarget()
                    item.get(get_redirect=True)
                self.process_new_fields(None, non_deprecated_items[nkcr_aut], row, item)


class BasePropertyProcessor():
    def set_claim_direct_from_wd(self, claim_direct_from_wd):
        self.claim_direct_from_wd = claim_direct_from_wd

    def set_repo(self, repo):
        self.repo = repo

    def set_row_new_fields(self, row_new_fields):
        self.row_new_fields = row_new_fields

    def set_column(self, column):
        self.column = column

    def set_debug(self, debug):
        self.debug = debug

    def set_item_new_field(self, item_new_field):
        self.item_new_field = item_new_field

    def set_property_for_new_field(self, property_for_new_field):
        self.property_for_new_field = property_for_new_field
class PropertyProcessor374a(BasePropertyProcessor):
    def process(self):
        qid_claims_direct_from_wd = []
        class_occupation = pywikibot.ItemPage(self.repo, 'Q12737077')
        claim_direct_from_wd = self.claim_direct_from_wd
        for cdfwd in claim_direct_from_wd:
            if type(cdfwd) is pywikibot.ItemPage:
                qid_claims_direct_from_wd.append(cdfwd.getID())
        for item_in_list in self.row_new_fields[self.column]:
            item_occupation = pywikibot.ItemPage(self.repo, item_in_list)

            if item_occupation.getID() not in qid_claims_direct_from_wd and item_occupation.getID() not in occupations_not_used_in_occupation_because_is_in_function:
                ocupp_qid = item_occupation.getID()
                if is_item_subclass_of(item_occupation, class_occupation):
                    if self.row_new_fields[self.column] not in claim_direct_from_wd:
                        add_new_field_to_item(self.debug, self.repo, self.item_new_field, self.property_for_new_field,
                                              item_occupation,
                                              self.row_new_fields['_id'])

class PropertyProcessor372a(BasePropertyProcessor):

    def set_datas_from_wd(self, datas_from_wd):
        self.datas_from_wd = datas_from_wd

    def process(self):
        qid_claims_direct_from_wd = []
        for cdfwd in self.claim_direct_from_wd:
            if type(cdfwd) is pywikibot.ItemPage:
                qid_claims_direct_from_wd.append(cdfwd.getID())
        for item_in_list in self.row_new_fields[self.column]:
            item_occupation = pywikibot.ItemPage(self.repo, item_in_list)

            if item_occupation.getID() not in qid_claims_direct_from_wd and item_occupation.getID() not in fields_of_work_not_used_in_field_of_work_because_is_not_ok:
                ocupp_qid = item_occupation.getID()
                occupations_direct_from_wd = get_claim_from_item_by_property(self.datas_from_wd,
                                                                             'P106')  # pro kontrolu
                qid_occupations_claims_direct_from_wd = []
                for odfwd in occupations_direct_from_wd:
                    if type(odfwd) is pywikibot.ItemPage:
                        qid_occupations_claims_direct_from_wd.append(odfwd.getID())
                if item_occupation.getID() not in qid_occupations_claims_direct_from_wd:
                    if self.row_new_fields[self.column] not in self.claim_direct_from_wd:
                        add_new_field_to_item(self.debug, self.repo, self.item_new_field,
                                              self.property_for_new_field,
                                              item_occupation,
                                              self.row_new_fields['_id'])

class PropertyProcessorOne(BasePropertyProcessor):
    def process(self):
        if self.row_new_fields[self.column] not in self.claim_direct_from_wd:
            add_new_field_to_item(self.debug, self.repo, self.item_new_field, self.property_for_new_field,
                                  self.row_new_fields[self.column],
                                  self.row_new_fields['_id'])
