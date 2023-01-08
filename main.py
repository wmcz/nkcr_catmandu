import argparse
import datetime
from typing import Union
import pywikibot.data.sparql
import requests

import time

import cleaners
from cleaners import clean_last_comma, clean_qid, \
    prepare_column_of_content, resolve_exist_claims
# from logger import Logger
from nkcr_exceptions import BadItemException
from pywikibot_extension import MyDataSite
from tools import write_log, print_info, add_new_field_to_item, get_nkcr_auts_from_item, make_qid_database, \
    get_all_non_deprecated_items, load_nkcr_items, get_claim_from_item_by_property, add_nkcr_aut_to_item, \
    get_all_non_deprecated_items_occupation, get_occupations, is_item_subclass_of, log_with_date_time, \
    get_all_non_deprecated_items_field_of_work

user_name = 'Frettiebot'
debug = False
count_first_step = 0
count_second_step = 0
limit = 50000

occupations_not_used_in_occupation_because_is_in_function = ['Q103163', 'Q29182', 'Q611644', 'Q102039658', 'Q212071', 'Q22132694', 'Q63970319', 'Q11165895', 'Q83460']

parser = argparse.ArgumentParser(description='NKČR catmandu pipeline.')
parser.add_argument('-i', '--input', help='NKČR CSV file name', required=True)
args = parser.parse_args()

print("Input file: %s" % args.input)
file_name = args.input

# isni = P213
# orcid = P496
properties = {'0247a-isni': 'P213', '0247a-orcid': 'P496', '374a' : 'P106', '372a' : 'P101'}

def process_new_fields(qid_new_fields: Union[str, None], wd_data: dict, row_new_fields: dict,
                       wd_item: Union[pywikibot.ItemPage, None] = None):
    # print('process')
    if wd_item is None:
        item_new_field = pywikibot.ItemPage(repo, qid_new_fields)
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
            if (type(row_new_fields[column]) == str and row_new_fields[column] not in claims and len(row_new_fields[column]) > 0) or (type(row_new_fields[column] == list) and len(array_diff) > 0):
                # insert
                if (item_new_field.isRedirectPage()):
                    item_new_field = item_new_field.getRedirectTarget()

                datas_from_wd = item_new_field.get(get_redirect=True)
                claim_direct_from_wd = get_claim_from_item_by_property(datas_from_wd,
                                                                       property_for_new_field) # pro kontrolu

                if type(row_new_fields[column]) is list:
                    if column == '374a':
                        qid_claims_direct_from_wd = []
                        class_occupation = pywikibot.ItemPage(repo, 'Q12737077')
                        for cdfwd in claim_direct_from_wd:
                            if type(cdfwd) is pywikibot.ItemPage:
                                qid_claims_direct_from_wd.append(cdfwd.getID())
                        for item_in_list in row_new_fields[column]:
                            item_occupation = pywikibot.ItemPage(repo, item_in_list)

                            if item_occupation.getID() not in qid_claims_direct_from_wd and item_occupation.getID() not in occupations_not_used_in_occupation_because_is_in_function:
                                ocupp_qid = item_occupation.getID()
                                if is_item_subclass_of(item_occupation, class_occupation):
                                    if row_new_fields[column] not in claim_direct_from_wd:
                                        add_new_field_to_item(debug, repo, item_new_field, property_for_new_field,
                                                              item_occupation,
                                                              row_new_fields['_id'])
                    elif (column == '372a'):
                        qid_claims_direct_from_wd = []
                        for cdfwd in claim_direct_from_wd:
                            if type(cdfwd) is pywikibot.ItemPage:
                                qid_claims_direct_from_wd.append(cdfwd.getID())
                        for item_in_list in row_new_fields[column]:
                            item_occupation = pywikibot.ItemPage(repo, item_in_list)

                            if item_occupation.getID() not in qid_claims_direct_from_wd:
                                ocupp_qid = item_occupation.getID()
                                occupations_direct_from_wd = get_claim_from_item_by_property(datas_from_wd,
                                                                                       'P106')  # pro kontrolu
                                qid_occupations_claims_direct_from_wd = []
                                for odfwd in occupations_direct_from_wd:
                                    if type(odfwd) is pywikibot.ItemPage:
                                        qid_occupations_claims_direct_from_wd.append(odfwd.getID())
                                if item_occupation.getID() not in qid_occupations_claims_direct_from_wd:
                                    if row_new_fields[column] not in claim_direct_from_wd:
                                        add_new_field_to_item(debug, repo, item_new_field, property_for_new_field,
                                                              item_occupation,
                                                              row_new_fields['_id'])
                else:
                    if row_new_fields[column] not in claim_direct_from_wd:
                        add_new_field_to_item(debug, repo, item_new_field, property_for_new_field,
                                              row_new_fields[column],
                                              row_new_fields['_id'])


        except ValueError as ve:
            log_with_date_time(str(ve))
            pass
        except pywikibot.exceptions.OtherPageSaveError as opse:
            log_with_date_time(str(opse))
            pass
        except KeyError as ke:
            # log_with_date_time(str(ke))
            pass

if __name__ == '__main__':
    print_info(debug)

    repo = MyDataSite('wikidata', 'wikidata', user=user_name)

    log_with_date_time('run')
    i = 0
    run = True
    fin = {}
    while run:
        lim = limit

        offset = i * limit
        if (i % 3 == 0):
            print(offset)
        non_deprecated_items_occupation = get_all_non_deprecated_items_occupation(lim, offset)
        if (len(fin) == 0):
            fin = non_deprecated_items_occupation
        else:
            fin.update(non_deprecated_items_occupation)
        if (len(non_deprecated_items_occupation) == 0):
            run = False
        i = i + 1

    non_deprecated_items_occupation = fin
    log_with_date_time('non deprecated items occupation read')

    i = 0
    run = True
    fin_field = {}
    while run:
        lim = limit

        offset = i * limit
        if (i % 3 == 0):
            print(offset)
        non_deprecated_items_field_of_work = get_all_non_deprecated_items_field_of_work(lim, offset)
        if (len(fin_field) == 0):
            fin_field = non_deprecated_items_field_of_work
        else:
            fin_field.update(non_deprecated_items_field_of_work)
        if (len(non_deprecated_items_field_of_work) == 0):
            run = False
        i = i + 1

    non_deprecated_items_field_of_work = fin_field
    log_with_date_time('non deprecated items field of work read')

    non_deprecated_items = get_all_non_deprecated_items()
    log_with_date_time('non deprecated items read')
    # non_deprecated_items = {}

    name_to_nkcr = get_occupations()
    log_with_date_time('occupations read')

    qid_to_nkcr = make_qid_database(non_deprecated_items)
    log_with_date_time('qid_to_nkcr read')
    cleaners.name_to_nkcr = name_to_nkcr
    chunks = load_nkcr_items(file_name)
    log_with_date_time('nkcr csv read')

    # logger = Logger('occupation' + 'Fill', 'saved')

    head = {'item': 'item', 'prop': 'property', 'value': 'value'}
    write_log(head, True)

    count = 0

    for chunk in chunks:
        chunk.fillna('', inplace=True)
        chunk = chunk[chunk['100a'] != '']
        for row in chunk.to_dict('records'):
            nkcr_aut = row['_id']

            # if logger.isCompleteFile(nkcr_aut):
            #     continue

            count = count + 1

            if (count % 10000 == 0):
                log_with_date_time('line: ' + str(count))

            if (count % 1000) == 0:
                log_with_date_time('line ' + str(count) + ' - ' + nkcr_aut)

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
                                    add_nkcr_aut_to_item(debug, repo, item, nkcr_aut, name)
                                    non_deprecated_items[nkcr_aut] = {
                                        'qid': qid,
                                        'isni': [],
                                        'orcid': []
                                    }
                                except pywikibot.exceptions.OtherPageSaveError as e:
                                    log_with_date_time(str(e))
                                except ValueError as e:
                                    log_with_date_time(str(e))
                    except KeyError as e:
                        log_with_date_time('key err')
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
                if nkcr_aut in non_deprecated_items_occupation:
                    exist_qid = non_deprecated_items_occupation[nkcr_aut]['qid']
                    if exist_qid != '':
                        exist_qid = clean_qid(exist_qid)
                        process_new_fields(exist_qid, non_deprecated_items_occupation[nkcr_aut], row)
                    if qid != '' and exist_qid != qid:
                        if (item.isRedirectPage()):
                            item = item.getRedirectTarget()
                            item.get(get_redirect=True)
                        process_new_fields(None, non_deprecated_items_occupation[nkcr_aut], row, item)
                if nkcr_aut in non_deprecated_items_field_of_work:
                    exist_qid = non_deprecated_items_field_of_work[nkcr_aut]['qid']
                    if exist_qid != '':
                        exist_qid = clean_qid(exist_qid)
                        process_new_fields(exist_qid, non_deprecated_items_field_of_work[nkcr_aut], row)
                    if qid != '' and exist_qid != qid:
                        if (item.isRedirectPage()):
                            item = item.getRedirectTarget()
                            item.get(get_redirect=True)
                        process_new_fields(None, non_deprecated_items_field_of_work[nkcr_aut], row, item)
            except BadItemException as e:
                log_with_date_time(str(e))
            except pywikibot.exceptions.NoPageError as e:
                log_with_date_time(str(e))
            except requests.exceptions.ConnectionError as e:
                log_with_date_time(str(e))
            except pywikibot.exceptions.APIError as e:
                log_with_date_time(str(e))

            # logger.logComplete(nkcr_aut)
