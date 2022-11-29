import argparse
from typing import Union
import pywikibot.data.sparql
import requests

import time

from cleaners import clean_last_comma, clean_qid, \
    prepare_column_of_content, resolve_exist_claims
from nkcr_exceptions import BadItemException
from pywikibot_extension import MyDataSite
from tools import write_log, print_info, add_new_field_to_item, get_nkcr_auts_from_item, make_qid_database, \
    get_all_non_deprecated_items, load_nkcr_items, get_claim_from_item_by_property, add_nkcr_aut_to_item

user_name = 'Frettiebot'
debug = False
count_first_step = 0
count_second_step = 0

parser = argparse.ArgumentParser(description='NKČR catmandu pipeline.')
parser.add_argument('-i', '--input', help='NKČR CSV file name', required=True)
args = parser.parse_args()

print("Input file: %s" % args.input)
file_name = args.input

# isni = P213
# orcid = P496
properties = {'0247a-isni': 'P213', '0247a-orcid': 'P496'}

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

            if row_new_fields[column] not in claims and row_new_fields[column] != '':
                # insert
                if (item_new_field.isRedirectPage()):
                    item_new_field = item_new_field.getRedirectTarget()

                datas_from_wd = item_new_field.get(get_redirect=True)
                claim_direct_from_wd = get_claim_from_item_by_property(datas_from_wd,
                                                                       property_for_new_field)  # pro kontrolu
                if row_new_fields[column] not in claim_direct_from_wd:
                    add_new_field_to_item(debug, repo, item_new_field, property_for_new_field, row_new_fields[column],
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

if __name__ == '__main__':
    print_info(debug)

    repo = MyDataSite('wikidata', 'wikidata', user=user_name)

    non_deprecated_items = get_all_non_deprecated_items()

    qid_to_nkcr = make_qid_database(non_deprecated_items)

    chunks = load_nkcr_items(file_name)

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
                                    add_nkcr_aut_to_item(debug, repo, item, nkcr_aut, name)
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
            except pywikibot.exceptions.NoPageError as e:
                print(e)
            except requests.exceptions.ConnectionError as e:
                print(e)
