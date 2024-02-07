import argparse
import configparser
import time

import requests

from cleaners import clean_qid
from config import *
# from logger import Logger
from nkcr_exceptions import BadItemException
from processor import Processor
from pywikibot_extension import MyDataSite
from sources import Loader
from tools import *

parser = argparse.ArgumentParser(description='NKČR catmandu pipeline.')
parser.add_argument('-i', '--input', help='NKČR CSV file name', required=True)
args = parser.parse_args()

print("Input file: %s" % args.input)
file_name = args.input


if __name__ == '__main__':
    print_info(Config.debug)
    gc.enable()

    repo = MyDataSite('wikidata', 'wikidata', user=Config.user_name)

    processor = Processor()
    processor.set_repo(repo)
    processor.set_debug(Config.debug)

    loader = Loader()
    loader.set_file_name(file_name)
    loader.load()

    head = {'item': 'item', 'prop': 'property', 'value': 'value'}
    write_log(head, True)

    count = 0

    for chunk in loader.chunks:
        chunk.fillna('', inplace=True)
        chunk = chunk[chunk['100a'] != '']
        for row in chunk.to_dict('records'):
            nkcr_aut = row['_id']
            save = True
            count = count + 1

            if count % 10000 == 0:
                log_with_date_time('line: ' + str(count))

            if (count % 1000) == 0:
                log_with_date_time('line ' + str(count) + ' - ' + nkcr_aut)

            item = None

            try:
                qid = row['0247a-wikidata']
                if qid != '':  # raději bych none, ale to tady nejde ... pandas, no
                    name = row['100a']

                    qid = clean_qid(qid)
                    item = pywikibot.ItemPage(repo, qid)

                    try:
                        nkcr_auts = loader.qid_to_nkcr.get(qid, [])
                        if nkcr_aut not in nkcr_auts:
                            datas = item.get(get_redirect=True)
                            instances_from_item = get_claim_from_item_by_property(datas, 'P31')
                            for instance_from_item in instances_from_item:
                                if instance_from_item.getID() in Config.instances_not_possible_for_nkcr:
                                    save = False
                                    raise ValueError('Nepovolená instance položky: ' + str(instance_from_item.getID()))
                            nkcr_auts_from_wd = get_nkcr_auts_from_item(datas)
                            if nkcr_aut not in nkcr_auts_from_wd:
                                try:
                                    if item.isRedirectPage():
                                        item = item.getRedirectTarget()
                                        item.get(get_redirect=True)
                                    if save:
                                        add_nkcr_aut_to_item(Config.debug, repo, item, nkcr_aut, name)

                                    loader.non_deprecated_items[nkcr_aut] = {
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
                    except ValueError as e:
                        log_with_date_time(str(e))
                ms = time.time()
                # print(ms)
                if save:
                    processor.set_nkcr_aut(nkcr_aut)
                    processor.set_qid(qid)
                    if item is not None:
                        processor.set_item(item)
                    else:
                        processor.set_item(None)
                    processor.set_row(row)

                    properties = {
                        '0247a-isni': 'P213',
                        '0247a-orcid': 'P496',
                    }
                    processor.set_enabled_columns(properties)
                    processor.process_occupation_type(loader.non_deprecated_items)

                    properties = {
                        '374a': 'P106',
                        '372a': 'P101',
                    }
                    processor.set_enabled_columns(properties)
                    processor.process_occupation_type(loader.non_deprecated_items_field_of_work_and_occupation)

                    properties = {
                        '377a': 'P1412',
                    }
                    processor.set_enabled_columns(properties)
                    processor.process_occupation_type(loader.non_deprecated_items_languages)

                    properties = {
                        '370a': 'P19',
                        '370b': 'P20',
                        '370f': 'P937',
                    }
                    processor.set_enabled_columns(properties)
                    processor.process_occupation_type(loader.non_deprecated_items_places)
            except BadItemException as e:
                log_with_date_time(str(e))
            except pywikibot.exceptions.NoPageError as e:
                log_with_date_time(str(e))
            except requests.exceptions.ConnectionError as e:
                log_with_date_time(str(e))
            except pywikibot.exceptions.APIError as e:
                log_with_date_time(str(e))
            except pywikibot.exceptions.InvalidTitleError as e:
                log_with_date_time(str(e))

            # logger.logComplete(nkcr_aut)
