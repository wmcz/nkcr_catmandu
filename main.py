import argparse
import logging
import time
import timeit
from logging.handlers import TimedRotatingFileHandler, RotatingFileHandler

from wikibaseintegrator.wbi_enums import WikibaseRank
from wikibaseintegrator.wbi_exceptions import MissingEntityException, MWApiError, ModificationFailed, SaveFailed, \
    NonExistentEntityError, MaxRetriesReachedException

import cleaners
import config
import tools
from cleaners import clean_qid
from nkcr_exceptions import BadItemException
from processor import Processor
from sources import Loader
from tools import *

from wikibaseintegrator.wbi_config import config as wbi_config
from wikibaseintegrator import WikibaseIntegrator, wbi_login

log = logging.getLogger(__name__)

parser = argparse.ArgumentParser(description='NKČR catmandu pipeline.')
parser.add_argument('-i', '--input', help='NKČR CSV file name', required=True)
args = parser.parse_args()

file_name = args.input

# logging.basicConfig(level=logging.INFO,
#                     format='%(levelname)s:%(module)s:%(asctime)s:%(message)s',
#                     filename='catmandu.log',
#                     filemode='a')
logging.basicConfig(
        handlers=[RotatingFileHandler('catmandu.log',
                               mode='a',
                               maxBytes=1024*1024*10,
                               backupCount=5)],
        level=logging.INFO,
        format='%(levelname)s:%(module)s:%(asctime)s:%(message)s',
        )

formatter = logging.Formatter('%(levelname)s:%(module)s:%(asctime)s:%(message)s')
console = logging.StreamHandler()
console.setLevel(logging.INFO)

console.setFormatter(formatter)
logging.getLogger().addHandler(console)

log.info("Input file: %s" % args.input)

print_info(config.Config.debug)
gc.enable()

bot_password = get_bot_password('bot_password')

wbi_config['USER_AGENT'] = 'Frettiebot/1.0 (https://www.wikidata.org/wiki/User:Frettiebot)'
wbi_config['SPARQL_ENDPOINT_URL'] = 'https://query-main.wikidata.org/sparql'
login_instance = wbi_login.Login(user='Frettiebot', password=bot_password)
wbi = WikibaseIntegrator(login=login_instance, is_bot=True)

if __name__ == '__main__':

    processor = Processor()

    loader = Loader()
    loader.set_file_name(file_name)
    loader.load()

    head = {'item': 'item', 'prop': 'property', 'value': 'value'}
    write_log(head, True)

    count = 0
    inserts = 0

    for chunk in loader.chunks:
        chunk.fillna('', inplace=True)
        chunk = chunk[chunk['100a'] != '']
        for row in chunk.to_dict('records'):
            time_start = time.time()
            nkcr_aut = row['_id']
            save = True
            count = count + 1

            if count % 10000 == 0:
                log_with_date_time('line: ' + str(count))

            if (count % 1000) == 0:
                log_with_date_time('line: ' + str(count) + ' - ' + nkcr_aut)

            item = None

            try:
                qid = row['0247a-wikidata']
                if qid != '':
                    name = row['100a']

                    qid = clean_qid(qid)

                    try:
                        nkcr_auts = loader.qid_to_nkcr.get(qid, [])
                        if nkcr_aut not in nkcr_auts:
                            item = wbi.item.get(qid)
                            time_load_item = time.time()
                            # log_with_date_time('time_from_start_to_load_item:' + str(time_load_item-time_start))
                            datas = item
                            instances_from_item = get_claim_from_item_by_property_wbi(datas, 'P31')
                            for instance_from_item in instances_from_item:
                                # if instance_from_item.getID() in Config.instances_not_possible_for_nkcr: #pywikibot
                                if instance_from_item in Config.instances_not_possible_for_nkcr:
                                    save = False
                                    raise ValueError('Nepovolená instance položky: ' + instance_from_item)

                                if qid in Config.qid_blacklist:
                                    save = False
                                    lab = item.labels.get('cs')
                                    if lab is None:
                                        lab = item.labels.get('en')
                                        if lab is None:
                                            lab = "label not in cz and en"
                                    raise ValueError('Blacklistovaná položka: ' + qid + ' – ' + lab.value)
                            # nkcr_auts_from_wd = get_nkcr_auts_from_item(datas)
                            nkcr_auts_from_wd = get_nkcr_auts_from_item_wbi(datas)
                            if nkcr_aut not in nkcr_auts_from_wd:
                                try:
                                    # if item.isRedirectPage():
                                    #     item = item.getRedirectTarget()
                                    #     item.get(get_redirect=True)
                                    if save:
                                        # add_nkcr_aut_to_item(Config.debug, repo, item, nkcr_aut, name)
                                        item = add_nkcr_aut_to_item_wbi(item, nkcr_aut, name)

                                    loader.non_deprecated_items[nkcr_aut] = {
                                        'qid': qid,
                                        'isni': [],
                                        'orcid': []
                                    }
                                    loader.non_deprecated_items_field_of_work_and_occupation[nkcr_aut] = {
                                        'qid': qid,
                                        'field': [],
                                        'occup': []
                                    }
                                    loader.non_deprecated_items_places[nkcr_aut] = {
                                        'qid': qid,
                                        'birth': [],
                                        'death': [],
                                        'work': [],
                                    }
                                    loader.non_deprecated_items_languages[nkcr_aut] = {
                                        'qid': qid,
                                        'language': [],
                                    }
                                except ValueError as e:
                                    log.error(str(e))
                    except KeyError as e:
                        log.warning('key err:' + str(e))
                    except ValueError as e:
                        log.error(str(e))
                ms = time.time()
                # print(ms)
                if save:
                    time_process = time.time()
                    # log_with_date_time('time_from_start_to_process_start:' + str(time_process - time_start))
                    processor.set_nkcr_aut(nkcr_aut)
                    processor.set_qid(qid)
                    processor.set_wbi(wbi)
                    processor.reset_instances_from_item(None)
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

                    properties = {
                        '046f': 'P569',
                        '046g': 'P570',
                        '678a': ['P569', 'P570'],
                    }
                    processor.set_enabled_columns(properties)
                    processor.process_occupation_type(loader.non_deprecated_items)
                    time_process_after = time.time()
                    if (time_process_after - time_start > 1):
                        log_with_date_time('time_from_start_to_process_after:' + str(time_process_after - time_start))
                        log_with_date_time('long AUT:' + nkcr_aut)
                    # if processor.item is None:
                    #     log_with_date_time('non item AUT:' + nkcr_aut)
                    if processor.item is not None and Config.debug is not True:
                        change_text_array = []
                        changed = False

                        if (processor.get_item().labels.get('cs') is None) and len(row['100a']) > 0:
                            whole_name = tools.first_name(row['100a']) + ' ' + tools.last_name(row['100a'])
                            processor.get_item().labels.set('cs', whole_name, ActionIfExists.REPLACE_ALL)
                            log_with_date_time('New CS label for ' + nkcr_aut + ' is ' + whole_name)
                            changed = True
                            change_text_array.append('cs label')

                        nkcrs_wd = processor.get_item().claims.get('P691')
                        for nkcr in nkcrs_wd:
                            if nkcr.rank == WikibaseRank.DEPRECATED and nkcr.mainsnak.datavalue.get('value') == nkcr_aut:
                                changed = False

                        if changed is not True:
                            for prop in Config.properties.values():
                                try:
                                    values = processor.get_item().claims.get(prop)
                                    for value in values:
                                        id = value.id
                                        if (id is None):
                                            change_text_array.append(prop)
                                            changed = True
                                except KeyError as e:
                                    pass

                        time_after_save = time.time()
                        # log_with_date_time('time_from_start_to_save_item:' + str(time_after_save - time_start))
                        if changed:
                            inserts = inserts + 1
                            # if inserts % 10 == 0:
                            #     log_with_date_time('inserted: ' + str(inserts))
                            #TADY kontrolovat časové
                            processor.item.write(
                                summary="Update NK ČR – " + ', '.join(change_text_array),
                                is_bot=True,
                                retry_after=10,
                                tags=['Czech-Authorities-Sync'])
            except BadItemException as e:
                log.error(str(e))
            except MissingEntityException as e:
                log.error(str(e))
            except requests.exceptions.ConnectionError as e:
                log.error(str(e))
            except SaveFailed as e:
                log.error(str(e))
            except NonExistentEntityError as e:
                log.error(str(e))
            except ModificationFailed as e:
                log.error(str(e))
            except MaxRetriesReachedException as e:
                log.error(str(e))
            except MWApiError as e:
                log.error(str(e))



print(cleaners.cachedData)



            # logger.logComplete(nkcr_aut)
