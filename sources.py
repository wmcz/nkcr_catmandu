# import timeit
# from typing import Union

import cleaners
from tools import *
log = logging.getLogger(__name__)


class Loader:

    limit: int = 30000

    def __init__(self):
        self.chunks: Union[pandas.DataFrame, None] = None
        self.qid_to_nkcr: dict[str, list[str]] = {}
        self.name_to_nkcr: dict[dict[str, list, list]] = {}
        self.non_deprecated_items: dict = {}
        self.non_deprecated_items_field_of_work_and_occupation: dict = {}
        self.non_deprecated_items_places: dict = {}
        self.non_deprecated_items_languages: dict = {}
        self.languages_dict: dict[str, str] = {}
        self.file_name: str = ''

    def set_limit(self, limit: int):
        self.limit = limit

    def set_file_name(self, file_name: str):
        self.file_name = file_name

    def load(self):
        log_with_date_time('run')

        self.name_to_nkcr = load_sparql_query_by_chunks(self.limit, get_occupations, 'occupations')
        log_with_date_time('occupations read, size: ' + str(len(self.name_to_nkcr)))

        self.languages_dict = load_language_dict_csv()
        log_with_date_time('loaded language dict from github')

        self.non_deprecated_items_languages = load_sparql_query_by_chunks(self.limit, get_all_non_deprecated_items_languages, 'languages')
        log_with_date_time('non deprecated items languages used read, size: ' + str(len(self.non_deprecated_items_languages)))

        limit_for_work_and_occupation = 100000
        self.non_deprecated_items_field_of_work_and_occupation = load_sparql_query_by_chunks(limit_for_work_and_occupation,
                                                                                             get_all_non_deprecated_items_field_of_work_and_occupation, 'field_of_work_and_occupation')
        log_with_date_time('non deprecated items field of work and occupation read, size: ' + str(len(self.non_deprecated_items_field_of_work_and_occupation)))

        self.non_deprecated_items_places = load_sparql_query_by_chunks(self.limit, get_all_non_deprecated_items_places, 'places')
        log_with_date_time('non deprecated items places read, size: ' + str(len(self.non_deprecated_items_places)))

        self.non_deprecated_items = load_sparql_query_by_chunks(self.limit, get_all_non_deprecated_items, 'non_deprecated_items')
        log_with_date_time('non deprecated items read, size: ' + str(len(self.non_deprecated_items)))

        self.qid_to_nkcr = make_qid_database(self.non_deprecated_items)
        log_with_date_time('qid_to_nkcr read, size: ' + str(len(self.qid_to_nkcr)))
        cleaners.name_to_nkcr = self.name_to_nkcr
        cleaners.language_dict = self.languages_dict
        self.chunks = load_nkcr_items(self.file_name)
        log_with_date_time('nkcr csv read')
