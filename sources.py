import cleaners
from tools import *


class Loader():

    limit: int = 50000

    def __init__(self):
        self.chunks: Union[pandas.DataFrame, None] = None
        self.qid_to_nkcr: dict[str, list[str]] = {}
        self.name_to_nkcr: dict[dict[str, list, list]] = {}
        self.non_deprecated_items_occupation: dict = {}
        self.non_deprecated_items: dict = {}
        self.non_deprecated_items_field_of_work: dict = {}
        self.file_name: str = ''

    def set_limit(self, limit: int):
        self.limit = limit

    def set_file_name(self, file_name: str):
        self.file_name = file_name

    def load(self):
        log_with_date_time('run')
        self.non_deprecated_items_occupation = load_sparql_query_by_chunks(self.limit, get_all_non_deprecated_items_occupation)
        log_with_date_time('non deprecated items occupation read')

        self.non_deprecated_items_field_of_work = load_sparql_query_by_chunks(self.limit,
                                                                         get_all_non_deprecated_items_field_of_work)
        log_with_date_time('non deprecated items field of work read')

        self.non_deprecated_items = load_sparql_query_by_chunks(self.limit, get_all_non_deprecated_items)
        log_with_date_time('non deprecated items read')

        self.name_to_nkcr = get_occupations()
        log_with_date_time('occupations read')

        self.qid_to_nkcr = make_qid_database(self.non_deprecated_items)
        log_with_date_time('qid_to_nkcr read')
        cleaners.name_to_nkcr = self.name_to_nkcr
        self.chunks = load_nkcr_items(self.file_name)
        log_with_date_time('nkcr csv read')