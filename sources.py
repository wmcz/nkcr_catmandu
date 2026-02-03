# import timeit
# from typing import Union

from context import PipelineContext
from tools import *
log = logging.getLogger(__name__)


class Loader:
    """
    Handles loading and managing datasets while maintaining mappings and metadata.

    Provides functionality to load various non-deprecated items, their
    relationships, and associated metadata from external sources. It also allows
    setting configurable attributes like file name and data limits.

    The load() method returns a PipelineContext containing all loaded data.

    :ivar limit: Maximum number of records to process in each dataset.
    :type limit: int
    :ivar file_name: Name of the file associated with the data.
    :type file_name: str
    """
    limit: int = 30000

    def __init__(self):
        """
        Initializes the Loader with default configuration.

        Attributes
        ----------
        file_name : str
            Represents the name of the file associated with the class instance,
            typically used for data storage or retrieval purposes.
        """
        self.file_name: str = ''

    def set_limit(self, limit: int):
        """
        Sets the limit for the object.

        This method assigns a given integer value to the limit attribute of the object.

        :param limit: The new limit value to set.
        :type limit: int
        """
        self.limit = limit

    def set_file_name(self, file_name: str):
        """
        Sets the name of the file to the specified value.

        :param file_name: The name to be assigned to the file.
        :type file_name: str
        """
        self.file_name = file_name

    def load(self) -> PipelineContext:
        """
        Loads various datasets and returns a PipelineContext containing all data.

        The method performs multiple operations such as fetching data from SPARQL queries,
        loading language dictionaries, and processing CSV files. Each dataset is logged with
        timestamps to monitor progress and execution time.

        :return: A PipelineContext containing all loaded data.
        :rtype: PipelineContext
        :raises: Any exceptions that may occur during the loading or processing
                 of data (e.g., network errors, file reading errors).
        """
        log_with_date_time('run')

        context = PipelineContext()

        limit_for_occupation = 100000
        context.name_to_nkcr = load_sparql_query_by_chunks(limit_for_occupation, get_occupations, 'occupations')
        log_with_date_time('occupations read, size: ' + str(len(context.name_to_nkcr)))

        context.language_dict = load_language_dict_csv()
        log_with_date_time('loaded language dict from github')

        context.non_deprecated_items_languages = load_sparql_query_by_chunks(self.limit, get_all_non_deprecated_items_languages, 'languages')
        log_with_date_time('non deprecated items languages used read, size: ' + str(len(context.non_deprecated_items_languages)))

        limit_for_work_and_occupation = 100000
        context.non_deprecated_items_field_of_work_and_occupation = load_sparql_query_by_chunks(limit_for_work_and_occupation,
                                                                                             get_all_non_deprecated_items_field_of_work_and_occupation, 'field_of_work_and_occupation')
        log_with_date_time('non deprecated items field of work and occupation read, size: ' + str(len(context.non_deprecated_items_field_of_work_and_occupation)))

        context.non_deprecated_items_places = load_sparql_query_by_chunks(self.limit, get_all_non_deprecated_items_places, 'places')
        log_with_date_time('non deprecated items places read, size: ' + str(len(context.non_deprecated_items_places)))

        context.non_deprecated_items = load_sparql_query_by_chunks(self.limit, get_all_non_deprecated_items, 'non_deprecated_items')
        log_with_date_time('non deprecated items read, size: ' + str(len(context.non_deprecated_items)))

        context.qid_to_nkcr = make_qid_database(context.non_deprecated_items)
        log_with_date_time('qid_to_nkcr read, size: ' + str(len(context.qid_to_nkcr)))

        context.chunks = load_nkcr_items(self.file_name)
        log_with_date_time('nkcr csv read')

        return context
