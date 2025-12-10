# import timeit
# from typing import Union

import cleaners
from tools import *
log = logging.getLogger(__name__)


class Loader:
    """
    Handles loading and managing datasets while maintaining mappings and metadata.

    Provides functionality to load various non-deprecated items, their
    relationships, and associated metadata from external sources. It also allows
    setting configurable attributes like file name and data limits.

    :ivar limit: Maximum number of records to process in each dataset.
    :type limit: int
    :ivar chunks: DataFrame containing processed data chunks.
    :type chunks: Union[pandas.DataFrame, None]
    :ivar qid_to_nkcr: Dictionary mapping QIDs to non-deprecated NKCR records.
    :type qid_to_nkcr: dict[str, list[str]]
    :ivar name_to_nkcr: Nested dictionary mapping names to non-deprecated NKCR
        records and other metadata.
    :type name_to_nkcr: dict[dict[str, list, list]]
    :ivar non_deprecated_items: Dictionary of all non-deprecated items.
    :type non_deprecated_items: dict
    :ivar non_deprecated_items_field_of_work_and_occupation: Dictionary of
        non-deprecated items categorized by field of work and occupation.
    :type non_deprecated_items_field_of_work_and_occupation: dict
    :ivar non_deprecated_items_places: Dictionary of non-deprecated items
        related to specific places.
    :type non_deprecated_items_places: dict
    :ivar non_deprecated_items_languages: Dictionary of non-deprecated items
        related to specific languages.
    :type non_deprecated_items_languages: dict
    :ivar languages_dict: Dictionary mapping language codes to language names
        or descriptions.
    :type languages_dict: dict[str, str]
    :ivar file_name: Name of the file associated with the data.
    :type file_name: str
    """
    limit: int = 30000

    def __init__(self):
        """
        Represents an initialization class that maintains multiple data structures
        to store and manipulate information about non-deprecated items, their
        relationships, and related metadata.

        Attributes
        ----------
        chunks : Union[pandas.DataFrame, None]
            Represents a DataFrame containing chunks of processed data. It is
            initialized as None.

        qid_to_nkcr : dict[str, list[str]]
            A dictionary mapping unique entity identifiers (QIDs) to a list of
            associated non-deprecated NKCR records.

        name_to_nkcr : dict[dict[str, list, list]]
            A nested dictionary that maps names to non-deprecated NKCR records
            and additional lists for metadata representation.

        non_deprecated_items : dict
            A dictionary holding non-deprecated items without any specific
            categorization.

        non_deprecated_items_field_of_work_and_occupation : dict
            Stores non-deprecated items categorized by field of work and
            occupation.

        non_deprecated_items_places : dict
            Maintains non-deprecated items related to specific geographical
            places.

        non_deprecated_items_languages : dict
            Contains non-deprecated items related to languages.

        languages_dict : dict[str, str]
            A dictionary mapping language codes or identifiers to their
            respective language names or descriptions.

        file_name : str
            Represents the name of the file associated with the class instance,
            typically used for data storage or retrieval purposes.
        """
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

    def load(self):
        """
        Loads various datasets and initializes class attributes required for further processing.
        The method performs multiple operations such as fetching data from SPARQL queries,
        loading language dictionaries, and processing CSV files. Each dataset is logged with
        timestamps to monitor progress and execution time.

        :raises: Any exceptions that may occur during the loading or processing
                 of data (e.g., network errors, file reading errors).
        """
        log_with_date_time('run')

        limit_for_occupation = 100000
        self.name_to_nkcr = load_sparql_query_by_chunks(limit_for_occupation, get_occupations, 'occupations')
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
