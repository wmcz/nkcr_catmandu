# import timeit
# from typing import Union

from context import PipelineContext
from memory_profiler import log_memory, MemoryTracker, get_object_size_mb
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
    limit: int = 100000

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
        log_memory('Loader.load() start')

        context = PipelineContext()

        # Only the terms the CSV actually uses are looked up - see
        # load_names_to_qid() for why the full-graph query had to go.
        with MemoryTracker('Loading occupations'):
            context.name_to_nkcr = load_name_dictionary(self.file_name)
        log_with_date_time('occupations read, size: ' + str(len(context.name_to_nkcr)))
        get_object_size_mb(context.name_to_nkcr, 'name_to_nkcr')

        with MemoryTracker('Loading language dict'):
            context.language_dict = load_language_dict_csv()
        log_with_date_time('loaded language dict from github')

        # Only the authorities present in the CSV are ever looked up, and asking
        # for them by name keeps every query on the index instead of scanning
        # the whole graph - see load_non_deprecated_items_by_nkcr().
        nkcr_ids = load_nkcr_ids(self.file_name)
        log_with_date_time('nkcr ids in csv: ' + str(len(nkcr_ids)))

        with MemoryTracker('Loading non deprecated items'):
            datasets = load_nkcr_datasets(nkcr_ids)

        context.non_deprecated_items = datasets['non_deprecated_items']
        context.non_deprecated_items_field_of_work_and_occupation = datasets['field_of_work_and_occupation']
        context.non_deprecated_items_places = datasets['places']
        context.non_deprecated_items_languages = datasets['languages']

        for label, dataset in (('non_deprecated_items', context.non_deprecated_items),
                               ('non_deprecated_items_field_of_work_and_occupation',
                                context.non_deprecated_items_field_of_work_and_occupation),
                               ('non_deprecated_items_places', context.non_deprecated_items_places),
                               ('non_deprecated_items_languages', context.non_deprecated_items_languages)):
            log_with_date_time(label + ' read, size: ' + str(len(dataset)))
            get_object_size_mb(dataset, label)

        context.qid_to_nkcr = make_qid_database(context.non_deprecated_items)
        log_with_date_time('qid_to_nkcr read, size: ' + str(len(context.qid_to_nkcr)))

        with MemoryTracker('Loading CSV chunks'):
            context.chunks = load_nkcr_items(self.file_name)
        log_with_date_time('nkcr csv read')

        log_memory('Loader.load() complete')
        return context
