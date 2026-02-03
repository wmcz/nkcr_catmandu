"""
Pipeline context module for NK ČR to Wikidata synchronization.

This module provides a centralized context object that holds all shared state
for the pipeline, replacing module-level global variables with explicit
dependency injection.
"""

from dataclasses import dataclass, field
from typing import Union
import pandas


@dataclass
class PipelineContext:
    """
    Centralized context for the NK ČR to Wikidata synchronization pipeline.

    This class holds all shared state that was previously scattered across
    module-level global variables. It enables explicit dependency injection,
    making the code easier to test and reason about.

    Attributes:
        name_to_nkcr: Mapping of occupation/place names to their Wikidata QIDs.
            Loaded from SPARQL query results.
        language_dict: Mapping of language codes to Wikidata QIDs.
            Loaded from external CSV file.
        qid_to_nkcr: Mapping of Wikidata QIDs to NK ČR authority IDs.
        non_deprecated_items: All non-deprecated NK ČR items on Wikidata.
        non_deprecated_items_field_of_work_and_occupation: Items with field of work/occupation claims.
        non_deprecated_items_places: Items with place-related claims.
        non_deprecated_items_languages: Items with language claims.
        not_found_occupations: Tracking dict for occupations not found during processing.
        not_found_places: Tracking dict for places not found during processing.
        subclass_cache: Cache for is_item_subclass_of_wbi SPARQL query results.
        chunks: DataFrame containing NK ČR CSV data in chunks.
    """

    # Lookup dictionaries (read-only after initialization)
    name_to_nkcr: dict = field(default_factory=dict)
    language_dict: dict = field(default_factory=dict)
    qid_to_nkcr: dict[str, list[str]] = field(default_factory=dict)

    # Non-deprecated items from SPARQL queries
    non_deprecated_items: dict = field(default_factory=dict)
    non_deprecated_items_field_of_work_and_occupation: dict = field(default_factory=dict)
    non_deprecated_items_places: dict = field(default_factory=dict)
    non_deprecated_items_languages: dict = field(default_factory=dict)

    # Tracking dictionaries (mutated during processing)
    not_found_occupations: dict = field(default_factory=dict)
    not_found_places: dict = field(default_factory=dict)

    # Cache for subclass queries
    subclass_cache: dict = field(default_factory=dict)

    # CSV data
    chunks: Union[pandas.DataFrame, None] = None

    def log_not_found_occupation(self, occupation: str) -> None:
        """Record an occupation that was not found in the lookup dictionary."""
        self.not_found_occupations[occupation] = self.not_found_occupations.get(occupation, 0) + 1

    def log_not_found_place(self, place: str) -> None:
        """Record a place that was not found in the lookup dictionary."""
        self.not_found_places[place] = self.not_found_places.get(place, 0) + 1

    def cache_subclass_result(self, subclass_qid: str, item_qid: str, is_subclass: bool) -> None:
        """Cache the result of a subclass check."""
        if subclass_qid not in self.subclass_cache:
            self.subclass_cache[subclass_qid] = {}
        self.subclass_cache[subclass_qid][item_qid] = is_subclass

    def get_cached_subclass_result(self, subclass_qid: str, item_qid: str) -> Union[bool, None]:
        """
        Get cached subclass result.

        Returns:
            True/False if cached, None if not in cache.
        """
        try:
            return self.subclass_cache[subclass_qid][item_qid]
        except KeyError:
            return None
