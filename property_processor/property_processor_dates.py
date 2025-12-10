from wikibaseintegrator.datatypes import Time
from wikibaseintegrator.models import Claims

from property_processor.property_processor import BasePropertyProcessor
from tools import add_new_field_to_item_wbi


class PropertyProcessorDates(BasePropertyProcessor):
    """
    Processes property-related date values with precision management and updates corresponding claims.

    This class is responsible for processing date fields from a new data row, comparing them against
    existing claims in Wikidata, and updating the claims if necessary. It handles cases where date
    fields are provided as a list or a single dictionary. Date precision is also managed to ensure
    higher precision values replace lesser ones.

    :ivar row_new_fields: The new data row containing fields to be processed.
    :ivar column: The column name in the data row that contains the relevant date field(s).
    :ivar property_for_new_field: The property name to associate new claims with.
    :ivar item_new_field: The item object where the processed claims are to be added.
    """
    def process(self):
        """
        Processes and updates claims for a given item based on comparison with existing data.

        This method interacts with and updates claims within an item's structure by comparing
        the precision and presence of data against existing claims in a Wikibase item. Depending
        on whether the input is a list or dictionary, it evaluates precision levels and ensures
        claims are updated accordingly. Claims with lower precision are replaced when higher
        precision data is available.

        :raises KeyError: If required keys (e.g., 'property', 'precision') are missing from a
            dictionary.

        :raises TypeError: If `self.row_new_fields[self.column]` is neither a list nor a dictionary.

        :param self: The instance of the class containing the method, which provides access to
            the necessary fields and helper methods.

        :return: None
        """
        qid_claims_direct_from_wd = self.get_qid_claims_direct_from_wd_wbi()

        if type(self.row_new_fields[self.column]) is list:
            for item_date in self.row_new_fields[self.column]:
                item_date: dict
                prop = item_date.get('property')
                if item_date not in qid_claims_direct_from_wd:
                    new_item_precision = item_date.get('precision', 0)
                    get_qid_claims_direct_from_wd_by_precision = self.get_qid_claims_direct_from_wd_by_precision(prop)
                    precisions = get_qid_claims_direct_from_wd_by_precision.keys()

                    if len(precisions) == 0:
                        highest_precisions = 0
                    else:
                        highest_precisions = max(precisions)

                    if (new_item_precision > highest_precisions):
                        self.item_new_field.claims.remove(prop)
                    self.item_new_field = add_new_field_to_item_wbi(
                        self.item_new_field,
                        self.property_for_new_field,
                        item_date,
                        self.row_new_fields['_id'])
        else:
            if type(self.row_new_fields[self.column]) is dict:
                item_date = self.row_new_fields[self.column]
                item_date: dict
                prop = item_date.get('property')
                if (item_date not in qid_claims_direct_from_wd):
                    new_item_precision = item_date.get('precision', 0)
                    get_qid_claims_direct_from_wd_by_precision = self.get_qid_claims_direct_from_wd_by_precision(prop)
                    precisions = get_qid_claims_direct_from_wd_by_precision.keys()
                    if len(precisions) == 0:
                        highest_precisions = 0
                    else:
                        highest_precisions = max(precisions)
                    if (new_item_precision > highest_precisions):
                        self.item_new_field = add_new_field_to_item_wbi(
                            self.item_new_field,
                            self.property_for_new_field,
                            self.row_new_fields[self.column],
                            self.row_new_fields['_id'])
