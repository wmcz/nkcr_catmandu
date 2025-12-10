from property_processor.property_processor import BasePropertyProcessor
from tools import add_new_field_to_item_wbi


class PropertyProcessor370b(BasePropertyProcessor):
    """
    Processes data fields for items by comparing the current dataset with an
    external source and updating the fields as needed.

    The main purpose of this class is to ensure that the item's fields are
    consistent with the external source by identifying missing or mismatched
    data and adding new fields when required.

    :ivar property_for_new_field: Property identifier used for associating new
        fields with items.
    :type property_for_new_field: str
    :ivar row_new_fields: Dictionary containing raw data for item fields,
        categorized by column.
    :type row_new_fields: dict
    :ivar column: Key representing the column name of interest within
        ``row_new_fields``.
    :type column: str
    :ivar item_new_field: The current representation of the item being updated
        with new fields, as applicable.
    :type item_new_field: dict
    :ivar claim_direct_from_wd: Existing claims fetched directly from the
        external source for comparison.
    :type claim_direct_from_wd: dict
    """
    def process(self):
        """
        Processes and updates item fields by comparing values between the current fields
        and an external source. It checks for mismatches or missing items and updates
        fields accordingly by adding new ones when necessary.

        :return: None
        """
        qid_claims_direct_from_wd = self.get_qid_claims_direct_from_wd_wbi()
        for item_in_list in self.row_new_fields[self.column]:
            item_place = item_in_list

            if item_place not in qid_claims_direct_from_wd:
                if self.row_new_fields[self.column] not in self.claim_direct_from_wd:
                    self.item_new_field = add_new_field_to_item_wbi(
                        self.item_new_field,
                        self.property_for_new_field,
                        item_place,
                        self.row_new_fields['_id'])
