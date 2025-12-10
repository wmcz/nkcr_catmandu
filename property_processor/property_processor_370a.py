from property_processor.property_processor import BasePropertyProcessor
from tools import add_new_field_to_item_wbi


class PropertyProcessor370a(BasePropertyProcessor):
    """
    Processes properties and manages updates for specific items.

    This class is responsible for retrieving claims from a source and comparing
    them with the provided data. If discrepancies are found, it updates the
    items by adding new fields to ensure consistency. Primarily used for
    managing property-related updates in the context of a specified repository.

    :ivar row_new_fields: Dictionary containing data for new fields, where keys
        are column identifiers and values are field data.
    :type row_new_fields: dict
    :ivar column: The column identifier used to determine relevant data for
        processing fields.
    :type column: str
    :ivar property_for_new_field: The property identifier for creating or
        updating fields in the respective objects.
    :type property_for_new_field: str
    :ivar claim_direct_from_wd: List of existing claims retrieved directly from
        the source for verification purposes.
    :type claim_direct_from_wd: list
    :ivar item_new_field: Object representing the modified item after adding
        new fields.
    :type item_new_field: Any
    """
    def process(self):
        """
        Processes and updates specific fields within the dataset by identifying and adding new
        fields if they are not present in the existing dataset obtained from an external source.

        This method iterates over a set of new field entries for a specific column and checks
        whether these entries exist in the direct claims provided. If not, it updates
        the corresponding item with the new information.

        :return: None
        """
        qid_claims_direct_from_wd = self.get_qid_claims_direct_from_wd_wbi()
        for item_in_list in self.row_new_fields[self.column]:
            # item_place = pywikibot.ItemPage(self.repo, item_in_list)
            item_place = item_in_list

            if item_place not in qid_claims_direct_from_wd:
                if self.row_new_fields[self.column] not in self.claim_direct_from_wd:
                    self.item_new_field = add_new_field_to_item_wbi(
                        self.item_new_field,
                        self.property_for_new_field,
                        item_place,
                        self.row_new_fields['_id'])
