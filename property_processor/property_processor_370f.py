from property_processor.property_processor import BasePropertyProcessor
from tools import add_new_field_to_item_wbi


class PropertyProcessor370f(BasePropertyProcessor):
    """
    Handles and processes property-related operations.

    This class extends the BasePropertyProcessor and provides functionality for
    processing properties by working with specific data and interacting with
    external systems. Its purpose is to handle and manipulate properties in a
    specific context.

    :ivar item_new_field: The new field being processed and modified.
    :type item_new_field: Any
    :ivar property_for_new_field: The property key associated with the new field.
    :type property_for_new_field: Any
    :ivar row_new_fields: A dictionary containing row-related data, such as fields and attributes.
    :type row_new_fields: dict
    :ivar column: The current column within row_new_fields being processed.
    :type column: Any
    :ivar claim_direct_from_wd: A data structure containing claims directly fetched.
    :type claim_direct_from_wd: Any
    """
    def process(self):
        """
        Processes and updates specific fields in a database item based on predefined logic.

        This method verifies whether certain values in the given columns are present
        in the claims (data/attributes) fetched from an external data source. If the
        value is missing, it checks whether that field has also not been previously
        updated and proceeds to add a new field to the item accordingly.

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
