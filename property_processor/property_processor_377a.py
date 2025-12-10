from property_processor.property_processor import BasePropertyProcessor
from tools import add_new_field_to_item_wbi


class PropertyProcessor377a(BasePropertyProcessor):
    """
    Facilitates processing property updates in a specific row and updates
    item fields based on property values if certain conditions are met.

    This class performs operations such as verifying existing claims and
    adding new fields to items when required. It ensures seamless integration
    of new fields into the data pipeline by leveraging provided property
    values and comparing them with existing claims.

    :ivar row_new_fields: Dictionary containing new field data for the row
        being processed.
    :type row_new_fields: dict
    :ivar column: Key identifying the specific column in `row_new_fields` that
        holds the property values to check and update.
    :type column: str
    :ivar claim_direct_from_wd: List or set of claims directly fetched from
        the external source "wd".
    :type claim_direct_from_wd: list or set
    :ivar item_new_field: Represents the item being updated with new fields.
    :type item_new_field: dict
    :ivar property_for_new_field: Identifier for the property corresponding
        to the new field being added.
    :type property_for_new_field: str
    """
    def process(self):
        """
        Processes new fields and updates the item with additional properties if necessary.

        This function evaluates the new fields from the specified column. If the field value
        is not presently associated in the direct claims retrieved from Wikibase, it verifies
        if the current row's data is also missing. If both conditions are true, a new property
        is added to the item by invoking the `add_new_field_to_item_wbi` function.

        :raises KeyError: If the specified keys are not present in `self.row_new_fields`.
        :param self: Instance of the class containing the method execution context.

        :return: None
        """
        qid_claims_direct_from_wd = self.get_qid_claims_direct_from_wd_wbi()
        for item_language in self.row_new_fields[self.column]:
            if item_language not in qid_claims_direct_from_wd:
                if self.row_new_fields[self.column] not in self.claim_direct_from_wd:
                    self.item_new_field = add_new_field_to_item_wbi(
                        self.item_new_field,
                        self.property_for_new_field,
                        item_language,
                        self.row_new_fields['_id'])
