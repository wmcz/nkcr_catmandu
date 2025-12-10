from property_processor.property_processor import BasePropertyProcessor
from tools import add_new_field_to_item_wbi


class PropertyProcessorOne(BasePropertyProcessor):
    """
    Processes specific properties and updates items based on given inputs.

    This class is responsible for handling the processing of a property field
    by verifying conditions and updating item fields accordingly. It serves the
    purpose of extending the base functionality with specific property
    processing logic. Usage involves subclassing or instantiation in an
    appropriate context for property updating workflows.

    :ivar row_new_fields: A dictionary containing the new row data fields to be
        processed.
    :type row_new_fields: dict
    :ivar column: The specific column within the row to process.
    :type column: str
    :ivar claim_direct_from_wd: A set of claims retrieved directly from external
        data sources (e.g., Wikidata) used for comparison during processing.
    :type claim_direct_from_wd: set
    :ivar item_new_field: The item field being updated as a result of the
        processing logic.
    :type item_new_field: Any
    :ivar property_for_new_field: The specific property to be applied to the
        `item_new_field` during updates.
    :type property_for_new_field: str
    """
    def process(self):
        """
        Processes a data row and updates the corresponding field in an item if the field
        does not exist in a predefined list of claims.

        This method checks if the value of a specified column in the current data row
        is not present in the `claim_direct_from_wd` list. If the value is not found,
        it invokes the function `add_new_field_to_item_wbi` to add the field to the
        item.

        :param self: Instance of the class containing the method.
        """
        if self.row_new_fields[self.column] not in self.claim_direct_from_wd:
            self.item_new_field = add_new_field_to_item_wbi(
                self.item_new_field,
                self.property_for_new_field,
                self.row_new_fields[self.column],
                self.row_new_fields['_id'])
