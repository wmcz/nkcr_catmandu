from property_processor.property_processor import BasePropertyProcessor
from tools import add_new_field_to_item_wbi


class PropertyProcessorOne(BasePropertyProcessor):
    def process(self):
        if self.row_new_fields[self.column] not in self.claim_direct_from_wd:
            self.item_new_field = add_new_field_to_item_wbi(
                self.item_new_field,
                self.property_for_new_field,
                self.row_new_fields[self.column],
                self.row_new_fields['_id'])
