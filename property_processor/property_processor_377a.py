from property_processor.property_processor import BasePropertyProcessor
from tools import add_new_field_to_item_wbi


class PropertyProcessor377a(BasePropertyProcessor):

    def process(self):
        qid_claims_direct_from_wd = self.get_qid_claims_direct_from_wd_wbi()
        for item_language in self.row_new_fields[self.column]:
            if item_language not in qid_claims_direct_from_wd:
                if self.row_new_fields[self.column] not in self.claim_direct_from_wd:
                    self.item_new_field = add_new_field_to_item_wbi(
                        self.item_new_field,
                        self.property_for_new_field,
                        item_language,
                        self.row_new_fields['_id'])
