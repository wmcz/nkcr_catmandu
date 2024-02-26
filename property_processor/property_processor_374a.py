from config import Config
from property_processor.property_processor import BasePropertyProcessor
from tools import add_new_field_to_item_wbi, is_item_subclass_of_wbi


class PropertyProcessor374a(BasePropertyProcessor):
    QID_OCCUPATION = 'Q12737077'

    def process(self):
        class_occupation = PropertyProcessor374a.QID_OCCUPATION
        qid_claims_direct_from_wd = self.get_qid_claims_direct_from_wd_wbi()
        for item_in_list in self.row_new_fields[self.column]:
            item_occupation = item_in_list

            if item_occupation not in qid_claims_direct_from_wd and item_occupation not in Config.occupations_not_used_in_occupation_because_is_in_function:
                if is_item_subclass_of_wbi(item_occupation, class_occupation):
                    if self.row_new_fields[self.column] not in self.claim_direct_from_wd:
                        self.item_new_field = add_new_field_to_item_wbi(
                            self.item_new_field,
                            self.property_for_new_field,
                            item_occupation,
                            self.row_new_fields['_id'])
