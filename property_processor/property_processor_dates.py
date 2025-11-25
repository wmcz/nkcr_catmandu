from property_processor.property_processor import BasePropertyProcessor
from tools import add_new_field_to_item_wbi


class PropertyProcessorDates(BasePropertyProcessor):

    def process(self):
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
