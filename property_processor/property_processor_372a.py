from wikibaseintegrator.entities import ItemEntity

from config import Config
from property_processor.property_processor import BasePropertyProcessor
from tools import get_claim_from_item_by_property_wbi, add_new_field_to_item_wbi


class PropertyProcessor372a(BasePropertyProcessor):

    def set_datas_from_wd(self, datas_from_wd: ItemEntity):
        self.datas_from_wd = datas_from_wd

    def process(self):
        qid_claims_direct_from_wd = self.get_qid_claims_direct_from_wd_wbi()
        for item_in_list in self.row_new_fields[self.column]:
            item_occupation = item_in_list

            if item_occupation not in qid_claims_direct_from_wd and item_occupation not in Config.fields_of_work_not_used_in_field_of_work_because_is_not_ok:
                occupations_direct_from_wd = get_claim_from_item_by_property_wbi(
                    self.datas_from_wd,
                    Config.property_occupation)  # pro kontrolu
                qid_occupations_claims_direct_from_wd = []
                for odfwd in occupations_direct_from_wd:
                    # if type(odfwd) is pywikibot.ItemPage:
                    qid_occupations_claims_direct_from_wd.append(odfwd)
                if item_occupation not in qid_occupations_claims_direct_from_wd:
                    if self.row_new_fields[self.column] not in self.claim_direct_from_wd:
                        self.item_new_field = add_new_field_to_item_wbi(
                            self.item_new_field,
                            self.property_for_new_field,
                            item_occupation,
                            self.row_new_fields['_id'])
