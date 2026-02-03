from property_processor.property_processor import BasePropertyProcessor
from tools import add_new_field_to_item_wbi


class PropertyProcessorSimpleList(BasePropertyProcessor):
    """
    Processes a list of values from a MARC field column and adds each value
    as a new claim if it doesn't already exist on the Wikidata item.

    Used for simple list-type properties: birthplace (370a/P19),
    death place (370b/P20), work location (370f/P937), language (377a/P1412).
    """
    def process(self):
        qid_claims_direct_from_wd = self.get_qid_claims_direct_from_wd_wbi()
        for item_in_list in self.row_new_fields[self.column]:
            if item_in_list not in qid_claims_direct_from_wd:
                if self.row_new_fields[self.column] not in self.claim_direct_from_wd:
                    self.item_new_field = add_new_field_to_item_wbi(
                        self.item_new_field,
                        self.property_for_new_field,
                        item_in_list,
                        self.row_new_fields['_id'])
