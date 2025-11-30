from wikibaseintegrator.models import Claims

from property_processor.property_processor import BasePropertyProcessor
from tools import add_new_field_to_item_wbi


class PropertyProcessorDates(BasePropertyProcessor):

    def process(self):
        qid_claims_direct_from_wd = self.get_qid_claims_direct_from_wd_wbi()

        if type(self.row_new_fields[self.column]) is list:
            for item_date in self.row_new_fields[self.column]:
                if item_date.mainsnak.datavalue['value'] not in qid_claims_direct_from_wd:
                    prop = item_date.mainsnak.property_number
                    new_item_precision = item_date.mainsnak.datavalue['value']['precision']
                    get_qid_claims_direct_from_wd_by_precision = self.get_qid_claims_direct_from_wd_by_precision(prop)
                    precisions = get_qid_claims_direct_from_wd_by_precision.keys()

                    if len(precisions) == 0:
                        highest_precisions = 0
                    else:
                        highest_precisions = max(precisions)

                    if (new_item_precision > highest_precisions):
                        self.item_new_field.claims.remove(prop)
                    self.item_new_field = add_new_field_to_item_wbi(
                        self.item_new_field,
                        self.property_for_new_field,
                        item_date,
                        self.row_new_fields['_id'])
        else:
            if self.row_new_fields[self.column] not in qid_claims_direct_from_wd:
                self.item_new_field = add_new_field_to_item_wbi(
                    self.item_new_field,
                    self.property_for_new_field,
                    self.row_new_fields[self.column],
                    self.row_new_fields['_id'])
