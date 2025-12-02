from wikibaseintegrator.datatypes import Time
from wikibaseintegrator.models import Claims

from property_processor.property_processor import BasePropertyProcessor
from tools import add_new_field_to_item_wbi


class PropertyProcessorDates(BasePropertyProcessor):

    def process(self):
        qid_claims_direct_from_wd = self.get_qid_claims_direct_from_wd_wbi()

        if type(self.row_new_fields[self.column]) is list:
            for item_date in self.row_new_fields[self.column]:
                item_date: dict
                prop = item_date.get('property')
                if item_date not in qid_claims_direct_from_wd:
                    new_item_precision = item_date.get('precision', 0)
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
            if type(self.row_new_fields[self.column]) is dict:
                item_date = self.row_new_fields[self.column]
                item_date: dict
                prop = item_date.get('property')
                if (item_date not in qid_claims_direct_from_wd):
                    new_item_precision = item_date.get('precision', 0)
                    get_qid_claims_direct_from_wd_by_precision = self.get_qid_claims_direct_from_wd_by_precision(prop)
                    precisions = get_qid_claims_direct_from_wd_by_precision.keys()
                    if len(precisions) == 0:
                        highest_precisions = 0
                    else:
                        highest_precisions = max(precisions)
                    if (new_item_precision > highest_precisions):
                        self.item_new_field = add_new_field_to_item_wbi(
                            self.item_new_field,
                            self.property_for_new_field,
                            self.row_new_fields[self.column],
                            self.row_new_fields['_id'])
