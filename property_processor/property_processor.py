from typing import Union

from wikibaseintegrator import WikibaseIntegrator
from tools import *


class BasePropertyProcessor:
    def __init__(self, column, row_new_fields, claim_direct_from_wd, property_for_new_field, item_new_field, wbi):
        self.property_for_new_field: Union[str, None] = property_for_new_field
        self.item_new_field: Union[ItemEntity, None] = item_new_field
        self.column: Union[str, None] = column
        self.row_new_fields: dict = row_new_fields
        self.wbi: WikibaseIntegrator = wbi
        self.claim_direct_from_wd = claim_direct_from_wd

    def set_claim_direct_from_wd(self, claim_direct_from_wd):
        self.claim_direct_from_wd = claim_direct_from_wd

    def set_row_new_fields(self, row_new_fields: dict):
        self.row_new_fields = row_new_fields

    def set_column(self, column: str):
        self.column = column

    def set_item_new_field(self, item_new_field: pywikibot.ItemPage):
        self.item_new_field = item_new_field

    def set_property_for_new_field(self, property_for_new_field: str):
        self.property_for_new_field = property_for_new_field

    def get_qid_claims_direct_from_wd_wbi(self) -> list:
        qid_claims_direct_from_wd = []
        for cdfwd in self.claim_direct_from_wd:
            # if type(cdfwd) is pywikibot.ItemPage:
            qid_claims_direct_from_wd.append(cdfwd)
        return qid_claims_direct_from_wd

    def get_qid_claims_direct_from_wd_by_precision(self, property) -> dict:
        qid_claims_direct_from_wd = {}
        for cdfwd in self.claim_direct_from_wd:
            # if type(cdfwd) is pywikibot.ItemPage:
            if (cdfwd.get('property') == property):
                qid_claims_direct_from_wd[cdfwd.get('precision')] = cdfwd
        return qid_claims_direct_from_wd
