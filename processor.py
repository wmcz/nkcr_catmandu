from datetime import datetime
from typing import Union

from wikibaseintegrator.entities import ItemEntity

from cleaners import clean_qid, resolve_exist_claims, prepare_column_of_content
from config import *
from property_processor.property_processor_370a import PropertyProcessor370a
from property_processor.property_processor_370b import PropertyProcessor370b
from property_processor.property_processor_370f import PropertyProcessor370f
from property_processor.property_processor_372a import PropertyProcessor372a
from property_processor.property_processor_374a import PropertyProcessor374a
from property_processor.property_processor_377a import PropertyProcessor377a
from property_processor.property_processor_dates import PropertyProcessorDates
from property_processor.property_processor_one import PropertyProcessorOne
from tools import log_with_date_time, get_claim_from_item_by_property_wbi
from wikibaseintegrator.datatypes import Item, ExternalID, Time, String


class Processor:

    def __init__(self):
        self.row = None
        self.item = None
        self.qid = None
        self.nkcr_aut = None
        self.enabled_columns: dict = {}

        self.wbi = None
        self.instances_from_item = None
        self.save = True

    def get_instances_from_item(self):
        if self.instances_from_item is not None:
            return self.instances_from_item
        else:
            self.instances_from_item = get_claim_from_item_by_property_wbi(self.item, 'P31')
            for instance_from_item in self.instances_from_item:
                # if instance_from_item.getID() in Config.instances_not_possible_for_nkcr: #pywikibot
                if instance_from_item in Config.instances_not_possible_for_nkcr:
                    self.save = False

    def reset_instances_from_item(self, instances_from_item):
        self.instances_from_item = instances_from_item
        self.save = True

    def process_new_fields_wbi(self, qid_new_fields: Union[str, None], wd_data: dict, row_new_fields: dict,
                           wd_item: Union[ItemEntity, None] = None):
        item_new_field = wd_item

        if self.item is None:
            self.item = item_new_field

        if len(self.enabled_columns) > 0:
            property_dict_for_this_processor = self.enabled_columns
        else:
            property_dict_for_this_processor = Config.properties

        for column, property_for_new_field in property_dict_for_this_processor.items():
            try:
                # claims_in_new_item = datas_new_field['claims'].get(property_for_new_field, [])
                claims = resolve_exist_claims(column, wd_data)

                row_new_fields[column] = prepare_column_of_content(column, row_new_fields)
                array_diff = []
                time_fields = False
                if type(row_new_fields[column]) is list:
                    for dt in row_new_fields[column]:
                        if type(dt) == Time:
                            time_fields = True
                    if not time_fields:
                        array_diff = set(row_new_fields[column]) - set(claims)
                    else:
                        if len(claims):
                            self.save = False
                if (self.save
                        and (
                            (type(row_new_fields[column]) == str and row_new_fields[column] not in claims and len(row_new_fields[column]) > 0)
                            or
                            (type(row_new_fields[column]) == list and len(array_diff) > 0 and not time_fields)
                            or
                            (type(row_new_fields[column]) == Time)
                            or
                            (time_fields)
                        )
                ):

                    if self.item is None:
                        item_new_field = self.wbi.item.get(qid_new_fields)
                        self.item = item_new_field
                    datas_from_wd = self.item
                    self.get_instances_from_item()

                    if self.item.id in Config.qid_blacklist:
                        self.save = False

                    if not self.save:
                        return None
                    claim_direct_from_wd = get_claim_from_item_by_property_wbi(datas_from_wd,
                                                                           property_for_new_field)  # pro kontrolu

                    if type(row_new_fields[column]) is list:
                        if column == '374a':
                            property_processor = PropertyProcessor374a(
                                wbi=self.wbi, property_for_new_field=property_for_new_field,
                                column=column, row_new_fields=row_new_fields,
                                claim_direct_from_wd=claim_direct_from_wd, item_new_field=item_new_field)
                            property_processor.process()
                        elif column == '372a':
                            property_processor = PropertyProcessor372a(
                                wbi=self.wbi, property_for_new_field=property_for_new_field,
                                column=column, row_new_fields=row_new_fields,
                                claim_direct_from_wd=claim_direct_from_wd, item_new_field=item_new_field)
                            property_processor.set_datas_from_wd(datas_from_wd)

                            property_processor.process()
                        elif column == '370a':
                            property_processor = PropertyProcessor370a(
                                wbi=self.wbi, property_for_new_field=property_for_new_field,
                                column=column, row_new_fields=row_new_fields,
                                claim_direct_from_wd=claim_direct_from_wd, item_new_field=item_new_field)
                            property_processor.process()
                        elif column == '370b':
                            property_processor = PropertyProcessor370b(
                                wbi=self.wbi, property_for_new_field=property_for_new_field,
                                column=column, row_new_fields=row_new_fields,
                                claim_direct_from_wd=claim_direct_from_wd, item_new_field=item_new_field)
                            property_processor.process()
                        elif column == '370f':
                            property_processor = PropertyProcessor370f(
                                wbi=self.wbi, property_for_new_field=property_for_new_field,
                                column=column, row_new_fields=row_new_fields,
                                claim_direct_from_wd=claim_direct_from_wd, item_new_field=item_new_field)
                            property_processor.process()
                        elif column == '377a':
                            property_processor = PropertyProcessor377a(
                                wbi=self.wbi, property_for_new_field=property_for_new_field,
                                column=column, row_new_fields=row_new_fields,
                                claim_direct_from_wd=claim_direct_from_wd, item_new_field=item_new_field)
                            property_processor.process()
                        elif time_fields:
                            property_processor = PropertyProcessorDates(
                                wbi=self.wbi, property_for_new_field=property_for_new_field,
                                column=column, row_new_fields=row_new_fields,
                                claim_direct_from_wd=claim_direct_from_wd, item_new_field=item_new_field)
                            property_processor.process()
                    elif type(row_new_fields[column]) is Time or type(row_new_fields[column]) is None or time_fields:
                        property_processor = PropertyProcessorDates(
                            wbi=self.wbi, property_for_new_field=property_for_new_field,
                            column=column, row_new_fields=row_new_fields,
                            claim_direct_from_wd=claim_direct_from_wd, item_new_field=item_new_field)
                        property_processor.process()
                    else:
                        property_processor = PropertyProcessorOne(
                            wbi=self.wbi, property_for_new_field=property_for_new_field,
                            column=column, row_new_fields=row_new_fields,
                            claim_direct_from_wd=claim_direct_from_wd, item_new_field=item_new_field)
                        property_processor.process()

            except ValueError as ve:
                log_with_date_time(str(ve))
                pass
            except KeyError:
                # log_with_date_time(str(ke))
                pass

    def set_nkcr_aut(self, nkcr_aut):
        self.nkcr_aut = nkcr_aut

    def set_qid(self, qid):
        self.qid = qid

    def set_item(self, item):
        self.item = item

    def get_item(self)->ItemEntity:
        return self.item

    def set_row(self, row):
        self.row = row

    def set_enabled_columns(self, columns: dict):
        self.enabled_columns = columns

    def set_wbi(self, wbi):
        self.wbi = wbi

    def process_occupation_type(self, non_deprecated_items):

        nkcr_aut = self.nkcr_aut
        qid = self.qid
        item = self.item
        row = self.row

        if nkcr_aut in non_deprecated_items:
            exist_qid = non_deprecated_items[nkcr_aut]['qid']
            if exist_qid != '':
                exist_qid = clean_qid(exist_qid)
                # rewrite label – open all
                # if self.item is None:
                #     item_new_field = self.wbi.item.get(exist_qid)
                #     self.item = item_new_field
                #     item = self.item
                if item is not None:
                    self.process_new_fields_wbi(exist_qid, non_deprecated_items[nkcr_aut], row, item)
                else:
                    self.process_new_fields_wbi(exist_qid, non_deprecated_items[nkcr_aut], row)
            if qid != '' and exist_qid != qid:
                self.process_new_fields_wbi(None, non_deprecated_items[nkcr_aut], row, item)
        else:
            d = ''

    def process_date_type(self, non_deprecated_items):

        nkcr_aut = self.nkcr_aut
        qid = self.qid
        item = self.item
        row = self.row

        if nkcr_aut in non_deprecated_items:
            exist_qid = non_deprecated_items[nkcr_aut]['qid']
            if exist_qid != '':
                exist_qid = clean_qid(exist_qid)
                # rewrite label – open all
                # if self.item is None:
                #     item_new_field = self.wbi.item.get(exist_qid)
                #     self.item = item_new_field
                #     item = self.item
                if item is not None:
                    self.process_new_fields_wbi(exist_qid, non_deprecated_items[nkcr_aut], row, item)
                else:
                    self.process_new_fields_wbi(exist_qid, non_deprecated_items[nkcr_aut], row)
            if qid != '' and exist_qid != qid:
                self.process_new_fields_wbi(None, non_deprecated_items[nkcr_aut], row, item)
        else:
            d = ''
