from cleaners import clean_qid, resolve_exist_claims, prepare_column_of_content
from config import *
from property_processor import *
from pywikibot_extension import MyDataSite
from tools import log_with_date_time, get_claim_from_item_by_property


class Processor:
    debug: bool = False

    def __init__(self):
        self.row = None
        self.item = None
        self.qid = None
        self.nkcr_aut = None
        self.repo: Union[MyDataSite, None] = None

    def set_repo(self, repo: MyDataSite):
        self.repo = repo

    def set_debug(self, debug: bool):
        self.debug = debug

    def process_new_fields(self, qid_new_fields: Union[str, None], wd_data: dict, row_new_fields: dict,
                           wd_item: Union[pywikibot.ItemPage, None] = None):
        # print('process')
        if wd_item is None:
            item_new_field = pywikibot.ItemPage(self.repo, qid_new_fields)
        else:
            item_new_field = wd_item

        for column, property_for_new_field in Config.properties.items():
            try:
                # claims_in_new_item = datas_new_field['claims'].get(property_for_new_field, [])
                claims = resolve_exist_claims(column, wd_data)

                row_new_fields[column] = prepare_column_of_content(column, row_new_fields)
                array_diff = []
                if type(row_new_fields[column]) is list:
                    array_diff = set(row_new_fields[column]) - set(claims)
                if (type(row_new_fields[column]) == str and row_new_fields[column] not in claims and len(
                        row_new_fields[column]) > 0) or (type(row_new_fields[column] == list) and len(array_diff) > 0):
                    # insert
                    if item_new_field.isRedirectPage():
                        item_new_field = item_new_field.getRedirectTarget()

                    datas_from_wd = item_new_field.get(get_redirect=True)
                    claim_direct_from_wd = get_claim_from_item_by_property(datas_from_wd,
                                                                           property_for_new_field)  # pro kontrolu

                    if type(row_new_fields[column]) is list:
                        if column == '374a':
                            property_processor = PropertyProcessor374a()
                            property_processor.set_repo(self.repo)
                            property_processor.set_debug(self.debug)
                            property_processor.set_claim_direct_from_wd(claim_direct_from_wd)
                            property_processor.set_property_for_new_field(property_for_new_field)
                            property_processor.set_column(column)
                            property_processor.set_item_new_field(item_new_field)
                            property_processor.set_row_new_fields(row_new_fields)
                            property_processor.process()
                        elif column == '372a':
                            property_processor = PropertyProcessor372a()
                            property_processor.set_repo(self.repo)
                            property_processor.set_debug(self.debug)
                            property_processor.set_claim_direct_from_wd(claim_direct_from_wd)
                            property_processor.set_property_for_new_field(property_for_new_field)
                            property_processor.set_column(column)
                            property_processor.set_item_new_field(item_new_field)
                            property_processor.set_row_new_fields(row_new_fields)

                            property_processor.set_datas_from_wd(datas_from_wd)

                            property_processor.process()
                    else:
                        property_processor = PropertyProcessorOne()
                        property_processor.set_repo(self.repo)
                        property_processor.set_debug(self.debug)
                        property_processor.set_claim_direct_from_wd(claim_direct_from_wd)
                        property_processor.set_property_for_new_field(property_for_new_field)
                        property_processor.set_column(column)
                        property_processor.set_item_new_field(item_new_field)
                        property_processor.set_row_new_fields(row_new_fields)
                        property_processor.process()

            except ValueError as ve:
                log_with_date_time(str(ve))
                pass
            except pywikibot.exceptions.OtherPageSaveError as opse:
                log_with_date_time(str(opse))
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

    def set_row(self, row):
        self.row = row

    def process_occupation_type(self, non_deprecated_items):

        nkcr_aut = self.nkcr_aut
        qid = self.qid
        item = self.item
        row = self.row

        if nkcr_aut in non_deprecated_items:
            exist_qid = non_deprecated_items[nkcr_aut]['qid']
            if exist_qid != '':
                exist_qid = clean_qid(exist_qid)
                self.process_new_fields(exist_qid, non_deprecated_items[nkcr_aut], row)
            if qid != '' and exist_qid != qid:
                if item.isRedirectPage():
                    item = item.getRedirectTarget()
                    item.get(get_redirect=True)
                self.process_new_fields(None, non_deprecated_items[nkcr_aut], row, item)
