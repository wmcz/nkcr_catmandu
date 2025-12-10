import logging
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

log = logging.getLogger(__name__)
class Processor:
    """
    Represents a processor for handling and managing wikibase entity data and its fields.

    This class is designed to process new fields, update data based on configured
    properties, and handle specific cases for time-related fields. It integrates methods
    for working with claims and column-specific property handling, potentially interacting
    with external configurations and property processors.

    :ivar row: Current row data being processed.
    :type row: Any
    :ivar item: Current item being processed as an entity.
    :type item: Any
    :ivar qid: The Wikibase ID for the current entity.
    :type qid: Any
    :ivar nkcr_aut: Stores specific authority data for updates.
    :type nkcr_aut: Any
    :ivar enabled_columns: A dictionary specifying the columns and their corresponding
        properties enabled for processing.
    :type enabled_columns: dict
    :ivar wbi: A WikibaseIntegrator object for interaction with Wikibase.
    :type wbi: Any
    :ivar instances_from_item: Cached instances derived from the current item.
    :type instances_from_item: Any
    :ivar save: A boolean flag indicating whether the current item should be saved.
    :type save: bool
    """
    def __init__(self):
        """
        Class representing initialization and storage of essential attributes for processing and managing
        specific entities within a given dataset. This class is intended to define and organize the
        attributes required for data manipulation, such as rows, items, and enabled columns.

        Attributes:
            row: Represents a specific row of the data.
            item: Represents an entity or object in the dataset.
            qid: Holds a unique identifier as a string for the current entity.
            nkcr_aut: Stores an identifier related to a specific external reference.
            enabled_columns (dict): A dictionary mapping enabled columns for analysis or processing.

            wbi: Represents an external interface or object for data interaction or processing.
            instances_from_item: Stores a collection of related data instances from a given item.
            save (bool): A flag indicating whether the data or entity should be saved after processing.
        """
        self.row = None
        self.item = None
        self.qid = None
        self.nkcr_aut = None
        self.enabled_columns: dict = {}

        self.wbi = None
        self.instances_from_item = None
        self.save = True

    def get_instances_from_item(self):
        """
        Retrieves and returns the instances associated with a specific item. If the
        instances are already available, the cached result is returned. Otherwise, it
        fetches the data, processes it, and performs additional configuration checks.

        :return: List of instances associated with the specific item.
        :rtype: list
        """
        if self.instances_from_item is not None:
            return self.instances_from_item
        else:
            self.instances_from_item = get_claim_from_item_by_property_wbi(self.item, 'P31')
            for instance_from_item in self.instances_from_item:
                # if instance_from_item.getID() in Config.instances_not_possible_for_nkcr: #pywikibot
                if instance_from_item in Config.instances_not_possible_for_nkcr:
                    self.save = False

    def reset_instances_from_item(self, instances_from_item):
        """
        Resets the value of the `instances_from_item` attribute and sets the save flag to True.

        :param instances_from_item: The new value to assign to the `instances_from_item` attribute
        :type instances_from_item: Any
        :return: None
        """
        self.instances_from_item = instances_from_item
        self.save = True

    def process_new_fields_wbi(self, qid_new_fields: Union[str, None], wd_data: dict, row_new_fields: dict,
                           wd_item: Union[ItemEntity, None] = None):
        """
        Processes new fields from the provided data and updates the item based on specified rules and
        conditions. This method handles data fields for specific columns, resolves existing claims, and
        utilizes property processors to manage values, including date-related fields. The processing
        also allows for conditional saving of data and prevents updates for blacklisted item IDs.

        :param qid_new_fields: Identifier of the item. Can be a string or None if not specified.
        :type qid_new_fields: Union[str, None]
        :param wd_data: Dictionary containing existing data for the item. Expected to include details
            such as "birth" and "death" date information, among others.
        :type wd_data: dict
        :param row_new_fields: Dictionary containing new field data to be processed and compared with
            existing item data. Column keys map to their respective field data, which can be a string,
            list, or dictionary.
        :type row_new_fields: dict
        :param wd_item: Item entity object representing the item being processed. Can be None if
            unavailable at the start.
        :type wd_item: Union[ItemEntity, None]
        :return: None if the item is blacklisted or data saving is disabled, otherwise the processing
            continues through property-specific handling mechanisms.
        :rtype: None
        """
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
                save_time = True
                if type(row_new_fields[column]) is list:
                    for dt in row_new_fields[column]:
                        if type(dt) == dict and dt.get('property') in ['P569', 'P570']:
                            time_fields = True
                    if not time_fields:
                        array_diff = set(row_new_fields[column]) - set(claims)
                    else:
                        birth = wd_data['birth']
                        death = wd_data['death']
                        if type(row_new_fields[column]) is list:
                            if (len(birth) > 0):
                                for row_key, row in enumerate(row_new_fields[column]):
                                    if (row.get('property') in ['P569']):
                                        del(row_new_fields[column][row_key])
                            if (len(death) > 0):
                                for row_key, row in enumerate(row_new_fields[column]):
                                    if (row.get('property') in ['P570']):
                                        del(row_new_fields[column][row_key])
                        else:
                            pass
                        if len(row_new_fields[column]) == 0:
                            save_time = False
                if (type(row_new_fields[column]) == dict and row_new_fields[column].get('property') in ['P569', 'P570']):
                    birth = wd_data['birth']
                    death = wd_data['death']
                    if (len(birth) > 0 and len(row_new_fields[column]) > 0):
                        if (row_new_fields[column].get('property') in ['P569']):
                            row_new_fields[column] = ''
                    if (len(death) > 0 and len(row_new_fields[column]) > 0):
                        if (row_new_fields[column].get('property') in ['P570']):
                            row_new_fields[column] = ''
                    if type(row_new_fields[column]) != dict:
                        save_time = False
                    time_fields = True

                if (self.save
                        and (
                            (type(row_new_fields[column]) == str and row_new_fields[column] not in claims and len(row_new_fields[column]) > 0)
                            or
                            (type(row_new_fields[column]) == list and len(array_diff) > 0 and not time_fields)
                            or
                            (time_fields)
                        )
                    and save_time
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
                    elif time_fields:
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
            except KeyError as ke:
                log_with_date_time(str(ke))
                pass

    def set_nkcr_aut(self, nkcr_aut):
        """
        Sets the value for the nkcr_aut attribute. Stores the provided value in the
        corresponding attribute. The value of nkcr_aut should match the intended
        datatype or structure expected by the implementation.

        :param nkcr_aut: The value to set for the nkcr_aut attribute.
        :return: None
        """
        self.nkcr_aut = nkcr_aut

    def set_qid(self, qid):
        """
        Sets the value of the 'qid' attribute.

        This method assigns a new value to the 'qid' attribute of the instance.

        :param qid: The value to set as the new identifier
        :type qid: Any
        :return: None
        """
        self.qid = qid

    def set_item(self, item):
        """
        Sets the value of the item attribute.

        This method allows setting a new value for the item attribute. The value
        provided replaces the current value of the attribute.

        :param item: The new value to assign to the item attribute.
        :type item: Any
        :return: None
        """
        self.item = item

    def get_item(self)->ItemEntity:
        """
        Retrieves the item associated with the current instance.

        :return: The item associated with the instance.
        :rtype: ItemEntity
        """
        return self.item

    def set_row(self, row):
        """
        Sets the value of the 'row' attribute.

        This method assigns a given value to the 'row' attribute, allowing the instance
        to store or update its internal state.

        :param row: The new value to be assigned to the 'row' attribute.
        :type row: Any
        :return: None
        """
        self.row = row

    def set_enabled_columns(self, columns: dict):
        """
        Sets the enabled columns for a dataset or configuration.

        This method updates the internal state of the object with a new dictionary
        of enabled columns. Each key-value pair in the dictionary represents a
        specific column and its corresponding state or configuration.

        :param columns: A dictionary where keys are column names and values represent
            the enabled states or configurations for those columns.
        :type columns: dict
        :return: None
        """
        self.enabled_columns = columns

    def set_wbi(self, wbi):
        """
        Sets the value of the `wbi` attribute.

        This method allows assigning a new value to the `wbi` attribute, which may
        be used for storing or updating specific state within the object.

        :param wbi: The value to be assigned to the `wbi` attribute.
        :type wbi: Any
        """
        self.wbi = wbi

    def process_occupation_type(self, non_deprecated_items):
        """
        Process occupation type by verifying and updating data based on existing and
        provided non-deprecated items.

        :param non_deprecated_items: A dictionary containing non-deprecated items where
            keys are identifiers and values contain associated details.
        :return: None
        """
        nkcr_aut = self.nkcr_aut
        qid = self.qid
        item = self.item
        row = self.row
        # log.info('start_proc')
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
        # log.info('end_proc')

    def process_date_type(self, non_deprecated_items):
        """
        Processes date-related information based on provided non-deprecated items.

        This method evaluates and processes items by linking their `qid` (if available)
        to the existing data, and updates fields where necessary. It ensures that
        the provided data is consistent with existing records and modifies the
        current item or creates new fields in alignment with the logic for `qid`
        matching.

        :param non_deprecated_items: A dictionary of non-deprecated items, mapping
            identifiers (e.g., `nkcr_aut`) to their corresponding data entries. Each
            entry must include a `qid` field.
        """
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
