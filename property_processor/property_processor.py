from typing import Union

from wikibaseintegrator import WikibaseIntegrator
from tools import *


class BasePropertyProcessor:
    """
    Handles processing and management of Wikibase property data.

    This class provides functionality to manage and manipulate property data,
    including setting attributes and retrieving claims directly from Wikibase.
    It's designed to facilitate operations involving property handling, ensuring
    data alignment with Wikibase standards.

    :ivar property_for_new_field: Represents the property targeted for the new field.
    :type property_for_new_field: Union[str, None]
    :ivar item_new_field: Represents the item entity corresponding to the new field.
    :type item_new_field: Union[ItemEntity, None]
    :ivar column: Specifies the column name associated with the operation.
    :type column: Union[str, None]
    :ivar row_new_fields: Stores the new field data in a dictionary structure.
    :type row_new_fields: dict
    :ivar wbi: Instance of WikibaseIntegrator for facilitating data operations.
    :type wbi: WikibaseIntegrator
    :ivar claim_direct_from_wd: List of claims directly retrieved from Wikibase.
    """
    def __init__(self, column, row_new_fields, claim_direct_from_wd, property_for_new_field, item_new_field, wbi):
        """
        Initializes the class with required attributes.

        :param column: The name of the column associated with this instance.
        :type column: str, optional
        :param row_new_fields: A dictionary containing the new field mappings for a row.
        :type row_new_fields: dict
        :param claim_direct_from_wd: The claim directly obtained from Wikidata.
        :type claim_direct_from_wd: Any
        :param property_for_new_field: The property identifier for the new field, if applicable.
        :type property_for_new_field: str, optional
        :param item_new_field: An instance of ItemEntity representing the new field, if applicable.
        :type item_new_field: ItemEntity, optional
        :param wbi: An instance of the WikibaseIntegrator used for interacting with the Wikibase system.
        :type wbi: WikibaseIntegrator
        """
        self.property_for_new_field: Union[str, None] = property_for_new_field
        self.item_new_field: Union[ItemEntity, None] = item_new_field
        self.column: Union[str, None] = column
        self.row_new_fields: dict = row_new_fields
        self.wbi: WikibaseIntegrator = wbi
        self.claim_direct_from_wd = claim_direct_from_wd

    def set_claim_direct_from_wd(self, claim_direct_from_wd):
        """
        Sets the claim_direct_from_wd attribute with the provided value.

        This method assigns the input value to the claim_direct_from_wd attribute of
        the instance. The attribute is utilized for enabling or disabling a certain
        functionality related to claim behaviour within the system.

        :param claim_direct_from_wd: Boolean value indicating whether claims are
            directly fetched from the specified WD source.
        :type claim_direct_from_wd: bool
        :return: None
        """
        self.claim_direct_from_wd = claim_direct_from_wd

    def set_row_new_fields(self, row_new_fields: dict):
        """
        Sets the new fields for a row.

        This method is used to update the `row_new_fields` attribute of the object with a
        given dictionary of new fields. The provided dictionary is expected to replace the
        current set of row fields fully.

        :param row_new_fields: Dictionary containing the new row fields to be set.
        :type row_new_fields: dict
        """
        self.row_new_fields = row_new_fields

    def set_column(self, column: str):
        """
        Sets the column attribute for the instance.

        This method allows setting the value of the `column` attribute, which is
        used to store a string representing the name of a specific column.

        :param column: The name of the column to set.
        :type column: str
        """
        self.column = column

    def set_item_new_field(self, item_new_field: pywikibot.ItemPage):
        """
        Sets the `item_new_field` attribute for the current instance.

        This method assigns a new value to the `item_new_field` attribute. The
        attribute is expected to be of type `pywikibot.ItemPage`.

        :param item_new_field: The new value to assign to the `item_new_field`
            attribute.
        :type item_new_field: pywikibot.ItemPage
        """
        self.item_new_field = item_new_field

    def set_property_for_new_field(self, property_for_new_field: str):
        """
        Sets the property_for_new_field attribute of the instance.

        :param property_for_new_field: New value to set for the property.
        :type property_for_new_field: str
        :return: None
        """
        self.property_for_new_field = property_for_new_field

    def get_qid_claims_direct_from_wd_wbi(self) -> list:
        """
        Retrieves a list of claims directly from a source.

        This method processes the `claim_direct_from_wd` attribute to extract and
        return a list of claims. Each claim is appended to a results list if certain
        criteria are met.

        :returns: A list of claims extracted from the source.
        :rtype: list
        """
        qid_claims_direct_from_wd = []
        for cdfwd in self.claim_direct_from_wd:
            # if type(cdfwd) is pywikibot.ItemPage:
            qid_claims_direct_from_wd.append(cdfwd)
        return qid_claims_direct_from_wd

    def get_qid_claims_direct_from_wd_by_precision(self, property) -> dict:
        """
        Retrieves claims directly from Wikidata by matching a specific property and categorizes
        them based on their precision.

        :param property: The property to match with claims in the Wikidata dictionary.
        :type property: str
        :return: A dictionary where the keys are precision values and the values are
                 the corresponding claims that match the specified property.
        :rtype: dict
        """
        qid_claims_direct_from_wd = {}
        for cdfwd in self.claim_direct_from_wd:
            # if type(cdfwd) is pywikibot.ItemPage:
            if (cdfwd.get('property') == property):
                qid_claims_direct_from_wd[cdfwd.get('precision')] = cdfwd
        return qid_claims_direct_from_wd
