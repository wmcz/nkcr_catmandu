import logging
import re
from typing import Union, Any, TYPE_CHECKING
from datetime import datetime
from wikibaseintegrator.datatypes import Item, ExternalID, Time, String
from wikibaseintegrator.wbi_enums import WikibaseTimePrecision

import config
from nkcr_exceptions import BadItemException

if TYPE_CHECKING:
    from context import PipelineContext

log = logging.getLogger(__name__)


def clean_last_comma(string: str) -> str:
    """
    Removes the trailing comma from the end of a given string, if present.

    This function checks if the input string ends with a comma. If a trailing comma
    is found, it removes the comma and returns the modified string. If the string
    does not end with a comma, the function returns the string unchanged.

    :param string: The input string to process.
    :type string: str
    :return: The string with the trailing comma removed, if it was present.
    :rtype: str
    """
    if string.endswith(','):
        return string[:-1]
    return string


def clean_qid(string: str) -> str:
    """
    Cleans a QID string by removing parentheses and verifying its validity. A QID is ensured to
    start with the letter 'Q', followed by one or more digits, and matches a specific regex pattern.

    :param string: The input QID string that needs to be cleaned and validated.
    :type string: str
    :return: The cleaned QID string if valid.
    :rtype: str
    :raises BadItemException: If the input string does not start with 'Q' or if it doesn't match
        the expected QID pattern.
    """
    string = string.replace(')', '').replace('(', '')
    first_letter = string[0]
    if first_letter.upper() != 'Q':
        raise BadItemException(string)

    regex = r"^Q[0-9]+$"

    match = re.search(regex, string, re.IGNORECASE)
    if not match:
        raise BadItemException(string)

    return string


def prepare_orcid_from_nkcr(orcid: str, column) -> str:
    """
    Prepare a properly formatted ORCID string from the given input by validating and
    removing unwanted spaces.

    :param orcid: The input string representing the ORCID ID.
    :type orcid: str
    :param column: Unused input parameter that serves as a placeholder.
    :return: A correctly formatted ORCID string if validation succeeds,
        otherwise an empty string.
    :rtype: str
    """
    # https://pythonexamples.org/python-split-string-into-specific-length-chunks/
    orcid = orcid.replace(' ', '')
    regex = u"^(\d{4}-){3}\d{3}(\d|X)$"

    match = re.search(regex, orcid, re.IGNORECASE)
    if not match:
        return ''
    else:
        return orcid


def prepare_occupation_from_nkcr(occupation_string: str, column, context: 'PipelineContext') -> Union[str, list]:
    """
    Transforms an occupation string by mapping it to its corresponding identifier(s) based on the
    context's name_to_nkcr dictionary. This function processes a pipe-separated string of occupations and
    returns a list of identifiers for valid occupations.

    :param occupation_string: A pipe-separated string of occupations to be transformed.
    :param column: An unused parameter included in the function signature.
    :param context: Pipeline context containing lookup dictionaries and tracking state.
    :return: A list of identifiers (QIDs) corresponding to the valid occupations found in the input
        string. Returns an empty list if the input string is empty or if no valid occupations are found.
    """
    nkcr_to_qid = context.name_to_nkcr
    occupations = []
    if isinstance(occupation_string, str):
        if occupation_string.strip() == '':
            return occupations
        splitted_occupations = occupation_string.strip().split('|')

        try:
            occupations = []
            for occupation in splitted_occupations:
                occupation = clean_last_comma(occupation)
                if occupation in nkcr_to_qid:
                    occupations.append(nkcr_to_qid[occupation])
                else:
                    context.log_not_found_occupation(occupation)
                    log.warning('not found occupation: ' + occupation)
        except KeyError as e:
            occupation_key = e.args[0]
            context.log_not_found_occupation(occupation_key)
            log.warning('not found occupation: ' + str(e))
    return occupations

def prepare_language_from_nkcr(language_string: str, column, context: 'PipelineContext') -> Union[str, list]:
    """
    Prepares a list of languages mapped to a specific identifier based on the input string
    and the context's language_dict mapping dictionary. The input string is split by a delimiter,
    and each resulting language is checked against the dictionary. If a match exists,
    it is appended to the result list. If no match exists, a warning is logged.

    :param language_string: The input string containing language identifiers, separated
        by a `$` delimiter.
    :param column: Unused parameter, included for potential future use or interface
        consistency.
    :param context: Pipeline context containing lookup dictionaries.
    :return: A list of identifiers corresponding to the languages found in the input
        string, or an empty list if no valid languages are found.
    """
    nkcr_to_qid = context.language_dict
    languages = []
    if isinstance(language_string, str):
        if language_string.strip() == '':
            return languages
        splitted_languages = language_string.strip().split('$')

        try:
            languages = []
            for language in splitted_languages:
                if language in nkcr_to_qid:
                    languages.append(nkcr_to_qid[language])
                else:
                    log.warning('not found language: ' + language)
        except KeyError as e:
            log.warning('not found language: ' + str(e))
    return languages

def prepare_places_from_nkcr(place_string: str, column, context: 'PipelineContext') -> Union[str, list]:
    """
    Prepares a list of normalized place identifiers (QIDs) based on a provided place string. The function
    processes the input to handle specific delimiters and applies corrections to ensure proper formatting.
    It then attempts to map the processed place names to corresponding QIDs using the context's name_to_nkcr
    mapping.

    :param place_string: A string containing place names, potentially separated by delimiters.
                         Place names might need normalization for proper identification.
    :type place_string: str
    :param column: Additional context parameter (not currently used in function logic).
    :param context: Pipeline context containing lookup dictionaries and tracking state.
    :return: A list containing the QIDs of the places found in the input string, or an empty list
             if none are found or the input is invalid.
    :rtype: list
    """
    nkcr_to_qid = context.name_to_nkcr
    places: list = []
    if isinstance(place_string, str):
        if place_string.strip() == '':
            return places
        if ('|' in place_string and '$' not in place_string):
            splitted_places = place_string.strip().split('|')
        else:
            splitted_places = place_string.strip().split('$')
        regex = r"(.*?),\W*(.*)"
        subst = "\\1 (\\2)"
        corrected_splitted_places: list = []
        for place in splitted_places:
            if (place.strip().find('(') != -1):
                corrected_splitted_places.append(place)
            else:
                result = re.sub(regex, subst, place.strip(), 0, re.MULTILINE)

                if result:
                    corrected_splitted_places.append(result)
                else:
                    corrected_splitted_places.append(place)

        try:
            places = []
            for corrected_place in corrected_splitted_places:
                if corrected_place in nkcr_to_qid:
                    places.append(nkcr_to_qid[corrected_place])
                else:
                    context.log_not_found_place(corrected_place)
                    log.warning('not found place: ' + corrected_place)
        except KeyError as e:
            context.log_not_found_place(corrected_place)
            log.warning('not found place: ' + str(e))
    return places


def prepare_isni_from_nkcr(isni: str, column) -> str:
    """
    Prepares an International Standard Name Identifier (ISNI) from a given string in
    NKCR format. The function removes spaces from the input string and verifies if
    it conforms to the correct ISNI format using a regular expression. If valid,
    it formats the string into 4-character chunks concatenated back together.

    :param isni: A string representing the ISNI in raw or NKCR format.
    :type isni: str
    :param column: An unused parameter for additional input (if needed in the future).
    :return: A reformatted ISNI string if valid, otherwise an empty string.
    :rtype: str
    """
    # https://pythonexamples.org/python-split-string-into-specific-length-chunks/
    isni = isni.replace(' ', '')
    regex = u"^(\d{16})$"

    match = re.search(regex, isni, re.IGNORECASE)
    if not match:
        return ''
    else:
        n = 4
        str_chunks = [isni[i:i + n] for i in range(0, len(isni), n)]
        return ''.join(str_chunks)

def create_time_dict(property, time_string: str, precision: int, timezone: int = 0, calendar_model: str = 'http://www.wikidata.org/entity/Q1985727') -> dict:
    """
    Creates a dictionary representing a time value with specific attributes.

    This function generates a dictionary structured to include attributes such
    as time string, timezone, precision, calendar model, and a property
    identifier. It is typically used for creating structured temporal data.

    :param property: A property used to describe the context or identifier
        associated with the time value.
    :param time_string: A string representing the time value in a specific format.
    :param precision: An integer indicating the precision level of the time value.
    :param timezone: An optional integer representing the timezone offset
        from UTC, defaulting to 0.
    :param calendar_model: An optional string representing the calendar model
        URL, defaulting to 'http://www.wikidata.org/entity/Q1985727'.
    :return: A dictionary containing the structured time value with its
        associated attributes.
    """
    return {
        'time' : time_string,
        'timezone' : timezone,
        'before' : 0,
        'after' : 0,
        'precision' : precision,
        'calendarmodel' : calendar_model,
        'property' : property,
    }

def prepare_date_from_date_field(date: Any, column) -> Union[None, dict]:
    """
    Prepares a date value from a given field by formatting it according to the Wikibase
    date format and returning a structured dictionary. Handles different date precisions
    (e.g., year, day) based on the input value length. Uses a property identifier based
    on the column input.

    :param date: The input date value to format. Can be a string in 'YYYY' or 'YYYYMMDD'
        format or any other type.
    :param column: The column identifier used to determine the corresponding property
        from the configuration.
    :return: A dictionary representing the formatted date with property identifier
        and precision, or None if the input is invalid.
    :rtype: Union[None, dict]
    """
    #První místo kam se podívat: Pole 046f (narození)/046g (úmrtí)
    #Splitnout výraz YYYYMMDD na YYYY-MM-DD. Příklad záznam xx0194367.
    #19420427 na YYYY-MM-DD
    #1942 na YYYY
    prop = config.Config.properties.get(column, 'P569')
    if isinstance(date, str):
        if len(date) == 8:
            date_str = f"{date[0:4]}-{date[4:6]}-{date[6:8]}"
            str_time = '+' + date_str + 'T00:00:00Z'
            # return Time(time=str_time, prop_nr=prop, precision=WikibaseTimePrecision.DAY)
            return create_time_dict(prop, str_time, WikibaseTimePrecision.DAY.value)
        if len(date) == 4:
            str_time = '+' + date + '-01-01T00:00:00Z'
            #return Time(time=str_time, prop_nr=prop, precision=WikibaseTimePrecision.YEAR)
            return create_time_dict(prop, str_time, WikibaseTimePrecision.YEAR.value)
    return None

def prepare_date_from_description(description: str, column) -> Union[list[dict], None]:
    """
    Extracts and processes date information from a given textual description. Identifies dates or years related to
    birth ("narozen/a") or death ("zemřel/a") patterns in the description and converts them into a structured list
    of dictionaries. The extracted date information is formatted according to the Wikibase Time format and includes
    corresponding properties for birth (`P569`) or death (`P570`). If no dates are found, the function returns None.

    :param description: The text containing potential date-related data.
    :type description: str
    :param column: Unused parameter included in the function signature for compatibility purposes.
    :return: A list of dictionaries containing processed dates and associated metadata, or None if no dates are identified.
    :rtype: Union[list[dict], None]
    """
    if not isinstance(description, str):
        return None

    dates = []

    # Pattern 1: narozen/a DD.MM.YYYY
    birth_date_matches = re.finditer(r'\b(narozen|narozena)\s+(\d{1,2})\.\s*(\d{1,2})\.\s*(\d{4})\b', description,
                                     re.IGNORECASE)
    for match in birth_date_matches:
        keyword, day, month, year = match.groups()
        try:
            date_obj = datetime(int(year), int(month), int(day))
            if date_obj.year > 1600:
                str_time = date_obj.strftime('+%Y-%m-%dT00:00:00Z')
                dates.append(create_time_dict('P569', str_time, WikibaseTimePrecision.DAY.value))
        except ValueError:
            pass

    # Pattern 2: narozen/a roku YYYY
    birth_year_matches = re.finditer(r'\b(narozen|narozena)\s+roku\s+(\d{4})\b', description, re.IGNORECASE)
    for match in birth_year_matches:
        keyword, year = match.groups()
        try:
            year_int = int(year)
            if 1600 < year_int <= datetime.now().year:
                str_time = '+' + year + '-01-01T00:00:00Z'
                dates.append(create_time_dict('P569', str_time, WikibaseTimePrecision.YEAR.value))
        except ValueError:
            pass

    # Pattern 3: zemřel/a DD.MM.YYYY
    death_date_matches = re.finditer(r'\b(zemřel|zemřela)\s+(\d{1,2})\.\s*(\d{1,2})\.\s*(\d{4})\b', description,
                                     re.IGNORECASE)
    for match in death_date_matches:
        keyword, day, month, year = match.groups()
        try:
            date_obj = datetime(int(year), int(month), int(day))
            if date_obj.year > 1600:
                str_time = date_obj.strftime('+%Y-%m-%dT00:00:00Z')
                dates.append(create_time_dict('P570', str_time, WikibaseTimePrecision.DAY.value))
        except ValueError:
            pass

    # Pattern 4: zemřel/a roku YYYY
    death_year_matches = re.finditer(r'\b(zemřel|zemřela)\s+roku\s+(\d{4})\b', description, re.IGNORECASE)
    for match in death_year_matches:
        keyword, year = match.groups()
        try:
            year_int = int(year)
            if 1600 < year_int <= datetime.now().year:
                str_time = '+' + year + '-01-01T00:00:00Z'
                dates.append(create_time_dict('P570', str_time, WikibaseTimePrecision.YEAR.value))
        except ValueError:
            pass

    return dates if dates else None

def prepare_column_of_content(column: str, row, context: 'PipelineContext') -> Union[str, Union[str, list]]:
    """
    Prepare content for a specified column based on its corresponding preparation method.

    This function maps a given column identifier to its respective preparation function
    and processes the data stored in the row for that column. The preparation logic
    depends on the function associated with the column.

    :param column: The column identifier that specifies the type of data in the row.
    :param row: The data row containing information for the specified column, typically
        in the form of a dictionary where column identifiers are keys.
    :param context: Pipeline context containing lookup dictionaries and tracking state.
    :return: The processed data for the specified column returned by its associated
        preparation function. The returned type may vary depending on the preparation
        function, which can return a `str`, a `list`, or other nested formats.
    """
    # Functions that don't need context
    simple_methods = {
        '0247a-isni': prepare_isni_from_nkcr,
        '0247a-orcid': prepare_orcid_from_nkcr,
        '046f': prepare_date_from_date_field,
        '046g': prepare_date_from_date_field,
        '678a': prepare_date_from_description,
    }
    # Functions that need context
    context_methods = {
        '374a': prepare_occupation_from_nkcr,
        '372a': prepare_occupation_from_nkcr,
        '370a': prepare_places_from_nkcr,
        '370b': prepare_places_from_nkcr,
        '370f': prepare_places_from_nkcr,
        '377a': prepare_language_from_nkcr,
    }

    if column in simple_methods:
        return simple_methods[column](row[column], column)
    elif column in context_methods:
        return context_methods[column](row[column], column, context)
    else:
        raise KeyError(f"Unknown column: {column}")


def resolve_exist_claims(column: str, wd_data: dict) -> Union[str, list]:
    """
    Resolves and retrieves claims associated with a specific column based on a given mapping in
    the provided wikidata dictionary.

    :param column: The key representing the specific claim to be resolved. Examples include
                   '0247a-isni', '0247a-orcid', '374a', etc.
    :type column: str
    :param wd_data: A dictionary containing various potential claims mapped to their respective keys.
                    Each key in the dictionary corresponds to a specific type of claim, such as
                    'isni', 'orcid', 'occup', and so on.
    :type wd_data: dict
    :return: A list of claims associated with the provided column key in the input dictionary. If no
             match exists for the given column, an empty list is returned.
    :rtype: Union[str, list]
    """
    column_to_key = {
        '0247a-isni': 'isni',
        '0247a-orcid': 'orcid',
        '374a': 'occup',
        '372a': 'field',
        '370a': 'birth',
        '370b': 'death',
        '370f': 'work',
        '377a': 'language',
        '046f': 'birth',
        '046g': 'death',
    }
    if column == '678a':
        return wd_data.get('birth', []) + wd_data.get('death', [])
    key = column_to_key.get(column)
    if key is not None:
        return wd_data[key]
    return []
