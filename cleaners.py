import logging
import re
from typing import Union, Any
from datetime import datetime
from wikibaseintegrator.datatypes import Item, ExternalID, Time, String
from wikibaseintegrator.wbi_enums import WikibaseTimePrecision

import config
from nkcr_exceptions import BadItemException

name_to_nkcr: dict = {}
language_dict: dict = {}

cachedData = {}

log = logging.getLogger(__name__)


def clean_last_comma(string: str) -> str:
    if string.endswith(','):
        return string[:-1]
    return string


def clean_qid(string: str) -> str:
    string = string.replace(')', '').replace('(', '')
    first_letter = string[0]
    if first_letter.upper() != 'Q':
        raise BadItemException(string)

    regex = r"Q[0-9]+"

    match = re.search(regex, string, re.IGNORECASE)
    if not match:
        raise BadItemException(string)

    return string


def prepare_orcid_from_nkcr(orcid: str, column) -> str:
    # https://pythonexamples.org/python-split-string-into-specific-length-chunks/
    orcid = orcid.replace(' ', '')
    regex = u"^(\d{4}-){3}\d{3}(\d|X)$"

    match = re.search(regex, orcid, re.IGNORECASE)
    if not match:
        return ''
    else:
        return orcid


def prepare_occupation_from_nkcr(occupation_string: str, column) -> Union[str, list]:
    nkcr_to_qid = name_to_nkcr
    occupations = []
    # log_with_date_time(occupation_string)
    if type(occupation_string) == str:
        if occupation_string.strip() == '':
            return occupations
        splitted_occupations = occupation_string.strip().split('|')

        try:
            # occupations = [nkcr_to_qid[occupation] for occupation in splitted_occupations]
            occupations = []
            for occupation in splitted_occupations:
                if occupation in nkcr_to_qid:
                    occupations.append(nkcr_to_qid[occupation])
                else:
                    log.warning('not found occupation: ' + occupation)
        except KeyError as e:
            log.warning('not found occupation: ' + str(e))
        # for occupation in splitted_occupations:
        #     try:
        #         occupation_qid = nkcr_to_qid[occupation]
        #         occupations.append(clean_qid(occupation_qid))
        #     except KeyError as e:
        #         print(e)
        #         pass
    return occupations

def prepare_language_from_nkcr(language_string: str, column) -> Union[str, list]:
    nkcr_to_qid = language_dict
    languages = []
    # log_with_date_time(occupation_string)
    if type(language_string) == str:
        if language_string.strip() == '':
            return languages
        splitted_languages = language_string.strip().split('$')

        try:
            # languages = [nkcr_to_qid[language] for language in splitted_languages]
            languages = []
            for language in splitted_languages:
                if language in nkcr_to_qid:
                    languages.append(nkcr_to_qid[language])
                else:
                    log.warning('not found language: ' + language)
        except KeyError as e:
            log.warning('not found language: ' + str(e))
        # for occupation in splitted_occupations:
        #     try:
        #         occupation_qid = nkcr_to_qid[occupation]
        #         occupations.append(clean_qid(occupation_qid))
        #     except KeyError as e:
        #         print(e)
        #         pass
    return languages

def prepare_places_from_nkcr(place_string: str, column) -> Union[str, list]:
    nkcr_to_qid = name_to_nkcr
    places: list = []
    # log_with_date_time(occupation_string)
    if type(place_string) == str:
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
            # places = [nkcr_to_qid[corrected_place] for corrected_place in corrected_splitted_places]
            places = []
            for corrected_place in corrected_splitted_places:
                if corrected_place in nkcr_to_qid:
                    places.append(nkcr_to_qid[corrected_place])
                else:
                    log.warning('not found place: ' + corrected_place)
        except KeyError as e:
            log.warning('not found place: ' + str(e))
        # for occupation in splitted_occupations:
        #     try:
        #         occupation_qid = nkcr_to_qid[occupation]
        #         occupations.append(clean_qid(occupation_qid))
        #     except KeyError as e:
        #         print(e)
        #         pass
    return places


def prepare_isni_from_nkcr(isni: str, column) -> str:
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

def prepare_date_from_date_field(date: Any, column) -> Union[None, Time]:
    #První místo kam se podívat: Pole 046f (narození)/046g (úmrtí)
    #Splitnout výraz YYYYMMDD na YYYY-MM-DD. Příklad záznam xx0194367.
    #19420427 na YYYY-MM-DD
    #1942 na YYYY
    prop = config.Config.properties.get(column, 'P569')
    if isinstance(date, str):
        if len(date) == 8:
            date_str = f"{date[0:4]}-{date[4:6]}-{date[6:8]}"
            str_time = '+' + date_str + 'T00:00:00Z'
            return Time(time=str_time, prop_nr=prop, precision=WikibaseTimePrecision.DAY)
        if len(date) == 4:
            str_time = '+' + date + '-01-01T00:00:00Z'
            return Time(time=str_time, prop_nr=prop, precision=WikibaseTimePrecision.YEAR)
    return None

def prepare_date_from_description(description: str, column) -> Union[list[Time], None]:
    dates = []
    # Regex for narozena/narozen
    birth_matches = re.finditer(r'\b(narozen|narozena)\b', description, re.IGNORECASE)
    for birth_match in birth_matches:
        substring = description[birth_match.end():]
        date_match = re.search(r'(\d{1,2})\.\s*(\d{1,2})\.\s*(\d{4})', substring)
        if date_match:
            day, month, year = date_match.groups()
            try:
                date_obj = datetime(int(year), int(month), int(day))
                if date_obj.year > 1600:
                    str_time = date_obj.strftime('+%Y-%m-%dT00:00:00Z')
                    dates.append(Time(time=str_time, prop_nr='P569', precision=WikibaseTimePrecision.DAY))
            except ValueError:
                pass
        else:
            date_match = re.search(r'\b(\d{4})\b', substring)
            if date_match:
                year = date_match.group(1)
                try:
                    year_int = int(year)
                    if 1600 < year_int <= datetime.now().year:
                        str_time = '+' + year + '-01-01T00:00:00Z'
                        dates.append(Time(time=str_time, prop_nr='P569', precision=WikibaseTimePrecision.YEAR))
                except ValueError:
                    pass

    # Regex for zemrel/zemrela
    death_matches = re.finditer(r'\b(zemřel|zemřela)\b', description, re.IGNORECASE)
    for death_match in death_matches:
        substring = description[death_match.end():]
        date_match = re.search(r'(\d{1,2})\.\s*(\d{1,2})\.\s*(\d{4})', substring)
        if date_match:
            day, month, year = date_match.groups()
            try:
                date_obj = datetime(int(year), int(month), int(day))
                if date_obj.year > 1600:
                    str_time = date_obj.strftime('+%Y-%m-%dT00:00:00Z')
                    dates.append(Time(time=str_time, prop_nr='P570', precision=WikibaseTimePrecision.DAY))
            except ValueError:
                pass
        else:
            date_match = re.search(r'\b(\d{4})\b', substring)
            if date_match:
                year = date_match.group(1)
                try:
                    year_int = int(year)
                    if 1600 < year_int <= datetime.now().year:
                        str_time = '+' + year + '-01-01T00:00:00Z'
                        dates.append(Time(time=str_time, prop_nr='P570', precision=WikibaseTimePrecision.YEAR))
                except ValueError:
                    pass

    return dates if dates else None

def prepare_column_of_content(column: str, row) -> Union[str, Union[str, list]]:
    column_to_method_dictionary = {
        '0247a-isni': prepare_isni_from_nkcr,
        '0247a-orcid': prepare_orcid_from_nkcr,
        '374a': prepare_occupation_from_nkcr,
        '372a': prepare_occupation_from_nkcr,
        '370a': prepare_places_from_nkcr,
        '370b': prepare_places_from_nkcr,
        '370f': prepare_places_from_nkcr,
        '377a': prepare_language_from_nkcr,
        '046f': prepare_date_from_date_field,
        '046g': prepare_date_from_date_field,
        '678a': prepare_date_from_description,
    }
    return column_to_method_dictionary[column](row[column], column)


def resolve_exist_claims(column: str, wd_data: dict) -> Union[str, list]:
    claims = []
    if column == '0247a-isni':
        claims = wd_data['isni']
    if column == '0247a-orcid':
        claims = wd_data['orcid']
    if column == '374a':
        claims = wd_data['occup']
    if column == '372a':
        claims = wd_data['field']
    if column == '370a':
        claims = wd_data['birth']
    if column == '370b':
        claims = wd_data['death']
    if column == '046f':
        claims = wd_data['birth']
    if column == '046g':
        claims = wd_data['death']
    if column == '678a':
        claims = wd_data.get('birth', []) + wd_data.get('death', [])
    if column == '370f':
        claims = wd_data['work']
    if column == '377a':
        claims = wd_data['language']
    return claims
