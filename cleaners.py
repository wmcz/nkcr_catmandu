import re
from typing import Union

from nkcr_exceptions import BadItemException

name_to_nkcr: dict = {}


def clean_last_comma(string: str) -> str:
    if string.endswith(','):
        return string[:-1]
    return string


def clean_qid(string: str) -> str:
    string = string.replace(')', '').replace('(', '')
    first_letter = string[0]
    if first_letter.upper() != 'Q':
        raise BadItemException(string)

    return string


def prepare_orcid_from_nkcr(orcid: str) -> str:
    # https://pythonexamples.org/python-split-string-into-specific-length-chunks/
    orcid = orcid.replace(' ', '')
    regex = u"^(\d{4}-){3}\d{3}(\d|X)$"

    match = re.search(regex, orcid, re.IGNORECASE)
    if not match:
        return ''
    else:
        return orcid


def prepare_occupation_from_nkcr(occupation_string: str) -> Union[str, list]:
    nkcr_to_qid = name_to_nkcr
    occupations = []
    # log_with_date_time(occupation_string)
    if type(occupation_string) == str:
        if occupation_string.strip() == '':
            return occupations
        splitted_occupations = occupation_string.strip().split('|')

        try:
            occupations = [nkcr_to_qid[occupation] for occupation in splitted_occupations]
        except KeyError as e:
            print(e)
        # for occupation in splitted_occupations:
        #     try:
        #         occupation_qid = nkcr_to_qid[occupation]
        #         occupations.append(clean_qid(occupation_qid))
        #     except KeyError as e:
        #         print(e)
        #         pass
    return occupations

def prepare_places_from_nkcr(place_string: str) -> Union[str, list]:
    nkcr_to_qid = name_to_nkcr
    places: list = []
    # log_with_date_time(occupation_string)
    if type(place_string) == str:
        if place_string.strip() == '':
            return places
        splitted_places = place_string.strip().split('$')
        regex = r"(.*?),\W*(.*)"
        subst = "\\1 (\\2)"
        corrected_splitted_places: list = []
        for place in splitted_places:
            # You can manually specify the number of replacements by changing the 4th argument
            result = re.sub(regex, subst, place.strip(), 0, re.MULTILINE)

            if result:
                corrected_splitted_places.append(result)
            else:
                corrected_splitted_places.append(place)

        try:
            places = [nkcr_to_qid[corrected_place] for corrected_place in corrected_splitted_places]
        except KeyError as e:
            print(e)
        # for occupation in splitted_occupations:
        #     try:
        #         occupation_qid = nkcr_to_qid[occupation]
        #         occupations.append(clean_qid(occupation_qid))
        #     except KeyError as e:
        #         print(e)
        #         pass
    return places


def prepare_isni_from_nkcr(isni: str) -> str:
    # https://pythonexamples.org/python-split-string-into-specific-length-chunks/
    isni = isni.replace(' ', '')
    regex = u"^(\d{16})$"

    match = re.search(regex, isni, re.IGNORECASE)
    if not match:
        return ''
    else:
        n = 4
        str_chunks = [isni[i:i + n] for i in range(0, len(isni), n)]
        return ' '.join(str_chunks)


def prepare_column_of_content(column: str, row) -> Union[str, Union[str, list]]:
    column_to_method_dictionary = {
        '0247a-isni': prepare_isni_from_nkcr,
        '0247a-orcid': prepare_orcid_from_nkcr,
        '374a': prepare_occupation_from_nkcr,
        '372a': prepare_occupation_from_nkcr,
        '370a': prepare_places_from_nkcr,
        '370b': prepare_places_from_nkcr,
        '370f': prepare_places_from_nkcr,
    }
    return column_to_method_dictionary[column](row[column])


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
    if column == '370f':
        claims = wd_data['work']
    return claims
