class Config:
    user_name: str = 'Frettiebot'
    debug: bool = False
    count_first_step: int = 0
    count_second_step: int = 0

    occupations_not_used_in_occupation_because_is_in_function: list[str] = [
        'Q103163',
        'Q29182',
        'Q611644',
        'Q102039658',
        'Q212071',
        'Q22132694',
        'Q63970319',
        'Q11165895',
        'Q83460',
        'Q97772204',
        'Q45722',
        'Q19546',
        'Q486839',
        'Q15686806',
        'Q12319698',
        'Q23305046',
        'Q83307',

    ]

    fields_of_work_not_used_in_field_of_work_because_is_not_ok: list[str] = [
        'Q11214'
    ]

    instances_not_possible_for_nkcr: list[str] = [
        'Q13442814',
        'Q637866',
        'Q13433827',
        'Q571',
        'Q14756018',
        'Q16684349',
    ]

    property_occupation = 'P106'

    # isni = P213
    # orcid = P496
    properties: dict[str, str] = {
        '0247a-isni': 'P213',
        '0247a-orcid': 'P496',
        '374a': 'P106',
        '372a': 'P101',
        '370a': 'P19',
        '370b': 'P20',
        '370f': 'P937',
        '377a': 'P1412',
    }

