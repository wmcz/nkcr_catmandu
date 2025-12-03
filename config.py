class Config:
    user_name: str = 'Frettiebot'
    debug: bool = False
    use_json_database: bool = True
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
        'Q3400985',
        'Q98834046',
        'Q2963013',

    ]

    fields_of_work_not_used_in_field_of_work_because_is_not_ok: list[str] = [
        'Q11214'
    ]

    instances_not_possible_for_nkcr: list[str] = [
        'Q571',
        'Q8436',
        'Q637866',
        'Q3046146',
        'Q10648343',
        'Q13417114',
        'Q13433827',
        'Q13442814',
        'Q14756018',
        'Q16017119',
        'Q16334295',
        'Q16684349',
        'Q54982412',
        'Q2088357',
        'Q1141470',
        'Q19389637',
        'Q484170',
        'Q875538',
        'Q4167410',
        'Q187685',
        'Q3305213'
    ]

    qid_blacklist:list[str] = [
        'Q102247991',
        'Q59584399',
        'Q29020960',
        'Q744961',
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
        '046f': 'P569',
        '046g': 'P570',
        '678a': ['P569','P570'],
        '_id': 'P691',
    }
