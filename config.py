class Config:
    """
    Represents the configuration settings for the application.

    This class serves as a centralized location for storing various configuration
    and data parameters. It includes options for debugging, database preferences,
    counters for specific steps, blacklists, and mappings of specific properties.

    :ivar user_name: The default username for the application.
    :ivar debug: A flag indicating whether the application is in debug mode.
    :ivar use_json_database: A flag indicating whether a JSON database is used.
    :ivar count_first_step: Counter for the first step in a specific process.
    :ivar count_second_step: Counter for the second step in a specific process.
    :ivar occupations_not_used_in_occupation_because_is_in_function: A list of
        occupation-related QIDs that are excluded because they are handled in
        another function.
    :ivar fields_of_work_not_used_in_field_of_work_because_is_not_ok: A list of
        field-of-work QIDs that are excluded as they are not acceptable.
    :ivar instances_not_possible_for_nkcr: A list of instance-related QIDs that
        are not considered possible for NKCR.
    :ivar qid_blacklist: A list of QIDs blacklisted for specific operations.
    :ivar property_occupation: The property key used for defining an occupation (P106).
    :ivar properties: A mapping of application-specific keys to their property IDs
        in the database. Keys denote application-specific identifiers, while the
        values point to database property keys.
    """
    user_name: str = 'Frettiebot'
    debug: bool = False
    use_json_database: bool = False
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
        'Q42603'

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
        'Q3305213',
        'Q482994'
    ]

    qid_blacklist:list[str] = [
        'Q102247991',
        'Q59584399',
        'Q29020960',
        'Q744961',
        'Q12023613',
        'Q744961',
        'Q9531806',
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
