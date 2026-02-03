import pytest
from wikibaseintegrator import wbi_login, WikibaseIntegrator
from wikibaseintegrator.wbi_config import config as wbi_config

from cleaners import prepare_column_of_content
from property_processor.property_processor_374a import PropertyProcessor374a
from property_processor.property_processor_one import PropertyProcessorOne
from tools import *

wbi_config['USER_AGENT'] = 'Frettiebot/1.0 (https://www.wikidata.org/wiki/User:Frettiebot)'
bot_password = get_bot_password('bot_password')
login_instance = wbi_login.Login(user='Frettiebot', password=bot_password)
wbi = WikibaseIntegrator(login=login_instance, is_bot=True)
@pytest.mark.parametrize(
    "qid,property_for_new_field,column,value,aut",
    [
        ('Q555628', 'P213', '0247a-isni', '1234567890123456', 'jn19990009817'),
        ('Q555628', 'P213', '0247a-isni', '0000000114667884', 'jn19990009817'),
        ('Q555628', 'P496', '0247a-orcid', '0000-0002-1825-0097', 'jn19990009817'),
    ],
)
def test_process_one(qid, property_for_new_field, column, value, aut):
    item_new_field = wbi.item.get(qid)
    claim_direct_from_wd = get_claim_from_item_by_property_wbi(item_new_field,
                                                           property_for_new_field)
    row_new_fields = {
        column: value,
        '_id': aut
    }
    row_new_fields[column] = prepare_column_of_content(column, row_new_fields)
    property_processor = PropertyProcessorOne(wbi=wbi,
                                              property_for_new_field=property_for_new_field, column=column,
                                              row_new_fields=row_new_fields, claim_direct_from_wd=claim_direct_from_wd,
                                              item_new_field=item_new_field)
    property_processor.process()

    claims_final = property_processor.item_new_field.claims.get(property_for_new_field)

    result = False
    for claim in claims_final:
        if (claim.mainsnak.datavalue['value'] == value):
            result = True

    assert result


@pytest.mark.parametrize(
    "qid,property_for_new_field,column,value,aut",
    [
        ('Q555628', 'P106', '374a', ['Q42973'], 'jn19990009817'),
        ('Q555628', 'P106', '374a', ['Q42973', 'Q5482740'], 'jn19990009817'),
        ('Q555628', 'P106', '374a', ['Q42973', 'Q33999'], 'jn19990009817'),
        ('Q555628', 'P106', '374a', ['Q33999'], 'jn19990009817'),
    ],
)
def test_process_occupation(qid, property_for_new_field, column, value, aut):
    item_new_field = wbi.item.get(qid)
    claim_direct_from_wd = get_claim_from_item_by_property_wbi(item_new_field,
                                                           property_for_new_field)

    row_new_fields = {
        column: value,
        '_id': aut
    }

    property_processor = PropertyProcessor374a(wbi=wbi, property_for_new_field=property_for_new_field, column=column, row_new_fields=row_new_fields, claim_direct_from_wd=claim_direct_from_wd, item_new_field=item_new_field)
    property_processor.process()

    claims_final = property_processor.item_new_field.claims.get(property_for_new_field)

    result = False
    for claim in claims_final:
        if (claim.mainsnak.datavalue['value']['id'] in value):
            result = True

    assert result

