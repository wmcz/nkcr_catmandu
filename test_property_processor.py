import pywikibot
import pytest
from config import *
from property_processor import *
from pywikibot_extension import MyDataSite
from tools import *


@pytest.mark.parametrize(
    "qid,property_for_new_field,column,value,aut",
    [
        ('Q555628', 'P213', '0247a-isni', '1234 5678 9012 3456', 'jn19990009817'),
        ('Q555628', 'P213', '0247a-isni', '0000 0001 1466 7884', 'jn19990009817'),
        ('Q555628', 'P213', '0247a-orcid', '1234567890', 'jn19990009817'),
    ],
)
def test_process_one(qid, property_for_new_field, column, value, aut):
    repo = MyDataSite('wikidata', 'wikidata', user=Config.user_name)

    item_new_field = pywikibot.ItemPage(repo, qid)
    datas_from_wd = item_new_field.get(get_redirect=True)
    claim_direct_from_wd = get_claim_from_item_by_property(datas_from_wd,
                                                           property_for_new_field)
    row_new_fields = {
        column: value,
        '_id': aut
    }

    reset_debug_file()

    property_processor = PropertyProcessorOne()
    property_processor.set_repo(repo)
    property_processor.set_debug(True)
    property_processor.set_claim_direct_from_wd(claim_direct_from_wd)
    property_processor.set_property_for_new_field(property_for_new_field)
    property_processor.set_column(column)
    property_processor.set_item_new_field(item_new_field)
    property_processor.set_row_new_fields(row_new_fields)
    property_processor.process()

    reader = read_log()
    for line in reader:
        # print(line)
        if line['value'] != value:
            assert False
        if line['prop'] != property_for_new_field:
            assert False
        if line['item'] != qid:
            assert False
        assert True


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
    repo = MyDataSite('wikidata', 'wikidata', user=Config.user_name)

    item_new_field = pywikibot.ItemPage(repo, qid)
    datas_from_wd = item_new_field.get(get_redirect=True)
    claim_direct_from_wd = get_claim_from_item_by_property(datas_from_wd,
                                                           property_for_new_field)

    row_new_fields = {
        column: value,
        '_id': aut
    }

    reset_debug_file()

    property_processor = PropertyProcessor374a()
    property_processor.set_repo(repo)
    property_processor.set_debug(True)
    property_processor.set_claim_direct_from_wd(claim_direct_from_wd)
    property_processor.set_property_for_new_field(property_for_new_field)
    property_processor.set_column(column)
    property_processor.set_item_new_field(item_new_field)
    property_processor.set_row_new_fields(row_new_fields)
    property_processor.process()

    reader = read_log()
    for line in reader:
        # print(line)
        if line['value'] not in value:
            assert False
        if line['prop'] != property_for_new_field:
            assert False
        if line['item'] != qid:
            assert False
        assert True
