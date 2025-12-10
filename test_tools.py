import pytest
import pywikibot

import tools
from pywikibot_extension import MyDataSite
from config import *


@pytest.mark.parametrize(
    "item_qid,subclass_qid,return_value",
    [
        ('Q15407657', 'Q12737077', False),
        ('Q33999', 'Q12737077', True),
    ],
)
def test_is_item_subclass_of(item_qid, subclass_qid, return_value):
    repo = MyDataSite('wikidata', 'wikidata', user=Config.user_name)
    item = pywikibot.ItemPage(repo, item_qid)
    subclass = pywikibot.ItemPage(repo, subclass_qid)
    assert tools.is_item_subclass_of_wbi(item, subclass) == return_value


@pytest.mark.parametrize(
    "item_qid,property,min_size",
    [
        ('Q555628', 'P213', 1),  # miroslav donutil
        ('Q555628', 'P496', 1),
        ('Q555628', '374a', 1),
    ],
)
def test_get_claim_from_item_by_property(item_qid, property, min_size):
    repo = MyDataSite('wikidata', 'wikidata', user=Config.user_name)
    item = pywikibot.ItemPage(repo, item_qid)
    data = item.get(get_redirect=True)
    assert len(tools.get_claim_from_item_by_property(data, property)) > min_size
