import cleaners
import pytest

import nkcr_exceptions

qids = {'Q123', '(Q123', '123', 'Q123)'}


@pytest.mark.parametrize("qid", qids)
def test_clean_qid(qid):
    try:
        assert cleaners.clean_qid(qid) == 'Q123'
    except nkcr_exceptions.BadItemException:
        with pytest.raises(nkcr_exceptions.BadItemException):
            cleaners.clean_qid(qid)


@pytest.mark.parametrize(
    "orcid,result_orcid",
    [
        ('0000-0002-1825-0097', '0000-0002-1825-0097'),
        ('0000-0002-1825-009X', '0000-0002-1825-009X'),
        ('0000000218250097', ''),
        ('0000-0002', ''),
    ],
)
def test_prepare_orcid_from_nkcr(orcid, result_orcid):
    assert cleaners.prepare_orcid_from_nkcr(orcid) == result_orcid


@pytest.mark.parametrize(
    "clean,result_clean",
    [
        ('Ahoj,', 'Ahoj'),
        ('0000-0002-1825-009X,', '0000-0002-1825-009X'),
        ('ahoj ,', 'ahoj '),
    ],
)
def test_clean_last_comma(clean, result_clean):
    assert cleaners.clean_last_comma(clean) == result_clean


@pytest.mark.parametrize(
    "isni,result_isni",
    [
        ('0000 0002 1825 0097', '0000 0002 1825 0097'),
        ('0000000218250097', '0000 0002 1825 0097'),
        ('000000021825009X', ''),
        ('000000021825', ''),
        ('0000-0002', ''),
    ],
)
def test_prepare_isni_from_nkcr(isni, result_isni):
    assert cleaners.prepare_isni_from_nkcr(isni) == result_isni
