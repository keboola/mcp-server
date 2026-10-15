from keboola_mcp_server.rls import RlsAccessDenied, RlsError
from keboola_mcp_server.rls_audit import refusal_code, subject_id


def test_subject_id_folds_ascii_only_like_rule_matching() -> None:
    assert subject_id('User@Example.com', 'k') == subject_id('user@example.com', 'k')
    # The Kelvin sign is a different principal, so it must be a different subject.
    assert subject_id('\u212a@example.com', 'k') != subject_id('k@example.com', 'k')
    assert subject_id('user@example.com', 'other') != subject_id('user@example.com', 'k')
    assert len(subject_id('user@example.com', 'k')) == 16


def test_refusal_code_is_one_of_the_documented_codes() -> None:
    assert refusal_code(RlsAccessDenied('secret detail')) == 'ACCESS_DENIED'
    assert refusal_code(RlsError('secret detail')) == 'UNSUPPORTED'
    assert refusal_code(ValueError('secret detail')) == 'UNSUPPORTED'
