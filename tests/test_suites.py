"""Suites écrites en style script, appelées depuis pytest."""


def test_primitives_t3_autotests():
    import primitives_t3

    primitives_t3.autotests()


def test_corpus_proprietes():
    import test_proprietes

    assert test_proprietes.main() == 0


def test_donnees_mbo_autotests():
    import donnees_mbo

    donnees_mbo.autotests()


def test_mesures_sip_selfcheck():
    import mesures_sip

    mesures_sip.selfcheck()


def test_build_univers_autotests():
    import build_univers

    build_univers.autotests()


def test_tirer_halts_selftest():
    import tirer_halts

    tirer_halts.selftest()


def test_invalidation_p16():
    import invalidation_p16

    invalidation_p16.test_dp_abs_coherent_avec_p16()
