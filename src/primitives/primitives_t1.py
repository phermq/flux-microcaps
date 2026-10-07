#!/usr/bin/env python3
"""Primitives de niveau 1 calculées sur la tape de trades SIP (T1).

P-01 (signe par test du tick), P-02 (part TRF), P-03 (sous-penny), P-04 (volume
relatif), P-13 (accélération de l'activité), P-14 (qualité de la tape) et P-16
(déplacement de prix), par ticker-jour puis agrégées par titre-mois. Les règles
de volume des conditions SIP, la table des venues TRF et la classification de
session sont importées de `src/donnees/mesures_sip.py`.

Python 3, bibliothèque standard uniquement.

Usage :
    python3 primitives_t1.py --test                  # tests unitaires, sans données
    python3 primitives_t1.py --run [--limit N]       # exécution sur data/data-t1/
"""

from __future__ import annotations

import argparse
import csv
import gzip
import json
import math
import statistics
import sys
import time
from collections import defaultdict
from datetime import datetime, time as dtime, timezone
from decimal import Decimal
from functools import lru_cache
from pathlib import Path

CODE_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import chemins  # noqa: E402
A_REF_DIR = chemins.REFERENCES
A_CRSP_DIR = chemins.DONNEES / "crsp"
DATA_T1_DIR = chemins.DONNEES / "data-t1"
SORTIES_DIR = chemins.SORTIES / "t1"

import mesures_sip as msip  # noqa: E402

# ---------------------------------------------------------------------------
# Constantes
# ---------------------------------------------------------------------------

# Venues TRF actives de p1-exchanges.json ; les venues 6 et 16 avaient fermé
# avant 2018, début de la période étudiée.
TRF_IDS = {4, 201, 202, 203}

# Codes d'enchère (ouverture, clôture, réouverture), catégorie sale_condition ;
# contrôlés contre p2-conditions.json par verify_auction_codes().
AUCTION_CODES = {9, 15, 16, 17, 18, 25, 28, 38, 55}

# Codes de correction dont la sémantique n'est pas documentée : les primitives
# de volume sont produites avec (incl810) et sans (excl810) ces trades.
CORRECTION_FLAGGED = {8, 10}

# Seule la condition Z (id 32) marque un trade réellement hors séquence.
Z_CONDITION_ID = 32

H_MIN = 20          # P-04 : historique minimal, en séances
WINDOW_DAYS = 63    # P-04 : fenêtre trailing, en séances


# ---------------------------------------------------------------------------
# Tables de référence
# ---------------------------------------------------------------------------

_TABLES_CACHE: dict | None = None


def load_tables() -> dict:
    global _TABLES_CACHE
    if _TABLES_CACHE is not None:
        return _TABLES_CACHE
    p1 = msip.load_json(A_REF_DIR / "p1-exchanges.json")
    p2 = msip.load_json(A_REF_DIR / "p2-conditions.json")
    volume_rules = msip.load_volume_rules(p2)
    active_trf = msip.load_active_trf_ids(p1)
    assert set(active_trf.keys()) == TRF_IDS, (
        f"venues TRF de p1-exchanges.json {sorted(active_trf)} != TRF_IDS {sorted(TRF_IDS)}"
    )
    _TABLES_CACHE = {"p1": p1, "p2": p2, "volume_rules": volume_rules, "active_trf": active_trf}
    return _TABLES_CACHE


def verify_auction_codes(p2: dict) -> None:
    """Vérifie que chaque code d'AUCTION_CODES existe dans la catégorie
    sale_condition (un même id peut désigner autre chose dans une autre catégorie)."""
    by_cat_id = {}
    for e in p2["results"]:
        by_cat_id[(e["type"], e["id"])] = e
    for cid in AUCTION_CODES:
        entry = by_cat_id.get(("sale_condition", cid))
        assert entry is not None, f"AUCTION_CODES id={cid} absent de la catégorie sale_condition"


# ---------------------------------------------------------------------------
# Prédicats par trade (P-02 / P-03 / P-04 / P-14)
# ---------------------------------------------------------------------------

def is_trf(trade: dict) -> bool:
    """Un trade est TRF dès que trf_id est présent, quelle que soit sa valeur :
    les trades de 2018 portent trf_id=12, absent de TRF_IDS. Même critère que
    mesures_sip.is_hors_bourse."""
    return msip.is_hors_bourse(trade)


def is_auction(trade: dict) -> bool:
    """Vrai si au moins une condition du trade appartient à AUCTION_CODES."""
    return any(c in AUCTION_CODES for c in (trade.get("conditions") or []))


def has_flagged_correction(trade: dict) -> bool:
    return trade.get("correction") in CORRECTION_FLAGGED


def counts_to_volume(trade: dict, volume_rules: dict[int, bool], unknown_ids: set[int]) -> bool:
    """Un trade compte au volume si toutes ses conditions ont updates_volume
    vrai ; une condition inconnue est comptée et enregistrée dans unknown_ids.
    Les trades de taille nulle sont exclus (et dénombrés par P-14)."""
    if trade.get("size", 0) == 0:
        return False
    return msip.counts_to_volume(trade, volume_rules, unknown_ids)


class NA:
    """Valeur manquante portant un code de raison, utilisée à la place de 0 ou ±inf."""

    __slots__ = ("code",)

    def __init__(self, code: str):
        self.code = code

    def __repr__(self):
        return f"NA({self.code})"

    def __eq__(self, other):
        return isinstance(other, NA) and self.code == other.code

    def __bool__(self):
        return False


def is_na(x) -> bool:
    return isinstance(x, NA)


# ---------------------------------------------------------------------------
# P-03 : représentation des prix en entiers 1e-4 $
# ---------------------------------------------------------------------------

class PriceNotRepresentable(ValueError):
    pass


def price_to_units_1e4(price) -> int:
    """Conversion exacte en entiers de 1e-4 $ via Decimal(str(price)) ; lève
    PriceNotRepresentable plutôt que d'arrondir."""
    d = Decimal(str(price)) * 10000
    i = int(d)
    if Decimal(i) != d:
        raise PriceNotRepresentable(f"prix non représentable en entiers 1e-4$: {price!r}")
    return i


# Toutes les sommes de notionnel (bucket, jour, mois) sont accumulées en entiers
# exacts d'unités de 1e-4 $ : une somme de flottants n'est pas associative et
# dépendrait de l'ordre d'ingestion. La conversion en dollars, une seule division,
# n'a lieu qu'au formatage CSV.
NOTIONNEL_UNITES_PAR_DOLLAR = 10_000


def notionnel_dollars(units: int) -> float:
    """Unités entières de 1e-4 $ -> dollars, pour le formatage uniquement."""
    return units / NOTIONNEL_UNITES_PAR_DOLLAR


def notionnel_units(size: int, price) -> int | None:
    """size × prix en entiers exacts de 1e-4 $, ou None si le prix n'est pas
    représentable à cette échelle. Le trade est alors exclu du notionnel et
    compté à part, comme pour P-03. Aucun cas sur l'échantillon de calibration
    (6 236 810 trades, au plus 4 décimales)."""
    try:
        return size * price_to_units_1e4(price)
    except PriceNotRepresentable:
        return None


def is_subpenny_ge1(price_units: int) -> bool:
    """Prix hors grille du cent (p mod 100 != 0 en unités de 1e-4 $), pour p >= 1 $."""
    return price_units % 100 != 0


def is_midpoint(price_units: int) -> bool:
    """Sous-penny au demi-cent exact (typique d'une exécution au midpoint)."""
    return price_units % 50 == 0 and price_units % 100 != 0


# ---------------------------------------------------------------------------
# P-04 : facteur cumulé de retraitement des opérations sur titres
# ---------------------------------------------------------------------------

def cum_factor(events: list[tuple[str, float]], d_prime: str, d: str) -> float:
    """F_{d'->d} = Prod_{e : d' < date(e) <= d} (1 + disfacshr_e), sur les
    événements CRSP de type FRS (date ISO, disfacshr) d'un titre, avec d' < d."""
    factor = 1.0
    for date_e, disfacshr in events:
        if d_prime < date_e <= d:
            factor *= (1 + disfacshr)
    return factor


def compute_rv(day_series: list[dict], idx: int, *, window: int = WINDOW_DAYS,
                h_min: int = H_MIN, ca_events: list[tuple[str, float]] | None = None) -> dict:
    """Volume relatif du jour idx : volume du jour divisé par la médiane des
    `window` séances précédentes, en actions (retraitées des splits) et en notionnel.

    day_series : séances cotées d'un titre, par date croissante, chacune
    {'date', 'dvact', 'dvnotional_u'} ; le notionnel est en unités de 1e-4 $,
    ce qui ne change pas le ratio."""
    d = day_series[idx]
    lo = max(0, idx - window)
    hist = day_series[lo:idx]
    if len(hist) < h_min:
        return {
            "n_histo": len(hist),
            "rvact": NA("histo"), "rvact_na_code": "histo",
            "rv_notional": NA("histo"), "rv_notional_na_code": "histo",
        }
    events = ca_events or []
    adjusted_act = [h["dvact"] * cum_factor(events, h["date"], d["date"]) for h in hist]
    med_act = statistics.median(adjusted_act)
    if med_act == 0:
        rvact, rvact_code = NA("med0"), "med0"
    else:
        rvact, rvact_code = d["dvact"] / med_act, ""
    med_not = statistics.median([h["dvnotional_u"] for h in hist])
    if med_not == 0:
        rv_not, rv_not_code = NA("med0"), "med0"
    else:
        rv_not, rv_not_code = d["dvnotional_u"] / med_not, ""
    return {
        "n_histo": len(hist),
        "rvact": rvact, "rvact_na_code": rvact_code,
        "rv_notional": rv_not, "rv_notional_na_code": rv_not_code,
    }


# ---------------------------------------------------------------------------
# P-01 (mode `tick`) et P-13 : grille de buckets
# ---------------------------------------------------------------------------
#
# Grille régulière par session, en heure de New York (ET, heure d'été comprise) :
#   premarket  04:00-09:30, buckets de 5 min -> 66
#   rth        09:30-16:00, buckets de 1 min -> 390
#   afterhours 16:00-20:00, buckets de 5 min -> 48
#
# Pour P-13, la grille complète est balayée, buckets vides compris (N_b = 0 est
# une observation, pas une valeur manquante) : la demi-vie de l'EWMA n'a de sens
# temporel que sur une grille régulière, et sur un titre peu liquide un pic
# d'activité après un long silence doit ressortir plus fortement qu'après un
# silence court. Pour P-01, seuls les buckets observés sont agrégés.
SESSION_GRID: dict[str, tuple[dtime, int, int]] = {
    "premarket": (dtime(4, 0), 5, 66),
    "rth": (dtime(9, 30), 1, 390),
    "afterhours": (dtime(16, 0), 5, 48),
}
SESSION_ORDER = ["premarket", "rth", "afterhours"]  # ordre chronologique ; l'EWMA de P-13 repart à chaque session

# Demi-séances : le RTH s'arrête à la clôture anticipée (13:00 en général) et
# l'after-hours court de cette clôture jusqu'à quatre heures plus tard. Un jour
# normal, les bornes sont celles de msip.SESSION_BOUNDS et la classification
# coïncide avec msip.classify_session (vérifié dans test_bucket_grid).
EARLY_CLOSES_PATH = Path(__file__).resolve().parent.parent / "references" / "early_closes.csv"


@lru_cache(maxsize=1)
def early_closes() -> dict[str, dtime]:
    """date ISO -> heure de clôture RTH anticipée (ET), lue dans early_closes.csv."""
    out: dict[str, dtime] = {}
    if not EARLY_CLOSES_PATH.exists():
        return out
    with EARLY_CLOSES_PATH.open(newline="") as f:
        for row in csv.DictReader(f):
            h, m = row["close_time_ET"].split(":")
            out[row["date"]] = dtime(int(h), int(m))
    return out


@lru_cache(maxsize=512)
def session_grid(date_iso: str) -> dict[str, tuple[dtime, int, int]]:
    """Grille du jour : SESSION_GRID, ou pour une demi-séance un RTH raccourci
    et un after-hours de 4 h commençant à la clôture anticipée."""
    close = early_closes().get(date_iso)
    if close is None:
        return SESSION_GRID
    rth_min = (close.hour * 60 + close.minute) - (9 * 60 + 30)
    return {
        "premarket": (dtime(4, 0), 5, 66),
        "rth": (dtime(9, 30), 1, rth_min),          # 09:30 -> clôture anticipée (210 min si 13:00)
        "afterhours": (close, 5, 48),               # clôture -> +4 h (13:00 -> 17:00), 240 min / 5
    }


def et_datetime(sip_timestamp_ns: int):
    seconds = sip_timestamp_ns // 1_000_000_000
    return datetime.fromtimestamp(seconds, tz=timezone.utc).astimezone(msip.ET)


def bucket_key(sip_timestamp_ns: int) -> tuple[str, int] | None:
    """(session, indice de bucket) dans la grille du jour, ou None hors session
    (22 trades sur 6 236 810 dans l'échantillon de calibration ; exclus de
    P-01/P-13 et comptés à part)."""
    dt = et_datetime(sip_timestamp_ns)
    grids = session_grid(dt.date().isoformat())
    minutes_du_jour = dt.hour * 60 + dt.minute
    session = grid = None
    for nom in SESSION_ORDER:
        start, width_min, n_buckets = grids[nom]
        debut = start.hour * 60 + start.minute
        if debut <= minutes_du_jour < debut + width_min * n_buckets:
            session, grid = nom, grids[nom]
            break
    if grid is None:
        return None
    start, width_min, n_buckets = grid
    minutes = minutes_du_jour - (start.hour * 60 + start.minute)
    idx = minutes // width_min
    assert 0 <= idx < n_buckets, (
        f"indice de bucket hors grille : session={session} idx={idx} ts={sip_timestamp_ns}"
    )
    return (session, idx)


def new_p01_bucket() -> dict:
    # Les champs `*_notional_u` sont en unités entières de 1e-4 $.
    d = {"vtrf": 0, "vtrf_notional_u": 0}
    for variant in ("all", "lit"):
        for k in ("vplus", "vminus", "vna"):
            d[f"{k}_{variant}"] = 0
            d[f"{k}_{variant}_notional_u"] = 0
    return d


def _apply_p01_sign(bucket: dict, variant: str, sign: int | None, size: int, notional_u: int) -> None:
    if sign is None:
        key = "vna"
    elif sign == 1:
        key = "vplus"
    else:
        key = "vminus"
    bucket[f"{key}_{variant}"] += size
    bucket[f"{key}_{variant}_notional_u"] += notional_u


def compute_p01_p13(trades_canonical: list[dict], volume_rules: dict[int, bool],
                     unknown_ids: set[int]) -> tuple[dict, dict, dict, dict]:
    """Une passe sur la tape triée en ordre canonique, qui alimente P-01 (test du
    tick, avec deux séries de référence : tick_all inclut les prix TRF, tick_lit
    non) et P-13 (nombre de trades et volume par bucket). Les deux partagent le
    filtre d'admissibilité et la grille de buckets.

    La référence du test du tick est le dernier prix différent du prix courant,
    tenue en O(1) par le couple (last_price, last_diff_price) : le signe vaut
    donc +1, -1 ou NA, jamais 0. Les prints d'enchère sont entièrement exclus,
    y compris de la mise à jour de la référence, car un prix de croisement ne
    fait pas partie du flux continu. Les trades hors volume (annulés compris)
    sont exclus de la même façon.

    Retourne (p01_buckets, p13_counts, p13_vact, stats) : p01_buckets indexé
    par (session, bucket) ; p13_counts et p13_vact, par session, des listes
    couvrant toute la grille ; stats compte les désaccords entre tick_all et
    tick_lit sur les mêmes trades lit et les trades hors grille.
    """
    p01_buckets: dict[tuple[str, int], dict] = defaultdict(new_p01_bucket)
    grids = session_grid(et_datetime(trades_canonical[0]["sip_timestamp"]).date().isoformat()) \
        if trades_canonical else SESSION_GRID
    p13_counts = {s: [0] * n for s, (_, _, n) in grids.items()}
    p13_vact = {s: [0] * n for s, (_, _, n) in grids.items()}

    last_price_all = last_diff_all = None
    last_price_lit = last_diff_lit = None
    n_both_defined = n_disagree = n_only_one_na = n_hors_grille = 0
    n_notionnel_non_representable = 0

    for t in trades_canonical:
        if not counts_to_volume(t, volume_rules, unknown_ids):
            continue
        if is_auction(t):
            continue
        key = bucket_key(t["sip_timestamp"])
        if key is None:
            n_hors_grille += 1
            continue
        session, idx = key
        size = t["size"]
        price = t["price"]

        # P-13 : trades lit et TRF, hors enchères.
        p13_counts[session][idx] += 1
        p13_vact[session][idx] += size

        b = p01_buckets[key]
        notional_u = notionnel_units(size, price)
        if notional_u is None:
            n_notionnel_non_representable += 1
            notional_u = 0
        if is_trf(t):
            b["vtrf"] += size
            b["vtrf_notional_u"] += notional_u
            if last_price_all is not None and price != last_price_all:
                last_diff_all = last_price_all
            last_price_all = price
            continue  # un trade TRF n'est pas signé ; il ne met à jour que la référence tick_all

        ref_all = last_diff_all if (last_price_all is not None and price == last_price_all) else last_price_all
        ref_lit = last_diff_lit if (last_price_lit is not None and price == last_price_lit) else last_price_lit
        s_all = None if ref_all is None else (1 if price > ref_all else -1)
        s_lit = None if ref_lit is None else (1 if price > ref_lit else -1)
        _apply_p01_sign(b, "all", s_all, size, notional_u)
        _apply_p01_sign(b, "lit", s_lit, size, notional_u)

        if s_all is not None and s_lit is not None:
            n_both_defined += 1
            if s_all != s_lit:
                n_disagree += 1
        elif (s_all is None) != (s_lit is None):
            n_only_one_na += 1

        if last_price_all is not None and price != last_price_all:
            last_diff_all = last_price_all
        last_price_all = price
        if last_price_lit is not None and price != last_price_lit:
            last_diff_lit = last_price_lit
        last_price_lit = price

    stats = {
        "n_both_defined": n_both_defined, "n_disagree": n_disagree,
        "n_only_one_na": n_only_one_na, "n_hors_grille": n_hors_grille,
        "n_notionnel_non_representable": n_notionnel_non_representable,
    }
    if n_notionnel_non_representable:
        print(f"[primitives_t1] avertissement : {n_notionnel_non_representable} trades "
              f"à prix non représentable en 1e-4 $ exclus du notionnel P-01",
              file=sys.stderr)
    return dict(p01_buckets), p13_counts, p13_vact, stats


def ewma_acc_series(n_values: list[int], h: int = 30) -> list:
    """P-13 sur une session : ACC_b = log((N_b + 1) / (Ñ_{b-1} + 1)), où Ñ est
    l'EWMA de demi-vie h buckets des N_b. Un appel par session, sans état
    reporté. Au premier bucket, ACC est NA et l'EWMA est amorcée à N_0 (et non à 0)."""
    alpha = 1 - 0.5 ** (1 / h)
    out: list = []
    ntilde_prev = None
    for i, n in enumerate(n_values):
        if i == 0:
            out.append(NA("premier_bucket_session"))
            ntilde_prev = float(n)
            continue
        acc = math.log((n + 1) / (ntilde_prev + 1))
        out.append(acc)
        ntilde_prev = alpha * n + (1 - alpha) * ntilde_prev
    return out


def summarize_p13_day(p13_counts: dict[str, list[int]], h: int = 30) -> dict:
    """Quantile 95 % journalier des |ACC_b| des trois sessions, chacune ayant sa
    propre EWMA."""
    abs_acc: list[float] = []
    for session in SESSION_ORDER:
        acc_series = ewma_acc_series(p13_counts[session], h=h)
        for a in acc_series:
            if not is_na(a):
                abs_acc.append(abs(a))
    if not abs_acc:
        return {"q95_abs_acc": NA("aucun_bucket"), "n_acc_buckets": 0}
    if len(abs_acc) >= 2:
        q95 = statistics.quantiles(abs_acc, n=100)[94]
    else:
        q95 = abs_acc[0]
    return {"q95_abs_acc": q95, "n_acc_buckets": len(abs_acc)}


def summarize_p01_day(p01_buckets: dict) -> dict:
    """Sommes journalières, OFI médian sur les buckets (tick_all ou tick_lit, en
    actions ou en notionnel) et part signable, calculée comme ratio de sommes."""
    sums = {
        "vplus_all": 0, "vminus_all": 0, "vna_all": 0, "vtrf": 0,
        "vplus_all_notional_u": 0, "vminus_all_notional_u": 0, "vna_all_notional_u": 0,
        "vtrf_notional_u": 0,
        "vplus_lit": 0, "vminus_lit": 0, "vna_lit": 0,
        "vplus_lit_notional_u": 0, "vminus_lit_notional_u": 0, "vna_lit_notional_u": 0,
    }
    ofi_lists = {"all_actions": [], "all_notional": [], "lit_actions": [], "lit_notional": []}
    for b in p01_buckets.values():
        for k in sums:
            sums[k] += b[k]
        # OFI = (V+ - V-)/(V+ + V-), invariant d'échelle : on le calcule directement
        # sur les notionnels entiers.
        for combo, (vp_key, vm_key) in (
            ("all_actions", ("vplus_all", "vminus_all")),
            ("all_notional", ("vplus_all_notional_u", "vminus_all_notional_u")),
            ("lit_actions", ("vplus_lit", "vminus_lit")),
            ("lit_notional", ("vplus_lit_notional_u", "vminus_lit_notional_u")),
        ):
            vp, vm = b[vp_key], b[vm_key]
            if vp + vm > 0:
                ofi_lists[combo].append((vp - vm) / (vp + vm))

    def med_or_na(lst):
        return statistics.median(lst) if lst else NA("aucun_bucket_signe")

    part_signable_all = part(sums["vplus_all"] + sums["vminus_all"],
                              sums["vplus_all"] + sums["vminus_all"] + sums["vna_all"])
    part_signable_lit = part(sums["vplus_lit"] + sums["vminus_lit"],
                              sums["vplus_lit"] + sums["vminus_lit"] + sums["vna_lit"])
    return {
        "sums": sums,
        "ofi_median_all_actions": med_or_na(ofi_lists["all_actions"]),
        "ofi_median_all_notional": med_or_na(ofi_lists["all_notional"]),
        "ofi_median_lit_actions": med_or_na(ofi_lists["lit_actions"]),
        "ofi_median_lit_notional": med_or_na(ofi_lists["lit_notional"]),
        "n_buckets_ofi_all": len(ofi_lists["all_actions"]),
        "n_buckets_ofi_lit": len(ofi_lists["lit_actions"]),
        "part_signable_all": part_signable_all,
        "part_signable_lit": part_signable_lit,
    }


# ---------------------------------------------------------------------------
# Agrégation par ticker-jour (une passe sur les trades triés en ordre canonique)
# ---------------------------------------------------------------------------

def new_variant_bucket() -> dict:
    return {
        "vol_all": 0, "trf_vol_all": 0,
        "vol_continu": 0, "trf_vol_continu": 0,
        "vol_ge1_all": 0, "sp_vol_all": 0, "mid_vol_all": 0,
        "vol_ge1_continu": 0, "sp_vol_continu": 0, "mid_vol_continu": 0,
        # `dvnotional_u` est en unités entières de 1e-4 $.
        "dvact": 0, "dvnotional_u": 0, "n_notionnel_non_representable": 0,
    }


def accumulate_trade(buckets: dict[str, dict], trade: dict, volume_rules: dict[int, bool],
                      unknown_ids: set[int], price_failures: list) -> None:
    """Ajoute un trade aux variantes incl810 et excl810 (cette dernière ignore les
    corrections 8 et 10) ; les trades hors volume sont ignorés."""
    admissible = counts_to_volume(trade, volume_rules, unknown_ids)
    if not admissible:
        return
    size = trade["size"]
    price = trade["price"]
    trf = is_trf(trade)
    auction = is_auction(trade)
    flagged = has_flagged_correction(trade)

    price_units = None
    ge1 = False
    if price >= 1:
        try:
            price_units = price_to_units_1e4(price)
            ge1 = True
        except PriceNotRepresentable:
            price_failures.append(trade)
            ge1 = False

    notional_u = notionnel_units(size, price)

    for variant, bucket in buckets.items():
        if variant == "excl810" and flagged:
            continue
        bucket["vol_all"] += size
        bucket["dvact"] += size
        if notional_u is None:
            bucket["n_notionnel_non_representable"] += 1
        else:
            bucket["dvnotional_u"] += notional_u
        if trf:
            bucket["trf_vol_all"] += size
        if not auction:
            bucket["vol_continu"] += size
            if trf:
                bucket["trf_vol_continu"] += size
        if ge1:
            bucket["vol_ge1_all"] += size
            sp = is_subpenny_ge1(price_units)
            if sp:
                bucket["sp_vol_all"] += size
                if is_midpoint(price_units):
                    bucket["mid_vol_all"] += size
            if not auction:
                bucket["vol_ge1_continu"] += size
                if sp:
                    bucket["sp_vol_continu"] += size
                    if is_midpoint(price_units):
                        bucket["mid_vol_continu"] += size


def part(num: int, den: int):
    return NA("vol0") if den == 0 else num / den


def compute_p14(trades_canonical: list[dict], trades_physical_order: list[dict],
                 volume_rules: dict[int, bool], unknown_ids_all: set[int]) -> dict:
    n = len(trades_physical_order)
    if n == 0:
        return {
            "n_trades_raw": 0, "taux_hors_sequence_Z": NA("n0"),
            "taux_correction": NA("n0"), "vol_correction_part": NA("n0"),
            "n_correction_8": 0, "n_correction_10": 0,
            "latence_mediane_ms": NA("n0"), "latence_p99_ms": NA("n0"),
            "taux_conditions_inconnues": NA("n0"), "taux_exclus": NA("n0"),
            "taux_desordre_fichier": NA("n0"),
        }
    n_z = sum(1 for t in trades_physical_order if Z_CONDITION_ID in (t.get("conditions") or []))
    n_corr = sum(1 for t in trades_physical_order if "correction" in t)
    vol_corr = sum(t["size"] for t in trades_physical_order if "correction" in t)
    vol_total_brut = sum(t["size"] for t in trades_physical_order)
    n_corr_8 = sum(1 for t in trades_physical_order if t.get("correction") == 8)
    n_corr_10 = sum(1 for t in trades_physical_order if t.get("correction") == 10)

    latencies_ms = [
        (t["sip_timestamp"] - t["participant_timestamp"]) / 1e6
        for t in trades_physical_order
        if "sip_timestamp" in t and "participant_timestamp" in t
    ]
    lat_med = statistics.median(latencies_ms) if latencies_ms else NA("absent")
    lat_p99 = (statistics.quantiles(latencies_ms, n=100)[98] if len(latencies_ms) >= 2
               else (latencies_ms[0] if latencies_ms else NA("absent")))

    n_unknown_trades = 0
    n_excluded = 0
    for t in trades_physical_order:
        local_unknown: set[int] = set()
        ok = counts_to_volume(t, volume_rules, local_unknown)
        if local_unknown:
            n_unknown_trades += 1
            unknown_ids_all |= local_unknown
        if not ok:
            n_excluded += 1

    n_disorder = 0
    for a, b in zip(trades_physical_order, trades_physical_order[1:]):
        if b["sip_timestamp"] < a["sip_timestamp"]:
            n_disorder += 1
    taux_disorder = n_disorder / (n - 1) if n > 1 else NA("n<2")

    return {
        "n_trades_raw": n,
        "taux_hors_sequence_Z": n_z / n,
        "taux_correction": n_corr / n,
        "vol_correction_part": part(vol_corr, vol_total_brut),
        "n_correction_8": n_corr_8, "n_correction_10": n_corr_10,
        "latence_mediane_ms": lat_med, "latence_p99_ms": lat_p99,
        "taux_conditions_inconnues": n_unknown_trades / n,
        "taux_exclus": n_excluded / n,
        "taux_desordre_fichier": taux_disorder,
    }


def check_sequence_number_order(trades_physical_order: list[dict]) -> dict:
    """Diagnostic : à sip_timestamp égal, les sequence_number sont-ils distincts ?
    C'est la condition pour qu'ils départagent le tri canonique."""
    groups: dict[int, list[int]] = defaultdict(list)
    for t in trades_physical_order:
        groups[t["sip_timestamp"]].append(t["sequence_number"])
    n_groups_multi = 0
    n_groups_all_distinct = 0
    n_groups_with_duplicate = 0
    for ts, seqs in groups.items():
        if len(seqs) < 2:
            continue
        n_groups_multi += 1
        if len(set(seqs)) == len(seqs):
            n_groups_all_distinct += 1
        else:
            n_groups_with_duplicate += 1
    return {
        "n_groups_timestamp_multi": n_groups_multi,
        "n_groups_all_distinct_seq": n_groups_all_distinct,
        "n_groups_with_duplicate_seq": n_groups_with_duplicate,
    }


# ---------------------------------------------------------------------------
# P-16 : déplacement de prix
# ---------------------------------------------------------------------------
#
# compute_p16_day ne lit que la tape RTH du jour d ; compute_p16_trailing ne
# lit que des fenêtres closes à d-1, plus les extrêmes du jour d. Cette
# séparation permet de tester l'absence de fuite d'information future.
#
# Un bucket est le couple d'entiers (notionnel en unités de 1e-4 $, actions) ;
# les VWAP ne sont jamais matérialisés en flottant, et tout log de rapport de
# VWAP passe par _log_ratio, qui fait une seule division sur des entiers.

N_TRAIL_P16 = 20       # séances de la fenêtre trailing
MIN_BUCKETS_P16 = 5    # buckets actifs minimum pour dp_efficience


def _log_ratio(num_a: int, den_a: int, num_b: int, den_b: int) -> float:
    """log((num_a/den_a) / (num_b/den_b)), une seule division flottante."""
    return math.log((num_a * den_b) / (den_a * num_b))


def vwap_dollars(nd: tuple[int, int]) -> float:
    """(notionnel_u, actions) -> VWAP en dollars, pour le formatage CSV."""
    num, den = nd
    return num / (den * NOTIONNEL_UNITES_PAR_DOLLAR)


def compute_p16_day(trades_canonical: list[dict], volume_rules: dict[int, bool],
                    unknown_ids: set[int]) -> dict:
    """Sorties intra-jour de P-16, sur les trades RTH admissibles hors enchères.

    Les buckets vides ne sont pas remplis par report du dernier prix, ce qui
    fabriquerait des rendements nuls non observés : les rendements relient les
    buckets actifs consécutifs. dp_abs dépend donc du remplissage, et
    n_buckets_actifs doit accompagner toute comparaison entre titres.

    Retourne dp_net, dp_abs, dp_efficience, dp_range, les extrêmes en unités de
    1e-4 $ et les VWAP du premier et du dernier bucket sous forme
    (notionnel_u, actions), qui servent à l'overnight du lendemain.
    """
    buckets: dict[int, list[int]] = {}          # idx RTH -> [notionnel_u, actions]
    p_max_u = p_min_u = None
    n_prix_non_repr = 0
    for t in trades_canonical:
        if not counts_to_volume(t, volume_rules, unknown_ids):
            continue
        if is_auction(t):                        # ouverture, clôture, réouverture post-halt
            continue
        key = bucket_key(t["sip_timestamp"])
        if key is None or key[0] != "rth":
            continue
        try:
            pu = price_to_units_1e4(t["price"])
        except PriceNotRepresentable:
            n_prix_non_repr += 1
            continue
        size = int(t["size"])
        b = buckets.setdefault(key[1], [0, 0])
        b[0] += size * pu
        b[1] += size
        if p_max_u is None or pu > p_max_u:
            p_max_u = pu
        if p_min_u is None or pu < p_min_u:
            p_min_u = pu

    idxs = sorted(buckets)
    n_act = len(idxs)
    out: dict = {
        "n_buckets_actifs": n_act,
        "p16_n_prix_non_repr": n_prix_non_repr,
        "p_max_u": p_max_u if n_act else NA("aucun_bucket_actif"),
        "p_min_u": p_min_u if n_act else NA("aucun_bucket_actif"),
        "vwap_first_nd": tuple(buckets[idxs[0]]) if n_act else NA("aucun_bucket_actif"),
        "vwap_last_nd": tuple(buckets[idxs[-1]]) if n_act else NA("aucun_bucket_actif"),
    }

    out["dp_range"] = (math.log(p_max_u / p_min_u) if n_act
                       else NA("aucun_bucket_actif"))

    if n_act < 2:
        code = "aucun_bucket_actif" if n_act == 0 else "buckets_insuffisants"
        out["dp_net"] = NA(code)
        out["dp_abs"] = NA(code)
        out["dp_efficience"] = NA(code)
        return out

    # dp_net en forme fermée (premier -> dernier bucket actif) : égal à la somme
    # des rendements, mais plus précis, et exactement 0.0 quand le prix revient
    # à son point de départ.
    n_first, s_first = buckets[idxs[0]]
    n_last, s_last = buckets[idxs[-1]]
    out["dp_net"] = _log_ratio(n_last, s_last, n_first, s_first)

    # dp_abs : somme dans l'ordre des buckets, pour un résultat déterministe.
    dp_abs = 0.0
    for prev, cur in zip(idxs, idxs[1:]):
        n_p, s_p = buckets[prev]
        n_c, s_c = buckets[cur]
        dp_abs += abs(_log_ratio(n_c, s_c, n_p, s_p))
    out["dp_abs"] = dp_abs

    if dp_abs == 0.0:
        out["dp_efficience"] = NA("chemin_nul")
    elif n_act < MIN_BUCKETS_P16:
        out["dp_efficience"] = NA("buckets_insuffisants")
    else:
        out["dp_efficience"] = out["dp_net"] / dp_abs
    return out


def compute_p16_trailing(day_series: list[dict], idx: int,
                         ca_events: list[tuple[str, float]] | None,
                         *, n: int = N_TRAIL_P16) -> dict:
    """Sorties de P-16 à fenêtre trailing, toutes closes à d-1.

    day_series : séances cotées d'un titre, par date croissante, avec les champs
    produits par compute_p16_day (les NA sont ignorés).

    ca_events : événements (date, disfacshr) du titre. Ici None signifie que la
    source est indisponible (les distributions CRSP s'arrêtent au 2025-12-31) :
    toute sortie qui compare des jours différents vaut alors NA("ca"). Une liste
    vide signifie « aucun événement ». compute_rv traite None comme une liste
    vide, d'où l'absence de valeur par défaut ici. Les prix sont retraités par
    p_{d'} / F_{d'->d}.
    """
    d = day_series[idx]
    hist = day_series[max(0, idx - n):idx]
    ca_indisponible = ca_events is None
    events = ca_events or []
    out: dict = {"n_histo_p16": len(hist)}

    # dp_overnight : dernier VWAP de la veille, retraité, contre premier VWAP du jour.
    if idx == 0:
        out["dp_overnight"] = NA("pas_de_veille")
    else:
        veille = day_series[idx - 1]
        if is_na(veille.get("vwap_last_nd", NA("x"))) or is_na(d.get("vwap_first_nd", NA("x"))):
            out["dp_overnight"] = NA("pas_de_veille")
        elif ca_indisponible:
            out["dp_overnight"] = NA("ca")
        else:
            f = cum_factor(events, veille["date"], d["date"])
            n_f, s_f = d["vwap_first_nd"]
            n_l, s_l = veille["vwap_last_nd"]
            # log(p_first(d) / (p_last(d-1) / F)) = log(rapport × F)
            out["dp_overnight"] = math.log((n_f * s_l) / (s_f * n_l) * f)

    # Franchissements et échelle, sur les N séances précédant d.
    if len(hist) < n:
        for k in ("franchissement_haut", "franchissement_bas", "dp_net_z", "dp_abs_z", "sigma_trail"):
            out[k] = NA("historique_court")
        return out

    if ca_indisponible:
        out["franchissement_haut"] = NA("ca")
        out["franchissement_bas"] = NA("ca")
    else:
        hauts = [h["p_max_u"] / cum_factor(events, h["date"], d["date"])
                 for h in hist if not is_na(h["p_max_u"])]
        bas = [h["p_min_u"] / cum_factor(events, h["date"], d["date"])
               for h in hist if not is_na(h["p_min_u"])]
        if not hauts or is_na(d["p_max_u"]):
            out["franchissement_haut"] = NA("aucun_bucket_actif")
            out["franchissement_bas"] = NA("aucun_bucket_actif")
        else:
            h_d, b_d = max(hauts), min(bas)
            out["franchissement_haut"] = max(0.0, math.log(d["p_max_u"] / h_d))
            out["franchissement_bas"] = min(0.0, math.log(d["p_min_u"] / b_d))

    # sigma_trail : médiane des |dp_net| des N séances précédentes. Un
    # log-rendement intra-jour n'a pas besoin de retraitement : le facteur se simplifie.
    nets = [abs(h["dp_net"]) for h in hist if not is_na(h["dp_net"])]
    if not nets:
        sigma = NA("historique_court")
    else:
        sigma = statistics.median(nets)
    out["sigma_trail"] = sigma
    if is_na(sigma):
        out["dp_net_z"] = out["dp_abs_z"] = sigma
    elif sigma == 0.0:
        out["dp_net_z"] = out["dp_abs_z"] = NA("echelle_nulle")
    else:
        out["dp_net_z"] = NA("buckets_insuffisants") if is_na(d["dp_net"]) else d["dp_net"] / sigma
        out["dp_abs_z"] = NA("buckets_insuffisants") if is_na(d["dp_abs"]) else d["dp_abs"] / sigma
    return out


# ---------------------------------------------------------------------------
# Tests unitaires (--test, aucun accès données)
# ---------------------------------------------------------------------------

def _mktr(price, size, conditions=None, trf_id=None, correction=None, exchange=4):
    t = {"price": price, "size": size, "exchange": exchange}
    if conditions is not None:
        t["conditions"] = conditions
    if trf_id is not None:
        t["trf_id"] = trf_id
    if correction is not None:
        t["correction"] = correction
    return t


def test_p02():
    tables = load_tables()
    vr = tables["volume_rules"]
    # Tailles distinctes : 2 TRF, 2 lit, 1 enchère (id 17, Opening Trade, compté au volume).
    trades = [
        _mktr(10.0, 11, trf_id=202),               # TRF
        _mktr(10.0, 23, trf_id=201),               # TRF
        _mktr(10.0, 37, exchange=11),               # lit
        _mktr(10.0, 59, exchange=12),               # lit
        _mktr(10.0, 71, conditions=[17]),            # enchère, non TRF
    ]
    buckets = {"incl810": new_variant_bucket(), "excl810": new_variant_bucket()}
    unknown: set[int] = set()
    failures: list = []
    for t in trades:
        accumulate_trade(buckets, t, vr, unknown, failures)
    b = buckets["incl810"]
    assert b["vol_all"] == 11 + 23 + 37 + 59 + 71 == 201
    assert b["trf_vol_all"] == 11 + 23 == 34
    assert b["vol_continu"] == 11 + 23 + 37 + 59 == 130   # enchère exclue
    assert b["trf_vol_continu"] == 34
    trf_part_all = part(b["trf_vol_all"], b["vol_all"])
    trf_part_continu = part(b["trf_vol_continu"], b["vol_continu"])
    assert abs(trf_part_all - 34 / 201) < 1e-15
    assert abs(trf_part_continu - 34 / 130) < 1e-15
    # volume nul -> NA
    assert part(0, 0) == NA("vol0")
    print("P-02 : OK")


def test_p03():
    tables = load_tables()
    vr = tables["volume_rules"]
    trades = [
        _mktr(1.005, 13),     # mid (10050 -> %100=50 -> sp, %50=0 -> mid)
        _mktr(1.0049, 17),    # sous-penny non-mid (10049 -> %100=49, %50=49)
        _mktr(1.00, 19),      # non sous-penny
        _mktr(0.9995, 23),    # hors somme (< 1$)
    ]
    buckets = {"incl810": new_variant_bucket(), "excl810": new_variant_bucket()}
    unknown: set[int] = set()
    failures: list = []
    for t in trades:
        accumulate_trade(buckets, t, vr, unknown, failures)
    b = buckets["incl810"]
    assert b["vol_ge1_all"] == 13 + 17 + 19 == 49   # 0.9995 hors somme
    assert b["sp_vol_all"] == 13 + 17 == 30
    assert b["mid_vol_all"] == 13
    assert not failures

    # représentation exacte malgré l'écriture binaire approchée de 1.10 et 2.675
    assert price_to_units_1e4(1.10) == 11000
    assert price_to_units_1e4(2.675) == 26750

    # prix à 5 décimales : exception
    try:
        price_to_units_1e4(1.00005)
        raised = False
    except PriceNotRepresentable:
        raised = True
    assert raised

    # reverse split qui fait passer le prix au-dessus de 1 $ : P-03 ne regarde
    # que le prix brut du jour, sans retraitement.
    pre_split = _mktr(0.15, 100)   # veille, < 1 $
    post_split = _mktr(1.50, 100)  # jour du split, >= 1 $
    b2 = {"incl810": new_variant_bucket(), "excl810": new_variant_bucket()}
    for t in [pre_split, post_split]:
        accumulate_trade(b2, t, vr, unknown, failures)
    assert b2["incl810"]["vol_ge1_all"] == 100   # seul post_split compte
    print("P-03 : OK")


def test_p04():
    # (a) un jour à volume nul compte dans l'historique avec la valeur 0.
    series = [{"date": f"2020-01-{i+1:02d}", "dvact": (0 if i == 5 else 100 + i),
               "dvnotional_u": (0 if i == 5 else 10_000_000 + i)} for i in range(25)]
    r = compute_rv(series, 24, window=63, h_min=20)
    assert r["n_histo"] == 24
    assert not is_na(r["rvact"])

    # (b) introduction en bourse récente : 14 jours d'historique -> NA("histo")
    ipo_series = [{"date": f"2020-02-{i+1:02d}", "dvact": 100 + i, "dvnotional_u": 10_000_000 + i}
                  for i in range(20)]
    r_ipo = compute_rv(ipo_series, 14, window=63, h_min=20)  # idx 14 = 15e jour, 14 précédents
    assert r_ipo["n_histo"] == 14
    assert r_ipo["rvact"] == NA("histo")
    assert r_ipo["rv_notional"] == NA("histo")

    # (c) 32 zéros sur 63 -> médiane nulle -> NA("med0")
    hist_vals = [0] * 32 + list(range(1, 32))
    med0_series = [{"date": f"2020-03-{(i % 28) + 1:02d}", "dvact": v, "dvnotional_u": v}
                   for i, v in enumerate(hist_vals)]
    med0_series.append({"date": "2020-05-15", "dvact": 500, "dvnotional_u": 50_000_000})
    r_med0 = compute_rv(med0_series, len(med0_series) - 1, window=63, h_min=20)
    assert r_med0["n_histo"] == 63
    assert r_med0["rvact"] == NA("med0")
    assert r_med0["rv_notional"] == NA("med0")

    # (d) reverse split 1:10 en milieu de fenêtre. Avec 15 jours avant le split
    # et 10 après, la médiane de l'historique tombe dans le bloc pré-split
    # retraité : un facteur inversé change donc RVact, ce qui rend le test
    # discriminant.
    ca_events = [("2020-06-16", -0.9)]  # F = 0.1
    pre = [{"date": f"2020-06-{d:02d}", "dvact": 500_000 + 137 * d, "dvnotional_u": 20_000_000_000 + 977 * d}
           for d in range(1, 16)]                        # jours 1-15, avant le split
    post = [{"date": f"2020-06-{d:02d}", "dvact": 48_000 + 23 * d, "dvnotional_u": 20_500_000_000 + 611 * d}
             for d in range(17, 27)]                      # jours 17-26, après le split
    full = pre + post
    d_eval = {"date": "2020-06-27", "dvact": 49_500, "dvnotional_u": 20_700_000_000}
    full.append(d_eval)
    idx = len(full) - 1
    r_correct = compute_rv(full, idx, window=63, h_min=20, ca_events=ca_events)
    assert r_correct["n_histo"] == 25
    assert not is_na(r_correct["rvact"])

    # valeur de référence recalculée directement
    hist = full[max(0, idx - 63):idx]
    adj_correct = [h["dvact"] * cum_factor(ca_events, h["date"], d_eval["date"]) for h in hist]
    med_correct = statistics.median(adj_correct)
    expected_rvact = d_eval["dvact"] / med_correct
    assert abs(r_correct["rvact"] - expected_rvact) < 1e-9

    # avec le facteur inversé (10 au lieu de 0.1), RVact doit différer
    inverted_events = [("2020-06-16", 1 / (1 - 0.9) - 1)]  # F' = 10 au lieu de 0.1
    adj_wrong = [h["dvact"] * cum_factor(inverted_events, h["date"], d_eval["date"]) for h in hist]
    med_wrong = statistics.median(adj_wrong)
    rvact_wrong = d_eval["dvact"] / med_wrong
    assert abs(rvact_wrong - expected_rvact) > 1e-6, "la série doit discriminer un facteur inversé"

    # RV$ n'est pas retraité : le notionnel est insensible aux splits
    med_not = statistics.median([h["dvnotional_u"] for h in hist])
    expected_rv_not = d_eval["dvnotional_u"] / med_not
    assert abs(r_correct["rv_notional"] - expected_rv_not) < 1e-9

    print("P-04 : OK")


def test_p14():
    tables = load_tables()
    vr = tables["volume_rules"]
    trades_physical = [
        _mktr(10.0, 11, conditions=[32]),          # Z hors-séquence
        _mktr(10.0, 13, correction=8),              # code correction 8
        _mktr(10.0, 17, conditions=[999999]),       # condition inconnue
        _mktr(10.0, 0),                             # taille nulle -> exclu
        _mktr(10.0, 19),                             # normal
    ]
    # une inversion dans l'ordre du fichier (3e trade antérieur au 2e)
    ts_base = 1_700_000_000_000_000_000
    physical_ts = [ts_base, ts_base + 10, ts_base + 5, ts_base + 20, ts_base + 30]
    for t, ts in zip(trades_physical, physical_ts):
        t["sip_timestamp"] = ts
        t["participant_timestamp"] = ts - 1_000_000  # 1 ms de latence constante
        t["sequence_number"] = 0  # non pertinent ici

    unknown_all: set[int] = set()
    r = compute_p14(trades_physical, trades_physical, vr, unknown_all)
    assert r["n_trades_raw"] == 5
    assert r["taux_hors_sequence_Z"] == 1 / 5
    assert r["taux_correction"] == 1 / 5
    assert r["n_correction_8"] == 1
    assert r["n_correction_10"] == 0
    assert r["taux_conditions_inconnues"] == 1 / 5
    assert r["taux_exclus"] == 1 / 5   # la taille nulle
    assert r["taux_desordre_fichier"] == 1 / 4   # 1 inversion sur 4 paires adjacentes
    assert abs(r["latence_mediane_ms"] - 1.0) < 1e-9
    print("P-14 : OK")


def _et_ts(y, mo, d, h, mi, s=0, extra_ns=0) -> int:
    """sip_timestamp (ns Unix) correspondant à une heure ET, pour les tapes de test."""
    dt = datetime(y, mo, d, h, mi, s, tzinfo=msip.ET)
    return int(dt.timestamp()) * 1_000_000_000 + extra_ns


def test_bucket_grid():
    # RTH : bucket 1 min ancré à 09:30. Premarket/AH : 5 min, ancrés à 04:00/16:00.
    assert bucket_key(_et_ts(2018, 6, 1, 9, 30, 0)) == ("rth", 0)
    assert bucket_key(_et_ts(2018, 6, 1, 9, 30, 59)) == ("rth", 0)
    assert bucket_key(_et_ts(2018, 6, 1, 9, 31, 0)) == ("rth", 1)
    assert bucket_key(_et_ts(2018, 6, 1, 15, 59, 59)) == ("rth", 389)
    assert bucket_key(_et_ts(2018, 6, 1, 4, 0, 0)) == ("premarket", 0)
    assert bucket_key(_et_ts(2018, 6, 1, 4, 4, 59)) == ("premarket", 0)
    assert bucket_key(_et_ts(2018, 6, 1, 4, 5, 0)) == ("premarket", 1)
    assert bucket_key(_et_ts(2018, 6, 1, 16, 0, 0)) == ("afterhours", 0)
    assert bucket_key(_et_ts(2018, 6, 1, 19, 59, 59)) == ("afterhours", 47)
    assert bucket_key(_et_ts(2018, 6, 1, 3, 59, 59)) is None   # hors session
    assert bucket_key(_et_ts(2018, 6, 1, 20, 0, 0)) is None    # hors session
    # heure d'hiver (UTC-5)
    assert bucket_key(_et_ts(2026, 1, 2, 9, 30, 0)) == ("rth", 0)
    assert bucket_key(_et_ts(2026, 1, 2, 15, 59, 59)) == ("rth", 389)

    # Un jour normal, la classification coïncide avec msip.classify_session.
    for h, mi in [(4, 0), (7, 13), (9, 29), (9, 30), (12, 0), (15, 59),
                  (16, 0), (18, 30), (19, 59), (3, 59), (20, 0), (21, 0)]:
        ts = _et_ts(2018, 6, 1, h, mi)
        attendu = msip.classify_session(ts)
        obtenu = bucket_key(ts)
        assert (obtenu[0] if obtenu else "hors_session") == attendu, (h, mi, obtenu, attendu)

    # Le 2018-11-23 est une demi-séance (clôture à 13:00).
    assert "2018-11-23" in early_closes()
    g = session_grid("2018-11-23")
    assert g["rth"][2] == 210 and g["afterhours"][0] == dtime(13, 0), g
    assert bucket_key(_et_ts(2018, 11, 23, 12, 59, 59)) == ("rth", 209)   # dernier bucket RTH
    # 13:00-17:00 : after-hours
    assert bucket_key(_et_ts(2018, 11, 23, 13, 0, 0)) == ("afterhours", 0)
    assert bucket_key(_et_ts(2018, 11, 23, 15, 0, 0)) == ("afterhours", 24)
    assert bucket_key(_et_ts(2018, 11, 23, 16, 59, 59)) == ("afterhours", 47)  # fin AH = 17:00
    assert bucket_key(_et_ts(2018, 11, 23, 17, 0, 0)) is None                  # hors session
    # le jour ouvré suivant est normal
    assert session_grid("2018-11-26")["rth"][2] == 390
    print("bucket_grid : OK")


def test_p01():
    tables = load_tables()
    vr = tables["volume_rules"]
    unknown: set[int] = set()

    def mk(price, size, ts, conditions=None, trf_id=None, exchange=4):
        t = {"price": price, "size": size, "exchange": exchange, "sip_timestamp": ts}
        if conditions is not None:
            t["conditions"] = conditions
        if trf_id is not None:
            t["trf_id"] = trf_id
        return t

    # Tape principale (tailles distinctes) : premier trade non signé, tick
    # montant et descendant, TRF, trade hors volume, enchère et taille nulle
    # exclus, référence conservée d'un bucket au suivant.
    trades = [
        mk(10.00, 11, _et_ts(2018, 6, 1, 9, 30, 0)),                                   # premier trade (NA/NA)
        mk(10.00, 97, _et_ts(2018, 6, 1, 9, 30, 2), conditions=[15]),                  # id 15 hors volume : exclu
        mk(10.00, 89, _et_ts(2018, 6, 1, 9, 30, 3), conditions=[17]),                  # enchère : exclue de P-01
        mk(10.05, 13, _et_ts(2018, 6, 1, 9, 30, 5)),                                   # tick montant (+1/+1)
        mk(10.05, 0, _et_ts(2018, 6, 1, 9, 30, 6)),                                    # taille nulle : exclu
        mk(10.02, 17, _et_ts(2018, 6, 1, 9, 31, 5)),                                   # tick descendant, bucket suivant (-1/-1)
        mk(10.10, 19, _et_ts(2018, 6, 1, 9, 31, 10), trf_id=202),                      # TRF : V^trf, non signé
        mk(10.10, 23, _et_ts(2018, 6, 1, 9, 31, 15)),                                  # +1/+1
    ]
    p01_buckets, p13_counts, p13_vact, stats = compute_p01_p13(trades, vr, unknown)

    b0 = p01_buckets[("rth", 0)]
    assert b0["vna_all"] == 11 and b0["vna_lit"] == 11          # premier trade
    assert b0["vplus_all"] == 13 and b0["vplus_lit"] == 13       # tick montant
    assert b0["vplus_all_notional_u"] == 13 * 100_500   # notionnel entier, égalité exacte
    # les trades exclus (89, 97, 0) n'apparaissent dans aucun compteur
    assert b0["vtrf"] == 0
    assert b0["vminus_all"] == 0 and b0["vna_all"] == 11

    b1 = p01_buckets[("rth", 1)]
    assert b1["vminus_all"] == 17 and b1["vminus_lit"] == 17     # tick descendant
    assert b1["vtrf"] == 19                                       # TRF
    assert b1["vtrf_notional_u"] == 19 * 101_000
    assert b1["vplus_all"] == 23 and b1["vplus_lit"] == 23        # dernier trade lit

    assert stats["n_hors_grille"] == 0
    assert stats["n_both_defined"] == 3   # le premier trade, NA/NA, n'est pas compté
    assert stats["n_disagree"] == 0
    assert stats["n_only_one_na"] == 0

    # Désaccord tick_all / tick_lit : un print TRF sous le prix courant place
    # les deux références de part et d'autre du trade suivant.
    trades2 = [
        mk(10.00, 201, _et_ts(2018, 6, 1, 10, 0, 0)),                    # premier (NA/NA)
        mk(11.00, 203, _et_ts(2018, 6, 1, 10, 0, 1)),                    # référence 10 -> +1/+1
        mk(9.00, 205, _et_ts(2018, 6, 1, 10, 0, 2), trf_id=202),         # TRF : seule la référence tick_all passe à 9
        mk(10.00, 207, _et_ts(2018, 6, 1, 10, 0, 3)),                    # tick_all +1 (réf. 9), tick_lit -1 (réf. 11)
    ]
    p01_b2, _, _, stats2 = compute_p01_p13(trades2, vr, unknown)
    assert stats2["n_both_defined"] == 2   # 2e et 4e trades
    assert stats2["n_disagree"] == 1
    assert stats2["n_only_one_na"] == 0
    key2 = bucket_key(_et_ts(2018, 6, 1, 10, 0, 3))
    b2 = p01_b2[key2]
    # 2e et 4e trades dans le même bucket : seul le 4e diverge
    assert b2["vplus_all"] == 203 + 207
    assert b2["vplus_lit"] == 203 and b2["vminus_lit"] == 207

    # Chaque appel repart d'un état vide : le premier trade du jour suivant
    # n'est pas signé, même au prix de la dernière transaction de la veille.
    trades_day2 = [mk(10.10, 301, _et_ts(2018, 6, 4, 9, 30, 0))]
    p01_d2, _, _, _ = compute_p01_p13(trades_day2, vr, unknown)
    b_d2 = p01_d2[("rth", 0)]
    assert b_d2["vna_all"] == 301 and b_d2["vna_lit"] == 301

    print("P-01 (mode tick) : OK")


def test_p13():
    # (1) série constante : ACC = 0 dès le 2e bucket, quel que soit h.
    const_series = ewma_acc_series([100] * 40, h=30)
    assert is_na(const_series[0])
    assert all(abs(a) < 1e-12 for a in const_series[1:])

    # (2) saut x10 après un plateau : Ñ vaut encore 10, donc ACC = log(101/11).
    jump_series = ewma_acc_series([10, 10, 10, 10, 10, 100], h=30)
    assert abs(jump_series[5] - math.log(101 / 11)) < 1e-9

    # (3) amorçage à N_0 : un démarrage à 0 donnerait log(78/1).
    warm = ewma_acc_series([77, 77], h=30)
    assert abs(warm[1] - 0.0) < 1e-12

    # (4) aucun état partagé entre deux appels.
    ewma_acc_series([50, 50, 50], h=30)
    fresh = ewma_acc_series([999], h=30)
    assert is_na(fresh[0])

    # (5) q95 sur les trois sessions : nul si tout est constant ; avec un pic
    # unique, comparé au quantile recalculé sur les 8 valeurs attendues.
    counts_flat = {"premarket": [1, 1, 1], "rth": [2, 2, 2, 2], "afterhours": [3, 3, 3]}
    r_flat = summarize_p13_day(counts_flat, h=30)
    assert r_flat["n_acc_buckets"] == 2 + 3 + 2
    assert abs(r_flat["q95_abs_acc"] - 0.0) < 1e-12

    counts_spike = {"premarket": [1, 1, 1], "rth": [2, 2, 2, 2, 200], "afterhours": [3, 3, 3]}
    r_spike = summarize_p13_day(counts_spike, h=30)
    assert r_spike["n_acc_buckets"] == 8
    expected_abs = [0.0, 0.0, 0.0, 0.0, 0.0, math.log(201 / 3), 0.0, 0.0]
    expected_q95 = statistics.quantiles(expected_abs, n=100)[94]
    assert abs(r_spike["q95_abs_acc"] - expected_q95) < 1e-9

    print("P-13 : OK")


def _p16_tr(price, size, minute, second=0, **kw):
    """Trade RTH du 2023-01-19, `minute` minutes après 09:30 (bucket = minute)."""
    t = _mktr(price, size, **kw)
    total = 30 + minute
    t["sip_timestamp"] = _et_ts(2023, 1, 19, 9 + total // 60, total % 60, second)
    return t


def _p16_jour(prices_par_bucket, minutes=None, vr=None, unknown=None, extra=()):
    """Un jour = un prix par bucket actif (100 actions), plus des trades `extra`."""
    minutes = minutes if minutes is not None else list(range(len(prices_par_bucket)))
    trades = [_p16_tr(p, 100, m) for p, m in zip(prices_par_bucket, minutes)]
    trades.extend(extra)
    trades.sort(key=lambda t: t["sip_timestamp"])
    return compute_p16_day(trades, vr, unknown)


def test_p16():
    tables = load_tables()
    vr, unknown = tables["volume_rules"], set()
    J = lambda prix, **kw: _p16_jour(prix, vr=vr, unknown=unknown, **kw)

    # (a) rampe monotone : chemin = déplacement, efficience 1. Comparaison avec
    # tolérance, dp_net (forme fermée) et dp_abs (somme) pouvant différer au dernier bit.
    ramp = J([10.00 + 0.10 * k for k in range(10)])
    assert ramp["n_buckets_actifs"] == 10
    assert abs(ramp["dp_net"] - math.log(10.90 / 10.00)) < 1e-12
    assert abs(ramp["dp_abs"] - sum(math.log((10.0 + 0.1 * (k + 1)) / (10.0 + 0.1 * k))
                                    for k in range(9))) < 1e-12
    assert abs(ramp["dp_efficience"] - 1.0) < 1e-12
    assert abs(ramp["dp_range"] - math.log(10.90 / 10.00)) < 1e-12

    # (b) aller-retour : dp_net vaut exactement 0.0 (rapport de deux VWAP
    # identiques), le chemin est non nul.
    zig = J([10.00, 11.00, 10.00, 11.00, 10.00])
    assert zig["dp_net"] == 0.0
    assert abs(zig["dp_abs"] - 4 * math.log(1.1)) < 1e-12
    assert zig["dp_efficience"] == 0.0
    assert zig["dp_abs"] > 3.9 * math.log(1.1)

    # (b bis) efficience strictement entre 0 et 1, qui distingue net/chemin
    # d'une formule dégénérée comme net/|net|.
    prix_mixte = [10.00, 10.50, 10.20, 10.80, 10.40]
    mix = J(prix_mixte)
    net_attendu = math.log(10.40 / 10.00)
    chemin_attendu = sum(abs(math.log(b / a)) for a, b in zip(prix_mixte, prix_mixte[1:]))
    assert abs(mix["dp_net"] - net_attendu) < 1e-12
    assert abs(mix["dp_abs"] - chemin_attendu) < 1e-12
    assert abs(mix["dp_efficience"] - net_attendu / chemin_attendu) < 1e-12
    assert 0.2 < mix["dp_efficience"] < 0.3

    # (c) un seul bucket actif : pas de trajectoire, mais une amplitude.
    seul = J([], extra=[_p16_tr(10.00, 100, 3), _p16_tr(10.50, 100, 3, second=30)])
    assert seul["n_buckets_actifs"] == 1
    assert seul["dp_net"] == NA("buckets_insuffisants")
    assert seul["dp_abs"] == NA("buckets_insuffisants")
    assert seul["dp_efficience"] == NA("buckets_insuffisants")
    assert abs(seul["dp_range"] - math.log(10.50 / 10.00)) < 1e-12

    # 2 <= n < 5 : dp_net et dp_abs définis, efficience NA.
    court = J([10.00, 10.50, 10.20])
    assert not is_na(court["dp_net"]) and not is_na(court["dp_abs"])
    assert court["dp_efficience"] == NA("buckets_insuffisants")

    # jour sans trade admissible : NA partout.
    vide = J([])
    assert vide["n_buckets_actifs"] == 0
    assert all(vide[k] == NA("aucun_bucket_actif")
               for k in ("dp_net", "dp_abs", "dp_efficience", "dp_range", "p_max_u"))

    # (d) buckets vides intercalés : le trou ne crée aucun rendement.
    prix = [10.00, 10.30, 9.90, 10.40, 10.10, 10.60]
    serre = J(prix)
    troue = J(prix, minutes=[0, 7, 50, 120, 300, 380])
    for k in ("dp_net", "dp_abs", "dp_efficience", "dp_range"):
        assert serre[k] == troue[k], k

    # (e) diviser tous les prix du jour par 2 ne change aucun log-rendement.
    moitie = J([p / 2 for p in prix])
    assert abs(moitie["dp_net"] - serre["dp_net"]) < 1e-12
    assert abs(moitie["dp_abs"] - serre["dp_abs"]) < 1e-12

    # (e bis) overnight à travers un split 2:1 : après retraitement, déplacement nul.
    def nd(prix_dollars, actions=100):
        return (actions * price_to_units_1e4(prix_dollars), actions)

    veille = {"date": "2023-01-19", "dp_net": 0.01, "dp_abs": 0.02,
              "p_max_u": price_to_units_1e4(10.00), "p_min_u": price_to_units_1e4(10.00),
              "vwap_first_nd": nd(10.00), "vwap_last_nd": nd(10.00)}
    jour = {"date": "2023-01-20", "dp_net": 0.01, "dp_abs": 0.02,
            "p_max_u": price_to_units_1e4(5.00), "p_min_u": price_to_units_1e4(5.00),
            "vwap_first_nd": nd(5.00), "vwap_last_nd": nd(5.00)}
    split = [("2023-01-20", 1.0)]                       # F = 2
    r_split = compute_p16_trailing([veille, jour], 1, ca_events=split)
    assert abs(r_split["dp_overnight"]) < 1e-12
    # sans retraitement, on lirait une chute de 50 %
    r_sans = compute_p16_trailing([veille, jour], 1, ca_events=[])
    assert abs(r_sans["dp_overnight"] - math.log(0.5)) < 1e-12
    # source CRSP indisponible : NA("ca")
    assert compute_p16_trailing([veille, jour], 1, ca_events=None)["dp_overnight"] == NA("ca")
    assert compute_p16_trailing([veille, jour], 0, ca_events=[])["dp_overnight"] == NA("pas_de_veille")

    # (f) absence de fuite : les fenêtres trailing ne voient ni le jour d ni la suite.
    serie = []
    for k in range(30):
        p = 10.00 + 0.01 * k
        serie.append({"date": f"2023-03-{k + 1:02d}", "dp_net": 0.002 * (1 if k % 2 else -1),
                      "dp_abs": 0.05, "p_max_u": price_to_units_1e4(p),
                      "p_min_u": price_to_units_1e4(p - 0.50),
                      "vwap_first_nd": nd(p), "vwap_last_nd": nd(p)})
    idx = 25
    complet = compute_p16_trailing(serie, idx, ca_events=[])
    tronque = compute_p16_trailing(serie[:idx + 1], idx, ca_events=[])
    assert complet == tronque                             # d+1..d+4 sans effet

    # un prix extrême au jour d change sa sortie, pas la fenêtre de normalisation.
    extreme = [dict(j) for j in serie]
    extreme[idx]["p_max_u"] = price_to_units_1e4(99.00)
    r_ext = compute_p16_trailing(extreme, idx, ca_events=[])
    assert r_ext["sigma_trail"] == complet["sigma_trail"]
    assert abs((r_ext["franchissement_haut"] - complet["franchissement_haut"])
               - math.log(99.00 / (10.00 + 0.01 * idx))) < 1e-9
    # franchissement : positif seulement au-dessus du plus haut des N précédents.
    assert complet["franchissement_haut"] > 0            # série croissante
    assert complet["franchissement_bas"] <= 0
    assert complet["n_histo_p16"] == N_TRAIL_P16
    # moins de N séances d'historique : NA.
    assert compute_p16_trailing(serie, 3, ca_events=[])["dp_net_z"] == NA("historique_court")
    # z = dp_net / médiane des |dp_net| des N séances précédentes.
    assert abs(complet["dp_net_z"] - serie[idx]["dp_net"] / 0.002) < 1e-9

    # (g) les prints d'enchère n'entrent ni dans les VWAP ni dans les extrêmes.
    sans_auction = J([10.00, 10.50])
    avec_auction = J([10.00, 10.50], extra=[
        _p16_tr(99.00, 100, 0, second=10, conditions=[17]),   # Opening Trade
        _p16_tr(0.50, 100, 1, second=10, conditions=[15]),    # Official Close
    ])
    for k in ("dp_net", "dp_abs", "dp_range", "p_max_u", "p_min_u"):
        assert sans_auction[k] == avec_auction[k], k

    # premarket et after-hours n'entrent pas.
    hors = list(J([10.00, 10.50]).items())
    with_pm = _p16_jour([10.00, 10.50], vr=vr, unknown=unknown, extra=[
        dict(_mktr(50.00, 100), sip_timestamp=_et_ts(2023, 1, 19, 8, 0, 0)),
        dict(_mktr(60.00, 100), sip_timestamp=_et_ts(2023, 1, 19, 17, 0, 0)),
    ])
    assert hors == list(with_pm.items())

    print("P-16 : OK")


def test_notionnel_exact():
    """La somme de notionnel est exacte et indépendante de l'ordre, sur une tape
    dont la somme flottante change quand on inverse l'ordre ; la référence est
    calculée en Decimal."""
    from decimal import Decimal as _D
    tables = load_tables()
    vr = tables["volume_rules"]

    # tape choisie pour que la somme flottante diffère entre ordre direct et
    # ordre inverse (16999956939.989702 contre ...698)
    prix = [2775.1635, 2308.0882, 3990.2113, 9191.7150, 5833.4877, 6342.8956]
    tailles = [623158, 965799, 538737, 945209, 281450, 88578]
    ts0 = _et_ts(2018, 6, 1, 10, 0, 0)
    trades = [{"price": p, "size": s, "exchange": 11, "conditions": [],
               "sip_timestamp": ts0 + i, "sequence_number": i}
              for i, (p, s) in enumerate(zip(prix, tailles))]

    def somme_u(seq):
        buckets = {"incl810": new_variant_bucket(), "excl810": new_variant_bucket()}
        pf: list = []
        for t in seq:
            accumulate_trade(buckets, t, vr, set(), pf)
        assert not pf
        assert buckets["incl810"]["n_notionnel_non_representable"] == 0
        return buckets["incl810"]["dvnotional_u"]

    direct, inverse = somme_u(trades), somme_u(list(reversed(trades)))
    attendu = int(sum((_D(str(p)) * 10000 * s for p, s in zip(prix, tailles)), _D(0)))
    assert direct == inverse == attendu, (direct, inverse, attendu)

    # Contrôle que la tape met bien en défaut une accumulation flottante. On
    # somme par `+=` : sum() applique une sommation compensée qui masquerait l'écart.
    f_direct = 0.0
    for p_, s_ in zip(prix, tailles):
        f_direct += p_ * s_
    f_inverse = 0.0
    for p_, s_ in reversed(list(zip(prix, tailles))):
        f_inverse += p_ * s_
    assert f_direct != f_inverse, "tape non discriminante : la somme flottante ne dépend pas de l'ordre"

    # prix non représentable en 1e-4 $ : exclu du notionnel et compté
    buckets = {"incl810": new_variant_bucket(), "excl810": new_variant_bucket()}
    pf = []
    accumulate_trade(buckets, {"price": 0.000123, "size": 100, "exchange": 11, "conditions": [],
                               "sip_timestamp": ts0, "sequence_number": 0}, vr, set(), pf)
    assert buckets["incl810"]["n_notionnel_non_representable"] == 1
    assert buckets["incl810"]["dvnotional_u"] == 0
    print("notionnel exact : OK")


def run_tests() -> bool:
    tables = load_tables()
    verify_auction_codes(tables["p2"])
    ok = True
    for name, fn in [("P-02", test_p02), ("P-03", test_p03), ("P-04", test_p04), ("P-14", test_p14),
                     ("bucket_grid", test_bucket_grid), ("P-01", test_p01), ("P-13", test_p13),
                     ("P-16", test_p16), ("notionnel_exact", test_notionnel_exact)]:
        try:
            fn()
        except AssertionError as e:
            print(f"{name} : ECHEC -- {e}", file=sys.stderr)
            ok = False
    if ok:
        print("tous les tests : OK", file=sys.stderr)
    return ok


# ---------------------------------------------------------------------------
# Exécution sur data-t1/
# ---------------------------------------------------------------------------

TITRE_JOUR_FIELDS = [
    "ticker", "date", "mois", "n_trades_raw",
    "p02_vol_all_incl810", "p02_trf_vol_all_incl810", "p02_trf_part_all_incl810",
    "p02_vol_continu_incl810", "p02_trf_vol_continu_incl810", "p02_trf_part_continu_incl810",
    "p02_vol_all_excl810", "p02_trf_vol_all_excl810", "p02_trf_part_all_excl810",
    "p02_vol_continu_excl810", "p02_trf_vol_continu_excl810", "p02_trf_part_continu_excl810",
    "p03_vol_ge1_all_incl810", "p03_sp_vol_all_incl810", "p03_sp_part_all_incl810",
    "p03_mid_vol_all_incl810", "p03_mid_part_all_incl810",
    "p03_vol_ge1_continu_incl810", "p03_sp_vol_continu_incl810", "p03_sp_part_continu_incl810",
    "p03_vol_ge1_all_excl810", "p03_sp_vol_all_excl810", "p03_sp_part_all_excl810",
    "p03_mid_vol_all_excl810", "p03_mid_part_all_excl810",
    "p03_vol_ge1_continu_excl810", "p03_sp_vol_continu_excl810", "p03_sp_part_continu_excl810",
    "p03_n_price_repr_failures",
    "p04_dvact_incl810", "p04_dvnotional_incl810", "p04_dvact_excl810", "p04_dvnotional_excl810",
    "p04_n_histo", "p04_rvact_incl810", "p04_rvact_na_code_incl810",
    "p04_rvnotional_incl810", "p04_rvnotional_na_code_incl810",
    "p04_rvact_excl810", "p04_rvact_na_code_excl810",
    "p04_rvnotional_excl810", "p04_rvnotional_na_code_excl810",
    "p04_ca_note",
    "p14_taux_hors_sequence_Z", "p14_taux_correction", "p14_vol_correction_part",
    "p14_n_correction_8", "p14_n_correction_10",
    "p14_delta_trf_part_all_810", "p14_delta_sp_part_all_810", "p14_delta_dvact_810",
    "p14_latence_mediane_ms", "p14_latence_p99_ms",
    "p14_taux_conditions_inconnues", "p14_taux_exclus", "p14_taux_desordre_fichier",
    "p14_taux_trous_mapping", "p14_note_mapping",
    "p14_desaccord_ssr", "p14_note_ssr",
]


def fmt(x):
    if isinstance(x, NA):
        return f"NA({x.code})"
    return x


def fmt_row(row: dict) -> dict:
    """fmt() appliqué à toutes les valeurs d'une ligne gardée brute jusqu'à l'écriture CSV."""
    return {k: fmt(v) for k, v in row.items()}


def list_ticker_days() -> list[tuple[str, str, Path]]:
    out = []
    for ticker_dir in sorted(DATA_T1_DIR.iterdir()):
        if not ticker_dir.is_dir():
            continue
        for f in sorted(ticker_dir.glob("*.json.gz")):
            date = f.name.replace(".json.gz", "")
            out.append((ticker_dir.name, date, f))
    return out


def load_trades(path: Path) -> list[dict]:
    with gzip.open(path, "rt", encoding="utf-8") as fh:
        return json.load(fh)


def process_ticker_day(ticker: str, date: str, trades_raw: list[dict], tables: dict,
                        seq_stats_accum: dict) -> dict:
    volume_rules = tables["volume_rules"]

    # P-14 et le contrôle des sequence_number portent sur l'ordre du fichier, avant tri.
    unknown_ids_all: set[int] = set()
    p14 = compute_p14(trades_raw, trades_raw, volume_rules, unknown_ids_all)
    seq_check = check_sequence_number_order(trades_raw)
    for k, v in seq_check.items():
        seq_stats_accum[k] += v

    # ordre canonique : tri par (sip_timestamp, sequence_number).
    trades = sorted(trades_raw, key=lambda t: (t["sip_timestamp"], t["sequence_number"]))

    buckets = {"incl810": new_variant_bucket(), "excl810": new_variant_bucket()}
    unknown_ids: set[int] = set()
    price_failures: list = []
    for t in trades:
        accumulate_trade(buckets, t, volume_rules, unknown_ids, price_failures)

    b_i, b_e = buckets["incl810"], buckets["excl810"]

    trf_part_all_i = part(b_i["trf_vol_all"], b_i["vol_all"])
    trf_part_all_e = part(b_e["trf_vol_all"], b_e["vol_all"])
    sp_part_all_i = part(b_i["sp_vol_all"], b_i["vol_ge1_all"])
    sp_part_all_e = part(b_e["sp_vol_all"], b_e["vol_ge1_all"])

    def delta(a, b):
        if is_na(a) or is_na(b):
            return NA("variant_na")
        return abs(a - b)

    row = {
        "ticker": ticker, "date": date, "mois": date[:7], "n_trades_raw": len(trades_raw),
        "p02_vol_all_incl810": b_i["vol_all"], "p02_trf_vol_all_incl810": b_i["trf_vol_all"],
        "p02_trf_part_all_incl810": fmt(trf_part_all_i),
        "p02_vol_continu_incl810": b_i["vol_continu"], "p02_trf_vol_continu_incl810": b_i["trf_vol_continu"],
        "p02_trf_part_continu_incl810": fmt(part(b_i["trf_vol_continu"], b_i["vol_continu"])),
        "p02_vol_all_excl810": b_e["vol_all"], "p02_trf_vol_all_excl810": b_e["trf_vol_all"],
        "p02_trf_part_all_excl810": fmt(trf_part_all_e),
        "p02_vol_continu_excl810": b_e["vol_continu"], "p02_trf_vol_continu_excl810": b_e["trf_vol_continu"],
        "p02_trf_part_continu_excl810": fmt(part(b_e["trf_vol_continu"], b_e["vol_continu"])),
        "p03_vol_ge1_all_incl810": b_i["vol_ge1_all"], "p03_sp_vol_all_incl810": b_i["sp_vol_all"],
        "p03_sp_part_all_incl810": fmt(sp_part_all_i),
        "p03_mid_vol_all_incl810": b_i["mid_vol_all"],
        "p03_mid_part_all_incl810": fmt(part(b_i["mid_vol_all"], b_i["vol_ge1_all"])),
        "p03_vol_ge1_continu_incl810": b_i["vol_ge1_continu"], "p03_sp_vol_continu_incl810": b_i["sp_vol_continu"],
        "p03_sp_part_continu_incl810": fmt(part(b_i["sp_vol_continu"], b_i["vol_ge1_continu"])),
        "p03_vol_ge1_all_excl810": b_e["vol_ge1_all"], "p03_sp_vol_all_excl810": b_e["sp_vol_all"],
        "p03_sp_part_all_excl810": fmt(sp_part_all_e),
        "p03_mid_vol_all_excl810": b_e["mid_vol_all"],
        "p03_mid_part_all_excl810": fmt(part(b_e["mid_vol_all"], b_e["vol_ge1_all"])),
        "p03_vol_ge1_continu_excl810": b_e["vol_ge1_continu"], "p03_sp_vol_continu_excl810": b_e["sp_vol_continu"],
        "p03_sp_part_continu_excl810": fmt(part(b_e["sp_vol_continu"], b_e["vol_ge1_continu"])),
        "p03_n_price_repr_failures": len(price_failures),
        "p04_dvact_incl810": b_i["dvact"],
        "p04_dvnotional_incl810": notionnel_dollars(b_i["dvnotional_u"]),
        "p04_dvact_excl810": b_e["dvact"],
        "p04_dvnotional_excl810": notionnel_dollars(b_e["dvnotional_u"]),
        "p14_taux_hors_sequence_Z": fmt(p14["taux_hors_sequence_Z"]),
        "p14_taux_correction": fmt(p14["taux_correction"]),
        "p14_vol_correction_part": fmt(p14["vol_correction_part"]),
        "p14_n_correction_8": p14["n_correction_8"], "p14_n_correction_10": p14["n_correction_10"],
        "p14_delta_trf_part_all_810": fmt(delta(trf_part_all_i, trf_part_all_e)),
        "p14_delta_sp_part_all_810": fmt(delta(sp_part_all_i, sp_part_all_e)),
        "p14_delta_dvact_810": abs(b_i["dvact"] - b_e["dvact"]),
        "p14_latence_mediane_ms": fmt(p14["latence_mediane_ms"]),
        "p14_latence_p99_ms": fmt(p14["latence_p99_ms"]),
        "p14_taux_conditions_inconnues": fmt(p14["taux_conditions_inconnues"]),
        "p14_taux_exclus": fmt(p14["taux_exclus"]),
        "p14_taux_desordre_fichier": fmt(p14["taux_desordre_fichier"]),
        "p14_taux_trous_mapping": 0,
        "p14_note_mapping": "sans objet sur l'echantillon (mapping direct ticker<->titre)",
        "p14_desaccord_ssr": "NA",
        "p14_note_ssr": "voir P-12b",
    }
    # Le second élément garde le notionnel en unités entières : il alimente la
    # fenêtre trailing de P-04 et l'agrégat titre-mois.
    return row, {"incl810_dvact": b_i["dvact"], "incl810_dvnotional_u": b_i["dvnotional_u"],
                 "excl810_dvact": b_e["dvact"], "excl810_dvnotional_u": b_e["dvnotional_u"],
                 "n_notionnel_non_representable": b_i["n_notionnel_non_representable"]}


# ---------------------------------------------------------------------------
# P-01 / P-13 : agrégation par ticker-jour
# ---------------------------------------------------------------------------

P01_P13_FIELDS = [
    "ticker", "date", "mois", "n_trades_raw",
    "p01_vplus_all", "p01_vminus_all", "p01_vna_all", "p01_vtrf",
    "p01_vplus_all_notional", "p01_vminus_all_notional", "p01_vna_all_notional", "p01_vtrf_notional",
    "p01_vplus_lit", "p01_vminus_lit", "p01_vna_lit",
    "p01_vplus_lit_notional", "p01_vminus_lit_notional", "p01_vna_lit_notional",
    "p01_ofi_median_all_actions", "p01_ofi_median_all_notional",
    "p01_ofi_median_lit_actions", "p01_ofi_median_lit_notional",
    "p01_n_buckets_ofi_all", "p01_n_buckets_ofi_lit",
    "p01_part_signable_all", "p01_part_signable_lit",
    "p01_n_both_defined", "p01_n_disagree", "p01_desaccord_rate", "p01_n_only_one_na",
    "p01_n_hors_grille",
    "p13_n_lit_trf", "p13_vact_lit_trf",
    "p13_q95_abs_acc", "p13_n_acc_buckets",
    "p13_ca_note",
]


def process_ticker_day_p01_p13(ticker: str, date: str, trades_raw: list[dict], tables: dict) -> dict:
    """P-01 (mode tick) et P-13 sur un ticker-jour, indépendamment de process_ticker_day."""
    volume_rules = tables["volume_rules"]
    trades = sorted(trades_raw, key=lambda t: (t["sip_timestamp"], t["sequence_number"]))
    unknown_ids: set[int] = set()

    p01_buckets, p13_counts, p13_vact, stats = compute_p01_p13(trades, volume_rules, unknown_ids)
    p01_day = summarize_p01_day(p01_buckets)
    p13_day = summarize_p13_day(p13_counts, h=30)

    n_lit_trf = sum(sum(v) for v in p13_counts.values())
    vact_lit_trf = sum(sum(v) for v in p13_vact.values())

    s = p01_day["sums"]
    desaccord_rate = part(stats["n_disagree"], stats["n_both_defined"])

    row = {
        "ticker": ticker, "date": date, "mois": date[:7], "n_trades_raw": len(trades_raw),
        "p01_vplus_all": s["vplus_all"], "p01_vminus_all": s["vminus_all"], "p01_vna_all": s["vna_all"],
        "p01_vtrf": s["vtrf"],
        "p01_vplus_all_notional": notionnel_dollars(s["vplus_all_notional_u"]),
        "p01_vminus_all_notional": notionnel_dollars(s["vminus_all_notional_u"]),
        "p01_vna_all_notional": notionnel_dollars(s["vna_all_notional_u"]),
        "p01_vtrf_notional": notionnel_dollars(s["vtrf_notional_u"]),
        "p01_vplus_lit": s["vplus_lit"], "p01_vminus_lit": s["vminus_lit"], "p01_vna_lit": s["vna_lit"],
        "p01_vplus_lit_notional": notionnel_dollars(s["vplus_lit_notional_u"]),
        "p01_vminus_lit_notional": notionnel_dollars(s["vminus_lit_notional_u"]),
        "p01_vna_lit_notional": notionnel_dollars(s["vna_lit_notional_u"]),
        # Valeurs laissées brutes (NA ou float) pour l'agrégat titre-mois ;
        # fmt_row les formate à l'écriture CSV.
        "p01_ofi_median_all_actions": p01_day["ofi_median_all_actions"],
        "p01_ofi_median_all_notional": p01_day["ofi_median_all_notional"],
        "p01_ofi_median_lit_actions": p01_day["ofi_median_lit_actions"],
        "p01_ofi_median_lit_notional": p01_day["ofi_median_lit_notional"],
        "p01_n_buckets_ofi_all": p01_day["n_buckets_ofi_all"], "p01_n_buckets_ofi_lit": p01_day["n_buckets_ofi_lit"],
        "p01_part_signable_all": p01_day["part_signable_all"],
        "p01_part_signable_lit": p01_day["part_signable_lit"],
        "p01_n_both_defined": stats["n_both_defined"], "p01_n_disagree": stats["n_disagree"],
        "p01_desaccord_rate": desaccord_rate, "p01_n_only_one_na": stats["n_only_one_na"],
        "p01_n_hors_grille": stats["n_hors_grille"],
        "p13_n_lit_trf": n_lit_trf, "p13_vact_lit_trf": vact_lit_trf,
        "p13_q95_abs_acc": p13_day["q95_abs_acc"], "p13_n_acc_buckets": p13_day["n_acc_buckets"],
        "p13_ca_note": ("sans objet (ACC intra-jour seulement, jamais de fenetre inter-jours -- "
                        "F_{d'->d}=1 trivialement ; 0 evenement FRS verifie sur les permnos "
                        "mappables des 738 ticker-jours de l'echantillon)"),
    }
    return row


def run_pipeline(limit: int | None = None) -> None:
    SORTIES_DIR.mkdir(parents=True, exist_ok=True)
    tables = load_tables()
    verify_auction_codes(tables["p2"])

    ticker_days = list_ticker_days()
    if limit:
        ticker_days = ticker_days[:limit]

    progress_path = SORTIES_DIR / ".progress.jsonl"
    # Pour P-04, l'historique est regroupé par (ticker, mois) : la tape locale ne
    # couvre qu'un mois par titre, et un ticker présent en 2018-06 et en 2026-01
    # ne doit pas enchaîner deux mois séparés de plusieurs années.
    rows_by_ticker_mois: dict[tuple[str, str], list[dict]] = defaultdict(list)
    all_rows: dict[tuple[str, str], dict] = {}
    all_rows_p01_p13: dict[tuple[str, str], dict] = {}
    seq_stats = defaultdict(int)

    t0 = time.time()
    with progress_path.open("a", encoding="utf-8") as pf:
        for i, (ticker, date, path) in enumerate(ticker_days):
            trades_raw = load_trades(path)
            row, dv = process_ticker_day(ticker, date, trades_raw, tables, seq_stats)
            all_rows[(ticker, date)] = row
            rows_by_ticker_mois[(ticker, date[:7])].append({"date": date, **dv})
            all_rows_p01_p13[(ticker, date)] = process_ticker_day_p01_p13(ticker, date, trades_raw, tables)
            pf.write(json.dumps({"ticker": ticker, "date": date, "n_trades": len(trades_raw),
                                  "t": time.time() - t0}) + "\n")
            pf.flush()
            if (i + 1) % 100 == 0:
                print(f"[primitives_t1] {i+1}/{len(ticker_days)} ticker-jours traités "
                      f"({time.time()-t0:.1f}s)", file=sys.stderr)

    print(f"[primitives_t1] {len(ticker_days)} ticker-jours traités en {time.time()-t0:.1f}s",
          file=sys.stderr)

    # Seconde passe : P-04 sur l'historique intra-mois. Aucun événement FRS ne
    # tombe dans les fenêtres de l'échantillon, d'où ca_events=None (aucun retraitement).
    n_ok = n_histo = n_med0 = 0
    for (ticker, mois_key), entries in rows_by_ticker_mois.items():
        entries.sort(key=lambda e: e["date"])
        for variant in ("incl810", "excl810"):
            series = [{"date": e["date"], "dvact": e[f"{variant}_dvact"],
                       "dvnotional_u": e[f"{variant}_dvnotional_u"]} for e in entries]
            for idx, e in enumerate(entries):
                r = compute_rv(series, idx, ca_events=None)
                row = all_rows[(ticker, e["date"])]
                row[f"p04_rvact_{variant}"] = fmt(r["rvact"])
                row[f"p04_rvact_na_code_{variant}"] = r["rvact_na_code"]
                row[f"p04_rvnotional_{variant}"] = fmt(r["rv_notional"])
                row[f"p04_rvnotional_na_code_{variant}"] = r["rv_notional_na_code"]
                if variant == "incl810":
                    row["p04_n_histo"] = r["n_histo"]
                    if row["mois"] == "2026-01":
                        row["p04_ca_note"] = "hors couverture CRSP (distributions arretees a fin 2025) : pas de retraitement des operations sur titres"
                    else:
                        row["p04_ca_note"] = ("verifie sans objet : aucun evenement FRS dans la table CRSP des distributions "
                                               "sur la fenetre intra-mois des titre-jours candidats")
                    if not is_na(r["rvact"]):
                        n_ok += 1
                    elif r["rvact_na_code"] == "histo":
                        n_histo += 1
                    elif r["rvact_na_code"] == "med0":
                        n_med0 += 1

    # CSV titre-jour
    out_path = SORTIES_DIR / "p02_p03_p04_p14_titre_jour.csv"
    with out_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=TITRE_JOUR_FIELDS)
        w.writeheader()
        for (ticker, date) in sorted(all_rows):
            w.writerow(all_rows[(ticker, date)])
    print(f"[primitives_t1] écrit {out_path}", file=sys.stderr)

    # Agrégat titre-mois : ratios de sommes, pas moyennes de ratios. Le notionnel
    # est sommé à part, en unités entières, et non depuis les valeurs en dollars.
    agg = defaultdict(lambda: defaultdict(float))
    agg_dvnotional_u: dict[tuple[str, str], int] = defaultdict(int)
    for (ticker, date) in sorted(all_rows):        # ordre fixe, pour un résultat déterministe
        row = all_rows[(ticker, date)]
        key = (ticker, row["mois"])
        a = agg[key]
        for f in ["p02_vol_all_incl810", "p02_trf_vol_all_incl810", "p02_vol_continu_incl810",
                  "p02_trf_vol_continu_incl810", "p02_vol_all_excl810", "p02_trf_vol_all_excl810",
                  "p02_vol_continu_excl810", "p02_trf_vol_continu_excl810",
                  "p03_vol_ge1_all_incl810", "p03_sp_vol_all_incl810", "p03_mid_vol_all_incl810",
                  "p03_vol_ge1_all_excl810", "p03_sp_vol_all_excl810", "p03_mid_vol_all_excl810",
                  "p04_dvact_incl810"]:
            a[f] += row[f]
        a["n_jours"] += 1
    for (ticker, mois_key), entries in rows_by_ticker_mois.items():
        agg_dvnotional_u[(ticker, mois_key)] = sum(e["incl810_dvnotional_u"] for e in entries)

    mois_fields = ["ticker", "mois", "n_jours",
                   "p02_vol_all_incl810", "p02_trf_vol_all_incl810", "p02_trf_part_all_incl810",
                   "p02_vol_continu_incl810", "p02_trf_vol_continu_incl810", "p02_trf_part_continu_incl810",
                   "p02_trf_part_all_excl810",
                   "p03_vol_ge1_all_incl810", "p03_sp_vol_all_incl810", "p03_sp_part_all_incl810",
                   "p03_mid_vol_all_incl810", "p03_mid_part_all_incl810",
                   "p03_sp_part_all_excl810",
                   "p04_dvact_incl810", "p04_dvnotional_incl810"]
    out_path_mois = SORTIES_DIR / "p02_p03_p04_p14_titre_mois.csv"
    with out_path_mois.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=mois_fields)
        w.writeheader()
        for (ticker, mois), a in sorted(agg.items()):
            w.writerow({
                "ticker": ticker, "mois": mois, "n_jours": int(a["n_jours"]),
                "p02_vol_all_incl810": int(a["p02_vol_all_incl810"]),
                "p02_trf_vol_all_incl810": int(a["p02_trf_vol_all_incl810"]),
                "p02_trf_part_all_incl810": fmt(part(a["p02_trf_vol_all_incl810"], a["p02_vol_all_incl810"])),
                "p02_vol_continu_incl810": int(a["p02_vol_continu_incl810"]),
                "p02_trf_vol_continu_incl810": int(a["p02_trf_vol_continu_incl810"]),
                "p02_trf_part_continu_incl810": fmt(part(a["p02_trf_vol_continu_incl810"], a["p02_vol_continu_incl810"])),
                "p02_trf_part_all_excl810": fmt(part(a["p02_trf_vol_all_excl810"], a["p02_vol_all_excl810"])),
                "p03_vol_ge1_all_incl810": int(a["p03_vol_ge1_all_incl810"]),
                "p03_sp_vol_all_incl810": int(a["p03_sp_vol_all_incl810"]),
                "p03_sp_part_all_incl810": fmt(part(a["p03_sp_vol_all_incl810"], a["p03_vol_ge1_all_incl810"])),
                "p03_mid_vol_all_incl810": int(a["p03_mid_vol_all_incl810"]),
                "p03_mid_part_all_incl810": fmt(part(a["p03_mid_vol_all_incl810"], a["p03_vol_ge1_all_incl810"])),
                "p03_sp_part_all_excl810": fmt(part(a["p03_sp_vol_all_excl810"], a["p03_vol_ge1_all_excl810"])),
                "p04_dvact_incl810": int(a["p04_dvact_incl810"]),
                "p04_dvnotional_incl810": notionnel_dollars(agg_dvnotional_u[(ticker, mois)]),
            })
    print(f"[primitives_t1] écrit {out_path_mois}", file=sys.stderr)

    # P-01 / P-13 : CSV titre-jour
    out_path_p01p13 = SORTIES_DIR / "p01_p13_titre_jour.csv"
    with out_path_p01p13.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=P01_P13_FIELDS)
        w.writeheader()
        for (ticker, date) in sorted(all_rows_p01_p13):
            w.writerow(fmt_row(all_rows_p01_p13[(ticker, date)]))
    print(f"[primitives_t1] écrit {out_path_p01p13}", file=sys.stderr)

    # P-01 / P-13 : agrégat titre-mois. Volumes et compteurs de désaccord sont
    # sommés puis divisés ; OFI médian, part signable et q95 d'ACC, déjà des
    # résumés journaliers, sont agrégés par la médiane des valeurs journalières.
    agg_p01p13 = defaultdict(lambda: defaultdict(float))
    daily_ofi_all_actions = defaultdict(list)
    daily_ofi_all_notional = defaultdict(list)
    daily_ofi_lit_actions = defaultdict(list)
    daily_ofi_lit_notional = defaultdict(list)
    daily_part_signable_all = defaultdict(list)
    daily_part_signable_lit = defaultdict(list)
    daily_q95_acc = defaultdict(list)
    for (ticker, date) in sorted(all_rows_p01_p13):        # ordre fixe, pour un résultat déterministe
        row = all_rows_p01_p13[(ticker, date)]
        key = (ticker, row["mois"])
        a = agg_p01p13[key]
        for f in ["p01_vplus_all", "p01_vminus_all", "p01_vna_all", "p01_vtrf",
                  "p01_vplus_lit", "p01_vminus_lit", "p01_vna_lit",
                  "p01_n_both_defined", "p01_n_disagree",
                  "p13_n_lit_trf", "p13_vact_lit_trf"]:
            a[f] += row[f]
        a["n_jours"] += 1
        if not is_na(row["p01_ofi_median_all_actions"]):
            daily_ofi_all_actions[key].append(row["p01_ofi_median_all_actions"])
        if not is_na(row["p01_ofi_median_all_notional"]):
            daily_ofi_all_notional[key].append(row["p01_ofi_median_all_notional"])
        if not is_na(row["p01_ofi_median_lit_actions"]):
            daily_ofi_lit_actions[key].append(row["p01_ofi_median_lit_actions"])
        if not is_na(row["p01_ofi_median_lit_notional"]):
            daily_ofi_lit_notional[key].append(row["p01_ofi_median_lit_notional"])
        if not is_na(row["p01_part_signable_all"]):
            daily_part_signable_all[key].append(row["p01_part_signable_all"])
        if not is_na(row["p01_part_signable_lit"]):
            daily_part_signable_lit[key].append(row["p01_part_signable_lit"])
        if not is_na(row["p13_q95_abs_acc"]):
            daily_q95_acc[key].append(row["p13_q95_abs_acc"])

    def med_or_na(d, key):
        vals = d.get(key)
        return statistics.median(vals) if vals else NA("aucun_jour")

    mois_fields_p01p13 = [
        "ticker", "mois", "n_jours",
        "p01_vplus_all", "p01_vminus_all", "p01_vna_all", "p01_vtrf",
        "p01_vplus_lit", "p01_vminus_lit", "p01_vna_lit",
        "p01_ofi_median_all_actions_mois", "p01_ofi_median_all_notional_mois",
        "p01_ofi_median_lit_actions_mois", "p01_ofi_median_lit_notional_mois",
        "p01_part_signable_all_mois", "p01_part_signable_lit_mois",
        "p01_desaccord_rate_mois", "p01_n_both_defined", "p01_n_disagree",
        "p13_q95_abs_acc_mediane_mois",
        "p13_n_lit_trf", "p13_vact_lit_trf",
    ]
    out_path_mois_p01p13 = SORTIES_DIR / "p01_p13_titre_mois.csv"
    with out_path_mois_p01p13.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=mois_fields_p01p13)
        w.writeheader()
        for (ticker, mois), a in sorted(agg_p01p13.items()):
            w.writerow({
                "ticker": ticker, "mois": mois, "n_jours": int(a["n_jours"]),
                "p01_vplus_all": int(a["p01_vplus_all"]), "p01_vminus_all": int(a["p01_vminus_all"]),
                "p01_vna_all": int(a["p01_vna_all"]), "p01_vtrf": int(a["p01_vtrf"]),
                "p01_vplus_lit": int(a["p01_vplus_lit"]), "p01_vminus_lit": int(a["p01_vminus_lit"]),
                "p01_vna_lit": int(a["p01_vna_lit"]),
                "p01_ofi_median_all_actions_mois": fmt(med_or_na(daily_ofi_all_actions, (ticker, mois))),
                "p01_ofi_median_all_notional_mois": fmt(med_or_na(daily_ofi_all_notional, (ticker, mois))),
                "p01_ofi_median_lit_actions_mois": fmt(med_or_na(daily_ofi_lit_actions, (ticker, mois))),
                "p01_ofi_median_lit_notional_mois": fmt(med_or_na(daily_ofi_lit_notional, (ticker, mois))),
                "p01_part_signable_all_mois": fmt(med_or_na(daily_part_signable_all, (ticker, mois))),
                "p01_part_signable_lit_mois": fmt(med_or_na(daily_part_signable_lit, (ticker, mois))),
                "p01_desaccord_rate_mois": fmt(part(int(a["p01_n_disagree"]), int(a["p01_n_both_defined"]))),
                "p01_n_both_defined": int(a["p01_n_both_defined"]), "p01_n_disagree": int(a["p01_n_disagree"]),
                "p13_q95_abs_acc_mediane_mois": fmt(med_or_na(daily_q95_acc, (ticker, mois))),
                "p13_n_lit_trf": int(a["p13_n_lit_trf"]), "p13_vact_lit_trf": int(a["p13_vact_lit_trf"]),
            })
    print(f"[primitives_t1] écrit {out_path_mois_p01p13}", file=sys.stderr)

    # Résumé : contrôle des sequence_number et répartition des NA de P-04.
    summary = {
        "seq_number_check": dict(seq_stats),
        "p04_n_titre_jours_ok": n_ok, "p04_n_titre_jours_histo": n_histo, "p04_n_titre_jours_med0": n_med0,
        "n_ticker_days": len(ticker_days),
    }
    with (SORTIES_DIR / ".run_summary.json").open("w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)
    print(f"[primitives_t1] résumé : {summary}", file=sys.stderr)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="Primitives de niveau 1 sur la tape de trades (P-01 tick, P-02, P-03, P-04, P-13, P-14).")
    parser.add_argument("--test", action="store_true", help="tests unitaires, sans accès aux données")
    parser.add_argument("--run", action="store_true", help="exécution sur data-t1/")
    parser.add_argument("--limit", type=int, default=None, help="limiter le nombre de ticker-jours (debug)")
    args = parser.parse_args()

    if args.test:
        ok = run_tests()
        sys.exit(0 if ok else 1)

    if args.run:
        run_pipeline(limit=args.limit)
        return

    parser.error("--test ou --run requis")


if __name__ == "__main__":
    main()
