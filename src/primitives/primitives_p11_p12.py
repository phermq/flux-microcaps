#!/usr/bin/env python3
"""P-11 (distance aux bandes LULD recalculées) et P-12 (flags halt, SSR,
session) sur l'échantillon de calibration.

P-11 reconstruit les bandes LULD du Plan (23e amendement, Tier 2) à partir des
seuls trades SIP. Elle a été retirée du projet faute de cotations (NBBO) : le
code est conservé, mais seule la partie P-12 est utilisée en aval.

Les règles de mise à jour par condition SIP et le critère TRF viennent de
`src/donnees/mesures_sip.py` ; les codes d'enchère sont redéclarés ici et
vérifiés contre la table P2 au chargement. Python 3, stdlib, plus
databento_dbn et zstandard pour la validation sur le status ITCH.

Usage :
    python3 src/primitives/primitives_p11_p12.py --test             # tests unitaires
    python3 src/primitives/primitives_p11_p12.py --ert [--budget S]   # choix du filtre ERT
    python3 src/primitives/primitives_p11_p12.py --run [--budget S]   # calcul sur l'échantillon
    python3 src/primitives/primitives_p11_p12.py --recap              # agrégation des sorties
"""

from __future__ import annotations

import argparse
import csv
import gzip
import heapq
import json
import math
import os
import statistics
import sys
import time
from collections import Counter, defaultdict, deque
from datetime import date as ddate, datetime, time as dtime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

CODE_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import chemins  # noqa: E402
A_REF_DIR = chemins.REFERENCES
A_CRSP_DIR = chemins.DONNEES / "crsp"
MBO_DIR = chemins.DONNEES / "mbo-echantillon"
ECH_CSV = chemins.DONNEES / "echantillon-calibration" / "echantillon.csv"
DATA_T1 = chemins.DONNEES / "data-t1"
DATA_Q18 = chemins.DONNEES / "halts"
HALTS_CSV = DATA_Q18 / "halts-nyse-2026-01.csv"
SHORTHALTS_DIR = DATA_Q18 / "shorthalts"
OUT_DIR = chemins.SORTIES / "p11-p12"
PROGRESS = OUT_DIR / ".progress.jsonl"

import mesures_sip as msip  # noqa: E402

ET = ZoneInfo("America/New_York")
NS = 1_000_000_000

# ---------------------------------------------------------------------------
# Constantes
# ---------------------------------------------------------------------------

# Codes sale_condition des prints d'enchère, vérifiés contre P2 au chargement.
AUCTION_CODES = {9, 15, 16, 17, 18, 25, 28, 38, 55}

# Sous-ensembles de AUCTION_CODES identifiant l'« Opening Price » et le
# « Reopening Price on the Primary Listing Exchange » du Plan LULD (§V(B), §V(C)).
OPENING_CODES = {16, 17, 25, 55}     # Official Open / Opening Trade / Opening Prints
REOPENING_CODES = {18, 28, 55}       # Reopening Trade / Re-Opening Prints
OFFICIAL_CLOSE_CODES = {15, 19}      # Market Center Official Close / Closing Trade
CORRECTED_CONSOLIDATED_CLOSE = 38

# Bornes de session en heure de l'Est ; l'heure d'été est gérée par zoneinfo.
PREMARKET = (dtime(4, 0), dtime(9, 30))
RTH = (dtime(9, 30), dtime(16, 0))
AFTERHOURS = (dtime(16, 0), dtime(20, 0))

# Paramètres du Plan LULD (23e amendement).
LULD_WINDOW_NS = 300 * NS            # moyenne 5 min
LULD_MIN_RP_LIFETIME_NS = 30 * NS    # "each new Reference Price shall remain in effect for at least 30 seconds"
LULD_HYSTERESIS = 0.01               # "moved by 1% or more"
DOUBLING_START = dtime(15, 35)       # 15:35 ET en séance normale ; 25 dernières minutes sinon
DOUBLING_LAST_MINUTES = 25

# Tous les titres de l'univers sont en Tier 2 (Plan, Appendix A).
TIER = 2

# Rule 201 (Reg SHO) : seuil de baisse par rapport à la clôture de la veille.
SSR_THRESHOLD = 0.9

# Tranches de prix utilisées pour ventiler les distributions.
PRICE_TRANCHES = (("[0.10-1)", 0.10, 1.0), ("[1-5)", 1.0, 5.0), ("[5+)", 5.0, float("inf")))

# Code de bourse de cotation (convention CRSP, echantillon.csv) -> id de venue
# de la table P1.
PRIMARY_EXCHANGE_ID = {"Q": 12, "N": 10, "A": 1}

# Demi-séances (clôture anticipée) lues dans `src/references/early_closes.csv`,
# la même table que celle utilisée par `primitives_t1.py`. L'échantillon de
# calibration (2018-06, 2026-01) n'en contient aucune.
EARLY_CLOSES_PATH = Path(__file__).resolve().parent.parent / "references" / "early_closes.csv"


def _charger_early_closes() -> dict[str, dtime]:
    if not EARLY_CLOSES_PATH.exists():
        raise FileNotFoundError(
            f"table des demi-séances absente : {EARLY_CLOSES_PATH} ; P-12c ne peut "
            "pas être calculée sans elle (la grille de séance serait fausse les jours "
            "de clôture anticipée)")
    with EARLY_CLOSES_PATH.open(newline="") as f:
        out = {}
        for row in csv.DictReader(f):
            h, m = row["close_time_ET"].split(":")
            out[row["date"]] = dtime(int(h), int(m))
    return out


EARLY_CLOSES: dict[str, dtime] = _charger_early_closes()


# ---------------------------------------------------------------------------
# Valeur manquante avec code de raison
# ---------------------------------------------------------------------------

class NA:
    __slots__ = ("code",)

    def __init__(self, code: str):
        self.code = code

    def __repr__(self):
        return f"NA({self.code})"

    def __eq__(self, other):
        return isinstance(other, NA) and self.code == other.code

    def __hash__(self):
        return hash(("NA", self.code))

    def __bool__(self):
        return False


def is_na(x) -> bool:
    return isinstance(x, NA)


def jsonable(x):
    if is_na(x):
        return {"na": x.code}
    return x


# ---------------------------------------------------------------------------
# Tables de référence
# ---------------------------------------------------------------------------

_TABLES = None

ERT_CANDIDATES = ("updates_volume", "updates_high_low", "updates_open_close")


def load_update_rules(p2: dict, field: str) -> dict[int, bool]:
    """Règle `update_rules.consolidated[field]` par id de condition de trade.

    Généralise `msip.load_volume_rules` aux trois champs. Un id présent dans
    plusieurs catégories est exclu et traité comme inconnu."""
    candidates: dict[int, list[dict]] = {}
    for entry in p2["results"]:
        if "trade" not in entry.get("data_types", []):
            continue
        rules = entry.get("update_rules", {}).get("consolidated")
        if rules is None or field not in rules:
            continue
        candidates.setdefault(entry["id"], []).append(entry)
    out = {}
    for cid, entries in candidates.items():
        if len(entries) > 1:
            continue
        out[cid] = entries[0]["update_rules"]["consolidated"][field]
    return out


def load_tables() -> dict:
    global _TABLES
    if _TABLES is not None:
        return _TABLES
    p1 = msip.load_json(A_REF_DIR / "p1-exchanges.json")
    p2 = msip.load_json(A_REF_DIR / "p2-conditions.json")
    rules = {f: load_update_rules(p2, f) for f in ERT_CANDIDATES}
    # Le champ updates_volume doit coïncider avec mesures_sip.
    assert rules["updates_volume"] == msip.load_volume_rules(p2), (
        "load_update_rules('updates_volume') diverge de msip.load_volume_rules"
    )
    by_cat = {(e["type"], e["id"]) for e in p2["results"]}
    for cid in AUCTION_CODES:
        assert ("sale_condition", cid) in by_cat, f"AUCTION_CODES id={cid} absent de sale_condition"
    for cid in OPENING_CODES | REOPENING_CODES | OFFICIAL_CLOSE_CODES | {CORRECTED_CONSOLIDATED_CLOSE}:
        assert ("sale_condition", cid) in by_cat, f"code d'enchère id={cid} absent de sale_condition"
    _TABLES = {"p1": p1, "p2": p2, "rules": rules}
    return _TABLES


# ---------------------------------------------------------------------------
# Prédicats par trade
# ---------------------------------------------------------------------------

def is_trf(trade: dict) -> bool:
    """Un print est reporté sur un TRF (hors bourse) si trf_id est présent."""
    return trade.get("trf_id") is not None


def conditions(trade: dict) -> list[int]:
    return trade.get("conditions") or []


def is_auction(trade: dict) -> bool:
    return any(c in AUCTION_CODES for c in conditions(trade))


def passes_filter(trade: dict, rules: dict[int, bool], unknown: set | None = None) -> bool:
    """Vrai si toutes les conditions du trade ont le champ à True. Une
    condition absente de la table compte comme True et est ajoutée à `unknown`."""
    for cid in conditions(trade):
        r = rules.get(cid)
        if r is None:
            if unknown is not None:
                unknown.add(cid)
            continue
        if not r:
            return False
    return True


def is_admissible_volume(trade: dict, vol_rules: dict[int, bool], unknown=None) -> bool:
    """Trade admissible en volume (noté T̃) : updates_volume vrai pour toutes
    ses conditions et taille non nulle."""
    if trade.get("size", 0) == 0:
        return False
    return passes_filter(trade, vol_rules, unknown)


def is_lit_admissible(trade: dict, vol_rules: dict[int, bool]) -> bool:
    """Print pris en compte pour la distance aux bandes : hors TRF, admissible
    en volume, hors enchère.

    Les enchères sont exclues car le Plan LULD (§VI(A)(1)) autorise les prints
    d'ouverture, de reprise et de clôture de la bourse primaire hors bandes ;
    ils sont comptés à part dans `auction_hors_bandes`."""
    return (not is_trf(trade)) and (not is_auction(trade)) and is_admissible_volume(trade, vol_rules)


# ---------------------------------------------------------------------------
# Calendrier / sessions
# ---------------------------------------------------------------------------

_CAL = None


def trading_days() -> list[str]:
    """Jours cotés. Le calendrier CRSP s'arrête au 2025-12-31 ; les jours
    ultérieurs sont déduits des dates présentes dans data-t1 et les fichiers
    shorthalts."""
    global _CAL
    if _CAL is not None:
        return _CAL
    days = set()
    p = A_CRSP_DIR / "calendrier_bourse.csv"
    if p.exists():
        with p.open() as fh:
            for r in csv.DictReader(fh):
                days.add(r["dlycaldt"])
    if DATA_T1.exists():
        for d in DATA_T1.glob("*/*.json.gz"):
            days.add(d.name[:10])
    if SHORTHALTS_DIR.exists():
        for f in SHORTHALTS_DIR.glob("shorthalts*.txt"):
            s = f.name[10:18]
            days.add(f"{s[:4]}-{s[4:6]}-{s[6:]}")
    _CAL = sorted(days)
    return _CAL


def prev_trading_day(d: str) -> str | None:
    cal = trading_days()
    i = _bisect(cal, d)
    return cal[i - 1] if i > 0 else None


def next_trading_day(d: str) -> str | None:
    cal = trading_days()
    i = _bisect(cal, d)
    if i < len(cal) and cal[i] == d:
        i += 1
    return cal[i] if i < len(cal) else None


def _bisect(seq, x):
    lo, hi = 0, len(seq)
    while lo < hi:
        mid = (lo + hi) // 2
        if seq[mid] < x:
            lo = mid + 1
        else:
            hi = mid
    return lo


def close_time(date_str: str) -> dtime:
    return EARLY_CLOSES.get(date_str, RTH[1])


def et_ns(date_str: str, t: dtime | str) -> int:
    """(date ET, heure ET) -> nanosecondes Unix."""
    if isinstance(t, str):
        hh, mm, ss = (t.split(":") + ["0", "0"])[:3]
        frac = 0
        if "." in ss:
            ss, f = ss.split(".")
            frac = int(round(float("0." + f) * 1e6))
        t = dtime(int(hh), int(mm), int(ss), frac)
    y, m, d = (int(x) for x in date_str.split("-"))
    dt = datetime(y, m, d, t.hour, t.minute, t.second, t.microsecond, tzinfo=ET)
    return int(dt.timestamp()) * NS + t.microsecond * 1000


def ns_to_et(ns: int) -> datetime:
    return datetime.fromtimestamp(ns / 1e9, ET)


def session_of(ns: int, date_str: str) -> str:
    t = ns_to_et(ns).time()
    cl = close_time(date_str)
    if PREMARKET[0] <= t < PREMARKET[1]:
        return "premarket"
    if RTH[0] <= t < cl:
        return "rth"
    if cl <= t < AFTERHOURS[1]:
        return "afterhours"
    return "hors_session"


def n_rth_buckets(date_str: str) -> int:
    cl = close_time(date_str)
    return (cl.hour * 60 + cl.minute) - (9 * 60 + 30)


def bucket_of(ns: int, date_str: str) -> tuple[str, int]:
    """(session, index de bucket) : buckets de 1 min en RTH, 5 min hors RTH."""
    sess = session_of(ns, date_str)
    dt = ns_to_et(ns)
    mins = dt.hour * 60 + dt.minute
    if sess == "rth":
        return sess, mins - (9 * 60 + 30)
    if sess == "premarket":
        return sess, (mins - 4 * 60) // 5
    if sess == "afterhours":
        cl = close_time(date_str)
        return sess, (mins - (cl.hour * 60 + cl.minute)) // 5
    return sess, -1


# ---------------------------------------------------------------------------
# P-11 : bandes LULD (Appendix A et doublement de fin de séance)
# ---------------------------------------------------------------------------

def band_amount(rp: float, in_doubling_window: bool) -> tuple[float, bool]:
    """Demi-largeur de bande en dollars autour du RP, Tier 2 (Appendix A).

    RP > 3 $ : 10 % ; 0,75 $ ≤ RP ≤ 3 $ : 20 % ; RP < 0,75 $ : min(0,15 $, 75 %).
    Dans la fenêtre de fin de séance, la bande est doublée pour RP ≤ 3 $
    uniquement. Retourne (montant, doublé)."""
    doubled = bool(in_doubling_window and rp <= 3.00)
    k = 2.0 if doubled else 1.0
    if rp > 3.00:
        return 0.10 * rp, False
    if rp >= 0.75:
        return k * 0.20 * rp, doubled
    return k * min(0.15, 0.75 * rp), doubled


def bands_from_rp(rp: float, ns: int, date_str: str) -> tuple[float, float, bool]:
    """(L, U, doublé). La fenêtre de doublement couvre les 25 dernières minutes
    de la séance."""
    cl = close_time(date_str)
    dbl_start_min = (cl.hour * 60 + cl.minute) - DOUBLING_LAST_MINUTES
    dt = ns_to_et(ns)
    in_win = (dt.hour * 60 + dt.minute) >= dbl_start_min and dt.time() < cl
    amt, doubled = band_amount(rp, in_win)
    return max(0.0, rp - amt), rp + amt, doubled


# ---------------------------------------------------------------------------
# P-11 : machine à états
# ---------------------------------------------------------------------------

EV_EXPIRY = 0     # sortie d'une ERT de la fenêtre 5 min
EV_UNBLOCK = 1    # fin du minimum de 30 s d'un RP
EV_DOUBLING = 2   # bascule de la fenêtre de doublement
EV_TRADE = 3      # trade


class LuldMachine:
    """Bandes LULD recalculées à partir des seuls trades SIP.

    Le prix de référence (RP) est la moyenne des ERT (trades éligibles) des
    5 dernières minutes, conservé si la fenêtre est vide. Il n'est remplacé que
    si le pro-forma s'en écarte d'au moins 1 % et qu'il est en vigueur depuis
    30 s. À l'ouverture et à chaque reprise, le RP est le prix d'enchère de la
    bourse primaire ; sans ouverture avant 09:35, le premier RP est la moyenne
    des 5 premières minutes.

    Le gel pendant un Limit State (Plan §VI(B)(2)) est défini sur les cotations
    et n'est pas reproductible ici. `freeze_on_touch=True` l'approxime : le RP
    est gelé quand un print touche une bande et dégelé au premier print
    strictement intérieur ou à la reprise suivante.
    """

    def __init__(self, date_str: str, freeze_on_touch: bool = False):
        self.date = date_str
        self.open_ns = et_ns(date_str, RTH[0])
        self.close_ns = et_ns(date_str, close_time(date_str))
        self.freeze_on_touch = freeze_on_touch
        self.window: deque[tuple[int, float]] = deque()
        self.wsum = 0.0
        self.rp: float | None = None
        self.rp_since: int | None = None
        self.frozen = False
        self.rp_changes: list[tuple[int, float, str]] = []   # (ts, rp, motif)
        self.n_blocked_30s = 0
        self.n_anchor_open = 0
        self.n_anchor_reopen = 0

    # -- fenêtre glissante ---------------------------------------------------
    def _expire(self, ns: int):
        while self.window and self.window[0][0] <= ns - LULD_WINDOW_NS:
            self.wsum -= self.window.popleft()[1]

    def _proforma(self) -> float | None:
        if not self.window:
            return None
        return self.wsum / len(self.window)

    # -- transitions ---------------------------------------------------------
    def set_anchor(self, ns: int, price: float, kind: str):
        """Fixe le RP au prix d'ouverture ou de reprise et réinitialise la
        fenêtre à ce seul print, qui entre donc dans le pro-forma suivant."""
        self.window.clear()
        self.wsum = 0.0
        self.window.append((ns, price))
        self.wsum += price
        self.rp = price
        self.rp_since = ns
        self.frozen = False
        self.rp_changes.append((ns, price, kind))
        if kind == "opening":
            self.n_anchor_open += 1
        else:
            self.n_anchor_reopen += 1

    def add_ert(self, ns: int, price: float):
        self.window.append((ns, price))
        self.wsum += price

    def reevaluate(self, ns: int) -> int | None:
        """Applique l'hystérésis à l'instant ns. Si le changement est bloqué
        par la durée minimale de 30 s, retourne l'instant où le réexaminer."""
        pf = self._proforma()
        if pf is None:
            return None
        if self.rp is None:
            # Plan §V(B)(2) : sans ouverture dans les 5 min, le premier RP est la
            # moyenne des 5 premières minutes ; aucune bande avant 09:35.
            if ns >= self.open_ns + LULD_WINDOW_NS:
                self.rp = pf
                self.rp_since = ns
                self.rp_changes.append((ns, pf, "premier_rp_moyenne5min"))
            return None
        if self.frozen:
            return None
        if self.rp <= 0:
            return None
        if abs(pf - self.rp) / self.rp < LULD_HYSTERESIS:
            return None
        if ns - self.rp_since >= LULD_MIN_RP_LIFETIME_NS:
            self.rp = pf
            self.rp_since = ns
            self.rp_changes.append((ns, pf, "hysteresis"))
            return None
        self.n_blocked_30s += 1
        return self.rp_since + LULD_MIN_RP_LIFETIME_NS

    def bands(self, ns: int):
        if self.rp is None:
            return None
        return bands_from_rp(self.rp, ns, self.date)


# ---------------------------------------------------------------------------
# P-11 : calcul journalier
# ---------------------------------------------------------------------------

def prepare_day(trades: list[dict], date_str: str, primary_id: int,
                vol_rules: dict[int, bool]) -> list[dict]:
    """Trades du jour triés par (sip_timestamp, sequence_number), avec les
    prédicats précalculés."""
    out = []
    for t in trades:
        out.append({
            "ts": t["sip_timestamp"],
            "seq": t.get("sequence_number", 0),
            "price": t["price"],
            "size": t.get("size", 0),
            "exchange": t.get("exchange"),
            "cond": conditions(t),
            "trf": is_trf(t),
            "lit_adm": is_lit_admissible(t, vol_rules),
            "adm": is_admissible_volume(t, vol_rules),
            "auction": is_auction(t),
            "raw": t,
        })
    out.sort(key=lambda x: (x["ts"], x["seq"]))
    return out


def find_anchors(day: list[dict], date_str: str, primary_id: int) -> dict[int, tuple[str, float]]:
    """Instants où le RP est fixé par une enchère de la bourse primaire.

    Ouverture : premier print OPENING_CODES dans [09:30, 09:35) (Plan §V(B)(1)).
    Reprise : tout print REOPENING_CODES (Plan §V(C)(1)).
    """
    open_ns = et_ns(date_str, RTH[0])
    anchors: dict[int, tuple[str, float]] = {}
    best_open = None
    for r in day:
        if r["exchange"] != primary_id:
            continue
        cs = set(r["cond"])
        if cs & REOPENING_CODES:
            anchors[r["ts"]] = ("reopening", r["price"])
        if cs & OPENING_CODES and open_ns <= r["ts"] < open_ns + LULD_WINDOW_NS:
            # priorité au Market Center Official Open (16) à instant égal
            key = (r["ts"], 0 if 16 in cs else 1)
            if best_open is None or key < best_open[0]:
                best_open = (key, r["ts"], r["price"])
    if best_open is not None:
        anchors.setdefault(best_open[1], ("opening", best_open[2]))
    return anchors


def run_day_p11(day: list[dict], date_str: str, primary_id: int,
                ert_rules: dict[int, bool], freeze_on_touch: bool = False) -> dict:
    """Distances relatives minimales aux bandes haute et basse par bucket RTH,
    touchers de bande et diagnostics de la machine, pour un jour.

    Trades et événements internes (expiration de fenêtre, fin des 30 s, début
    du doublement) sont traités dans l'ordre chronologique ; à instant égal,
    les événements internes passent en premier."""
    m = LuldMachine(date_str, freeze_on_touch=freeze_on_touch)
    anchors = find_anchors(day, date_str, primary_id)
    nb = n_rth_buckets(date_str)
    min_du = [None] * nb
    min_dl = [None] * nb
    n_samples = [0] * nb
    touch_up = [0] * nb
    touch_down = [0] * nb
    touches: list[dict] = []
    last_price = None
    auction_hors_bandes = 0
    n_prints_rth = 0
    n_prints_sans_bande = 0

    heap: list[tuple[int, int, object]] = []
    cl = close_time(date_str)
    dbl_min = (cl.hour * 60 + cl.minute) - DOUBLING_LAST_MINUTES
    heapq.heappush(heap, (et_ns(date_str, dtime(dbl_min // 60, dbl_min % 60)), EV_DOUBLING, None))
    # premier RP possible sans enchère d'ouverture
    heapq.heappush(heap, (m.open_ns + LULD_WINDOW_NS, EV_UNBLOCK, None))

    def sample(ns: int):
        nonlocal last_price
        if last_price is None:
            return
        b = m.bands(ns)
        if b is None:
            return
        L, U, _ = b
        sess, idx = bucket_of(ns, date_str)
        if sess != "rth" or not (0 <= idx < nb):
            return
        du = (U - last_price) / last_price
        dl = (last_price - L) / last_price
        if min_du[idx] is None or du < min_du[idx]:
            min_du[idx] = du
        if min_dl[idx] is None or dl < min_dl[idx]:
            min_dl[idx] = dl
        n_samples[idx] += 1

    i = 0
    n = len(day)
    while i < n or heap:
        t_trade = day[i]["ts"] if i < n else None
        t_ev = heap[0][0] if heap else None
        if t_ev is not None and (t_trade is None or t_ev <= t_trade):
            ns, kind, _ = heapq.heappop(heap)
            if ns > m.close_ns:
                continue
            m._expire(ns)
            if kind == EV_DOUBLING:
                sample(ns)
                continue
            nxt = m.reevaluate(ns)
            if nxt is not None and nxt > ns:
                heapq.heappush(heap, (nxt, EV_UNBLOCK, None))
            if m.rp_changes and m.rp_changes[-1][0] == ns:
                sample(ns)
            continue

        r = day[i]
        i += 1
        ns = r["ts"]
        if ns < m.open_ns or ns >= m.close_ns:
            continue
        m._expire(ns)

        # Distance et toucher évalués avant que le trade n'alimente le RP.
        if r["lit_adm"]:
            n_prints_rth += 1
            b = m.bands(ns)
            if b is None:
                n_prints_sans_bande += 1
            else:
                L, U, doubled = b
                p = r["price"]
                sess, idx = bucket_of(ns, date_str)
                if sess == "rth" and 0 <= idx < nb:
                    du = (U - p) / p
                    dl = (p - L) / p
                    if min_du[idx] is None or du < min_du[idx]:
                        min_du[idx] = du
                    if min_dl[idx] is None or dl < min_dl[idx]:
                        min_dl[idx] = dl
                    n_samples[idx] += 1
                    if p >= U:
                        touch_up[idx] += 1
                        touches.append({"ts": ns, "side": "up", "price": p, "band": U,
                                        "rp": m.rp, "doubled": doubled, "bucket": idx})
                    if p <= L:
                        touch_down[idx] += 1
                        touches.append({"ts": ns, "side": "down", "price": p, "band": L,
                                        "rp": m.rp, "doubled": doubled, "bucket": idx})
                    if m.freeze_on_touch:
                        if p >= U or p <= L:
                            m.frozen = True
                        elif L < p < U:
                            m.frozen = False
            last_price = r["price"]
        elif r["auction"] and not r["trf"]:
            b = m.bands(ns)
            if b is not None and (r["price"] >= b[1] or r["price"] <= b[0]):
                auction_hors_bandes += 1

        # Ancre d'ouverture ou de reprise.
        if ns in anchors:
            kind, price = anchors[ns]
            m.set_anchor(ns, price, kind)
            sample(ns)
            continue

        # Alimentation de la fenêtre ERT puis hystérésis.
        if passes_filter(r["raw"], ert_rules) and r["size"] > 0 and not r["auction"]:
            m.add_ert(ns, r["price"])
            heapq.heappush(heap, (ns + LULD_WINDOW_NS, EV_EXPIRY, None))
            before = len(m.rp_changes)
            nxt = m.reevaluate(ns)
            if nxt is not None and nxt > ns:
                heapq.heappush(heap, (nxt, EV_UNBLOCK, None))
            if len(m.rp_changes) > before:
                sample(ns)

    return {
        "min_du": min_du,
        "min_dl": min_dl,
        "n_samples": n_samples,
        "touch_up": touch_up,
        "touch_down": touch_down,
        "touches": touches,
        "rp_changes": m.rp_changes,
        "n_rp_changes": len(m.rp_changes),
        "n_blocked_30s": m.n_blocked_30s,
        "n_anchor_open": m.n_anchor_open,
        "n_anchor_reopen": m.n_anchor_reopen,
        "n_prints_rth": n_prints_rth,
        "n_prints_sans_bande": n_prints_sans_bande,
        "auction_hors_bandes": auction_hors_bandes,
        "last_price": last_price,
    }


# ---------------------------------------------------------------------------
# P-12a : halts (API NYSE)
# ---------------------------------------------------------------------------

_HALTS = None


def load_halts() -> dict[tuple[str, str], list[dict]]:
    """(symbole, date) -> halts, depuis l'export de l'API NYSE trade-halts.

    Un même halt peut figurer deux fois, avec et sans heure de reprise ; les
    deux lignes sont fusionnées en gardant la reprise."""
    global _HALTS
    if _HALTS is not None:
        return _HALTS
    out: dict[tuple[str, str], dict[tuple, dict]] = defaultdict(dict)
    if HALTS_CSV.exists():
        with HALTS_CSV.open(newline="") as fh:
            for r in csv.DictReader(fh):
                key = (r["Symbol"], r["Halt Date"], r["Halt Time"], r["Reason"])
                rec = out[(r["Symbol"], r["Halt Date"])].get(key)
                resume = None
                if r["Resume Date"] and r["NYSE Resume Time"]:
                    resume = (r["Resume Date"], r["NYSE Resume Time"])
                if rec is None:
                    out[(r["Symbol"], r["Halt Date"])][key] = {
                        "halt_date": r["Halt Date"], "halt_time": r["Halt Time"],
                        "reason": r["Reason"], "exchange": r["Exchange"],
                        "resume": resume,
                    }
                elif resume is not None and rec["resume"] is None:
                    rec["resume"] = resume
    _HALTS = {k: sorted(v.values(), key=lambda x: x["halt_time"]) for k, v in out.items()}
    return _HALTS


HALTS_COVERAGE_FROM = "2019-09-01"   # début de couverture de l'API NYSE


def p12a_day(ticker: str, date_str: str) -> dict:
    """Buckets touchés par un halt. Avant HALTS_COVERAGE_FROM : NA("source")."""
    if date_str < HALTS_COVERAGE_FROM:
        return {"couverture": "NA", "na_code": "source", "intervalles": [],
                "buckets_halted": [], "first_print_post_halt_bucket": NA("source"),
                "auction_reopen_buckets": []}
    halts = load_halts()
    rows = list(halts.get((ticker, date_str), []))
    # Un halt de la veille peut ne reprendre que ce jour-là.
    prev = prev_trading_day(date_str)
    if prev:
        for h in halts.get((ticker, prev), []):
            if h["resume"] and h["resume"][0] == date_str:
                rows.append(h)
    intervals = []
    for h in rows:
        t0 = et_ns(h["halt_date"], h["halt_time"])
        if h["resume"]:
            t1 = et_ns(h["resume"][0], h["resume"][1])
        else:
            t1 = et_ns(h["halt_date"], AFTERHOURS[1])
        intervals.append({"t_halt": t0, "t_resume": t1, "reason": h["reason"]})
    buckets = set()
    day_start = et_ns(date_str, PREMARKET[0])
    day_end = et_ns(date_str, AFTERHOURS[1])
    for iv in intervals:
        a = max(iv["t_halt"], day_start)
        b = min(iv["t_resume"], day_end)
        if b <= a:
            continue
        ns = a
        while ns < b:
            sess, idx = bucket_of(ns, date_str)
            if sess != "hors_session":
                buckets.add((sess, idx))
            ns += 60 * NS
        sess, idx = bucket_of(b - 1, date_str)
        if sess != "hors_session":
            buckets.add((sess, idx))
    return {"couverture": "OK", "na_code": None,
            "intervalles": intervals,
            "buckets_halted": sorted(buckets),
            "first_print_post_halt_bucket": None,
            "auction_reopen_buckets": []}


# ---------------------------------------------------------------------------
# P-12b : SSR (Rule 201)
# ---------------------------------------------------------------------------

_SHORTHALTS = None


def load_shorthalts() -> dict[str, list[dict]]:
    """Date de fichier -> déclenchements, depuis les fichiers Nasdaq
    `shorthaltsYYYYMMDD.txt` (la dernière ligne, horodatage de génération sans
    Trigger Time, est ignorée)."""
    global _SHORTHALTS
    if _SHORTHALTS is not None:
        return _SHORTHALTS
    out: dict[str, list[dict]] = {}
    if SHORTHALTS_DIR.exists():
        for f in sorted(SHORTHALTS_DIR.glob("shorthalts*.txt")):
            s = f.name[10:18]
            d = f"{s[:4]}-{s[4:6]}-{s[6:]}"
            rows = []
            with f.open(newline="") as fh:
                for r in csv.DictReader(fh):
                    if not r.get("Trigger Time"):
                        continue
                    rows.append({"symbol": r["Symbol"], "cat": r["Market Category"],
                                 "trigger": r["Trigger Time"]})
            out[d] = rows
    _SHORTHALTS = out
    return _SHORTHALTS


def parse_trigger(s: str) -> tuple[str, int] | None:
    """'6/1/2018 3:53:39 PM' -> ('2018-06-01', ns ET)."""
    try:
        dt = datetime.strptime(s.strip(), "%m/%d/%Y %I:%M:%S %p")
    except ValueError:
        return None
    d = dt.strftime("%Y-%m-%d")
    return d, et_ns(d, dtime(dt.hour, dt.minute, dt.second))


def ssr_intervals_from_files(symbol: str) -> list[tuple[int, int, str]]:
    """Intervalles de restriction [t_trigger, fin du jour coté suivant] issus
    des fichiers Nasdaq : la restriction court jusqu'à la fin de la séance
    suivante (Rule 201(b)(1)(ii))."""
    sh = load_shorthalts()
    seen = set()
    out = []
    for fdate, rows in sh.items():
        for r in rows:
            if r["symbol"] != symbol:
                continue
            p = parse_trigger(r["trigger"])
            if p is None:
                continue
            tdate, tns = p
            if (tdate, tns) in seen:
                continue
            seen.add((tdate, tns))
            nxt = next_trading_day(tdate)
            end = et_ns(nxt, AFTERHOURS[1]) if nxt else et_ns(tdate, AFTERHOURS[1])
            out.append((tns, end, tdate))
    return sorted(out)


def ssr_aggregate(intervals: list[tuple[int, int, str]], date_str: str) -> str:
    """Couverture de la séance régulière par la restriction : "0", "partiel"
    ou "1"."""
    a = et_ns(date_str, RTH[0])
    b = et_ns(date_str, close_time(date_str))
    covered = []
    for t0, t1, _ in intervals:
        s, e = max(t0, a), min(t1, b)
        if e > s:
            covered.append((s, e))
    if not covered:
        return "0"
    covered.sort()
    merged = [list(covered[0])]
    for s, e in covered[1:]:
        if s <= merged[-1][1]:
            merged[-1][1] = max(merged[-1][1], e)
        else:
            merged.append([s, e])
    if len(merged) == 1 and merged[0][0] <= a and merged[0][1] >= b:
        return "1"
    return "partiel"


def official_close(day: list[dict], date_str: str, primary_id: int) -> tuple[float | None, str]:
    """Clôture officielle de la séance reconstruite depuis les trades.

    Par ordre de priorité : Corrected Consolidated Close (condition 38), puis
    clôture officielle de la bourse primaire (15 ou 19), qui fait foi pour la
    Rule 201, puis dernier print RTH admissible. Retourne (prix, source).
    """
    cl_ns = et_ns(date_str, close_time(date_str))
    op_ns = et_ns(date_str, RTH[0])
    cand38 = [r for r in day if CORRECTED_CONSOLIDATED_CLOSE in r["cond"]]
    if cand38:
        return cand38[-1]["price"], "corrected_consolidated_close"
    cand15 = [r for r in day if (set(r["cond"]) & OFFICIAL_CLOSE_CODES) and r["exchange"] == primary_id]
    if cand15:
        return cand15[-1]["price"], "market_center_official_close_primaire"
    last = [r for r in day if op_ns <= r["ts"] < cl_ns and r["adm"] and not r["trf"]]
    if last:
        return last[-1]["price"], "dernier_print_rth_admissible"
    return None, "aucun"


def last_rth_admissible(day: list[dict], date_str: str) -> float | None:
    cl_ns = et_ns(date_str, close_time(date_str))
    op_ns = et_ns(date_str, RTH[0])
    last = [r for r in day if op_ns <= r["ts"] < cl_ns and r["adm"] and not r["trf"]]
    return last[-1]["price"] if last else None


def ssr_recalc_day(day: list[dict], date_str: str, prev_close: float | None,
                   include_trf: bool = True):
    """Recalcul de la Rule 201 : déclenchement si le plus bas RTH des trades
    admissibles est ≤ 0,9 × la clôture de la veille, exprimée sur la base de
    prix du jour.

    Retourne (déclenché, plus bas, seuil, instant du premier print sous le
    seuil). Cet instant ne sert qu'à la validation croisée ; il n'est pas
    l'heure officielle de déclenchement.
    """
    if prev_close is None:
        return NA("no_prev_close"), None, None, None
    thr = SSR_THRESHOLD * prev_close
    op_ns = et_ns(date_str, RTH[0])
    cl_ns = et_ns(date_str, close_time(date_str))
    low = None
    t_first = None
    for r in day:
        if not (op_ns <= r["ts"] < cl_ns):
            continue
        # Les prints TRF comptent dans le plus bas : la restriction est
        # déclenchée par le prix consolidé, quelle que soit la venue.
        # include_trf=False sert de variante de comparaison.
        if not r["adm"]:
            continue
        if not include_trf and r["trf"]:
            continue
        p = r["price"]
        if low is None or p < low:
            low = p
        if t_first is None and p <= thr:
            t_first = r["ts"]
    if low is None:
        return NA("aucun_print_rth"), None, thr, None
    return (low <= thr), low, thr, t_first


# ---------------------------------------------------------------------------
# Validation sur le status ITCH
# ---------------------------------------------------------------------------

def read_status(path: Path) -> list:
    import databento_dbn as dbn
    import zstandard
    dec = dbn.DBNDecoder()
    out = []
    with open(path, "rb") as fh:
        with zstandard.ZstdDecompressor().stream_reader(fh) as reader:
            while True:
                chunk = reader.read(1 << 20)
                if not chunk:
                    break
                dec.write(chunk)
                for rec in dec.decode():
                    if isinstance(rec, dbn.Metadata):
                        continue
                    out.append(rec)
    return out


LULD_PAUSE_REASON = 50
ACTION_PAUSE = 9
ACTION_HALT = 8
ACTION_SSR_CHANGE = 14


def itch_luld_events(ticker: str, mois: str) -> list[dict]:
    """Pauses LULD (pause ou halt de motif LULD) du fichier status ITCH."""
    p = MBO_DIR / f"{ticker}_{mois}_status.dbn.zst"
    if not p.exists():
        return []
    out = []
    for r in read_status(p):
        if int(r.reason) == LULD_PAUSE_REASON and int(r.action) in (ACTION_PAUSE, ACTION_HALT):
            ns = r.ts_event
            out.append({"ts": ns, "date": ns_to_et(ns).strftime("%Y-%m-%d"),
                        "action": int(r.action)})
    return out


def itch_ssr_intervals(ticker: str, mois: str) -> list[tuple[int, bool]]:
    """Transitions de l'état SSR dans le status ITCH (diagnostic)."""
    p = MBO_DIR / f"{ticker}_{mois}_status.dbn.zst"
    if not p.exists():
        return []
    out = []
    for r in read_status(p):
        v = r.is_short_sell_restricted
        if v is None:
            continue
        out.append((r.ts_event, bool(v)))
    return out


# ---------------------------------------------------------------------------
# Échantillon
# ---------------------------------------------------------------------------

def load_sample() -> list[dict]:
    rows = list(csv.DictReader(ECH_CSV.open()))
    out = []
    for r in rows:
        mois = "2018-06" if r["date_t"].startswith("2018") else "2026-01"
        out.append({"ticker": r["ticker"], "exchange": r["exchange"], "mois": mois,
                    "permno": r["permno"], "strate_prix": r["strate_prix"]})
    return out


def day_files(ticker: str, mois: str) -> list[Path]:
    d = DATA_T1 / ticker
    if not d.exists():
        return []
    return sorted(f for f in d.glob("*.json.gz") if f.name[:7] == mois)


def load_day(path: Path) -> list[dict]:
    with gzip.open(path, "rt") as fh:
        return json.load(fh)


def tranche_of(price: float | None) -> str:
    if price is None:
        return "NA"
    for name, lo, hi in PRICE_TRANCHES:
        if lo <= price < hi:
            return name
    return "hors_tranche"


# ---------------------------------------------------------------------------
# Choix du filtre ERT
# ---------------------------------------------------------------------------

# Fenêtres de coïncidence entre un toucher de bande et une pause LULD. Une
# pause n'est déclarée qu'après 15 s de Limit State (Plan §VI(B), §VII(A)(1)),
# donc le toucher la précède d'au moins 15 s : la fenêtre principale est
# [t_pause − 20 s, t_pause], les autres servent d'analyse de sensibilité.
TOLERANCES_NS = {"1s": 1 * NS, "5s": 5 * NS, "20s": 20 * NS, "30s": 30 * NS, "60s": 60 * NS}
TOLERANCE_PRINCIPALE = "20s"


def evaluate_candidate(cand: str, freeze: bool, sample: list[dict], tables: dict,
                       budget_deadline: float | None = None,
                       done: set | None = None, acc: dict | None = None) -> dict:
    """Calcule, pour un filtre ERT candidat, la coïncidence entre touchers de
    bandes recalculées et événements LULD observés dans le status ITCH."""
    rules = tables["rules"][cand]
    vol = tables["rules"]["updates_volume"]
    acc = acc if acc is not None else {"events": [], "n_touch_days": 0, "n_days": 0,
                                       "n_touches": 0, "n_days_avec_touch": 0}
    for s in sample:
        if s["exchange"] != "Q":       # status ITCH disponible pour Nasdaq seulement
            continue
        key = (cand, freeze, s["ticker"], s["mois"])
        if done is not None and key in done:
            continue
        pid = PRIMARY_EXCHANGE_ID[s["exchange"]]
        ev = itch_luld_events(s["ticker"], s["mois"])
        ev_by_day = defaultdict(list)
        for e in ev:
            ev_by_day[e["date"]].append(e["ts"])
        for f in day_files(s["ticker"], s["mois"]):
            date_str = f.name[:10]
            day = prepare_day(load_day(f), date_str, pid, vol)
            res = run_day_p11(day, date_str, pid, rules, freeze_on_touch=freeze)
            acc["n_days"] += 1
            tt = [t["ts"] for t in res["touches"]]
            acc["n_touches"] += len(tt)
            if tt:
                acc["n_days_avec_touch"] += 1
            for e_ts in ev_by_day.get(date_str, []):
                rec = {"ticker": s["ticker"], "date": date_str, "ts": e_ts, "match": {}}
                for name, tol in TOLERANCES_NS.items():
                    rec["match"][name] = any(e_ts - tol <= x <= e_ts for x in tt)
                acc["events"].append(rec)
        if done is not None:
            done.add(key)
        if budget_deadline and time.time() > budget_deadline:
            acc["incomplet"] = True
            return acc
    acc["incomplet"] = acc.get("incomplet", False)
    return acc


# ---------------------------------------------------------------------------
# Tests unitaires
# ---------------------------------------------------------------------------

def _mk(ts, price, size=100, exchange=12, cond=None, seq=0, trf=None):
    t = {"sip_timestamp": ts, "price": price, "size": size, "exchange": exchange,
         "conditions": list(cond or []), "sequence_number": seq}
    if trf is not None:
        t["trf_id"] = trf
    return t


def _run_synth(trades, date_str="2026-01-05", primary=12, cand="updates_high_low",
               freeze=False):
    tables = load_tables()
    vol = tables["rules"]["updates_volume"]
    day = prepare_day(trades, date_str, primary, vol)
    return run_day_p11(day, date_str, primary, tables["rules"][cand], freeze_on_touch=freeze)


def test_band_amount():
    # Tier 2, hors fenêtre de doublement
    assert band_amount(10.0, False) == (1.0, False)
    a, d = band_amount(2.0, False)
    assert abs(a - 0.4) < 1e-12 and not d
    a, d = band_amount(0.50, False)
    assert abs(a - 0.15) < 1e-12 and not d          # min(0,15 ; 0,375) = 0,15
    a, d = band_amount(0.10, False)
    assert abs(a - 0.075) < 1e-12                    # min(0,15 ; 0,075) = 0,075
    # doublement : uniquement Tier 2 <= 3 $
    a, d = band_amount(2.0, True)
    assert abs(a - 0.8) < 1e-12 and d
    a, d = band_amount(3.0, True)
    assert abs(a - 1.2) < 1e-12 and d                # « equal to or below $3.00 »
    a, d = band_amount(3.01, True)
    assert abs(a - 0.301) < 1e-12 and not d          # > 3 $ : jamais doublé
    a, d = band_amount(0.50, True)
    assert abs(a - 0.30) < 1e-12 and d
    # bornes de tier
    assert abs(band_amount(0.75, False)[0] - 0.15) < 1e-12    # 20 % de 0,75
    assert abs(band_amount(0.7499, False)[0] - min(0.15, 0.75 * 0.7499)) < 1e-12
    print("  ok test_band_amount")


def test_distances_bandes_imposees():
    """Distances sur des bandes connues : RP fixé par l'ouverture, un seul
    print ensuite."""
    d = "2026-01-05"
    o = et_ns(d, "09:30:00")
    # Un odd lot (condition 37) est admissible en volume mais pas ERT sous
    # updates_high_low : le RP reste à 2,00, ce qui isole le calcul de distance.
    trades = [
        _mk(o, 2.00, cond=[16], seq=1),                    # ouverture -> RP = 2,00
        _mk(o + 10 * NS, 2.10, cond=[37], seq=2),          # print lit
    ]
    r = _run_synth(trades, d)
    assert [c for c in r["rp_changes"] if c[2] == "hysteresis"] == [], r["rp_changes"]
    # RP = 2,00 -> bandes 20 % : L = 1,60 ; U = 2,40
    idx = 0
    du = (2.40 - 2.10) / 2.10
    dl = (2.10 - 1.60) / 2.10
    assert abs(r["min_du"][idx] - du) < 1e-12, (r["min_du"][idx], du)
    assert abs(r["min_dl"][idx] - dl) < 1e-12
    assert r["touch_up"][idx] == 0 and r["touch_down"][idx] == 0
    print("  ok test_distances_bandes_imposees")


def test_hysteresis_09_puis_11():
    """Pro-forma sous 1 % : pas de nouvelle bande ; au-dessus de 1 % avant
    30 s : bloqué, puis nouvelle bande au réexamen de 30 s."""
    d = "2026-01-05"
    o = et_ns(d, "09:30:00")
    # RP = 100 à l'ouverture. Pro-forma successifs : 100,45 (+0,45 %),
    # 100,90 (+0,90 %), puis 102,50 (+2,5 %) à o+10 s, bloqué jusqu'à o+30 s.
    trades = [
        _mk(o, 100.00, cond=[16], seq=1),
        _mk(o + 2 * NS, 100.90, seq=2),
        _mk(o + 4 * NS, 101.80, seq=3),
        _mk(o + 10 * NS, 107.30, seq=4),
    ]
    r = _run_synth(trades, d)
    ch = r["rp_changes"]
    assert ch[0] == (o, 100.00, "opening"), ch
    assert all(t >= o + 30 * NS for t, _, m in ch if m == "hysteresis"), ch
    # Seule la première fenêtre de 5 min est examinée : ensuite, l'expiration
    # des ERT produit d'autres changements.
    hys = [c for c in ch if c[2] == "hysteresis" and c[0] <= o + LULD_WINDOW_NS]
    assert len(hys) == 1, ch
    assert hys[0][0] == o + 30 * NS
    assert abs(hys[0][1] - 102.50) < 1e-9, hys
    assert r["n_blocked_30s"] >= 1
    # Sans le dernier trade, le pro-forma reste sous 1 %.
    r2 = _run_synth(trades[:3], d)
    assert [c for c in r2["rp_changes"]
            if c[2] == "hysteresis" and c[0] < o + LULD_WINDOW_NS] == [], r2["rp_changes"]
    print("  ok test_hysteresis_09_puis_11")


def test_hysteresis_seuil_exact():
    """Écart de 0,9 % puis de 1,1 % après plus de 30 s."""
    d = "2026-01-05"
    o = et_ns(d, "09:30:00")
    # Ouverture à 100 et une ERT x : pro-forma (100 + x) / 2.
    # x = 101,8 donne +0,9 % : pas de nouvelle bande.
    t1 = [_mk(o, 100.0, cond=[16], seq=1), _mk(o + 40 * NS, 101.8, seq=2)]
    r1 = _run_synth(t1, d)
    assert [c for c in r1["rp_changes"]
            if c[2] == "hysteresis" and c[0] < o + LULD_WINDOW_NS] == [], r1["rp_changes"]
    # x = 102,2 donne +1,1 % à o+40 s : nouvelle bande.
    t2 = [_mk(o, 100.0, cond=[16], seq=1), _mk(o + 40 * NS, 102.2, seq=2)]
    r2 = _run_synth(t2, d)
    hys = [c for c in r2["rp_changes"]
           if c[2] == "hysteresis" and c[0] < o + LULD_WINDOW_NS]
    assert len(hys) == 1 and abs(hys[0][1] - 101.1) < 1e-9, r2["rp_changes"]
    print("  ok test_hysteresis_seuil_exact")


def test_doublement_1535():
    """Doublement à 15:35 : la bande double sans changement de RP."""
    d = "2026-01-05"
    o = et_ns(d, "09:30:00")
    t1534 = et_ns(d, "15:34:00")
    t1536 = et_ns(d, "15:36:00")
    trades = [
        _mk(o, 2.00, cond=[16], seq=1),
        _mk(t1534, 2.00, seq=2),
        _mk(t1536, 2.00, seq=3),
    ]
    r = _run_synth(trades, d)
    b1534 = bands_from_rp(2.00, t1534, d)
    b1536 = bands_from_rp(2.00, t1536, d)
    assert b1534 == (1.6, 2.4, False), b1534
    assert b1536[2] is True and abs(b1536[0] - 1.2) < 1e-12 and abs(b1536[1] - 2.8) < 1e-12
    i1534 = bucket_of(t1534, d)[1]
    i1536 = bucket_of(t1536, d)[1]
    assert abs(r["min_du"][i1534] - (2.4 - 2.0) / 2.0) < 1e-12
    assert abs(r["min_du"][i1536] - (2.8 - 2.0) / 2.0) < 1e-12
    # RP > 3 $ : jamais doublé
    trades2 = [_mk(o, 10.0, cond=[16], seq=1), _mk(t1536, 10.0, seq=2)]
    r2 = _run_synth(trades2, d)
    assert abs(r2["min_du"][i1536] - 0.10) < 1e-12
    print("  ok test_doublement_1535")


def test_rp_fige_sans_ert():
    """Le RP est conservé si la fenêtre de 5 min ne contient aucune ERT
    (Plan §V(A)(1))."""
    d = "2026-01-05"
    o = et_ns(d, "09:30:00")
    trades = [
        _mk(o, 2.00, cond=[16], seq=1),
        # Odd lot à +20 min, non ERT : fenêtre vide, le RP reste à 2,00.
        _mk(o + 1200 * NS, 2.30, cond=[37], seq=2),
    ]
    r = _run_synth(trades, d)
    assert [c for c in r["rp_changes"] if c[2] == "hysteresis"] == [], r["rp_changes"]
    idx = bucket_of(o + 1200 * NS, d)[1]
    assert abs(r["min_du"][idx] - (2.4 - 2.3) / 2.3) < 1e-12
    print("  ok test_rp_fige_sans_ert")


def test_ouverture_tardive_et_reprise():
    """Sans ouverture avant 09:35, le premier RP est la moyenne des 5 premières
    minutes ; un print de reprise (condition 18) fixe le RP."""
    d = "2026-01-05"
    o = et_ns(d, "09:30:00")
    trades = [_mk(o + 60 * NS, 4.00, seq=1), _mk(o + 120 * NS, 6.00, seq=2)]
    r = _run_synth(trades, d)
    ch = r["rp_changes"]
    assert ch[0][2] == "premier_rp_moyenne5min", ch
    assert ch[0][0] == o + LULD_WINDOW_NS and abs(ch[0][1] - 5.00) < 1e-12, ch
    trades2 = [
        _mk(o, 2.00, cond=[16], seq=1),
        _mk(et_ns(d, "10:00:00"), 1.20, cond=[18], seq=2),
        _mk(et_ns(d, "10:00:05"), 1.20, seq=3),
    ]
    r2 = _run_synth(trades2, d)
    kinds = [c[2] for c in r2["rp_changes"]]
    assert kinds[0] == "opening" and "reopening" in kinds, r2["rp_changes"]
    rp_reopen = [c for c in r2["rp_changes"] if c[2] == "reopening"][0][1]
    assert rp_reopen == 1.20
    idx = bucket_of(et_ns(d, "10:00:05"), d)[1]
    # RP = 1,20 -> 20 % -> L = 0,96 ; U = 1,44
    assert abs(r2["min_du"][idx] - (1.44 - 1.20) / 1.20) < 1e-12
    print("  ok test_ouverture_tardive_et_reprise")


def test_toucher_de_bande():
    d = "2026-01-05"
    o = et_ns(d, "09:30:00")
    trades = [
        _mk(o, 2.00, cond=[16], seq=1),
        _mk(o + 5 * NS, 2.40, seq=2),        # exactement U -> toucher
        _mk(o + 6 * NS, 1.60, seq=3),        # exactement L -> toucher (moins de 30 s : bandes inchangées)
    ]
    r = _run_synth(trades, d)
    assert r["touch_up"][0] == 1 and r["touch_down"][0] == 1, (r["touch_up"], r["touch_down"])
    assert len(r["touches"]) == 2
    print("  ok test_toucher_de_bande")


def test_filtre_ert_discriminant():
    """Un odd lot (condition 37) est ERT sous updates_volume mais pas sous
    updates_high_low : les deux candidats divergent."""
    d = "2026-01-05"
    o = et_ns(d, "09:30:00")
    trades = [
        _mk(o, 10.00, cond=[16], seq=1),
        _mk(o + 40 * NS, 30.00, cond=[37], seq=2),   # odd lot
    ]
    rv = _run_synth(trades, d, cand="updates_volume")
    rh = _run_synth(trades, d, cand="updates_high_low")
    assert [c for c in rv["rp_changes"] if c[2] == "hysteresis"], rv["rp_changes"]
    assert [c for c in rh["rp_changes"] if c[2] == "hysteresis"] == [], rh["rp_changes"]
    print("  ok test_filtre_ert_discriminant")


def test_p12b_seuil_exact():
    """close 10,00 -> low 9,00 déclenche (≤) ; low 9,001 ne déclenche pas."""
    d = "2026-01-05"
    o = et_ns(d, "09:30:00")
    tables = load_tables()
    vol = tables["rules"]["updates_volume"]
    day = prepare_day([_mk(o + 60 * NS, 9.00, seq=1)], d, 12, vol)
    trig, low, thr, tfirst = ssr_recalc_day(day, d, 10.00)
    assert trig is True and abs(thr - 9.0) < 1e-12 and low == 9.00, (trig, low, thr)
    assert tfirst == o + 60 * NS
    day2 = prepare_day([_mk(o + 60 * NS, 9.001, seq=1)], d, 12, vol)
    trig2, low2, thr2, tf2 = ssr_recalc_day(day2, d, 10.00)
    assert trig2 is False and tf2 is None, (trig2, low2)
    # Sans clôture de la veille, le résultat est NA et non False.
    day3 = prepare_day([_mk(o + 60 * NS, 1.0, seq=1)], d, 12, vol)
    trig3, *_ = ssr_recalc_day(day3, d, None)
    assert is_na(trig3) and trig3.code == "no_prev_close"
    print("  ok test_p12b_seuil_exact")


def test_p12b_split_pas_de_faux_declenchement():
    """Regroupement 1:10 entre la veille et le jour : la clôture de la veille
    est divisée par F = 1 + disfacshr = 0,1 avant comparaison."""
    d = "2026-01-05"
    o = et_ns(d, "09:30:00")
    tables = load_tables()
    vol = tables["rules"]["updates_volume"]
    # Clôture brute de la veille 1,00, soit 10,00 sur la base du jour.
    prev_raw = 1.00
    F = 0.1                       # 1 + disfacshr, disfacshr = −0,9
    prev_base_d = prev_raw / F
    day = prepare_day([_mk(o + 60 * NS, 9.60, seq=1)], d, 12, vol)
    trig_naif, *_ = ssr_recalc_day(day, d, prev_raw)
    trig_correct, low, thr, _ = ssr_recalc_day(day, d, prev_base_d)
    assert trig_naif is False       # comparaison naïve : 9,60 > 0,9 -> pas de déclenchement
    assert trig_correct is False and abs(thr - 9.0) < 1e-12   # 9,60 > 9,00
    # 8,99 déclenche sur la base corrigée.
    day2 = prepare_day([_mk(o + 60 * NS, 8.99, seq=1)], d, 12, vol)
    assert ssr_recalc_day(day2, d, prev_base_d)[0] is True
    print("  ok test_p12b_split_pas_de_faux_declenchement")


def test_p12b_agregat_a_cheval():
    """Déclenchement en cours de séance : "partiel" ce jour-là, "1" le
    lendemain."""
    d0, d1 = "2026-01-05", "2026-01-06"
    t0 = et_ns(d0, "13:00:00")
    end = et_ns(d1, "20:00:00")
    iv = [(t0, end, d0)]
    assert ssr_aggregate(iv, d0) == "partiel"
    assert ssr_aggregate(iv, d1) == "1"
    assert ssr_aggregate(iv, "2026-01-07") == "0"
    # Un déclenchement à 09:30 couvre toute la séance.
    iv2 = [(et_ns(d0, "09:30:00"), end, d0)]
    assert ssr_aggregate(iv2, d0) == "1"
    print("  ok test_p12b_agregat_a_cheval")


def test_p12a_halt_deux_buckets():
    """Un halt à cheval sur deux buckets RTH les marque tous les deux."""
    d = "2026-01-05"
    iv = {"t_halt": et_ns(d, "10:00:30"), "t_resume": et_ns(d, "10:01:30")}
    buckets = set()
    ns = iv["t_halt"]
    while ns < iv["t_resume"]:
        buckets.add(bucket_of(ns, d))
        ns += 30 * NS
    buckets.add(bucket_of(iv["t_resume"] - 1, d))
    assert ("rth", 30) in buckets and ("rth", 31) in buckets, buckets
    print("  ok test_p12a_halt_deux_buckets")


def test_sessions_et_buckets():
    d = "2026-01-05"
    assert session_of(et_ns(d, "04:00:00"), d) == "premarket"
    assert session_of(et_ns(d, "09:29:59"), d) == "premarket"
    assert session_of(et_ns(d, "09:30:00"), d) == "rth"
    assert session_of(et_ns(d, "15:59:59"), d) == "rth"
    assert session_of(et_ns(d, "16:00:00"), d) == "afterhours"
    assert session_of(et_ns(d, "20:00:00"), d) == "hors_session"
    assert bucket_of(et_ns(d, "09:30:00"), d) == ("rth", 0)
    assert bucket_of(et_ns(d, "15:59:00"), d) == ("rth", 389)
    assert n_rth_buckets(d) == 390
    # 2018-06 est en heure d'été (UTC−4).
    assert ns_to_et(et_ns("2018-06-01", "09:30:00")).utcoffset().total_seconds() == -4 * 3600
    print("  ok test_sessions_et_buckets")


def test_lit_admissible():
    tables = load_tables()
    vol = tables["rules"]["updates_volume"]
    assert is_lit_admissible(_mk(0, 1.0, cond=[]), vol)
    assert not is_lit_admissible(_mk(0, 1.0, cond=[], trf=202), vol)      # TRF
    assert not is_lit_admissible(_mk(0, 1.0, cond=[16]), vol)             # enchère
    assert not is_lit_admissible(_mk(0, 1.0, size=0, cond=[]), vol)       # taille nulle
    assert not is_lit_admissible(_mk(0, 1.0, cond=[15]), vol)             # official close
    print("  ok test_lit_admissible")


def test_parse_trigger():
    d, ns = parse_trigger("6/1/2018 3:53:39 PM")
    assert d == "2018-06-01" and ns == et_ns("2018-06-01", "15:53:39")
    d2, ns2 = parse_trigger("1/2/2026 9:36:29 AM")
    assert d2 == "2026-01-02" and ns2 == et_ns("2026-01-02", "09:36:29")
    print("  ok test_parse_trigger")


def test_official_close_cascade():
    d = "2026-01-05"
    tables = load_tables()
    vol = tables["rules"]["updates_volume"]
    o = et_ns(d, "09:30:00")
    base = [_mk(o + 60 * NS, 5.00, seq=1), _mk(et_ns(d, "15:59:00"), 5.10, seq=2)]
    p, src = official_close(prepare_day(base, d, 12, vol), d, 12)
    assert p == 5.10 and src == "dernier_print_rth_admissible"
    with_close = base + [_mk(et_ns(d, "16:00:02"), 5.25, cond=[15], seq=3)]
    p, src = official_close(prepare_day(with_close, d, 12, vol), d, 12)
    assert p == 5.25 and src == "market_center_official_close_primaire"
    with_corr = with_close + [_mk(et_ns(d, "17:00:00"), 5.30, cond=[38], seq=4)]
    p, src = official_close(prepare_day(with_corr, d, 12, vol), d, 12)
    assert p == 5.30 and src == "corrected_consolidated_close"
    # La clôture officielle d'une autre bourse que la primaire est ignorée.
    other = base + [_mk(et_ns(d, "16:00:02"), 9.99, cond=[15], exchange=11, seq=3)]
    p, src = official_close(prepare_day(other, d, 12, vol), d, 12)
    assert p == 5.10 and src == "dernier_print_rth_admissible"
    print("  ok test_official_close_cascade")


def test_determinisme_et_permutation():
    """Le résultat ne dépend pas de l'ordre d'arrivée des trades."""
    import random
    d = "2026-01-05"
    o = et_ns(d, "09:30:00")
    trades = [_mk(o, 2.0, cond=[16], seq=1)]
    for k in range(1, 60):
        trades.append(_mk(o + k * 7 * NS, 2.0 + 0.01 * ((k * 13) % 17), seq=k + 1, size=100 + k))
    r1 = _run_synth(list(trades), d)
    sh = list(trades)
    random.Random(7).shuffle(sh)
    r2 = _run_synth(sh, d)
    assert r1["min_du"] == r2["min_du"] and r1["min_dl"] == r2["min_dl"]
    assert r1["rp_changes"] == r2["rp_changes"]
    print("  ok test_determinisme_et_permutation")


def run_tests():
    load_tables()
    print("[tests] P-11 / P-12")
    test_sessions_et_buckets()
    test_lit_admissible()
    test_band_amount()
    test_distances_bandes_imposees()
    test_hysteresis_09_puis_11()
    test_hysteresis_seuil_exact()
    test_doublement_1535()
    test_rp_fige_sans_ert()
    test_ouverture_tardive_et_reprise()
    test_toucher_de_bande()
    test_filtre_ert_discriminant()
    test_determinisme_et_permutation()
    test_parse_trigger()
    test_official_close_cascade()
    test_p12b_seuil_exact()
    test_p12b_split_pas_de_faux_declenchement()
    test_p12b_agregat_a_cheval()
    test_p12a_halt_deux_buckets()
    print("[tests] tous OK")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def progress_log(rec: dict):
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    rec = dict(rec)
    rec["t"] = datetime.now().isoformat(timespec="seconds")
    with PROGRESS.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(rec, ensure_ascii=False) + "\n")


def progress_done(phase: str) -> set:
    done = set()
    if PROGRESS.exists():
        with PROGRESS.open(encoding="utf-8") as fh:
            for line in fh:
                try:
                    r = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if r.get("phase") == phase and r.get("key"):
                    done.add(tuple(r["key"]))
    return done


# ---------------------------------------------------------------------------
# Phase ERT
# ---------------------------------------------------------------------------

ERT_OUT = OUT_DIR / "ert-procedure.json"


def phase_ert(budget: float):
    """Évalue chaque filtre ERT candidat, avec et sans gel au toucher, contre
    les pauses LULD du status ITCH. Reprend au couple (ticker, mois) suivant
    si le budget de temps est épuisé."""
    tables = load_tables()
    sample = [s for s in load_sample() if s["exchange"] == "Q"]
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    state = json.loads(ERT_OUT.read_text()) if ERT_OUT.exists() else {"combos": {}}
    deadline = time.time() + budget
    combos = [(c, f) for c in ERT_CANDIDATES for f in (False, True)]
    for cand, freeze in combos:
        ck = f"{cand}|freeze={int(freeze)}"
        st = state["combos"].setdefault(ck, {"events": [], "n_days": 0, "n_touches": 0,
                                             "n_days_avec_touch": 0, "done": [],
                                             "touch_ratios": []})
        done = {tuple(x) for x in st["done"]}
        rules = tables["rules"][cand]
        vol = tables["rules"]["updates_volume"]
        for s in sample:
            key = (s["ticker"], s["mois"])
            if key in done:
                continue
            if time.time() > deadline:
                state["complete"] = False
                ERT_OUT.write_text(json.dumps(state, ensure_ascii=False))
                print(f"[ert] budget atteint, reprise possible ({ck} @ {key})")
                return False
            pid = PRIMARY_EXCHANGE_ID[s["exchange"]]
            ev_by_day = defaultdict(list)
            for e in itch_luld_events(s["ticker"], s["mois"]):
                ev_by_day[e["date"]].append(e["ts"])
            for f in day_files(s["ticker"], s["mois"]):
                date_str = f.name[:10]
                day = prepare_day(load_day(f), date_str, pid, vol)
                res = run_day_p11(day, date_str, pid, rules, freeze_on_touch=freeze)
                st["n_days"] += 1
                tt = sorted(t["ts"] for t in res["touches"])
                st["n_touches"] += len(tt)
                if tt:
                    st["n_days_avec_touch"] += 1
                for t in res["touches"][:200]:
                    if t["band"] > 0:
                        st["touch_ratios"].append(round(t["price"] / t["band"], 6))
                for e_ts in ev_by_day.get(date_str, []):
                    rec = {"ticker": s["ticker"], "date": date_str,
                           "ts": e_ts, "match": {}}
                    for name, tol in TOLERANCES_NS.items():
                        rec["match"][name] = any(e_ts - tol <= x <= e_ts for x in tt)
                    st["events"].append(rec)
            done.add(key)
            st["done"] = sorted(done)
            progress_log({"phase": "ert", "key": [ck, s["ticker"], s["mois"]]})
        ERT_OUT.write_text(json.dumps(state, ensure_ascii=False))
        print(f"[ert] {ck}: {st['n_days']} jours, {st['n_touches']} touchers, "
              f"{len(st['events'])} événements LULD")
    state["complete"] = True
    ERT_OUT.write_text(json.dumps(state, ensure_ascii=False))
    return True


def ert_verdict() -> dict:
    st = json.loads(ERT_OUT.read_text())
    out = {}
    for ck, s in st["combos"].items():
        ev = s["events"]
        n = len(ev)
        row = {"n_evenements": n, "n_jours": s["n_days"], "n_touchers": s["n_touches"],
               "n_jours_avec_toucher": s["n_days_avec_touch"]}
        for name in TOLERANCES_NS:
            k = sum(1 for e in ev if e["match"].get(name))
            row[f"desaccord_{name}"] = (n - k) / n if n else None
            row[f"coincidences_{name}"] = k
        out[ck] = row
    return out


# ---------------------------------------------------------------------------
# Phase RUN : exécution sur l'échantillon
# ---------------------------------------------------------------------------

def ca_events_check() -> dict:
    """Liste les divisions et regroupements d'actions (distype 'FRS') CRSP sur
    les permnos de l'échantillon pendant les mois étudiés."""
    out = {"2018-06": {"couverture_crsp": True, "evenements": []},
           "2026-01": {"couverture_crsp": False, "evenements": [],
                       "note": "l'archive CRSP locale s'arrête au 2025-12-31 : l'absence "
                               "d'action de société n'est pas vérifiable pour 2026-01"}}
    permnos = {r["permno"] for r in csv.DictReader(ECH_CSV.open())}
    p = A_CRSP_DIR / "univers_distributions.csv"
    if not p.exists():
        out["2018-06"]["couverture_crsp"] = False
        return out
    with p.open() as fh:
        for r in csv.DictReader(fh):
            if r["permno"] not in permnos:
                continue
            if r.get("distype") != "FRS":
                continue
            d = r["disexdt"]
            if d.startswith("2018-06"):
                out["2018-06"]["evenements"].append({"permno": r["permno"], "date": d,
                                                     "disfacshr": r.get("disfacshr")})
            elif d.startswith("2026-01"):
                out["2026-01"]["evenements"].append({"permno": r["permno"], "date": d})
    return out


def q(vals, p):
    vals = sorted(v for v in vals if v is not None)
    if not vals:
        return None
    k = (len(vals) - 1) * p
    lo, hi = math.floor(k), math.ceil(k)
    if lo == hi:
        return vals[int(k)]
    return vals[lo] + (vals[hi] - vals[lo]) * (k - lo)


def phase_run(budget: float, cand: str):
    tables = load_tables()
    vol = tables["rules"]["updates_volume"]
    rules = tables["rules"][cand]
    sample = load_sample()
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    done = progress_done("run")
    deadline = time.time() + budget
    for s in sample:
        key = (s["ticker"], s["mois"])
        if key in done:
            continue
        if time.time() > deadline:
            print(f"[run] budget atteint, reprise possible @ {key}")
            return False
        pid = PRIMARY_EXCHANGE_ID[s["exchange"]]
        out = {"ticker": s["ticker"], "mois": s["mois"], "exchange_cotation": s["exchange"],
               "permno": s["permno"], "filtre_ert": cand, "jours": {}}
        ssr_files = ssr_intervals_from_files(s["ticker"]) if s["exchange"] == "Q" else []
        itch_ssr = itch_ssr_intervals(s["ticker"], s["mois"]) if s["exchange"] == "Q" else []
        prev_close_cache: dict[str, tuple] = {}
        files = day_files(s["ticker"], s["mois"])
        for idx_f, f in enumerate(files):
            date_str = f.name[:10]
            day = prepare_day(load_day(f), date_str, pid, vol)
            res = run_day_p11(day, date_str, pid, rules)
            # P-12b : clôture officielle de la veille, reconstruite depuis les trades.
            prev = prev_trading_day(date_str)
            pc, pc_src, pc_last = None, "hors_perimetre_donnees", None
            if prev and idx_f > 0:
                pday = prev_close_cache.get(prev)
                if pday is None:
                    pf = DATA_T1 / s["ticker"] / f"{prev}.json.gz"
                    if pf.exists():
                        pday = prepare_day(load_day(pf), prev, pid, vol)
                        prev_close_cache[prev] = pday
                if pday is not None:
                    pc, pc_src = official_close(pday, prev, pid)
                    pc_last = last_rth_admissible(pday, prev)
            trig, low, thr, t_first = ssr_recalc_day(day, date_str, pc)
            trig_last, *_ = ssr_recalc_day(day, date_str, pc_last)
            trig_lit, low_lit, _, _ = ssr_recalc_day(day, date_str, pc, include_trf=False)
            agg_files = ssr_aggregate(ssr_files, date_str) if s["exchange"] == "Q" else NA("source_non_nasdaq")
            # P-12a
            halts = p12a_day(s["ticker"], date_str)
            fpp, arb = [], []
            for iv in halts["intervalles"]:
                nxt = [r for r in day if r["ts"] >= iv["t_resume"] and r["lit_adm"]]
                if nxt:
                    sess, bi = bucket_of(nxt[0]["ts"], date_str)
                    fpp.append([sess, bi])
            for r in day:
                if r["exchange"] == pid and set(r["cond"]) & REOPENING_CODES:
                    sess, bi = bucket_of(r["ts"], date_str)
                    arb.append([sess, bi])
            halts["first_print_post_halt_bucket"] = fpp if halts["couverture"] == "OK" else NA("source")
            halts["auction_reopen_buckets"] = arb
            # P-12c : nombre de prints par session
            nb_sess = Counter(session_of(r["ts"], date_str) for r in day)
            last_p = res["last_price"]
            out["jours"][date_str] = {
                "p11": {
                    "min_du": [None if v is None else round(v, 6) for v in res["min_du"]],
                    "min_dl": [None if v is None else round(v, 6) for v in res["min_dl"]],
                    "n_samples": res["n_samples"],
                    "touch_up": res["touch_up"], "touch_down": res["touch_down"],
                    "n_rp_changes": res["n_rp_changes"],
                    "n_blocked_30s": res["n_blocked_30s"],
                    "n_anchor_open": res["n_anchor_open"],
                    "n_anchor_reopen": res["n_anchor_reopen"],
                    "n_prints_rth": res["n_prints_rth"],
                    "n_prints_sans_bande": res["n_prints_sans_bande"],
                    "auction_hors_bandes": res["auction_hors_bandes"],
                    "touches": [{"ts": t["ts"], "side": t["side"], "price": t["price"],
                                 "band": round(t["band"], 6), "rp": round(t["rp"], 6),
                                 "doubled": t["doubled"], "bucket": t["bucket"]}
                                for t in res["touches"][:500]],
                },
                "p12a": {"couverture": halts["couverture"], "na_code": halts["na_code"],
                         "n_intervalles": len(halts["intervalles"]),
                         "intervalles": [{"t_halt": i["t_halt"], "t_resume": i["t_resume"],
                                          "reason": i["reason"]} for i in halts["intervalles"]],
                         "buckets_halted": [[a, b] for a, b in halts["buckets_halted"]],
                         "first_print_post_halt_bucket": jsonable(halts["first_print_post_halt_bucket"]),
                         "auction_reopen_buckets": arb},
                "p12b": {
                    "source_primaire": "shorthalts" if s["exchange"] == "Q" else "recalcul_rule201",
                    "ssr_fichiers": jsonable(agg_files),
                    "ssr_recalcul": jsonable(trig),
                    "ssr_recalcul_variante_dernier_print": jsonable(trig_last),
                    "ssr_recalcul_variante_lit_seul": jsonable(trig_lit),
                    "low_rth_lit_seul": low_lit,
                    "low_rth": low, "seuil": thr,
                    "prev_close": pc, "prev_close_source": pc_src,
                    "prev_close_dernier_print": pc_last,
                    # Sans données CRSP pour 2026-01, une division non retraitée
                    # est détectée par heuristique : un rapport hors ]1/3, 3[
                    # entre le dernier prix du jour et la clôture de la veille.
                    "ca_suspect": bool(pc and last_p and not (1 / 3 < last_p / pc < 3)),
                    "t_trigger": jsonable(NA("intraday_indispo")),
                    "t_premiere_infraction_diag": t_first,
                    "ssr_itch_diag": None,
                },
                "p12c": {"n_par_session": dict(nb_sess),
                         "n_buckets_rth": n_rth_buckets(date_str),
                         "cloture": close_time(date_str).strftime("%H:%M")},
                "tranche_prix": tranche_of(last_p),
                "dernier_prix_lit": last_p,
            }
            # SSR selon le status ITCH : état à l'ouverture et transitions du jour.
            if itch_ssr:
                a, b = et_ns(date_str, RTH[0]), et_ns(date_str, close_time(date_str))
                tr = [(t, v) for t, v in itch_ssr if a <= t < b]
                before = [v for t, v in itch_ssr if t < a]
                st0 = before[-1] if before else None
                any_true = (st0 is True) or any(v for _, v in tr)
                all_true = (st0 is True) and all(v for _, v in tr)
                out["jours"][date_str]["p12b"]["ssr_itch_diag"] = (
                    "1" if all_true else ("partiel" if any_true else "0"))
        p = OUT_DIR / f"p11_p12_{s['ticker']}_{s['mois']}.json"
        p.write_text(json.dumps(out, ensure_ascii=False))
        progress_log({"phase": "run", "key": [s["ticker"], s["mois"]],
                      "jours": len(out["jours"])})
        done.add(key)
        print(f"[run] {s['ticker']} {s['mois']} : {len(out['jours'])} jours")
    return True


# ---------------------------------------------------------------------------
# Phase RECAP
# ---------------------------------------------------------------------------

def _agg_from_triggers(triggers: list[tuple[str, int]], date_str: str) -> str:
    """Agrégat journalier à partir de déclenchements (jour, t), avec la même
    durée de restriction que `ssr_intervals_from_files`."""
    ivs = []
    for tdate, tns in triggers:
        nxt = next_trading_day(tdate)
        end = et_ns(nxt, AFTERHOURS[1]) if nxt else et_ns(tdate, AFTERHOURS[1])
        ivs.append((tns, end, tdate))
    return ssr_aggregate(ivs, date_str)


def phase_recap():
    files = sorted(OUT_DIR.glob("p11_p12_*.json"))
    data = [json.loads(p.read_text()) for p in files]
    ert = ert_verdict()

    # P-12b : fichiers Nasdaq contre recalcul.
    comp = {"n_compares": 0, "n_ssr": 0, "n_desaccords": 0, "details": [],
            "n_exclus_amorce": 0, "n_exclus_na": 0, "n_exclus_ca_suspect": 0,
            "jours_ca_suspect": []}
    itch_comp = {"n": 0, "n_desaccords": 0}
    for d in data:
        if d["exchange_cotation"] != "Q":
            continue
        days = sorted(d["jours"])
        triggers = []
        for ds in days:
            j = d["jours"][ds]["p12b"]
            r = j["ssr_recalcul"]
            if r is True and j["t_premiere_infraction_diag"]:
                triggers.append((ds, j["t_premiere_infraction_diag"]))
        for i, ds in enumerate(days):
            j = d["jours"][ds]["p12b"]
            if i < 2:                     # la clôture de la veille doit être disponible
                comp["n_exclus_amorce"] += 1
                continue
            if isinstance(j["ssr_recalcul"], dict):
                comp["n_exclus_na"] += 1
                continue
            if j.get("ca_suspect"):
                comp["n_exclus_ca_suspect"] += 1
                comp["jours_ca_suspect"].append([d["ticker"], ds])
                continue
            a_files = j["ssr_fichiers"]
            a_recalc = _agg_from_triggers(triggers, ds)
            comp["n_compares"] += 1
            if a_files != "0" or a_recalc != "0":
                comp["n_ssr"] += 1
                if a_files != a_recalc:
                    comp["n_desaccords"] += 1
                    comp["details"].append({"ticker": d["ticker"], "date": ds,
                                            "fichiers": a_files, "recalcul": a_recalc,
                                            "low": j["low_rth"], "seuil": j["seuil"],
                                            "prev_close_source": j["prev_close_source"]})
            it = j.get("ssr_itch_diag")
            if it is not None:
                itch_comp["n"] += 1
                if it != a_files:
                    itch_comp["n_desaccords"] += 1
    taux = comp["n_desaccords"] / comp["n_ssr"] if comp["n_ssr"] else None

    # P-11 : distributions par tranche de prix.
    dist = defaultdict(lambda: {"du": [], "dl": [], "n_buckets": 0, "n_na": 0})
    touch_days = []
    p11_diag = Counter()
    for d in data:
        for ds, j in d["jours"].items():
            tr = j["tranche_prix"]
            p = j["p11"]
            for du, dl in zip(p["min_du"], p["min_dl"]):
                dist[tr]["n_buckets"] += 1
                if du is None:
                    dist[tr]["n_na"] += 1
                else:
                    dist[tr]["du"].append(du)
                    dist[tr]["dl"].append(dl)
            p11_diag["n_rp_changes"] += p["n_rp_changes"]
            p11_diag["n_blocked_30s"] += p["n_blocked_30s"]
            p11_diag["n_anchor_open"] += p["n_anchor_open"]
            p11_diag["n_anchor_reopen"] += p["n_anchor_reopen"]
            p11_diag["n_prints_rth"] += p["n_prints_rth"]
            p11_diag["n_prints_sans_bande"] += p["n_prints_sans_bande"]
            p11_diag["auction_hors_bandes"] += p["auction_hors_bandes"]
            nt = sum(p["touch_up"]) + sum(p["touch_down"])
            if nt:
                touch_days.append((d["ticker"], ds, nt))
            p11_diag["n_touches"] += nt

    # P-12a : prévalence des halts.
    halt = {"n_jours_couverts": 0, "n_jours_na": 0, "n_jours_avec_halt": 0,
            "n_intervalles": 0, "raisons": Counter()}
    for d in data:
        for ds, j in d["jours"].items():
            a = j["p12a"]
            if a["couverture"] == "OK":
                halt["n_jours_couverts"] += 1
                if a["n_intervalles"]:
                    halt["n_jours_avec_halt"] += 1
                halt["n_intervalles"] += a["n_intervalles"]
                for iv in a["intervalles"]:
                    halt["raisons"][iv["reason"]] += 1
            else:
                halt["n_jours_na"] += 1

    # P-12b : prévalence de la restriction.
    ssr_prev = Counter()
    for d in data:
        for ds, j in d["jours"].items():
            v = j["p12b"]["ssr_fichiers"]
            ssr_prev[v if isinstance(v, str) else "NA"] += 1

    ca = ca_events_check()
    recap = {"ert": ert, "p12b": comp, "p12b_taux": taux, "itch": itch_comp,
             "p11_diag": dict(p11_diag), "touch_days": touch_days,
             "halt": {**halt, "raisons": dict(halt["raisons"])},
             "ssr_prev": dict(ssr_prev), "ca": ca,
             "dist": {k: {"n_buckets": v["n_buckets"], "n_na": v["n_na"],
                          "du": {f"q{int(p*100)}": q(v["du"], p)
                                 for p in (0.01, 0.05, 0.25, 0.5, 0.75, 0.95, 0.99)},
                          "dl": {f"q{int(p*100)}": q(v["dl"], p)
                                 for p in (0.01, 0.05, 0.25, 0.5, 0.75, 0.95, 0.99)}}
                      for k, v in dist.items()}}
    (OUT_DIR / "recap-b2-10.json").write_text(json.dumps(recap, ensure_ascii=False, indent=1))
    print(json.dumps({k: v for k, v in recap.items() if k not in ("touch_days",)},
                     ensure_ascii=False, indent=1)[:6000])
    return recap


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--test", action="store_true")
    ap.add_argument("--recap", action="store_true")
    ap.add_argument("--ert", action="store_true")
    ap.add_argument("--ert-verdict", action="store_true")
    ap.add_argument("--run", action="store_true")
    ap.add_argument("--ca-check", action="store_true")
    ap.add_argument("--budget", type=float, default=90.0)
    ap.add_argument("--cand", default="updates_high_low")
    args = ap.parse_args()
    if args.test:
        run_tests()
        return
    if args.ca_check:
        print(json.dumps(ca_events_check(), ensure_ascii=False, indent=1))
        return
    if args.ert:
        ok = phase_ert(args.budget)
        print("[ert] complet" if ok else "[ert] partiel")
        return
    if args.ert_verdict:
        print(json.dumps(ert_verdict(), ensure_ascii=False, indent=1))
        return
    if args.run:
        ok = phase_run(args.budget, args.cand)
        print("[run] complet" if ok else "[run] partiel")
        return
    if args.recap:
        phase_recap()
        return
    raise SystemExit("choisir --test / --ert / --ert-verdict / --run / --ca-check")


if __name__ == "__main__":
    main()
