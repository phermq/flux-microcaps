#!/usr/bin/env python3
"""Primitives sur la tape SIP (T1) : P-05 (rotation), P-10a (ISO), P-15 (borne d'observabilité).

Réutilise par import les briques de `primitives_t1.py` (tables de conditions,
prédicats de volume et de TRF, lecture de la tape, accumulateur des variantes
incl810/excl810). Les quantités nécessaires (DVact, part TRF, volume lit par
venue) sont recalculées depuis la tape brute, sans lire les CSV de primitives_t1.

Usage :
    python3 src/primitives/primitives_t1b.py --test             # tests unitaires, sans données
    python3 src/primitives/primitives_t1b.py --run [--limit N]  # exécution sur data/data-t1/
"""

from __future__ import annotations

import argparse
import csv
import json
import statistics
import sys
import time
from collections import defaultdict
from datetime import datetime, timezone, time as dtime
from pathlib import Path
from zoneinfo import ZoneInfo

CODE_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import chemins  # noqa: E402
import primitives_t1 as t1  # noqa: E402

UNIVERS_DIR = chemins.SORTIES / "univers"
EDGAR_SO_PATH = chemins.DONNEES / "edgar-so" / "so_edgar.csv"
COMPOSITION_A_PATH = chemins.SORTIES / "tests-mbo" / "composition_borne.csv"
SORTIES_T3_DIR = chemins.SORTIES / "t3"
SORTIES_T1_DIR = chemins.SORTIES / "t1"

ET = ZoneInfo("America/New_York")

# Mois -> instantané d'univers qui le gouverne : l'univers du mois M est construit
# à la fin du mois M-1. Seuls les deux mois de l'échantillon T1 sont couverts.
MOIS_TO_UFILE = {"2018-06": "U_20180531.csv", "2026-01": "U_20251231.csv"}

# Identifiants vérifiés au chargement contre p2-conditions.json et p1-exchanges.json.
ISO_CONDITION_ID = 14         # sale_condition « Intermarket Sweep » (code F)
NASDAQ_EXCHANGE_ID = 12       # type=exchange, mic=XNAS


def verify_iso_condition(p2: dict) -> None:
    entry = next((e for e in p2["results"] if e["id"] == ISO_CONDITION_ID and e["type"] == "sale_condition"), None)
    assert entry is not None and entry.get("name") == "Intermarket Sweep", (
        f"ISO_CONDITION_ID={ISO_CONDITION_ID} ne correspond pas a 'Intermarket Sweep' "
        f"dans p2-conditions.json (categorie sale_condition) : {entry}"
    )


def verify_nasdaq_id(p1: dict) -> None:
    entry = next((e for e in p1["results"] if e["id"] == NASDAQ_EXCHANGE_ID), None)
    assert entry is not None and entry.get("type") == "exchange" and entry.get("mic") == "XNAS", (
        f"NASDAQ_EXCHANGE_ID={NASDAQ_EXCHANGE_ID} ne correspond pas a Nasdaq (XNAS) "
        f"dans p1-exchanges.json : {entry}"
    )


# ---------------------------------------------------------------------------
# P-05 : nombre d'actions en circulation (SO) et rotation
# ---------------------------------------------------------------------------

_UNIVERS_CACHE: dict[str, dict[str, dict]] = {}


def load_univers_snapshot(filename: str) -> dict[str, dict]:
    path = UNIVERS_DIR / filename
    rows: dict[str, dict] = {}
    with path.open(encoding="utf-8") as f:
        for row in csv.DictReader(f):
            rows[row["ticker"]] = row
    return rows


def get_univers(filename: str) -> dict[str, dict]:
    if filename not in _UNIVERS_CACHE:
        _UNIVERS_CACHE[filename] = load_univers_snapshot(filename)
    return _UNIVERS_CACHE[filename]


def load_so_edgar() -> dict[str, list[tuple[str, float]]]:
    """permno -> liste (filed, val) triée par date de dépôt croissante.

    Quelques lignes de la source (93 sur 203 813) ont une valeur non entière,
    artefact XBRL ; elles sont lues en float. Aucun permno de l'échantillon
    n'est concerné.
    """
    by_permno: dict[str, list[tuple[str, float]]] = defaultdict(list)
    with EDGAR_SO_PATH.open(encoding="utf-8") as f:
        for row in csv.DictReader(f):
            by_permno[row["permno"]].append((row["filed"], float(row["val"])))
    for permno in by_permno:
        by_permno[permno].sort(key=lambda x: x[0])
    return dict(by_permno)


def so_from_edgar(by_permno: dict, permno: str, d: str):
    """SO en escalier : dernière valeur déposée à la date d ou avant, NA s'il n'y en a pas."""
    entries = by_permno.get(permno)
    if not entries:
        return t1.NA("so_missing_edgar_permno")
    val = None
    for filed, v in entries:
        if filed <= d:
            val = v
        else:
            break
    if val is None:
        return t1.NA("so_missing_edgar_date")
    return val


def resolve_so_source(ticker: str, mois: str) -> dict:
    """Source et datation du SO d'un titre pour le mois donné, lues dans l'univers du mois.

    `so_dating` vaut "filed" (EDGAR, SO en escalier selon les dépôts) ou
    "snapshot" (CRSP, SO constant sur le mois).
    """
    ufile = MOIS_TO_UFILE.get(mois)
    if ufile is None:
        raise ValueError(
            f"mois {mois} absent de MOIS_TO_UFILE : ajouter l'instantane "
            f"d'univers correspondant."
        )
    row = get_univers(ufile).get(ticker)
    if row is None:
        return {"permno": None, "so_source": None, "so_dating": None, "shrout_snapshot": None,
                "tranche_prix": None}
    so_source = row["so_source"]
    so_dating = "filed" if so_source == "edgar" else "snapshot"
    shrout = row.get("shrout_milliers", "")
    shrout_snapshot = int(round(float(shrout) * 1000)) if shrout not in (None, "") else None
    return {"permno": row["permno"], "so_source": so_source, "so_dating": so_dating,
            "shrout_snapshot": shrout_snapshot, "tranche_prix": row.get("tranche_prix")}


def compute_so(ticker_info: dict, by_permno_edgar: dict, d: str):
    if ticker_info["permno"] is None:
        return t1.NA("ticker_absent_univers")
    if ticker_info["so_dating"] == "filed":
        return so_from_edgar(by_permno_edgar, ticker_info["permno"], d)
    if ticker_info["shrout_snapshot"] is None:
        return t1.NA("shrout_snapshot_absent")
    return ticker_info["shrout_snapshot"]


def compute_rot(dvact, so):
    """Rotation ROT_{i,d} = DVact / SO ; NA si l'un des termes manque ou si SO = 0."""
    if t1.is_na(so):
        return t1.NA(so.code if hasattr(so, "code") else "so_na")
    if t1.is_na(dvact):
        return t1.NA("dvact_na")
    if so == 0:
        return t1.NA("so_zero")
    return dvact / so


# ---------------------------------------------------------------------------
# P-10a : buckets temporels (1 min en RTH, 5 min en pré- et post-séance)
# ---------------------------------------------------------------------------
# Les demi-séances (clôture à 13:00 ET) ne sont pas prises en compte : aucune ne
# tombe en juin 2018 ni en janvier 2026. Il faudra charger leur calendrier
# (src/references/early_closes.csv) si l'échantillon s'étend.

SESSION_BOUNDS = [
    ("premarket", dtime(4, 0), dtime(9, 30), 5),
    ("rth", dtime(9, 30), dtime(16, 0), 1),
    ("afterhours", dtime(16, 0), dtime(20, 0), 5),
]


def bucket_label(sip_ts_ns: int) -> tuple[str, int | None]:
    """(session, minute ET de début du bucket) ; ("hors_session", None) hors 4:00-20:00 ET."""
    seconds = sip_ts_ns // 1_000_000_000
    dt_et = datetime.fromtimestamp(seconds, tz=timezone.utc).astimezone(ET)
    t = dt_et.time()
    minute_of_day = dt_et.hour * 60 + dt_et.minute
    for name, start, end, delta in SESSION_BOUNDS:
        if start <= t < end:
            return (name, (minute_of_day // delta) * delta)
    return ("hors_session", None)


# ---------------------------------------------------------------------------
# Passe sur la tape d'un ticker-jour
# ---------------------------------------------------------------------------

def scan_ticker_day(trades_raw: list[dict], volume_rules: dict[int, bool]) -> dict:
    """Agrégats d'un ticker-jour, tape triée par (sip_timestamp, sequence_number).

    DVact et volume TRF viennent de t1.accumulate_trade (variante incl810) ; s'y
    ajoutent le volume lit sur Nasdaq (pour s_nasdaq, P-15) et les comptes ISO
    par bucket (P-10a).
    """
    trades = sorted(trades_raw, key=lambda t: (t["sip_timestamp"], t["sequence_number"]))

    # DVact et TRF, variante incl810.
    buckets = {"incl810": t1.new_variant_bucket(), "excl810": t1.new_variant_bucket()}
    unknown_ids: set[int] = set()
    price_failures: list = []
    for tr in trades:
        t1.accumulate_trade(buckets, tr, volume_rules, unknown_ids, price_failures)
    b = buckets["incl810"]

    lit_vol_nasdaq = 0
    n_admissible_total = 0
    vol_admissible_total = 0
    n_iso_total = 0
    vol_iso_total = 0
    iso_by_bucket: dict[tuple, dict] = defaultdict(lambda: {"n_iso": 0, "vol_iso": 0,
                                                              "n_admissible": 0, "vol_admissible": 0})
    for tr in trades:
        local_unknown: set[int] = set()
        admissible = t1.counts_to_volume(tr, volume_rules, local_unknown)
        if not admissible:
            continue
        size = tr["size"]
        n_admissible_total += 1
        vol_admissible_total += size
        if not t1.is_trf(tr) and tr.get("exchange") == NASDAQ_EXCHANGE_ID:
            lit_vol_nasdaq += size
        is_iso = ISO_CONDITION_ID in (tr.get("conditions") or [])
        bk = bucket_label(tr["sip_timestamp"])
        cell = iso_by_bucket[bk]
        cell["n_admissible"] += 1
        cell["vol_admissible"] += size
        if is_iso:
            n_iso_total += 1
            vol_iso_total += size
            cell["n_iso"] += 1
            cell["vol_iso"] += size

    # Les deux chemins d'admissibilité (accumulate_trade et counts_to_volume)
    # doivent donner le même volume total.
    assert vol_admissible_total == b["vol_all"], (
        f"volume admissible incoherent avec t1.accumulate_trade : {vol_admissible_total} != {b['vol_all']}"
    )

    return {
        "dvact": b["dvact"], "trf_vol_all": b["trf_vol_all"], "vol_all": b["vol_all"],
        "lit_vol_all": b["vol_all"] - b["trf_vol_all"], "lit_vol_nasdaq": lit_vol_nasdaq,
        "n_admissible": n_admissible_total, "vol_admissible": vol_admissible_total,
        "n_iso": n_iso_total, "vol_iso": vol_iso_total,
        "iso_by_bucket": dict(iso_by_bucket),
    }


# ---------------------------------------------------------------------------
# P-15 : borne d'observabilité
# ---------------------------------------------------------------------------
# La part non observable du volume combine la part TRF et la part cachée du volume
# lit : B = trf_part + h * (1 - trf_part). Le ratio caché h est mesuré sur le
# carnet ITCH quand il est disponible, sinon imputé par la médiane de la tranche
# de prix.

def load_sorties_t3() -> dict[tuple[str, str], dict]:
    """{(ticker, mois): HR_vol, vol_cache, vol_affiche, strate_prix} pour les ticker-mois ITCH."""
    out = {}
    for p in sorted(SORTIES_T3_DIR.glob("*.json")):
        d = json.loads(p.read_text(encoding="utf-8"))
        ticker, mois = d["ticker"], d["mois"]
        p07 = d["p07"]
        out[(ticker, mois)] = {
            "HR_vol": p07["HR_vol"], "vol_cache": p07["vol_cache"], "vol_affiche": p07["vol_affiche"],
            "strate_prix": d["strate_prix"],
        }
    return out


def compute_strate_stats(itch: dict[tuple[str, str], dict]) -> dict[str, dict]:
    """Médiane, quartiles et effectif de HR_vol par tranche de prix, pour l'imputation."""
    by_tranche: dict[str, list[float]] = defaultdict(list)
    for v in itch.values():
        by_tranche[v["strate_prix"]].append(v["HR_vol"])
    stats = {}
    for tranche, vals in by_tranche.items():
        vals_sorted = sorted(vals)
        med = statistics.median(vals_sorted)
        if len(vals_sorted) >= 2:
            q = statistics.quantiles(vals_sorted, n=4, method="inclusive")
            q25, q75 = q[0], q[2]
        else:
            q25 = q75 = vals_sorted[0]
        stats[tranche] = {"mediane": med, "q25": q25, "q75": q75, "n": len(vals_sorted)}
    return stats


def compute_p15_row(ticker: str, mois: str, trf_part, s_nasdaq, itch_entry,
                     strate_stats: dict, tranche_prix: str | None, h_max: float) -> dict:
    """Ligne P-15 d'un ticker-mois.

    B_basse n'existe qu'avec ITCH (volume caché exécuté rapporté au volume SIP).
    B_haute suppose que le volume lit hors Nasdaq a le ratio caché maximal h_max.
    B_q25 et B_q75 remplacent h par les quartiles de la tranche.
    """
    hidden_source = "mbo" if itch_entry is not None else "strate"
    if hidden_source == "mbo":
        h_hat = itch_entry["HR_vol"]
        tranche = itch_entry["strate_prix"]
    else:
        h_hat = t1.NA("tranche_absente") if tranche_prix not in strate_stats else strate_stats[tranche_prix]["mediane"]
        tranche = tranche_prix

    tstats = strate_stats.get(tranche)
    if tstats is not None:
        n_strate = tstats["n"]
        q25, q75 = tstats["q25"], tstats["q75"]
    else:
        n_strate, q25, q75 = 0, t1.NA("tranche_absente"), t1.NA("tranche_absente")

    def is_ok(x):
        return not t1.is_na(x)

    if is_ok(trf_part) and is_ok(h_hat):
        b_ext = trf_part + h_hat * (1 - trf_part)
    else:
        b_ext = t1.NA("composant_manquant")

    if hidden_source == "mbo" and is_ok(trf_part):
        vol_sip = itch_entry.get("_vol_sip")  # volume SIP du mois, fourni par l'appelant
        if vol_sip:
            b_basse = trf_part + itch_entry["vol_cache"] / vol_sip
        else:
            b_basse = t1.NA("vol_sip_zero")
    else:
        b_basse = t1.NA("non_calculable_hors_mbo")

    if is_ok(trf_part) and is_ok(h_hat) and is_ok(s_nasdaq):
        b_haute = trf_part + (1 - trf_part) * (h_hat * s_nasdaq + h_max * (1 - s_nasdaq))
    else:
        b_haute = t1.NA("composant_manquant")

    if is_ok(trf_part) and is_ok(q25):
        b_q25 = trf_part + q25 * (1 - trf_part)
    else:
        b_q25 = t1.NA("composant_manquant")
    if is_ok(trf_part) and is_ok(q75):
        b_q75 = trf_part + q75 * (1 - trf_part)
    else:
        b_q75 = t1.NA("composant_manquant")

    return {
        "ticker": ticker, "mois": mois, "trf_part": t1.fmt(trf_part), "s_nasdaq": t1.fmt(s_nasdaq),
        "h_hat": t1.fmt(h_hat), "tranche_prix": tranche,
        "B_extrapolee": t1.fmt(b_ext), "B_basse": t1.fmt(b_basse), "B_haute": t1.fmt(b_haute),
        "B_q25": t1.fmt(b_q25), "B_q75": t1.fmt(b_q75), "n_strate": n_strate,
        "hidden_source": hidden_source,
    }


# ---------------------------------------------------------------------------
# Tests unitaires (--test, aucun accès données)
# ---------------------------------------------------------------------------

def test_p05():
    # (a) SO et volume connus -> valeur exacte
    assert compute_rot(1000, 5000) == 0.2
    # (b) SO manquant -> NA
    r = compute_rot(1000, t1.NA("shrout_snapshot_absent"))
    assert t1.is_na(r) and r.code == "shrout_snapshot_absent"
    r2 = compute_rot(t1.NA("x"), 5000)
    assert t1.is_na(r2) and r2.code == "dvact_na"
    r3 = compute_rot(1000, 0)
    assert t1.is_na(r3) and r3.code == "so_zero"

    # (c) régime "filed" (EDGAR) : le SO change en cours de mois à la date de dépôt.
    by_permno = {"999": [("2020-01-05", 1000.0), ("2020-01-15", 2000.0)]}
    assert so_from_edgar(by_permno, "999", "2020-01-01") == t1.NA("so_missing_edgar_date")
    assert so_from_edgar(by_permno, "999", "2020-01-05") == 1000.0
    assert so_from_edgar(by_permno, "999", "2020-01-10") == 1000.0  # pas encore la 2e marche
    assert so_from_edgar(by_permno, "999", "2020-01-15") == 2000.0
    assert so_from_edgar(by_permno, "999", "2020-01-20") == 2000.0
    assert so_from_edgar(by_permno, "888", "2020-01-20") == t1.NA("so_missing_edgar_permno")

    # (d) régime "snapshot" (CRSP) : SO constant sur le mois, la table EDGAR
    # n'est pas consultée.
    ticker_info_snapshot = {"permno": "777", "so_source": "crsp_ads", "so_dating": "snapshot",
                             "shrout_snapshot": 5_000_000, "tranche_prix": "[1-5)"}
    for d in ("2020-06-01", "2020-06-15", "2020-06-30"):
        assert compute_so(ticker_info_snapshot, {}, d) == 5_000_000

    ticker_info_filed = {"permno": "999", "so_source": "edgar", "so_dating": "filed",
                          "shrout_snapshot": None, "tranche_prix": "[1-5)"}
    assert compute_so(ticker_info_filed, by_permno, "2020-01-01") == t1.NA("so_missing_edgar_date")
    assert compute_so(ticker_info_filed, by_permno, "2020-01-10") == 1000.0
    assert compute_so(ticker_info_filed, by_permno, "2020-01-20") == 2000.0

    # (e) ticker absent de l'univers -> NA
    r4 = compute_so({"permno": None, "so_source": None, "so_dating": None,
                      "shrout_snapshot": None, "tranche_prix": None}, {}, "2020-01-01")
    assert t1.is_na(r4) and r4.code == "ticker_absent_univers"

    print("P-05 : OK")


def test_bucket_label():
    # RTH : 1 min. Premarket/afterhours : 5 min. Hors session : None.
    # 09:35:12 ET un jour hiver (EST, UTC-5) -> 14:35:12 UTC.
    ts_rth = int(datetime(2020, 1, 15, 14, 35, 12, tzinfo=timezone.utc).timestamp()) * 1_000_000_000
    name, bmin = bucket_label(ts_rth)
    assert name == "rth" and bmin == 9 * 60 + 35

    # 08:03 ET premarket -> bucket 5 min floor = 8*60+0
    ts_pre = int(datetime(2020, 1, 15, 13, 3, 0, tzinfo=timezone.utc).timestamp()) * 1_000_000_000
    name2, bmin2 = bucket_label(ts_pre)
    assert name2 == "premarket" and bmin2 == 8 * 60 + 0

    # 17:07 ET afterhours -> bucket 5 min floor = 17*60+5
    ts_ah = int(datetime(2020, 1, 15, 22, 7, 0, tzinfo=timezone.utc).timestamp()) * 1_000_000_000
    name3, bmin3 = bucket_label(ts_ah)
    assert name3 == "afterhours" and bmin3 == 17 * 60 + 5

    # 02:00 ET -> hors_session
    ts_out = int(datetime(2020, 1, 15, 7, 0, 0, tzinfo=timezone.utc).timestamp()) * 1_000_000_000
    name4, bmin4 = bucket_label(ts_out)
    assert name4 == "hors_session" and bmin4 is None

    # Heure d'été : 09:35 ET en juillet (EDT, UTC-4) reste en RTH.
    ts_jul = int(datetime(2020, 7, 15, 13, 35, 0, tzinfo=timezone.utc).timestamp()) * 1_000_000_000
    name5, _ = bucket_label(ts_jul)
    assert name5 == "rth"
    print("bucket_label : OK")


def test_p15_composition():
    """Formules de B_extrapolee et B_basse sur ALPN 2018-06, comparées aux valeurs
    de référence de composition_borne.csv."""
    trf_part = 0.5008029398146792
    hr_vol = 0.29318069858546997
    vol_cache = 24996
    vol_sip = 341246
    b_ext = trf_part + hr_vol * (1 - trf_part)
    b_bas = trf_part + vol_cache / vol_sip
    assert abs(b_ext - 0.647158) < 1e-6
    assert abs(b_bas - 0.574052) < 1e-6
    print("P-15 composition (formule) : OK")


def run_tests() -> bool:
    ok = True
    for name, fn in [("P-05", test_p05), ("bucket_label", test_bucket_label),
                      ("P-15-formule", test_p15_composition)]:
        try:
            fn()
        except AssertionError as e:
            print(f"{name} : ECHEC -- {e}", file=sys.stderr)
            ok = False
    if ok:
        print("tous les tests (t1b) : OK", file=sys.stderr)
    return ok


# ---------------------------------------------------------------------------
# Exécution sur data/data-t1/
# ---------------------------------------------------------------------------

def run_pipeline(limit: int | None = None) -> None:
    SORTIES_T1_DIR.mkdir(parents=True, exist_ok=True)
    tables = t1.load_tables()
    verify_iso_condition(tables["p2"])
    verify_nasdaq_id(tables["p1"])
    volume_rules = tables["volume_rules"]

    by_permno_edgar = load_so_edgar()

    ticker_days = t1.list_ticker_days()
    if limit:
        ticker_days = ticker_days[:limit]

    t0 = time.time()
    # par (ticker, mois) : liste de (date, scan_result)
    scans_by_tm: dict[tuple[str, str], list[tuple[str, dict]]] = defaultdict(list)
    for i, (ticker, date, path) in enumerate(ticker_days):
        trades_raw = t1.load_trades(path)
        scan = scan_ticker_day(trades_raw, volume_rules)
        mois = date[:7]
        scans_by_tm[(ticker, mois)].append((date, scan))
        if (i + 1) % 100 == 0:
            print(f"[t1b] {i+1}/{len(ticker_days)} ticker-jours scannes ({time.time()-t0:.1f}s)",
                  file=sys.stderr)
    print(f"[t1b] {len(ticker_days)} ticker-jours scannes en {time.time()-t0:.1f}s", file=sys.stderr)

    # ---------------- P-05 : ROT titre-jour ----------------
    ticker_info_by_tm: dict[tuple[str, str], dict] = {}
    for (ticker, mois) in scans_by_tm:
        ticker_info_by_tm[(ticker, mois)] = resolve_so_source(ticker, mois)

    # so_regime_switch : le régime de datation du SO change d'un mois à l'autre pour ce ticker.
    regimes_by_ticker: dict[str, set[str]] = defaultdict(set)
    for (ticker, mois), info in ticker_info_by_tm.items():
        if info["so_dating"] is not None:
            regimes_by_ticker[ticker].add(info["so_dating"])
    tickers_with_switch = {t for t, regs in regimes_by_ticker.items() if len(regs) > 1}

    p05_rows_jour = []
    p05_titre_jour_by_key: dict[tuple[str, str], dict] = {}
    for (ticker, mois), entries in sorted(scans_by_tm.items()):
        entries.sort(key=lambda e: e[0])
        info = ticker_info_by_tm[(ticker, mois)]
        mois_sorted_for_ticker = sorted(regimes_by_ticker.get(ticker, []))
        first_month_for_ticker = min(m for (tk, m) in scans_by_tm if tk == ticker)
        for date, scan in entries:
            so = compute_so(info, by_permno_edgar, date)
            rot = compute_rot(scan["dvact"], so)
            regime_switch = bool(ticker in tickers_with_switch and mois != first_month_for_ticker and date == entries[0][0])
            row = {
                "ticker": ticker, "date": date, "mois": mois,
                "dvact_incl810": scan["dvact"],
                "so_source": info["so_source"], "so_dating": info["so_dating"],
                "so_value": t1.fmt(so), "rot": t1.fmt(rot),
                "so_regime_switch": regime_switch,
                "ca_so_desync": False,  # aucune opération sur titre dans l'échantillon
                "ca_so_desync_note": "sans_objet_echantillon_sans_CA",
            }
            p05_rows_jour.append(row)
            p05_titre_jour_by_key[(ticker, date)] = row

    p05_jour_path = SORTIES_T1_DIR / "p05_titre_jour.csv"
    with p05_jour_path.open("w", newline="", encoding="utf-8") as f:
        fields = ["ticker", "date", "mois", "dvact_incl810", "so_source", "so_dating",
                   "so_value", "rot", "so_regime_switch", "ca_so_desync", "ca_so_desync_note"]
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for row in p05_rows_jour:
            w.writerow(row)
    print(f"[t1b] ecrit {p05_jour_path} ({len(p05_rows_jour)} lignes)", file=sys.stderr)

    # ---------------- P-10a : ISO titre-jour ----------------
    p10a_rows_jour = []
    bucket_iso_fracs: list[float] = []  # part ISO par bucket, pour le résumé d'exécution
    for (ticker, mois), entries in sorted(scans_by_tm.items()):
        for date, scan in sorted(entries, key=lambda e: e[0]):
            part_n = t1.part(scan["n_iso"], scan["n_admissible"])
            part_vol = t1.part(scan["vol_iso"], scan["vol_admissible"])
            p10a_rows_jour.append({
                "ticker": ticker, "date": date, "mois": mois,
                "n_admissible": scan["n_admissible"], "vol_admissible": scan["vol_admissible"],
                "n_iso": scan["n_iso"], "vol_iso": scan["vol_iso"],
                "part_n_iso": t1.fmt(part_n), "part_vol_iso": t1.fmt(part_vol),
            })
            for (sess, bmin), cell in scan["iso_by_bucket"].items():
                if cell["n_admissible"] > 0:
                    bucket_iso_fracs.append(cell["n_iso"] / cell["n_admissible"])

    p10a_jour_path = SORTIES_T1_DIR / "p10a_titre_jour.csv"
    with p10a_jour_path.open("w", newline="", encoding="utf-8") as f:
        fields = ["ticker", "date", "mois", "n_admissible", "vol_admissible",
                   "n_iso", "vol_iso", "part_n_iso", "part_vol_iso"]
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for row in p10a_rows_jour:
            w.writerow(row)
    print(f"[t1b] ecrit {p10a_jour_path} ({len(p10a_rows_jour)} lignes)", file=sys.stderr)

    # ---------------- P-15 : composition mensuelle ----------------
    itch = load_sorties_t3()
    strate_stats = compute_strate_stats(itch)
    h_max = max(v["HR_vol"] for v in itch.values())

    # Agrégats mensuels par ticker-mois, à partir des passes journalières.
    monthly_agg: dict[tuple[str, str], dict] = {}
    for (ticker, mois), entries in scans_by_tm.items():
        vol_all = sum(s["vol_all"] for _, s in entries)
        trf_vol_all = sum(s["trf_vol_all"] for _, s in entries)
        lit_vol_all = sum(s["lit_vol_all"] for _, s in entries)
        lit_vol_nasdaq = sum(s["lit_vol_nasdaq"] for _, s in entries)
        trf_part = t1.part(trf_vol_all, vol_all)
        s_nasdaq = t1.part(lit_vol_nasdaq, lit_vol_all)
        monthly_agg[(ticker, mois)] = {
            "vol_all": vol_all, "trf_part": trf_part, "s_nasdaq": s_nasdaq,
            "dvact_mois": sum(s["dvact"] for _, s in entries),
        }

    p15_rows = []
    reproduction_deltas = []
    for (ticker, mois), agg in sorted(monthly_agg.items()):
        info = ticker_info_by_tm[(ticker, mois)]
        itch_entry = itch.get((ticker, mois))
        if itch_entry is not None:
            itch_entry = dict(itch_entry)
            itch_entry["_vol_sip"] = agg["vol_all"]
        tranche_prix = itch_entry["strate_prix"] if itch_entry is not None else info.get("tranche_prix")
        row = compute_p15_row(ticker, mois, agg["trf_part"], agg["s_nasdaq"], itch_entry,
                               strate_stats, tranche_prix, h_max)
        p15_rows.append(row)

        if (ticker, mois) in itch:
            ref_path = None

    # Écart, titre par titre, aux valeurs de référence de composition_borne.csv.
    ref_rows = {}
    with COMPOSITION_A_PATH.open(encoding="utf-8") as f:
        for r in csv.DictReader(f):
            ref_rows[r["ticker_mois"]] = r
    n_compare = 0
    max_delta_ext = 0.0
    max_delta_bas = 0.0
    for row in p15_rows:
        if row["hidden_source"] != "mbo":
            continue
        tm_key = f"{row['ticker']}_{row['mois']}"
        ref = ref_rows.get(tm_key)
        if ref is None:
            continue
        n_compare += 1
        d_ext = abs(row["B_extrapolee"] - float(ref["B_extrapole"]))
        d_bas = abs(row["B_basse"] - float(ref["B_basse"]))
        max_delta_ext = max(max_delta_ext, d_ext)
        max_delta_bas = max(max_delta_bas, d_bas)
        reproduction_deltas.append({"ticker_mois": tm_key, "delta_B_extrapolee": d_ext, "delta_B_basse": d_bas})

    p15_path = SORTIES_T1_DIR / "p15_titre_mois.csv"
    with p15_path.open("w", newline="", encoding="utf-8") as f:
        fields = ["ticker", "mois", "trf_part", "s_nasdaq", "h_hat", "tranche_prix",
                   "B_extrapolee", "B_basse", "B_haute", "B_q25", "B_q75", "n_strate", "hidden_source"]
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for row in p15_rows:
            w.writerow(row)
    print(f"[t1b] ecrit {p15_path} ({len(p15_rows)} lignes) ; ecart a la reference : "
          f"n={n_compare}, max|delta B_extrapolee|={max_delta_ext:.2e}, max|delta B_basse|={max_delta_bas:.2e}",
          file=sys.stderr)

    # ---------------- fichier composite titre-mois ----------------
    p15_by_tm = {(r["ticker"], r["mois"]): r for r in p15_rows}
    p05_by_tm: dict[tuple[str, str], dict] = {}
    for (ticker, mois), entries in scans_by_tm.items():
        rots = [p05_titre_jour_by_key[(ticker, d)]["rot"] for d, _ in entries]
        rots_num = [r for r in rots if not (isinstance(r, str) and r.startswith("NA"))]
        info = ticker_info_by_tm[(ticker, mois)]
        p05_by_tm[(ticker, mois)] = {
            "so_source": info["so_source"], "so_dating": info["so_dating"],
            "dvact_mois": monthly_agg[(ticker, mois)]["dvact_mois"],
            "n_jours_rot_ok": len(rots_num), "n_jours_total": len(entries),
        }
    p10a_by_tm: dict[tuple[str, str], dict] = defaultdict(lambda: {"n_admissible": 0, "vol_admissible": 0,
                                                                     "n_iso": 0, "vol_iso": 0})
    for row in p10a_rows_jour:
        k = (row["ticker"], row["mois"])
        p10a_by_tm[k]["n_admissible"] += row["n_admissible"]
        p10a_by_tm[k]["vol_admissible"] += row["vol_admissible"]
        p10a_by_tm[k]["n_iso"] += row["n_iso"]
        p10a_by_tm[k]["vol_iso"] += row["vol_iso"]

    composite_path = SORTIES_T1_DIR / "p05_p15_p10a_titre_mois.csv"
    with composite_path.open("w", newline="", encoding="utf-8") as f:
        fields = ["ticker", "mois",
                   "p05_so_source", "p05_so_dating", "p05_dvact_mois", "p05_n_jours_rot_ok", "p05_n_jours_total",
                   "p15_trf_part", "p15_s_nasdaq", "p15_h_hat", "p15_tranche_prix",
                   "p15_B_extrapolee", "p15_B_basse", "p15_B_haute", "p15_B_q25", "p15_B_q75",
                   "p15_n_strate", "p15_hidden_source",
                   "p10a_n_admissible", "p10a_vol_admissible", "p10a_n_iso", "p10a_vol_iso",
                   "p10a_part_n_iso", "p10a_part_vol_iso"]
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for key in sorted(p15_by_tm):
            ticker, mois = key
            p05 = p05_by_tm[key]
            p15 = p15_by_tm[key]
            p10a = p10a_by_tm[key]
            w.writerow({
                "ticker": ticker, "mois": mois,
                "p05_so_source": p05["so_source"], "p05_so_dating": p05["so_dating"],
                "p05_dvact_mois": p05["dvact_mois"], "p05_n_jours_rot_ok": p05["n_jours_rot_ok"],
                "p05_n_jours_total": p05["n_jours_total"],
                "p15_trf_part": p15["trf_part"], "p15_s_nasdaq": p15["s_nasdaq"], "p15_h_hat": p15["h_hat"],
                "p15_tranche_prix": p15["tranche_prix"],
                "p15_B_extrapolee": p15["B_extrapolee"], "p15_B_basse": p15["B_basse"],
                "p15_B_haute": p15["B_haute"], "p15_B_q25": p15["B_q25"], "p15_B_q75": p15["B_q75"],
                "p15_n_strate": p15["n_strate"], "p15_hidden_source": p15["hidden_source"],
                "p10a_n_admissible": p10a["n_admissible"], "p10a_vol_admissible": p10a["vol_admissible"],
                "p10a_n_iso": p10a["n_iso"], "p10a_vol_iso": p10a["vol_iso"],
                "p10a_part_n_iso": t1.fmt(t1.part(p10a["n_iso"], p10a["n_admissible"])),
                "p10a_part_vol_iso": t1.fmt(t1.part(p10a["vol_iso"], p10a["vol_admissible"])),
            })
    print(f"[t1b] ecrit {composite_path}", file=sys.stderr)

    # ---------------- résumé d'exécution ----------------
    rot_values = [r["rot"] for r in p05_rows_jour if isinstance(r["rot"], float)]
    b_extent_extrap = [(p15_by_tm[k]["B_haute"] - p15_by_tm[k]["B_basse"])
                        for k in p15_by_tm
                        if isinstance(p15_by_tm[k]["B_haute"], float) and isinstance(p15_by_tm[k]["B_basse"], float)]
    part_iso_vals = [row["part_vol_iso"] for row in p10a_rows_jour if isinstance(row["part_vol_iso"], float)]

    summary = {
        "n_ticker_jours": len(ticker_days),
        "n_ticker_mois": len(scans_by_tm),
        "p05_n_rot_ok": len(rot_values),
        "p05_n_rot_na": len(p05_rows_jour) - len(rot_values),
        "p05_so_regime_switch_count": sum(1 for r in p05_rows_jour if r["so_regime_switch"]),
        "p05_so_source_repartition": dict(
            (s, sum(1 for r in p05_rows_jour if r["so_source"] == s)) for s in
            {r["so_source"] for r in p05_rows_jour}
        ),
        "p15_reproduction_n": n_compare,
        "p15_reproduction_max_delta_B_extrapolee": max_delta_ext,
        "p15_reproduction_max_delta_B_basse": max_delta_bas,
        "p15_etendue_intervalle_mediane": statistics.median(b_extent_extrap) if b_extent_extrap else None,
        "p15_etendue_intervalle_min": min(b_extent_extrap) if b_extent_extrap else None,
        "p15_etendue_intervalle_max": max(b_extent_extrap) if b_extent_extrap else None,
        "p10a_prevalence_vol_mediane_titre_jour": statistics.median(part_iso_vals) if part_iso_vals else None,
        "p10a_prevalence_vol_min": min(part_iso_vals) if part_iso_vals else None,
        "p10a_prevalence_vol_max": max(part_iso_vals) if part_iso_vals else None,
        "p10a_bucket_iso_frac_mediane": statistics.median(bucket_iso_fracs) if bucket_iso_fracs else None,
        "p10a_n_buckets_observes": len(bucket_iso_fracs),
    }
    with (SORTIES_T1_DIR / ".run_summary_t1b.json").open("w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2, default=str)
    print(f"[t1b] resume : {json.dumps(summary, indent=2, default=str)}", file=sys.stderr)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="Primitives P-05, P-10a et P-15 sur la tape SIP.")
    parser.add_argument("--test", action="store_true", help="tests unitaires, sans acces aux donnees")
    parser.add_argument("--run", action="store_true", help="execution sur data/data-t1/")
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
