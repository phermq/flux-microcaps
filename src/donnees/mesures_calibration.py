#!/usr/bin/env python3
"""Mesures SIP sur tous les titres-jours de l'échantillon de calibration.

S'appuie sur `mesures_sip.py` (tables de conditions et de venues, classification,
téléchargement) et ajoute des indicateurs de qualité de la tape : taux de trades
hors séquence (condition Z), latence sip_timestamp - participant_timestamp
(p50/p99) et trous de sequence_number par tape. Chaque titre-jour est sauvegardé
sous .progress/ ; `--run` s'arrête au bout d'un budget de temps et se relance
jusqu'à épuisement de la liste.

  python3 donnees/mesures_calibration.py --build-worklist
  python3 donnees/mesures_calibration.py --run [--budget-seconds 80]
  python3 donnees/mesures_calibration.py --status
  python3 donnees/mesures_calibration.py --aggregate
"""

from __future__ import annotations

import csv
import hashlib
import json
import os
import statistics
import sys
import time
import urllib.error
from decimal import Decimal
from pathlib import Path

# Certaines installations de Python (python.org sur macOS) n'ont pas de magasin de
# certificats configuré : certifi le fournit s'il est installé.
try:
    import certifi
    os.environ.setdefault("SSL_CERT_FILE", certifi.where())
except ImportError:
    pass

CODE_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import chemins  # noqa: E402
import mesures_sip as ms  # noqa: E402

OUT_DIR = chemins.SORTIES / "mesures-calibration"
PROGRESS_DIR = OUT_DIR / ".progress"
WORKLIST_PATH = OUT_DIR / "work_list.json"
FAILURES_PATH = OUT_DIR / ".progress" / "_failures.json"
LOG_PATH = OUT_DIR / "run_log.jsonl"
ECHANTILLON_PATH = chemins.DONNEES / "echantillon-calibration" / "echantillon.csv"
CALENDRIER_PATH = chemins.DONNEES / "crsp" / "calendrier_bourse.csv"

MAX_RETRIES = 3

# Jours de bourse de janvier 2026. Le calendrier CRSP s'arrête au 2025-12-31 ; la
# liste exclut le 1er janvier et le Martin Luther King Day (19 janvier), et a été
# contrôlée avec l'endpoint grouped daily de l'API (aucun titre coté ces deux jours).
JAN_2026_TRADING_DAYS = [
    "2026-01-02",
    "2026-01-05", "2026-01-06", "2026-01-07", "2026-01-08", "2026-01-09",
    "2026-01-12", "2026-01-13", "2026-01-14", "2026-01-15", "2026-01-16",
    "2026-01-20", "2026-01-21", "2026-01-22", "2026-01-23",
    "2026-01-26", "2026-01-27", "2026-01-28", "2026-01-29", "2026-01-30",
]

Z_OUT_OF_SEQUENCE_ID = 32  # Z seul ; U marque des trades after-hours ordinaires


# Liste de travail : un titre-jour par jour du mois de mesure de chaque titre

def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    h.update(path.read_bytes())
    return h.hexdigest()


def measurement_month_days(date_t: str) -> list[str]:
    """Jours de bourse du mois qui suit la date de constitution de l'échantillon."""
    if date_t == "2018-05-31":
        with CALENDRIER_PATH.open() as f:
            reader = csv.DictReader(f)
            days = [row["dlycaldt"] for row in reader if row["dlycaldt"].startswith("2018-06")]
        return sorted(days)
    if date_t == "2025-12-31":
        return list(JAN_2026_TRADING_DAYS)
    raise ValueError(f"date_t inattendue (seules 2018-05-31 et 2025-12-31 sont prévues) : {date_t}")


def load_sample_rows() -> list[dict]:
    with ECHANTILLON_PATH.open() as f:
        return list(csv.DictReader(f))


def build_work_list() -> dict:
    rows = load_sample_rows()
    # L'époque récente (2025-12-31, mesurée en janvier 2026) est traitée en premier.
    rows_sorted = sorted(rows, key=lambda r: (r["date_t"] != "2025-12-31", r["date_t"], r["ticker"]))
    units = []
    titre_mois = []
    for row in rows_sorted:
        days = measurement_month_days(row["date_t"])
        titre_mois.append({
            "date_t": row["date_t"],
            "ticker": row["ticker"],
            "permno": row["permno"],
            "strate_cap": row["strate_cap"],
            "strate_prix": row["strate_prix"],
            "rang_dans_strate": row["rang_dans_strate"],
            "n_jours": len(days),
            "jours": days,
        })
        for d in days:
            units.append({"ticker": row["ticker"], "date": d, "date_t": row["date_t"]})
    return {
        "echantillon_sha256": sha256_file(ECHANTILLON_PATH),
        "echantillon_path": str(ECHANTILLON_PATH),
        "n_titre_mois": len(titre_mois),
        "n_ticker_jours": len(units),
        "titre_mois": titre_mois,
        "units": units,
    }


def cmd_build_worklist() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    wl = build_work_list()
    WORKLIST_PATH.write_text(json.dumps(wl, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"work_list.json : {wl['n_titre_mois']} titres-mois, {wl['n_ticker_jours']} ticker-jours "
          f"(échantillon sha256={wl['echantillon_sha256'][:12]}...)")


# Indicateurs de qualité de la tape, absents de mesures_sip.py

def quality_extras(trades: list[dict]) -> dict:
    """Taux de trades Z, latence SIP - participant (p50/p99 en ms) et trous de
    sequence_number par tape, sur une liste de trades (jour ou session). La tape
    sert d'approximation du canal de diffusion, dont la structure exacte n'est
    pas connue."""
    n = len(trades)
    n_z = sum(1 for t in trades if Z_OUT_OF_SEQUENCE_ID in (t.get("conditions") or []))

    latencies_ms = []
    for t in trades:
        sip = t.get("sip_timestamp")
        part = t.get("participant_timestamp")
        if sip is not None and part is not None:
            latencies_ms.append((sip - part) / 1_000_000.0)

    p50 = p99 = None
    if latencies_ms:
        latencies_ms.sort()
        p50 = latencies_ms[int(0.50 * (len(latencies_ms) - 1))]
        p99 = latencies_ms[int(0.99 * (len(latencies_ms) - 1))]

    by_tape: dict[str, list[int]] = {}
    for t in trades:
        tape = t.get("tape")
        seq = t.get("sequence_number")
        if tape is None or seq is None:
            continue
        by_tape.setdefault(str(tape), []).append(seq)

    gaps_by_tape = {}
    for tape, seqs in by_tape.items():
        seqs_sorted = sorted(set(seqs))
        n_gaps = 0
        missing = 0
        for a, b in zip(seqs_sorted, seqs_sorted[1:]):
            if b - a > 1:
                n_gaps += 1
                missing += (b - a - 1)
        gaps_by_tape[tape] = {
            "n_sequences": len(seqs_sorted),
            "n_gaps": n_gaps,
            "n_manquants_estimes": missing,
            "seq_min": seqs_sorted[0] if seqs_sorted else None,
            "seq_max": seqs_sorted[-1] if seqs_sorted else None,
        }

    return {
        "n_trades": n,
        "taux_z_hors_sequence": (n_z / n) if n else None,
        "n_z_hors_sequence": n_z,
        "latence_sip_participant_ms": {"p50": p50, "p99": p99, "n": len(latencies_ms)},
        "trous_sequence_par_tape": gaps_by_tape,
    }


def subpenny_venue_cross(trades: list[dict], volume_rules: dict[int, bool]) -> dict:
    """Volume sous-penny croisé par venue (TRF ou bourse affichée) et par tranche de
    prix (au-dessus ou en dessous de 1 $), pour estimer P(hors bourse | sous-penny).

    Reprend les prédicats de `mesures_sip.py` (admissibilité au volume, sous-penny,
    hors bourse) et le même seuil de 1 $, pour rester cohérent avec ses totaux."""
    out = {
        "subpenny_ge1_trf_volume": 0, "subpenny_ge1_lit_volume": 0,
        "subpenny_lt1_trf_volume": 0, "subpenny_lt1_lit_volume": 0,
    }
    unknown: set[int] = set()
    for t in trades:
        if not ms.counts_to_volume(t, volume_rules, unknown):
            continue
        if not ms.is_subpenny(t["price"]):
            continue
        tranche = "ge1" if Decimal(str(t["price"])) >= 1 else "lt1"
        venue = "trf" if ms.is_hors_bourse(t) else "lit"
        out[f"subpenny_{tranche}_{venue}_volume"] += t.get("size", 0)
    return out


# Téléchargement borné par une échéance

class DeadlineExceeded(Exception):
    pass


def fetch_all_trades_with_deadline(ticker: str, date: str, api_key: str, deadline: float) -> list[dict]:
    trades: list[dict] = []
    url = ms.build_first_url(ticker, date)
    while url:
        if time.monotonic() > deadline:
            raise DeadlineExceeded(f"{ticker} {date}: {len(trades)} trades récupérés avant échéance")
        # L'échéance est transmise à fetch_page pour borner aussi ses reprises.
        data = ms.fetch_page(url, api_key, deadline)
        trades.extend(data.get("results") or [])
        url = data.get("next_url")
    return trades


# Traitement d'un ticker-jour

def progress_path(ticker: str, date: str) -> Path:
    return PROGRESS_DIR / f"{ticker}_{date}.json"


def load_failures() -> dict:
    if FAILURES_PATH.exists():
        return json.loads(FAILURES_PATH.read_text())
    return {}


def save_failures(fails: dict) -> None:
    FAILURES_PATH.write_text(json.dumps(fails, indent=2, ensure_ascii=False), encoding="utf-8")


def log_event(event: dict) -> None:
    event["ts"] = time.strftime("%Y-%m-%dT%H:%M:%S")
    with LOG_PATH.open("a", encoding="utf-8") as f:
        f.write(json.dumps(event, ensure_ascii=False) + "\n")


def process_unit(ticker: str, date: str, api_key: str, volume_rules, active_trf_ids, exchange_names,
                  per_fetch_deadline: float) -> dict:
    """Télécharge et mesure un ticker-jour ; les erreurs réseau sont propagées."""
    trades = fetch_all_trades_with_deadline(ticker, date, api_key, per_fetch_deadline)
    base = ms.compute_measures(trades, volume_rules, active_trf_ids, exchange_names, ticker, date)

    # Les indicateurs de qualité portent sur tous les trades, y compris ceux qui ne
    # comptent pas au volume ; le croisement sous-penny applique la règle de volume.
    session_trades: dict[str, list[dict]] = {name: [] for name in ms.SESSION_NAMES}
    for t in trades:
        session_trades[ms.classify_session(t["sip_timestamp"])].append(t)

    base["qualite_extra"] = {
        "global": quality_extras(trades),
        "par_session": {name: quality_extras(session_trades[name]) for name in ms.SESSION_NAMES},
    }
    base["subpenny_venue_cross"] = {
        "global": subpenny_venue_cross(trades, volume_rules),
        "par_session": {name: subpenny_venue_cross(session_trades[name], volume_rules)
                         for name in ms.SESSION_NAMES},
    }
    base["_ticker"] = ticker
    base["_date"] = date
    return base


# --run : traite les titres-jours restants dans la limite d'un budget de temps.
# Un titre-jour en échec est retenté aux invocations suivantes, puis marqué
# en échec définitif après MAX_RETRIES tentatives.

def cmd_run(budget_seconds: float, per_fetch_budget_seconds: float) -> None:
    if not WORKLIST_PATH.exists():
        cmd_build_worklist()
    wl = json.loads(WORKLIST_PATH.read_text())
    PROGRESS_DIR.mkdir(parents=True, exist_ok=True)

    fails = load_failures()
    p1 = ms.load_json(ms.REFERENCES_DIR / "p1-exchanges.json")
    p2 = ms.load_json(ms.REFERENCES_DIR / "p2-conditions.json")
    volume_rules = ms.load_volume_rules(p2)
    active_trf_ids = ms.load_active_trf_ids(p1)
    exchange_names = ms.load_exchange_names(p1)
    api_key = ms.load_api_key()

    start = time.monotonic()
    deadline = start + budget_seconds
    n_done_this_run = 0
    n_skipped_done = 0
    n_failed_this_run = 0
    n_gave_up_this_run = 0

    for unit in wl["units"]:
        if time.monotonic() > deadline:
            break
        ticker, date = unit["ticker"], unit["date"]
        pp = progress_path(ticker, date)
        if pp.exists():
            n_skipped_done += 1
            continue
        key = f"{ticker}_{date}"
        if fails.get(key, 0) >= MAX_RETRIES:
            continue  # abandonné, marqueur d'échec déjà écrit

        per_fetch_deadline = min(deadline, time.monotonic() + per_fetch_budget_seconds)
        try:
            result = process_unit(ticker, date, api_key, volume_rules, active_trf_ids, exchange_names,
                                   per_fetch_deadline)
            pp.write_text(json.dumps(result, indent=2, ensure_ascii=False), encoding="utf-8")
            n_done_this_run += 1
            log_event({"event": "ok", "ticker": ticker, "date": date, "n_trades": result["n_trades_total"]})
        except Exception as e:  # noqa: BLE001 (toute erreur est journalisée et retentée)
            fails[key] = fails.get(key, 0) + 1
            n_failed_this_run += 1
            err_repr = f"{type(e).__name__}: {e}"
            is_http = isinstance(e, urllib.error.HTTPError)
            code = e.code if is_http else None
            log_event({"event": "erreur", "ticker": ticker, "date": date, "tentative": fails[key],
                       "erreur": err_repr, "http_code": code})
            if fails[key] >= MAX_RETRIES:
                n_gave_up_this_run += 1
                pp.write_text(json.dumps({
                    "_ticker": ticker, "_date": date, "_statut": "echec_definitif",
                    "n_tentatives": fails[key], "derniere_erreur": err_repr, "http_code": code,
                }, indent=2, ensure_ascii=False), encoding="utf-8")
                log_event({"event": "abandon", "ticker": ticker, "date": date, "n_tentatives": fails[key]})
            save_failures(fails)

    elapsed = time.monotonic() - start
    print(json.dumps({
        "budget_s": budget_seconds, "elapsed_s": round(elapsed, 1),
        "fait_cette_invocation": n_done_this_run,
        "deja_fait_ignore": n_skipped_done,
        "echecs_cette_invocation": n_failed_this_run,
        "abandons_definitifs_cette_invocation": n_gave_up_this_run,
    }, ensure_ascii=False))


# --status : avancement, sans appel réseau

def cmd_status() -> None:
    if not WORKLIST_PATH.exists():
        print("work_list.json absent : lancer --build-worklist d'abord")
        return
    wl = json.loads(WORKLIST_PATH.read_text())
    fails = load_failures()
    n_ok = 0
    n_echec_def = 0
    n_pending = 0
    par_date_t = {}
    for unit in wl["units"]:
        ticker, date, date_t = unit["ticker"], unit["date"], unit["date_t"]
        d = par_date_t.setdefault(date_t, {"ok": 0, "echec_def": 0, "pending": 0, "total": 0})
        d["total"] += 1
        pp = progress_path(ticker, date)
        if pp.exists():
            data = json.loads(pp.read_text())
            if data.get("_statut") == "echec_definitif":
                n_echec_def += 1
                d["echec_def"] += 1
            else:
                n_ok += 1
                d["ok"] += 1
        else:
            n_pending += 1
            d["pending"] += 1
    print(json.dumps({
        "n_ticker_jours_total": wl["n_ticker_jours"],
        "ok": n_ok, "echec_definitif": n_echec_def, "en_attente": n_pending,
        "en_cours_retry": sum(1 for v in fails.values() if 0 < v < MAX_RETRIES),
        "par_date_t": par_date_t,
    }, indent=2, ensure_ascii=False))


def _part(numerator: int, denominator: int):
    return (numerator / denominator) if denominator else None


ACTIVE_TRF_KEYS = {"4", "201", "202", "203"}

JOUR_SESSION_FIELDS = [
    "date_t", "ticker", "permno", "strate_cap", "strate_prix", "rang_dans_strate",
    "date", "session",
    "n_trades", "volume_total",
    "trf_vol", "trf_part", "trf_vol_4", "trf_vol_201", "trf_vol_202", "trf_vol_203",
    "trf_vol_autre",
    "subpenny_vol", "subpenny_part", "subpenny_ge1_vol", "subpenny_lt1_vol",
    "subpenny_vol_trf", "subpenny_vol_lit",
    "subpenny_ge1_trf_volume", "subpenny_ge1_lit_volume",
    "subpenny_lt1_trf_volume", "subpenny_lt1_lit_volume",
    "oddlot_vol", "oddlot_part",
    "n_trades_qualite", "correction_8", "correction_10", "correction_autres",
    "n_z_hors_sequence", "taux_z_hors_sequence",
    "latence_p50_ms", "latence_p99_ms", "latence_n",
    "n_conditions_inconnues",
]


def _venue_vol(par_venue: dict, key: str) -> int:
    d = par_venue.get(key)
    return d["volume"] if d else 0


def session_row(meta: dict, date: str, session: str, block: dict, qual: dict,
                 cross: dict | None = None) -> dict:
    hb = block["hors_bourse"]
    sp = block["sous_penny"]
    ol = block["odd_lot"]
    corr = block["correction"]["n_trades_par_valeur"]
    subpenny_vol_trf = sum(v["volume"] for k, v in sp["par_venue"].items() if k in ACTIVE_TRF_KEYS)
    subpenny_vol_lit = sp["volume"] - subpenny_vol_trf
    lat = qual["latence_sip_participant_ms"]
    # Un JSON de progression sans croisement sous-penny donne des compteurs nuls.
    cross = cross or {}
    return {
        "date_t": meta["date_t"], "ticker": meta["ticker"], "permno": meta["permno"],
        "strate_cap": meta["strate_cap"], "strate_prix": meta["strate_prix"],
        "rang_dans_strate": meta["rang_dans_strate"],
        "date": date, "session": session,
        "n_trades": block["n_trades"], "volume_total": block["volume_total"],
        "trf_vol": hb["volume"], "trf_part": hb["part"],
        "trf_vol_4": _venue_vol(hb["par_venue"], "4"),
        "trf_vol_201": _venue_vol(hb["par_venue"], "201"),
        "trf_vol_202": _venue_vol(hb["par_venue"], "202"),
        "trf_vol_203": _venue_vol(hb["par_venue"], "203"),
        # ids TRF historiques absents de la table de référence actuelle
        "trf_vol_autre": _venue_vol(hb["par_venue"], "autre"),
        "subpenny_vol": sp["volume"], "subpenny_part": sp["part"],
        "subpenny_ge1_vol": sp["par_tranche_prix"]["ge_1"]["volume"],
        "subpenny_lt1_vol": sp["par_tranche_prix"]["lt_1"]["volume"],
        "subpenny_vol_trf": subpenny_vol_trf, "subpenny_vol_lit": subpenny_vol_lit,
        "subpenny_ge1_trf_volume": cross.get("subpenny_ge1_trf_volume", 0),
        "subpenny_ge1_lit_volume": cross.get("subpenny_ge1_lit_volume", 0),
        "subpenny_lt1_trf_volume": cross.get("subpenny_lt1_trf_volume", 0),
        "subpenny_lt1_lit_volume": cross.get("subpenny_lt1_lit_volume", 0),
        "oddlot_vol": ol["volume"], "oddlot_part": ol["part"],
        "n_trades_qualite": qual["n_trades"],
        "correction_8": corr.get("8", 0), "correction_10": corr.get("10", 0),
        "correction_autres": sum(v for k, v in corr.items() if k not in ("8", "10")),
        "n_z_hors_sequence": qual["n_z_hors_sequence"],
        "taux_z_hors_sequence": qual["taux_z_hors_sequence"],
        "latence_p50_ms": lat["p50"], "latence_p99_ms": lat["p99"], "latence_n": lat["n"],
        "n_conditions_inconnues": block["n_conditions_inconnues"],
    }


def cmd_aggregate() -> None:
    wl = json.loads(WORKLIST_PATH.read_text())
    rows_jour_session = []
    par_ticker_mois: dict[tuple, list[dict]] = {}
    gaps = []

    for tm in wl["titre_mois"]:
        meta = tm
        acc_key = (tm["date_t"], tm["ticker"])
        par_ticker_mois[acc_key] = []
        for date in tm["jours"]:
            pp = progress_path(tm["ticker"], date)
            if not pp.exists():
                gaps.append({"date_t": tm["date_t"], "ticker": tm["ticker"], "date": date, "raison": "absent"})
                continue
            data = json.loads(pp.read_text())
            if data.get("_statut") == "echec_definitif":
                gaps.append({"date_t": tm["date_t"], "ticker": tm["ticker"], "date": date,
                             "raison": data.get("derniere_erreur", "echec_definitif")})
                continue
            par_ticker_mois[acc_key].append(data)
            cross_par_session = data.get("subpenny_venue_cross", {}).get("par_session", {})
            cross_global = data.get("subpenny_venue_cross", {}).get("global", {})
            for session in ms.SESSION_NAMES:
                block = data["par_session"][session]
                qual = data["qualite_extra"]["par_session"][session]
                if block["n_trades"] == 0:
                    continue
                rows_jour_session.append(
                    session_row(meta, date, session, block, qual, cross_par_session.get(session)))
            rows_jour_session.append(
                session_row(meta, date, "tous", data["global"], data["qualite_extra"]["global"], cross_global))

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    with (OUT_DIR / "mesures_titre_jour_session.csv").open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=JOUR_SESSION_FIELDS)
        w.writeheader()
        for r in rows_jour_session:
            w.writerow(r)

    # Agrégats titre-mois (une ligne « tous » et une par session) : les volumes sont
    # sommés puis les parts recalculées, plutôt que de moyenner des parts quotidiennes.
    mois_fields = [
        "date_t", "ticker", "permno", "strate_cap", "strate_prix", "rang_dans_strate",
        "scope", "n_jours_couverts", "n_jours_total",
        "n_trades", "volume_total",
        "trf_vol", "trf_part", "trf_vol_4", "trf_vol_201", "trf_vol_202", "trf_vol_203",
        "trf_vol_autre",
        "subpenny_vol", "subpenny_part", "subpenny_ge1_vol", "subpenny_lt1_vol",
        "subpenny_vol_trf", "subpenny_vol_lit",
        "subpenny_ge1_trf_volume", "subpenny_ge1_lit_volume",
        "subpenny_lt1_trf_volume", "subpenny_lt1_lit_volume",
        "oddlot_vol", "oddlot_part",
        "n_trades_qualite", "correction_8", "correction_10", "correction_autres",
        "taux_correction",
        "n_z_hors_sequence", "taux_z_hors_sequence",
        "latence_p50_ms_proxy", "latence_p99_ms_proxy",
        "n_conditions_inconnues",
    ]
    rows_mois = []
    for tm in wl["titre_mois"]:
        acc_key = (tm["date_t"], tm["ticker"])
        days_data = par_ticker_mois[acc_key]
        n_jours_couverts = len(days_data)
        for scope in ["tous"] + ms.SESSION_NAMES:
            agg = {"n_trades": 0, "volume_total": 0, "trf_vol": 0, "trf_vol_4": 0, "trf_vol_201": 0,
                   "trf_vol_202": 0, "trf_vol_203": 0, "trf_vol_autre": 0,
                   "subpenny_vol": 0, "subpenny_ge1_vol": 0,
                   "subpenny_lt1_vol": 0, "subpenny_vol_trf": 0, "subpenny_vol_lit": 0,
                   "subpenny_ge1_trf_volume": 0, "subpenny_ge1_lit_volume": 0,
                   "subpenny_lt1_trf_volume": 0, "subpenny_lt1_lit_volume": 0,
                   "oddlot_vol": 0, "n_trades_qualite": 0, "correction_8": 0, "correction_10": 0,
                   "correction_autres": 0, "n_z_hors_sequence": 0, "n_conditions_inconnues": 0}
            p50s, p99s, weights = [], [], []
            for data in days_data:
                block = data["global"] if scope == "tous" else data["par_session"][scope]
                qual = data["qualite_extra"]["global"] if scope == "tous" else data["qualite_extra"]["par_session"][scope]
                cross_racine = data.get("subpenny_venue_cross", {})
                cross = (cross_racine.get("global", {}) if scope == "tous"
                          else cross_racine.get("par_session", {}).get(scope, {}))
                hb, sp, ol = block["hors_bourse"], block["sous_penny"], block["odd_lot"]
                corr = block["correction"]["n_trades_par_valeur"]
                agg["n_trades"] += block["n_trades"]
                agg["volume_total"] += block["volume_total"]
                agg["trf_vol"] += hb["volume"]
                agg["trf_vol_4"] += _venue_vol(hb["par_venue"], "4")
                agg["trf_vol_201"] += _venue_vol(hb["par_venue"], "201")
                agg["trf_vol_202"] += _venue_vol(hb["par_venue"], "202")
                agg["trf_vol_203"] += _venue_vol(hb["par_venue"], "203")
                agg["trf_vol_autre"] += _venue_vol(hb["par_venue"], "autre")
                agg["subpenny_vol"] += sp["volume"]
                agg["subpenny_ge1_vol"] += sp["par_tranche_prix"]["ge_1"]["volume"]
                agg["subpenny_lt1_vol"] += sp["par_tranche_prix"]["lt_1"]["volume"]
                sp_trf = sum(v["volume"] for k, v in sp["par_venue"].items() if k in ACTIVE_TRF_KEYS)
                agg["subpenny_vol_trf"] += sp_trf
                agg["subpenny_vol_lit"] += sp["volume"] - sp_trf
                agg["subpenny_ge1_trf_volume"] += cross.get("subpenny_ge1_trf_volume", 0)
                agg["subpenny_ge1_lit_volume"] += cross.get("subpenny_ge1_lit_volume", 0)
                agg["subpenny_lt1_trf_volume"] += cross.get("subpenny_lt1_trf_volume", 0)
                agg["subpenny_lt1_lit_volume"] += cross.get("subpenny_lt1_lit_volume", 0)
                agg["oddlot_vol"] += ol["volume"]
                agg["n_trades_qualite"] += qual["n_trades"]
                agg["correction_8"] += corr.get("8", 0)
                agg["correction_10"] += corr.get("10", 0)
                agg["correction_autres"] += sum(v for k, v in corr.items() if k not in ("8", "10"))
                agg["n_z_hors_sequence"] += qual["n_z_hors_sequence"]
                agg["n_conditions_inconnues"] += block["n_conditions_inconnues"]
                lat = qual["latence_sip_participant_ms"]
                if lat["p50"] is not None:
                    p50s.append(lat["p50"]); p99s.append(lat["p99"]); weights.append(qual["n_trades"])

            vt = agg["volume_total"]
            row = {
                "date_t": tm["date_t"], "ticker": tm["ticker"], "permno": tm["permno"],
                "strate_cap": tm["strate_cap"], "strate_prix": tm["strate_prix"],
                "rang_dans_strate": tm["rang_dans_strate"],
                "scope": scope, "n_jours_couverts": n_jours_couverts, "n_jours_total": tm["n_jours"],
                "n_trades": agg["n_trades"], "volume_total": vt,
                "trf_vol": agg["trf_vol"], "trf_part": _part(agg["trf_vol"], vt),
                "trf_vol_4": agg["trf_vol_4"], "trf_vol_201": agg["trf_vol_201"],
                "trf_vol_202": agg["trf_vol_202"], "trf_vol_203": agg["trf_vol_203"],
                "trf_vol_autre": agg["trf_vol_autre"],
                "subpenny_vol": agg["subpenny_vol"], "subpenny_part": _part(agg["subpenny_vol"], vt),
                "subpenny_ge1_vol": agg["subpenny_ge1_vol"], "subpenny_lt1_vol": agg["subpenny_lt1_vol"],
                "subpenny_vol_trf": agg["subpenny_vol_trf"], "subpenny_vol_lit": agg["subpenny_vol_lit"],
                "subpenny_ge1_trf_volume": agg["subpenny_ge1_trf_volume"],
                "subpenny_ge1_lit_volume": agg["subpenny_ge1_lit_volume"],
                "subpenny_lt1_trf_volume": agg["subpenny_lt1_trf_volume"],
                "subpenny_lt1_lit_volume": agg["subpenny_lt1_lit_volume"],
                "oddlot_vol": agg["oddlot_vol"], "oddlot_part": _part(agg["oddlot_vol"], vt),
                "n_trades_qualite": agg["n_trades_qualite"],
                "correction_8": agg["correction_8"], "correction_10": agg["correction_10"],
                "correction_autres": agg["correction_autres"],
                "taux_correction": _part(agg["correction_8"] + agg["correction_10"] + agg["correction_autres"],
                                          agg["n_trades_qualite"]),
                "n_z_hors_sequence": agg["n_z_hors_sequence"],
                "taux_z_hors_sequence": _part(agg["n_z_hors_sequence"], agg["n_trades_qualite"]),
                # Approximations, les latences individuelles n'étant pas conservées :
                # moyenne des p50 quotidiens pondérée par n_trades, maximum des p99.
                "latence_p50_ms_proxy": (
                    sum(p * w for p, w in zip(p50s, weights)) / sum(weights) if weights else None
                ),
                "latence_p99_ms_proxy": max(p99s) if p99s else None,
                "n_conditions_inconnues": agg["n_conditions_inconnues"],
            }
            rows_mois.append(row)

    with (OUT_DIR / "mesures_titre_mois.csv").open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=mois_fields)
        w.writeheader()
        for r in rows_mois:
            w.writerow(r)

    (OUT_DIR / "gaps.json").write_text(json.dumps(gaps, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"agrégation : {len(rows_jour_session)} lignes titre-jour-session, {len(rows_mois)} lignes titre-mois, "
          f"{len(gaps)} trous")


# CLI

def main() -> None:
    import argparse
    parser = argparse.ArgumentParser(description="Mesures SIP sur l'échantillon de calibration.")
    parser.add_argument("--build-worklist", action="store_true")
    parser.add_argument("--run", action="store_true")
    parser.add_argument("--status", action="store_true")
    parser.add_argument("--aggregate", action="store_true")
    parser.add_argument("--budget-seconds", type=float, default=80.0)
    parser.add_argument("--per-fetch-budget-seconds", type=float, default=60.0)
    args = parser.parse_args()

    if args.build_worklist:
        cmd_build_worklist()
    elif args.run:
        cmd_run(args.budget_seconds, args.per_fetch_budget_seconds)
    elif args.status:
        cmd_status()
    elif args.aggregate:
        cmd_aggregate()
    else:
        parser.error("préciser --build-worklist, --run, --status ou --aggregate")


if __name__ == "__main__":
    main()
