#!/usr/bin/env python3
"""Calcul en flux et reprenable des primitives à conditions sur l'univers.

Calcule P-01 (mode `tick`), P-02, P-03, P-13 et P-14 pour chaque titre-jour d'un
sous-échantillon 1/10 des PERMNO de l'univers. La composante TRF de P-15 est
p02_trf_part_all_incl810 ; sa composante cachée exige les données ITCH et n'est
pas calculée ici, pas plus que P-04/P-05 (calculées à partir de barres
journalières). Les trades bruts ne sont jamais écrits sur disque : chaque
titre-jour est téléchargé en mémoire, traité par `primitives_t1`, puis ajouté au
CSV de sortie ; un journal JSONL des événements permet de reprendre une
exécution interrompue.

Usage :
    python3 runner_univers.py --build-worklist
    python3 runner_univers.py --run --budget-seconds 80 --workers 8
    python3 runner_univers.py --status
"""
from __future__ import annotations

import argparse
import concurrent.futures
import csv
import hashlib
import json
import os
import sys
import threading
import time
import urllib.error
from collections import defaultdict
from pathlib import Path

CODE_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import chemins  # noqa: E402
UNIVERS_DIR = chemins.SORTIES / "univers"
CALENDRIER_PATH = chemins.DONNEES / "crsp" / "calendrier_bourse.csv"
EARLY_CLOSES_PATH = chemins.REFERENCES / "early_closes.csv"

OUT_DIR = chemins.SORTIES / "primitives-univers"
OUT_DIR.mkdir(parents=True, exist_ok=True)
WORKLIST_PATH = OUT_DIR / "worklist.json"
PROGRESS_PATH = OUT_DIR / "progress.jsonl"
CSV_PATH = OUT_DIR / "primitives_conditions_titre_jour.csv"
DONE_INDEX_PATH = OUT_DIR / ".done_index.json"  # cache dérivé de progress.jsonl, reconstruit si absent

FRACTION_DENOM = 10  # 1/10 des PERMNO : environ 48 h de calcul au débit mesuré avec 8 workers

try:
    import certifi
    os.environ.setdefault("SSL_CERT_FILE", certifi.where())
except ImportError:
    pass

import mesures_sip as msip  # noqa: E402
import mesures_calibration as mcal  # noqa: E402  -- fournit JAN_2026_TRADING_DAYS
import primitives_t1 as t1  # noqa: E402

MAX_RETRIES = 3
PER_FETCH_BUDGET_S = 30.0

_csv_lock = threading.Lock()
_progress_lock = threading.Lock()


# Calendrier de mesure pour tous les snapshots : jours de bourse CRSP, sauf
# janvier 2026, hors couverture CRSP, pris dans mesures_calibration.

_CALENDAR_BY_MONTH: dict[str, list[str]] | None = None


def _load_calendar_by_month() -> dict[str, list[str]]:
    global _CALENDAR_BY_MONTH
    if _CALENDAR_BY_MONTH is not None:
        return _CALENDAR_BY_MONTH
    by_month: dict[str, list[str]] = defaultdict(list)
    with CALENDRIER_PATH.open() as f:
        reader = csv.DictReader(f)
        for row in reader:
            d = row["dlycaldt"]
            by_month[d[:7]].append(d)
    _CALENDAR_BY_MONTH = {k: sorted(v) for k, v in by_month.items()}
    return _CALENDAR_BY_MONTH


def measurement_month_days_general(date_t: str) -> tuple[str, list[str]]:
    """date_t (dernier jour de bourse du mois M, au format YYYY-MM-DD) ->
    ('YYYY-MM' du mois M+1, jours de bourse de ce mois) : le snapshot daté de
    la fin du mois M régit le mois calendaire suivant."""
    y, m = int(date_t[:4]), int(date_t[5:7])
    if m == 12:
        y1, m1 = y + 1, 1
    else:
        y1, m1 = y, m + 1
    mois_key = f"{y1:04d}-{m1:02d}"
    if mois_key == "2026-01":
        return mois_key, list(mcal.JAN_2026_TRADING_DAYS)
    cal = _load_calendar_by_month()
    days = cal.get(mois_key)
    if not days:
        raise ValueError(f"aucun jour de bourse trouvé pour {mois_key} (date_t={date_t}) dans {CALENDRIER_PATH}")
    return mois_key, days


def load_early_closes() -> set[str]:
    out = set()
    with EARLY_CLOSES_PATH.open() as f:
        for row in csv.DictReader(f):
            out.add(row["date"])
    return out


# Liste de travail : le premier dixième des PERMNO distincts de toute la
# période, triés par SHA-256(permno). La sélection porte sur les PERMNO et non
# sur les mois, pour suivre chaque titre retenu sur toute sa présence dans
# l'univers. Chaque unité est un (ticker du snapshot, jour de bourse du mois M+1).

def sha256_hex(s: str) -> str:
    return hashlib.sha256(s.encode()).hexdigest()


def list_snapshots() -> list[Path]:
    return sorted(UNIVERS_DIR.glob("U_*.csv"))


def select_permnos(snapshots: list[Path], denom: int) -> set[str]:
    all_permnos: set[str] = set()
    for p in snapshots:
        with p.open() as f:
            for row in csv.DictReader(f):
                all_permnos.add(row["permno"])
    ranked = sorted(all_permnos, key=sha256_hex)
    n_keep = len(ranked) // denom
    return set(ranked[:n_keep])


def build_work_list(denom: int = FRACTION_DENOM) -> dict:
    snapshots = list_snapshots()
    selected = select_permnos(snapshots, denom)
    early_closes = load_early_closes()

    units = []  # {"ticker","date","permno","date_t","mois"}
    n_deferred = 0
    for snap in snapshots:
        date_t = snap.stem.replace("U_", "")
        date_t_iso = f"{date_t[:4]}-{date_t[4:6]}-{date_t[6:]}"
        mois_key, days = measurement_month_days_general(date_t_iso)
        with snap.open() as f:
            rows = [r for r in csv.DictReader(f) if r["permno"] in selected]
        for r in rows:
            ticker = r["ticker"]
            permno = r["permno"]
            for d in days:
                status0 = "deferred_early_close" if d in early_closes else "pending"
                if status0 == "deferred_early_close":
                    n_deferred += 1
                units.append({"ticker": ticker, "date": d, "permno": permno,
                              "date_t": date_t_iso, "mois": mois_key, "status0": status0})

    wl = {
        "denom": denom,
        "n_permno_total": len(set(p.stem for p in snapshots)),  # compte en fait les snapshots
        "n_permno_selected": len(selected),
        "n_snapshots": len(snapshots),
        "n_units": len(units),
        "n_deferred_early_close_preworklist": n_deferred,
        "early_closes_source": str(EARLY_CLOSES_PATH),
        "units": units,
    }
    return wl


def cmd_build_worklist(force: bool = False) -> None:
    if WORKLIST_PATH.exists() and not force:
        print(f"worklist déjà construite : {WORKLIST_PATH} (--force pour reconstruire)")
        return
    t0 = time.time()
    wl = build_work_list()
    WORKLIST_PATH.write_text(json.dumps(wl, ensure_ascii=False), encoding="utf-8")
    print(f"worklist.json : {wl['n_permno_selected']} PERMNO sélectionnés (1/{wl['denom']}), "
          f"{wl['n_units']} ticker-jours ({wl['n_deferred_early_close_preworklist']} "
          f"marqués deferred_early_close), construit en {time.time()-t0:.1f}s")


# Traitement d'un ticker-jour, entièrement en mémoire.

# Champs P-02/P-03/P-14 retenus. P-04 n'est pas calculé ici : RVact demande
# une fenêtre glissante de 63 jours.
P02_P03_P14_FIELDS = [
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
    "p14_taux_hors_sequence_Z", "p14_taux_correction", "p14_vol_correction_part",
    "p14_n_correction_8", "p14_n_correction_10",
    "p14_delta_trf_part_all_810", "p14_delta_sp_part_all_810", "p14_delta_dvact_810",
    "p14_latence_mediane_ms", "p14_latence_p99_ms",
    "p14_taux_conditions_inconnues", "p14_taux_exclus", "p14_taux_desordre_fichier",
]

P01_P13_FIELDS_KEEP = [f for f in t1.P01_P13_FIELDS if f not in ("ticker", "date", "mois", "n_trades_raw")]

OUT_FIELDS = ["ticker", "date", "mois", "date_t", "permno", "n_trades_raw"] + \
    P02_P03_P14_FIELDS + P01_P13_FIELDS_KEEP


def process_unit_in_memory(ticker: str, date: str, permno: str, date_t: str, mois: str,
                            api_key: str, tables: dict, deadline: float) -> dict:
    trades_raw = msip.fetch_all_trades(ticker, date, api_key, deadline)
    seq_stats_local: dict = defaultdict(int)
    row_p02, _dv = t1.process_ticker_day(ticker, date, trades_raw, tables, seq_stats_local)
    row_p01p13 = t1.process_ticker_day_p01_p13(ticker, date, trades_raw, tables)
    row_p01p13_fmt = t1.fmt_row(row_p01p13)

    out = {"ticker": ticker, "date": date, "mois": mois, "date_t": date_t, "permno": permno,
           "n_trades_raw": row_p02["n_trades_raw"]}
    for f in P02_P03_P14_FIELDS:
        out[f] = row_p02[f]
    for f in P01_P13_FIELDS_KEEP:
        out[f] = row_p01p13_fmt[f]
    return out


# Progression et reprise

def load_done_index() -> dict[str, dict]:
    """{"ticker|date": {"status": ..., "tentatives": ...}} reconstruit depuis
    progress.jsonl, qui fait foi. Le résultat est mis en cache dans
    .done_index.json, ignoré si progress.jsonl est plus récent."""
    if (DONE_INDEX_PATH.exists() and PROGRESS_PATH.exists()
            and DONE_INDEX_PATH.stat().st_mtime >= PROGRESS_PATH.stat().st_mtime):
        return json.loads(DONE_INDEX_PATH.read_text())
    idx: dict[str, dict] = {}
    if PROGRESS_PATH.exists():
        with PROGRESS_PATH.open() as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    ev = json.loads(line)
                except json.JSONDecodeError:
                    continue
                key = f"{ev['ticker']}|{ev['date']}"
                cur = idx.get(key, {"status": None, "tentatives": 0})
                if ev.get("event") == "erreur":
                    cur["tentatives"] = cur.get("tentatives", 0) + 1
                    cur["status"] = "erreur"
                elif ev.get("event") in ("ok", "deferred_early_close", "abandon"):
                    cur["status"] = ev["event"]
                idx[key] = cur
    DONE_INDEX_PATH.write_text(json.dumps(idx), encoding="utf-8")
    return idx


def log_event(event: dict) -> None:
    event["ts"] = time.strftime("%Y-%m-%dT%H:%M:%S")
    with _progress_lock:
        with PROGRESS_PATH.open("a", encoding="utf-8") as f:
            f.write(json.dumps(event, ensure_ascii=False) + "\n")


def ensure_csv_header() -> None:
    if not CSV_PATH.exists():
        with CSV_PATH.open("w", newline="", encoding="utf-8") as f:
            csv.DictWriter(f, fieldnames=OUT_FIELDS).writeheader()


def append_csv_row(row: dict) -> None:
    with _csv_lock:
        with CSV_PATH.open("a", newline="", encoding="utf-8") as f:
            csv.DictWriter(f, fieldnames=OUT_FIELDS).writerow(row)


# --run : boucle bornée par un budget de temps. Des threads suffisent : le temps
# réseau domine largement le calcul d'un ticker-jour.

def cmd_run(budget_seconds: float, n_workers: int) -> None:
    if not WORKLIST_PATH.exists():
        cmd_build_worklist()
    wl = json.loads(WORKLIST_PATH.read_text())
    ensure_csv_header()

    done_idx = load_done_index()
    # `primitives_t1` construit sa grille de session à partir de
    # `references/early_closes.csv`, si bien que les demi-séances sont traitables.
    # Les unités journalisées `deferred_early_close` par des exécutions
    # antérieures sont donc soumises comme les autres.
    n_defer_repris = sum(1 for v in done_idx.values()
                         if v["status"] == "deferred_early_close")

    tables = t1.load_tables()
    t1.verify_auction_codes(tables["p2"])
    api_key = msip.load_api_key()

    todo = []
    for u in wl["units"]:
        key = f"{u['ticker']}|{u['date']}"
        st = done_idx.get(key)
        if st is None:
            todo.append(u)
        elif st["status"] == "erreur" and st.get("tentatives", 0) < MAX_RETRIES:
            todo.append(u)
        elif st["status"] == "deferred_early_close":
            todo.append(u)
        # "ok" et "abandon" sont des états terminaux

    start = time.monotonic()
    deadline = start + budget_seconds
    n_done = n_err = n_gaveup = 0
    lock = threading.Lock()

    def worker(u):
        nonlocal n_done, n_err, n_gaveup
        ticker, date, permno, date_t, mois = u["ticker"], u["date"], u["permno"], u["date_t"], u["mois"]
        key = f"{ticker}|{date}"
        per_fetch_deadline = min(deadline, time.monotonic() + PER_FETCH_BUDGET_S)
        try:
            out = process_unit_in_memory(ticker, date, permno, date_t, mois, api_key, tables,
                                          per_fetch_deadline)
            append_csv_row(out)
            log_event({"event": "ok", "ticker": ticker, "date": date, "n_trades": out["n_trades_raw"]})
            with lock:
                n_done += 1
        except Exception as e:  # noqa: BLE001 -- 429, 404, timeout... : tout est journalisé
            is_http = isinstance(e, urllib.error.HTTPError)
            code = e.code if is_http else None
            tentatives = done_idx.get(key, {}).get("tentatives", 0) + 1
            with lock:
                done_idx[key] = {"status": "erreur", "tentatives": tentatives}
            event = "abandon" if tentatives >= MAX_RETRIES else "erreur"
            log_event({"event": event, "ticker": ticker, "date": date, "tentative": tentatives,
                       "erreur": f"{type(e).__name__}: {e}", "http_code": code})
            with lock:
                if event == "abandon":
                    n_gaveup += 1
                else:
                    n_err += 1

    with concurrent.futures.ThreadPoolExecutor(max_workers=n_workers) as ex:
        it = iter(todo)
        in_flight = {}
        for _ in range(n_workers):
            try:
                u = next(it)
            except StopIteration:
                break
            in_flight[ex.submit(worker, u)] = u
        while in_flight and time.monotonic() < deadline:
            done, _ = concurrent.futures.wait(in_flight, timeout=1.0,
                                               return_when=concurrent.futures.FIRST_COMPLETED)
            for fut in done:
                del in_flight[fut]
                if time.monotonic() < deadline:
                    try:
                        u = next(it)
                    except StopIteration:
                        continue
                    in_flight[ex.submit(worker, u)] = u

    # progress.jsonl a été complété : le cache d'index est périmé.
    if DONE_INDEX_PATH.exists():
        DONE_INDEX_PATH.unlink()

    elapsed = time.monotonic() - start
    n_total_units = len(wl["units"])
    n_terminal = sum(1 for v in done_idx.values() if v["status"] in ("ok", "abandon"))
    print(json.dumps({
        "budget_s": budget_seconds, "elapsed_s": round(elapsed, 1), "n_workers": n_workers,
        "fait_cette_invocation": n_done, "erreurs_cette_invocation": n_err,
        "abandons_cette_invocation": n_gaveup, "deferred_early_close_repris": n_defer_repris,
        "n_restant_dans_todo_initial": len(todo),
        "n_units_non_differees_total": n_total_units,
        "n_terminal_cumule_estime": n_terminal,
    }, ensure_ascii=False))


def cmd_status() -> None:
    if not WORKLIST_PATH.exists():
        print("worklist absente : lancer --build-worklist")
        return
    wl = json.loads(WORKLIST_PATH.read_text())
    done_idx = load_done_index()
    n_ok = sum(1 for v in done_idx.values() if v["status"] == "ok")
    n_abandon = sum(1 for v in done_idx.values() if v["status"] == "abandon")
    n_deferred = sum(1 for v in done_idx.values() if v["status"] == "deferred_early_close")
    n_err_retry = sum(1 for v in done_idx.values() if v["status"] == "erreur" and v.get("tentatives", 0) < MAX_RETRIES)
    # Les unités deferred_early_close restent à traiter et comptent au dénominateur.
    n_total = wl["n_units"]
    print(json.dumps({
        "n_units_total": n_total,
        "n_deferred_early_close_a_rejouer": n_deferred,
        "n_units_a_traiter": n_total,
        "n_ok": n_ok, "n_abandon_definitif": n_abandon, "n_en_attente_retry": n_err_retry,
        "n_reste_a_faire": n_total - n_ok - n_abandon,
        "pct_fait": round(100 * n_ok / n_total, 2) if n_total else None,
    }, ensure_ascii=False))


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--build-worklist", action="store_true")
    ap.add_argument("--force", action="store_true")
    ap.add_argument("--run", action="store_true")
    ap.add_argument("--budget-seconds", type=float, default=80.0)
    ap.add_argument("--workers", type=int, default=8)
    ap.add_argument("--status", action="store_true")
    args = ap.parse_args()

    if args.build_worklist:
        cmd_build_worklist(force=args.force)
    elif args.run:
        cmd_run(args.budget_seconds, args.workers)
    elif args.status:
        cmd_status()
    else:
        ap.error("choisir --build-worklist, --run ou --status")


if __name__ == "__main__":
    main()
