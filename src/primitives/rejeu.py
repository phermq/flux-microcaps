#!/usr/bin/env python3
"""Harnais de rejeu : stabilité des primitives sous permutation et découpage.

Importe les modules de primitives sans les modifier et rejoue leurs fonctions
avec (a) un ordre d'ingestion permuté (mélange déterministe à graine fixe,
appliqué avant le tri canonique propre à chaque module) et (b) un ordre de
traitement des unités (ticker-jour ou ticker-mois) différent. Les sorties sont
comparées champ par champ à celles produites par l'exécution normale.

Usage :
    python3 src/primitives/rejeu.py --stage {t1,t1b,p12}
    python3 src/primitives/rejeu.py --stage {t3,t3q} [--limit N]

Chaque stage écrit sorties/rejeu/report_<stage>.json et journalise sa
progression dans sorties/rejeu/.progress.jsonl.
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import random
import sys
import time
from array import array
from collections import Counter, defaultdict
from pathlib import Path

CODE_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import chemins  # noqa: E402
SORTIES_REJEU = chemins.SORTIES / "rejeu"
SORTIES_REJEU.mkdir(parents=True, exist_ok=True)
PROGRESS_PATH = SORTIES_REJEU / ".progress.jsonl"

SEED = 20260726  # graine fixe : les permutations sont reproductibles



def log(**kw):
    kw["t"] = time.time()
    with PROGRESS_PATH.open("a", encoding="utf-8") as f:
        f.write(json.dumps(kw) + "\n")
    print(f"[rejeu] {kw}", file=sys.stderr)


def stable_seed(*parts) -> int:
    h = hashlib.sha256("|".join(str(p) for p in parts).encode()).hexdigest()
    return int(h[:8], 16)


def shuffled(seq, *seed_parts):
    rng = random.Random(stable_seed(SEED, *seed_parts))
    lst = list(seq)
    rng.shuffle(lst)
    return lst


# =============================================================================
# Stage T1 (primitives_t1.py) — P-01(tick)/P-02/P-03/P-04/P-13/P-14
# =============================================================================

def stage_t1():
    import primitives_t1 as t1
    import statistics as _stat

    out_dir = SORTIES_REJEU / "t1"
    out_dir.mkdir(exist_ok=True)
    orig_dir = chemins.SORTIES / "t1"

    tables = t1.load_tables()
    t1.verify_auction_codes(tables["p2"])

    ticker_days = t1.list_ticker_days()
    log(stage="t1", step="list_ticker_days", n=len(ticker_days))

    # Les enregistrements de chaque fichier sont mélangés avant le tri interne du
    # module, et les ticker-jours (unité atomique de traitement : un fichier par
    # ticker-jour) sont parcourus dans un ordre global mélangé. Les agrégations
    # en aval re-trient par date ou par clé, ce qui doit rendre le résultat
    # indépendant de ces deux ordres.
    ticker_days_shuffled = shuffled(ticker_days, "t1", "ticker_days")

    seq_stats = defaultdict(int)
    seq_stats_orig = defaultdict(int)
    all_rows = {}
    all_rows_p01_p13 = {}
    rows_by_ticker_mois = defaultdict(list)

    t0 = time.time()
    for i, (ticker, date, path) in enumerate(ticker_days_shuffled):
        trades_raw_orig = t1.load_trades(path)
        trades_raw = shuffled(trades_raw_orig, "t1", ticker, date)

        row, dv = t1.process_ticker_day(ticker, date, trades_raw, tables, seq_stats)
        all_rows[(ticker, date)] = row
        rows_by_ticker_mois[(ticker, date[:7])].append({"date": date, **dv})
        all_rows_p01_p13[(ticker, date)] = t1.process_ticker_day_p01_p13(ticker, date, trades_raw, tables)

        # Diagnostic de désordre calculé sur l'ordre physique d'origine du fichier.
        _ =t1.check_sequence_number_order(trades_raw_orig)

        if (i + 1) % 200 == 0:
            log(stage="t1", step="processing", i=i + 1, n=len(ticker_days_shuffled),
                elapsed=round(time.time() - t0, 1))

    log(stage="t1", step="processed_all", elapsed=round(time.time() - t0, 1))

    # P-04 : seconde passe sur la fenêtre intra-mois, comme dans primitives_t1.run_pipeline.
    n_ok = n_histo = n_med0 = 0
    for (ticker, mois_key), entries in rows_by_ticker_mois.items():
        entries.sort(key=lambda e: e["date"])
        for variant in ("incl810", "excl810"):
            series = [{"date": e["date"], "dvact": e[f"{variant}_dvact"],
                       "dvnotional_u": e[f"{variant}_dvnotional_u"]} for e in entries]
            for idx, e in enumerate(entries):
                r = t1.compute_rv(series, idx, ca_events=None)
                row = all_rows[(ticker, e["date"])]
                row[f"p04_rvact_{variant}"] = t1.fmt(r["rvact"])
                row[f"p04_rvact_na_code_{variant}"] = r["rvact_na_code"]
                row[f"p04_rvnotional_{variant}"] = t1.fmt(r["rv_notional"])
                row[f"p04_rvnotional_na_code_{variant}"] = r["rv_notional_na_code"]
                if variant == "incl810":
                    row["p04_n_histo"] = r["n_histo"]
                    if row["mois"] == "2026-01":
                        row["p04_ca_note"] = "hors couverture CRSP (distributions arretees a fin 2025) : pas de retraitement des operations sur titres"
                    else:
                        row["p04_ca_note"] = ("verifie sans objet : aucun evenement FRS dans la table CRSP des distributions "
                                               "sur la fenetre intra-mois des titre-jours candidats")

    # Les notes P-04 ci-dessus reprennent mot pour mot celles de primitives_t1,
    # puisqu'elles sont comparées au texte des sorties de référence.

    # CSV titre-jour : mêmes champs et même tri que run_pipeline.
    out_jour = out_dir / "p02_p03_p04_p14_titre_jour.csv"
    with out_jour.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=t1.TITRE_JOUR_FIELDS)
        w.writeheader()
        for (ticker, date) in sorted(all_rows):
            w.writerow(all_rows[(ticker, date)])

    # Agrégat titre-mois (sommes, puis ratios). `all_rows` a été rempli dans l'ordre
    # mélangé : on l'agrège dans l'ordre trié, comme run_pipeline, pour que les
    # sommes flottantes soient effectuées dans le même ordre. Le notionnel est sommé
    # en entiers d'unités de 1e-4 $, donc sans erreur d'arrondi.
    agg = defaultdict(lambda: defaultdict(float))
    agg_dvnotional_u = defaultdict(int)
    for (ticker, date) in sorted(all_rows):
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
    out_mois = out_dir / "p02_p03_p04_p14_titre_mois.csv"
    with out_mois.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=mois_fields)
        w.writeheader()
        for (ticker, mois), a in sorted(agg.items()):
            w.writerow({
                "ticker": ticker, "mois": mois, "n_jours": int(a["n_jours"]),
                "p02_vol_all_incl810": int(a["p02_vol_all_incl810"]),
                "p02_trf_vol_all_incl810": int(a["p02_trf_vol_all_incl810"]),
                "p02_trf_part_all_incl810": t1.fmt(t1.part(a["p02_trf_vol_all_incl810"], a["p02_vol_all_incl810"])),
                "p02_vol_continu_incl810": int(a["p02_vol_continu_incl810"]),
                "p02_trf_vol_continu_incl810": int(a["p02_trf_vol_continu_incl810"]),
                "p02_trf_part_continu_incl810": t1.fmt(t1.part(a["p02_trf_vol_continu_incl810"], a["p02_vol_continu_incl810"])),
                "p02_trf_part_all_excl810": t1.fmt(t1.part(a["p02_trf_vol_all_excl810"], a["p02_vol_all_excl810"])),
                "p03_vol_ge1_all_incl810": int(a["p03_vol_ge1_all_incl810"]),
                "p03_sp_vol_all_incl810": int(a["p03_sp_vol_all_incl810"]),
                "p03_sp_part_all_incl810": t1.fmt(t1.part(a["p03_sp_vol_all_incl810"], a["p03_vol_ge1_all_incl810"])),
                "p03_mid_vol_all_incl810": int(a["p03_mid_vol_all_incl810"]),
                "p03_mid_part_all_incl810": t1.fmt(t1.part(a["p03_mid_vol_all_incl810"], a["p03_vol_ge1_all_incl810"])),
                "p03_sp_part_all_excl810": t1.fmt(t1.part(a["p03_sp_vol_all_excl810"], a["p03_vol_ge1_all_excl810"])),
                "p04_dvact_incl810": int(a["p04_dvact_incl810"]),
                "p04_dvnotional_incl810": t1.notionnel_dollars(agg_dvnotional_u[(ticker, mois)]),
            })

    # P-01/P-13 titre-jour
    out_p01p13_jour = out_dir / "p01_p13_titre_jour.csv"
    with out_p01p13_jour.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=t1.P01_P13_FIELDS)
        w.writeheader()
        for (ticker, date) in sorted(all_rows_p01_p13):
            w.writerow(t1.fmt_row(all_rows_p01_p13[(ticker, date)]))

    # P-01/P-13 titre-mois
    agg_p01p13 = defaultdict(lambda: defaultdict(float))
    daily = {k: defaultdict(list) for k in
             ("ofi_all_actions", "ofi_all_notional", "ofi_lit_actions", "ofi_lit_notional",
              "part_signable_all", "part_signable_lit", "q95_acc")}
    for (ticker, date) in sorted(all_rows_p01_p13):        # même ordre que run_pipeline
        row = all_rows_p01_p13[(ticker, date)]
        key = (ticker, row["mois"])
        a = agg_p01p13[key]
        for f in ["p01_vplus_all", "p01_vminus_all", "p01_vna_all", "p01_vtrf",
                  "p01_vplus_lit", "p01_vminus_lit", "p01_vna_lit",
                  "p01_n_both_defined", "p01_n_disagree",
                  "p13_n_lit_trf", "p13_vact_lit_trf"]:
            a[f] += row[f]
        a["n_jours"] += 1
        for fld, dkey in [("p01_ofi_median_all_actions", "ofi_all_actions"),
                           ("p01_ofi_median_all_notional", "ofi_all_notional"),
                           ("p01_ofi_median_lit_actions", "ofi_lit_actions"),
                           ("p01_ofi_median_lit_notional", "ofi_lit_notional"),
                           ("p01_part_signable_all", "part_signable_all"),
                           ("p01_part_signable_lit", "part_signable_lit"),
                           ("p13_q95_abs_acc", "q95_acc")]:
            if not t1.is_na(row[fld]):
                daily[dkey][key].append(row[fld])

    def med_or_na(d, key):
        vals = d.get(key)
        return _stat.median(vals) if vals else t1.NA("aucun_jour")

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
    out_p01p13_mois = out_dir / "p01_p13_titre_mois.csv"
    with out_p01p13_mois.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=mois_fields_p01p13)
        w.writeheader()
        for (ticker, mois), a in sorted(agg_p01p13.items()):
            w.writerow({
                "ticker": ticker, "mois": mois, "n_jours": int(a["n_jours"]),
                "p01_vplus_all": int(a["p01_vplus_all"]), "p01_vminus_all": int(a["p01_vminus_all"]),
                "p01_vna_all": int(a["p01_vna_all"]), "p01_vtrf": int(a["p01_vtrf"]),
                "p01_vplus_lit": int(a["p01_vplus_lit"]), "p01_vminus_lit": int(a["p01_vminus_lit"]),
                "p01_vna_lit": int(a["p01_vna_lit"]),
                "p01_ofi_median_all_actions_mois": t1.fmt(med_or_na(daily["ofi_all_actions"], (ticker, mois))),
                "p01_ofi_median_all_notional_mois": t1.fmt(med_or_na(daily["ofi_all_notional"], (ticker, mois))),
                "p01_ofi_median_lit_actions_mois": t1.fmt(med_or_na(daily["ofi_lit_actions"], (ticker, mois))),
                "p01_ofi_median_lit_notional_mois": t1.fmt(med_or_na(daily["ofi_lit_notional"], (ticker, mois))),
                "p01_part_signable_all_mois": t1.fmt(med_or_na(daily["part_signable_all"], (ticker, mois))),
                "p01_part_signable_lit_mois": t1.fmt(med_or_na(daily["part_signable_lit"], (ticker, mois))),
                "p01_desaccord_rate_mois": t1.fmt(t1.part(int(a["p01_n_disagree"]), int(a["p01_n_both_defined"]))),
                "p01_n_both_defined": int(a["p01_n_both_defined"]), "p01_n_disagree": int(a["p01_n_disagree"]),
                "p13_q95_abs_acc_mediane_mois": t1.fmt(med_or_na(daily["q95_acc"], (ticker, mois))),
                "p13_n_lit_trf": int(a["p13_n_lit_trf"]), "p13_vact_lit_trf": int(a["p13_vact_lit_trf"]),
            })

    # Comparaison aux sorties de l'exécution normale. Le taux de désordre P-14 dépend
    # de l'ordre physique du fichier : il est exclu de la comparaison.
    report = compare_csv_dirs(out_dir, orig_dir,
                               ["p02_p03_p04_p14_titre_jour.csv", "p02_p03_p04_p14_titre_mois.csv",
                                "p01_p13_titre_jour.csv", "p01_p13_titre_mois.csv"],
                               exempt_cols={"p02_p03_p04_p14_titre_jour.csv": {"p14_taux_desordre_fichier"}})
    (SORTIES_REJEU / "report_t1.json").write_text(json.dumps(report, indent=2, default=str))
    log(stage="t1", step="done", report_summary={k: v["verdict"] for k, v in report.items()})
    return report


def compare_csv_dirs(dir_a: Path, dir_b: Path, filenames: list[str], exempt_cols: dict[str, set] | None = None):
    """Compare deux répertoires de CSV champ par champ, sur le texte écrit.

    `exempt_cols` donne, par fichier, les colonnes dont les écarts sont
    recensés à part sans entrer dans le verdict.
    """
    exempt_cols = exempt_cols or {}
    out = {}
    for fn in filenames:
        pa, pb = dir_a / fn, dir_b / fn
        exempt = exempt_cols.get(fn, set())
        if not pa.exists() or not pb.exists():
            out[fn] = {"verdict": "ERREUR", "detail": f"fichier manquant : a={pa.exists()} b={pb.exists()}"}
            continue
        def row_key(r):
            return (r["ticker"], r["date"]) if "date" in r else (r["ticker"], r["mois"])

        with pa.open(newline="", encoding="utf-8") as fh:
            rows_a = {row_key(r): r for r in csv.DictReader(fh)}
        with pb.open(newline="", encoding="utf-8") as fh:
            rows_b = {row_key(r): r for r in csv.DictReader(fh)}
        diffs = []
        exempt_diffs = []
        if set(rows_a) != set(rows_b):
            out[fn] = {"verdict": "ECART", "detail": "clefs de lignes different",
                       "only_a": sorted(map(str, set(rows_a) - set(rows_b)))[:20],
                       "only_b": sorted(map(str, set(rows_b) - set(rows_a)))[:20]}
            continue
        for key in rows_a:
            ra, rb = rows_a[key], rows_b[key]
            for col in ra:
                va, vb = ra.get(col), rb.get(col)
                if va != vb:
                    if col in exempt:
                        exempt_diffs.append({"key": key, "col": col, "a": va, "b": vb})
                    else:
                        diffs.append({"key": key, "col": col, "a": va, "b": vb})
        verdict = "IDENTIQUE" if not diffs else "ECART"
        out[fn] = {"verdict": verdict, "n_rows": len(rows_a), "n_diffs": len(diffs),
                   "diffs_sample": diffs[:20],
                   "n_exempt_diffs": len(exempt_diffs), "exempt_diffs_sample": exempt_diffs[:5]}
    return out


# =============================================================================
# Stage T1b (primitives_t1b.py) — P-05 / P-10a / P-15
# =============================================================================

def stage_t1b():
    import primitives_t1 as t1
    import primitives_t1b as t1b

    out_dir = SORTIES_REJEU / "t1b"
    out_dir.mkdir(exist_ok=True)
    orig_dir = chemins.SORTIES / "t1"

    tables = t1.load_tables()
    t1b.verify_iso_condition(tables["p2"])
    t1b.verify_nasdaq_id(tables["p1"])
    volume_rules = tables["volume_rules"]
    by_permno_edgar = t1b.load_so_edgar()

    ticker_days = t1.list_ticker_days()
    ticker_days_shuffled = shuffled(ticker_days, "t1b", "ticker_days")
    log(stage="t1b", step="list_ticker_days", n=len(ticker_days))

    scans_by_tm = defaultdict(list)
    t0 = time.time()
    for i, (ticker, date, path) in enumerate(ticker_days_shuffled):
        trades_raw = shuffled(t1.load_trades(path), "t1b", ticker, date)
        scan = t1b.scan_ticker_day(trades_raw, volume_rules)
        scans_by_tm[(ticker, date[:7])].append((date, scan))
        if (i + 1) % 200 == 0:
            log(stage="t1b", step="processing", i=i + 1, n=len(ticker_days_shuffled),
                elapsed=round(time.time() - t0, 1))
    log(stage="t1b", step="processed_all", elapsed=round(time.time() - t0, 1))

    # P-05
    ticker_info_by_tm = {}
    for (ticker, mois) in scans_by_tm:
        ticker_info_by_tm[(ticker, mois)] = t1b.resolve_so_source(ticker, mois)

    regimes_by_ticker = defaultdict(set)
    for (ticker, mois), info in ticker_info_by_tm.items():
        if info["so_dating"] is not None:
            regimes_by_ticker[ticker].add(info["so_dating"])
    tickers_with_switch = {t for t, regs in regimes_by_ticker.items() if len(regs) > 1}

    p05_rows_jour = []
    p05_titre_jour_by_key = {}
    for (ticker, mois), entries in sorted(scans_by_tm.items()):
        entries.sort(key=lambda e: e[0])
        info = ticker_info_by_tm[(ticker, mois)]
        first_month_for_ticker = min(m for (tk, m) in scans_by_tm if tk == ticker)
        for date, scan in entries:
            so = t1b.compute_so(info, by_permno_edgar, date)
            rot = t1b.compute_rot(scan["dvact"], so)
            regime_switch = bool(ticker in tickers_with_switch and mois != first_month_for_ticker
                                  and date == entries[0][0])
            row = {
                "ticker": ticker, "date": date, "mois": mois,
                "dvact_incl810": scan["dvact"],
                "so_source": info["so_source"], "so_dating": info["so_dating"],
                "so_value": t1.fmt(so), "rot": t1.fmt(rot),
                "so_regime_switch": regime_switch,
                "ca_so_desync": False,
                "ca_so_desync_note": "sans_objet_echantillon_sans_CA",
            }
            p05_rows_jour.append(row)
            p05_titre_jour_by_key[(ticker, date)] = row

    p05_jour_path = out_dir / "p05_titre_jour.csv"
    with p05_jour_path.open("w", newline="", encoding="utf-8") as f:
        fields = ["ticker", "date", "mois", "dvact_incl810", "so_source", "so_dating",
                   "so_value", "rot", "so_regime_switch", "ca_so_desync", "ca_so_desync_note"]
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for row in p05_rows_jour:
            w.writerow(row)

    # P-10a
    p10a_rows_jour = []
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

    p10a_jour_path = out_dir / "p10a_titre_jour.csv"
    with p10a_jour_path.open("w", newline="", encoding="utf-8") as f:
        fields = ["ticker", "date", "mois", "n_admissible", "vol_admissible",
                   "n_iso", "vol_iso", "part_n_iso", "part_vol_iso"]
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for row in p10a_rows_jour:
            w.writerow(row)

    # P-15
    itch = t1b.load_sorties_t3()
    strate_stats = t1b.compute_strate_stats(itch)
    h_max = max(v["HR_vol"] for v in itch.values())

    monthly_agg = {}
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
    for (ticker, mois), agg in sorted(monthly_agg.items()):
        itch_entry = itch.get((ticker, mois))
        if itch_entry is not None:
            itch_entry = dict(itch_entry)
            itch_entry["_vol_sip"] = agg["vol_all"]
        info = ticker_info_by_tm[(ticker, mois)]
        tranche_prix = itch_entry["strate_prix"] if itch_entry is not None else info.get("tranche_prix")
        row = t1b.compute_p15_row(ticker, mois, agg["trf_part"], agg["s_nasdaq"], itch_entry,
                                   strate_stats, tranche_prix, h_max)
        p15_rows.append(row)

    p15_path = out_dir / "p15_titre_mois.csv"
    with p15_path.open("w", newline="", encoding="utf-8") as f:
        fields = ["ticker", "mois", "trf_part", "s_nasdaq", "h_hat", "tranche_prix",
                   "B_extrapolee", "B_basse", "B_haute", "B_q25", "B_q75", "n_strate", "hidden_source"]
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for row in p15_rows:
            w.writerow(row)

    # Fichier composite titre-mois
    p15_by_tm = {(r["ticker"], r["mois"]): r for r in p15_rows}
    p05_by_tm = {}
    for (ticker, mois), entries in scans_by_tm.items():
        rots = [p05_titre_jour_by_key[(ticker, d)]["rot"] for d, _ in entries]
        rots_num = [r for r in rots if not (isinstance(r, str) and r.startswith("NA"))]
        info = ticker_info_by_tm[(ticker, mois)]
        p05_by_tm[(ticker, mois)] = {
            "so_source": info["so_source"], "so_dating": info["so_dating"],
            "dvact_mois": monthly_agg[(ticker, mois)]["dvact_mois"],
            "n_jours_rot_ok": len(rots_num), "n_jours_total": len(entries),
        }
    p10a_by_tm = defaultdict(lambda: {"n_admissible": 0, "vol_admissible": 0, "n_iso": 0, "vol_iso": 0})
    for row in p10a_rows_jour:
        k = (row["ticker"], row["mois"])
        p10a_by_tm[k]["n_admissible"] += row["n_admissible"]
        p10a_by_tm[k]["vol_admissible"] += row["vol_admissible"]
        p10a_by_tm[k]["n_iso"] += row["n_iso"]
        p10a_by_tm[k]["vol_iso"] += row["vol_iso"]

    composite_path = out_dir / "p05_p15_p10a_titre_mois.csv"
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

    report = compare_csv_dirs(out_dir, orig_dir,
                               ["p05_titre_jour.csv", "p10a_titre_jour.csv",
                                "p15_titre_mois.csv", "p05_p15_p10a_titre_mois.csv"])
    (SORTIES_REJEU / "report_t1b.json").write_text(json.dumps(report, indent=2, default=str))
    log(stage="t1b", step="done", report_summary={k: v["verdict"] for k, v in report.items()})
    return report


# =============================================================================
# Stage P-12 (primitives_p11_p12.py, volet P-12 seul ; P-11 a été abandonnée)
# =============================================================================

def compare_json_nested(a, b, path=""):
    diffs = []
    if isinstance(a, dict) and isinstance(b, dict):
        for k in sorted(set(a) | set(b)):
            if k not in a:
                diffs.append({"path": f"{path}.{k}", "only_in": "b"})
            elif k not in b:
                diffs.append({"path": f"{path}.{k}", "only_in": "a"})
            else:
                diffs.extend(compare_json_nested(a[k], b[k], f"{path}.{k}"))
    elif isinstance(a, list) and isinstance(b, list):
        if len(a) != len(b):
            diffs.append({"path": path, "a_len": len(a), "b_len": len(b)})
        else:
            for i, (x, y) in enumerate(zip(a, b)):
                diffs.extend(compare_json_nested(x, y, f"{path}[{i}]"))
    else:
        if a != b:
            diffs.append({"path": path, "a": a, "b": b})
    return diffs


def stage_p12():
    import primitives_p11_p12 as pp

    out_dir = SORTIES_REJEU / "p12"
    out_dir.mkdir(exist_ok=True)
    orig_dir = chemins.SORTIES / "p11-p12"

    tables = pp.load_tables()
    vol = tables["rules"]["updates_volume"]
    cand = "updates_high_low"  # valeur par défaut de --cand ; sans effet sur P-12
    rules = tables["rules"][cand]
    sample = pp.load_sample()
    # Les ticker-mois sont traités dans un ordre mélangé : ils ne partagent que des
    # caches en lecture seule (halts, shorthalts). Les jours d'un même ticker-mois
    # restent en ordre chronologique, car le module s'appuie sur cet ordre (la
    # condition `idx_f > 0` repère le premier jour du mois, sans clôture veille).
    sample_shuffled = shuffled(sample, "p12", "sample_order")

    reports = {}
    t0 = time.time()
    for i, s in enumerate(sample_shuffled):
        pid = pp.PRIMARY_EXCHANGE_ID[s["exchange"]]
        ssr_files = pp.ssr_intervals_from_files(s["ticker"]) if s["exchange"] == "Q" else []
        itch_ssr = pp.itch_ssr_intervals(s["ticker"], s["mois"]) if s["exchange"] == "Q" else []
        prev_close_cache = {}
        files = pp.day_files(s["ticker"], s["mois"])
        out_jours = {}
        for idx_f, f in enumerate(files):
            date_str = f.name[:10]
            trades_raw = shuffled(pp.load_day(f), "p12", s["ticker"], date_str)
            day = pp.prepare_day(trades_raw, date_str, pid, vol)
            res = pp.run_day_p11(day, date_str, pid, rules)  # seul last_price est utilisé
            prev = pp.prev_trading_day(date_str)
            pc, pc_src, pc_last = None, "hors_perimetre_donnees", None
            if prev and idx_f > 0:
                pday = prev_close_cache.get(prev)
                if pday is None:
                    pf = pp.DATA_T1 / s["ticker"] / f"{prev}.json.gz"
                    if pf.exists():
                        prev_trades = shuffled(pp.load_day(pf), "p12", s["ticker"], prev)
                        pday = pp.prepare_day(prev_trades, prev, pid, vol)
                        prev_close_cache[prev] = pday
                if pday is not None:
                    pc, pc_src = pp.official_close(pday, prev, pid)
                    pc_last = pp.last_rth_admissible(pday, prev)
            trig, low, thr, t_first = pp.ssr_recalc_day(day, date_str, pc)
            trig_last, *_ = pp.ssr_recalc_day(day, date_str, pc_last)
            trig_lit, low_lit, _, _ = pp.ssr_recalc_day(day, date_str, pc, include_trf=False)
            agg_files = pp.ssr_aggregate(ssr_files, date_str) if s["exchange"] == "Q" else pp.NA("source_non_nasdaq")
            halts = pp.p12a_day(s["ticker"], date_str)
            fpp, arb = [], []
            for iv in halts["intervalles"]:
                nxt = [r for r in day if r["ts"] >= iv["t_resume"] and r["lit_adm"]]
                if nxt:
                    sess, bi = pp.bucket_of(nxt[0]["ts"], date_str)
                    fpp.append([sess, bi])
            for r in day:
                if r["exchange"] == pid and set(r["cond"]) & pp.REOPENING_CODES:
                    sess, bi = pp.bucket_of(r["ts"], date_str)
                    arb.append([sess, bi])
            halts["first_print_post_halt_bucket"] = fpp if halts["couverture"] == "OK" else pp.NA("source")
            halts["auction_reopen_buckets"] = arb
            nb_sess = Counter(pp.session_of(r["ts"], date_str) for r in day)
            last_p = res["last_price"]

            jour = {
                "p12a": {"couverture": halts["couverture"], "na_code": halts["na_code"],
                         "n_intervalles": len(halts["intervalles"]),
                         "intervalles": [{"t_halt": iv["t_halt"], "t_resume": iv["t_resume"],
                                          "reason": iv["reason"]} for iv in halts["intervalles"]],
                         "buckets_halted": [[a, b] for a, b in halts["buckets_halted"]],
                         "first_print_post_halt_bucket": pp.jsonable(halts["first_print_post_halt_bucket"]),
                         "auction_reopen_buckets": arb},
                "p12b": {
                    "source_primaire": "shorthalts" if s["exchange"] == "Q" else "recalcul_rule201",
                    "ssr_fichiers": pp.jsonable(agg_files),
                    "ssr_recalcul": pp.jsonable(trig),
                    "ssr_recalcul_variante_dernier_print": pp.jsonable(trig_last),
                    "ssr_recalcul_variante_lit_seul": pp.jsonable(trig_lit),
                    "low_rth_lit_seul": low_lit,
                    "low_rth": low, "seuil": thr,
                    "prev_close": pc, "prev_close_source": pc_src,
                    "prev_close_dernier_print": pc_last,
                    "ca_suspect": bool(pc and last_p and not (1 / 3 < last_p / pc < 3)),
                    "t_trigger": pp.jsonable(pp.NA("intraday_indispo")),
                    "t_premiere_infraction_diag": t_first,
                    "ssr_itch_diag": None,
                },
                "p12c": {"n_par_session": dict(nb_sess),
                         "n_buckets_rth": pp.n_rth_buckets(date_str),
                         "cloture": pp.close_time(date_str).strftime("%H:%M")},
                "tranche_prix": pp.tranche_of(last_p),
                "dernier_prix_lit": last_p,
            }
            if itch_ssr:
                a, b = pp.et_ns(date_str, pp.RTH[0]), pp.et_ns(date_str, pp.close_time(date_str))
                tr = [(t, v) for t, v in itch_ssr if a <= t < b]
                before = [v for t, v in itch_ssr if t < a]
                st0 = before[-1] if before else None
                any_true = (st0 is True) or any(v for _, v in tr)
                all_true = (st0 is True) and all(v for _, v in tr)
                jour["p12b"]["ssr_itch_diag"] = "1" if all_true else ("partiel" if any_true else "0")
            out_jours[date_str] = jour

        key = f"{s['ticker']}_{s['mois']}"
        (out_dir / f"p11_p12_{key}.json").write_text(
            json.dumps({"ticker": s["ticker"], "mois": s["mois"], "jours": out_jours},
                       ensure_ascii=False))

        orig_path = orig_dir / f"p11_p12_{key}.json"
        if orig_path.exists():
            orig = json.loads(orig_path.read_text())
            orig_jours_no_p11 = {d: {kk: vv for kk, vv in j.items() if kk != "p11"}
                                  for d, j in orig["jours"].items()}
            diffs = compare_json_nested(out_jours, orig_jours_no_p11)
            reports[key] = {"verdict": "IDENTIQUE" if not diffs else "ECART",
                            "n_diffs": len(diffs), "diffs_sample": diffs[:15]}
        else:
            reports[key] = {"verdict": "ERREUR", "detail": "pas de sortie de reference"}

        if (i + 1) % 10 == 0:
            log(stage="p12", step="processing", i=i + 1, n=len(sample_shuffled),
                elapsed=round(time.time() - t0, 1))

    verdict_global = "IDENTIQUE" if all(r["verdict"] == "IDENTIQUE" for r in reports.values()) else "ECART"
    summary = {"verdict_global": verdict_global, "n_ticker_mois": len(reports), "par_ticker_mois": reports}
    (SORTIES_REJEU / "report_p12.json").write_text(json.dumps(summary, indent=2, default=str))
    log(stage="p12", step="done", verdict_global=verdict_global,
        n_ecarts=sum(1 for r in reports.values() if r["verdict"] == "ECART"))
    return summary


# =============================================================================
# Stage T3 (primitives_t3.py, ITCH MBO) — P-06 / P-07 / P-08 / P-10b
# =============================================================================

# derive() (primitives_t3.py) regroupe les délais de tous les ticker-mois pour
# estimer le δ de diagnostic « croisement au nul homogène ». Ce δ et les comptes de
# chaînes calculés à ce δ dépendent donc de l'ensemble des ticker-mois traités : avec
# --limit N, ils diffèrent de la référence (31,62 s sur les 29 ticker-mois, 100 s sur
# 3) sans que cela traduise un défaut de reproductibilité. Ces chemins JSON ne sont
# comparés que sur le pool complet. Les grandeurs au δ fixe de 100 µs n'en dépendent pas.
CHEMINS_DERIVES_DU_POOL = (
    ".p06.delta_croisement_nul_homogene_ns",
    ".p06_multi_delta.grille_ns.31,62 s (croisement nul homogène — diagnostic)",
    ".p06_multi_delta.par_delta.31,62 s (croisement nul homogène — diagnostic)",
)


def _derive_du_pool(path: str) -> bool:
    return any(path == p or path.startswith(p + ".") or path.startswith(p + "[")
               for p in CHEMINS_DERIVES_DU_POOL)


def stage_t3(limit=None):
    import primitives_t3 as t3

    out_dir = SORTIES_REJEU / "t3"
    out_dir.mkdir(exist_ok=True)
    orig_dir = chemins.SORTIES / "t3"

    strates = t3.load_strates()
    tms_complet = t3.tickers_itch(strates)
    tms = tms_complet[:limit] if limit else tms_complet
    pool_complet = (len(tms) == len(tms_complet))
    # Chaque ticker-mois a son propre Pipeline (carnet d'ordres) : l'ordre de
    # traitement est mélangé. Un découpage plus fin que le mois n'est pas possible,
    # la fenêtre glissante de P-08 (20 séances) vivant dans un seul Pipeline.
    tms_shuffled = shuffled(tms, "t3", "tm_order")
    log(stage="t3", step="tickers_mois", n=len(tms))

    per_tm_events = {}
    per_tm_res = {}
    physical_order_preserved = {}
    tie_stats = {}
    t0 = time.time()
    for i, tm in enumerate(tms_shuffled):
        meta = strates.get(tm, {"ticker": tm.split("_")[0], "mois": tm.split("_")[1]})
        path = t3.DATA / f"{tm}_mbo.dbn.zst"
        records = list(t3.stream_records(path))

        # Dans le schéma `mbo`, la clé de tri (ts_event, sequence) n'est pas un ordre
        # total : un message ITCH (par exemple une exécution contre un ordre affiché)
        # donne plusieurs enregistrements DBN de même clé, et aucun champ ne permet de
        # retrouver leur ordre relatif après mélange. Deux variantes sont donc rejouées.
        # (i) « groupes » : les enregistrements sont regroupés par clé en conservant
        # leur ordre interne, puis seul l'ordre des groupes est mélangé avant le tri ;
        # c'est le test de permutation proprement dit. (ii) « brut » : mélange
        # enregistrement par enregistrement puis tri sur la clé, ce qui mesure la
        # sensibilité aux égalités de clé (diagnostic).
        groups = defaultdict(list)
        for r in records:
            groups[(r.ts_event, r.sequence)].append(r)
        group_keys = list(groups.keys())
        n_tied_records = sum(len(v) for v in groups.values() if len(v) > 1)
        n_tied_groups = sum(1 for v in groups.values() if len(v) > 1)
        tie_stats[tm] = {"n_records": len(records), "n_groupes": len(group_keys),
                          "n_groupes_lies": n_tied_groups, "n_enreg_dans_groupe_lie": n_tied_records,
                          "part_enreg_dans_groupe_lie": n_tied_records / len(records) if records else 0.0}

        group_keys_shuffled = shuffled(group_keys, "t3", tm, "groups")
        group_keys_canon = sorted(group_keys_shuffled)
        recs_canon = [r for k in group_keys_canon for r in groups[k]]  # (i) "groupes"

        recs_shuffled_brut = shuffled(records, "t3", tm, "brut")
        recs_canon_brut = sorted(recs_shuffled_brut, key=lambda r: (r.ts_event, r.sequence))  # (ii) "brut"

        phys_keys = [(r.ts_event, r.sequence, id(r)) for r in records]
        canon_keys_i = [(r.ts_event, r.sequence, id(r)) for r in recs_canon]
        physical_order_preserved[tm] = (phys_keys == canon_keys_i)

        sink = []
        pl = t3.Pipeline(tm, sink=sink)
        for rec in recs_canon:
            pl.feed(rec)
        pl.finish()

        h16 = t3.run_h16(tm, meta, records=recs_canon)
        t7 = h16["totaux"]
        n_seances = max(1, h16["n_jours"])
        crosses = (t7.get("n_cross_open", 0) + t7.get("n_cross_close", 0) + t7.get("n_cross_halt", 0))
        vol_F, vol_hid = t7.get("vol_F", 0), t7.get("vol_cache", 0)
        p07 = {
            "HR_vol": h16["ratio_cache_vol"], "HR_vol_rth": h16["ratio_cache_vol_rth"],
            "HR_nb": h16["ratio_cache_nb"],
            "HR_vol_variante_Vdisp_F": (vol_hid / (vol_hid + vol_F)) if (vol_hid + vol_F) else None,
            "n_jours": h16["n_jours"], "crosses_imputes": crosses,
            "crosses_par_seance_moyen": crosses / n_seances,
            "n_cross_open": t7.get("n_cross_open", 0), "n_cross_close": t7.get("n_cross_close", 0),
            "n_cross_halt": t7.get("n_cross_halt", 0),
            "vol_cache": vol_hid, "vol_affiche": t7.get("vol_affiche", 0), "vol_F": vol_F,
            "anomalie_T_AB_non_apparie": t7.get("anomalie_T_AB_non_apparie", 0),
            "n_T_N_non_apparie_oid_non_nul": t7.get("n_T_N_non_apparie_oid_non_nul", 0),
        }
        res = {
            "p07": p07, "p08": pl.p08(), "p10b": pl.p10b(),
            "p06_capture": {
                "n_epuisements_exec": pl.cnt["n_epuisements_exec"],
                "n_epuisements_exec_vidage": pl.cnt["n_epuisements_exec_vidage"],
                "n_retraits_annulation": pl.cnt["n_retraits_annulation"],
                "vol_exec_par_jour": dict(pl.vol_exec_jour),
            },
            "compteurs_carnet": dict(pl.cnt), "checksums_carnet": pl.checksums,
        }
        events = [t3._to_evt(row) for row in sink]
        per_tm_events[tm] = events
        per_tm_res[tm] = res

        # Variante (ii) : on ne compare que le carnet (sommes de contrôle de fin de
        # jour) et les événements P-06, sans refaire tout le calcul.
        sink_brut = []
        pl_brut = t3.Pipeline(tm, sink=sink_brut)
        for rec in recs_canon_brut:
            pl_brut.feed(rec)
        pl_brut.finish()
        events_brut = [t3._to_evt(row) for row in sink_brut]
        tie_stats[tm]["checksums_carnet_brut_identiques_a_groupes"] = (pl_brut.checksums == pl.checksums)
        tie_stats[tm]["evenements_p06_brut_identiques_a_groupes"] = (events_brut == events)
        tie_stats[tm]["n_events_brut"] = len(sink_brut)
        tie_stats[tm]["n_events_groupes"] = len(sink)
        if events_brut != events:
            c_brut = t3.comptes_p06(events_brut, t3.DELTA_MAX_NS, {})
            c_grp = t3.comptes_p06(events, t3.DELTA_MAX_NS, {})
            tie_stats[tm]["exemple_ecart_brut"] = {
                "chaines_libre.volume_execute_en_chaine_brut": c_brut["chaines_libre"]["volume_execute_en_chaine"],
                "chaines_libre.volume_execute_en_chaine_groupes": c_grp["chaines_libre"]["volume_execute_en_chaine"],
            }

        if (i + 1) % 5 == 0 or (i + 1) == len(tms_shuffled):
            log(stage="t3", step="processing", i=i + 1, n=len(tms_shuffled),
                elapsed=round(time.time() - t0, 1))

    log(stage="t3", step="pipeline_done", elapsed=round(time.time() - t0, 1),
        n_ordre_physique_deja_canonique=sum(physical_order_preserved.values()),
        n_total=len(physical_order_preserved))

    # Reproduction de derive() : regroupement des délais sur tous les ticker-mois traités.
    delais = array("q")
    delais_non_vide = array("q")
    lam_hist = Counter()
    n_zero = n_censure = n_total = n_nv_total = 0
    for tm in tms:
        for e in per_tm_events[tm]:
            if not e[t3.VIDE]:
                n_nv_total += 1
                if e[t3.DELAI] > 0:
                    delais_non_vide.append(e[t3.DELAI])
                continue
            n_total += 1
            d = e[t3.DELAI]
            if d < 0:
                n_censure += 1
                continue
            if d == 0:
                n_zero += 1
                continue
            delais.append(d)
            if e[t3.LAMJ] > 0:
                lam_hist[math.floor(math.log10(e[t3.LAMJ]) * t3.LAMBDA_BINS_PAR_DECADE)] += 1

    # Comme dans derive(), le δ_max principal est la constante t3.DELTA_MAX_NS
    # (100 µs) ; le δ issu de la procédure de croisement au nul homogène (dmax_proc)
    # n'est publié qu'à titre de diagnostic.
    d_princ = t3.derive_delta_max(delais, lam_hist, t3.BINS_PAR_DECADE)
    dmax_proc = d_princ["delta_max_ns"]
    dmax = t3.DELTA_MAX_NS
    deltas = [d for d in ((dmax // 10 if dmax else None), dmax, (dmax * 10 if dmax else None)) if d]
    grille = list(t3.DELTAS_PROVISOIRES) + (
        [(dmax_proc, "31,62 s (croisement nul homogène — diagnostic)")] if dmax_proc else [])

    ancien_path = t3.PROG / "ancien_detecteur.json"
    ancien = json.loads(ancien_path.read_text()) if ancien_path.exists() else {}

    reports = {}
    for tm in tms:
        evts = per_tm_events[tm]
        vej = per_tm_res[tm]["p06_capture"]["vol_exec_par_jour"]
        p06_multi_delta = {
            "statut": t3.STATUT_DELTA,
            "grille_ns": {lab: d for d, lab in grille},
            "par_delta": {lab: t3.comptes_p06(evts, d, vej) for d, lab in grille},
        }
        p06 = {
            "delta_max_ns": dmax,
            "convention": "100 µs descriptif, variante principale strict_size",
            "delta_croisement_nul_homogene_ns": dmax_proc,
            "sensibilite": {str(d): t3.comptes_p06(evts, d, vej) for d in deltas},
            "principal": t3.comptes_p06(evts, dmax, vej) if dmax else None,
            "bridge_2s": t3.comptes_bridge(evts, t3.DELTA_ANCIEN_NS),
            "ancien_detecteur_A": ancien.get(tm),
            "quantiles_delais_ns": t3.quantiles([e[t3.DELAI] for e in evts if e[t3.VIDE] and e[t3.DELAI] >= 0]),
        }
        my_res = dict(per_tm_res[tm])
        my_res["p06"] = p06
        my_res["p06_multi_delta"] = p06_multi_delta
        my_path = out_dir / f"{tm}.json"
        my_path.write_text(json.dumps(my_res, indent=1, default=str))
        # On compare le JSON relu, qui a subi les mêmes conversions que la référence
        # (clés entières devenues des chaînes, par exemple).
        my_res_json = json.loads(my_path.read_text())

        orig_path = orig_dir / f"{tm}.json"
        if not orig_path.exists():
            reports[tm] = {"verdict": "ERREUR", "detail": "pas de sortie de reference"}
            continue
        orig = json.loads(orig_path.read_text())
        orig_slim = {k: orig.get(k) for k in
                     ("p07", "p08", "p10b", "p06_capture", "compteurs_carnet", "checksums_carnet",
                      "p06", "p06_multi_delta")}
        toutes_diffs = compare_json_nested(my_res_json, orig_slim)
        if pool_complet:
            diffs, hors_perimetre = toutes_diffs, []
        else:
            diffs = [d for d in toutes_diffs if not _derive_du_pool(d["path"])]
            hors_perimetre = [d for d in toutes_diffs if _derive_du_pool(d["path"])]
        reports[tm] = {"verdict": "IDENTIQUE" if not diffs else "ECART",
                       "ordre_physique_deja_canonique": physical_order_preserved[tm],
                       "n_diffs": len(diffs), "diffs_sample": diffs[:20],
                       "n_diffs_hors_perimetre_pool": len(hors_perimetre),
                       "diffs_hors_perimetre_pool_sample": hors_perimetre[:5]}

    verdict_global = "IDENTIQUE" if all(r["verdict"] == "IDENTIQUE" for r in reports.values()) else "ECART"
    n_brut_identique = sum(1 for v in tie_stats.values() if v.get("checksums_carnet_brut_identiques_a_groupes"))
    n_brut_evts_identique = sum(1 for v in tie_stats.values() if v.get("evenements_p06_brut_identiques_a_groupes"))
    summary = {"verdict_global": verdict_global, "delta_max_ns_rejeu": dmax,
               "n_ticker_mois": len(reports),
               "perimetre_pool": {
                   "n_ticker_mois_rejoues": len(tms),
                   "n_ticker_mois_committes": len(tms_complet),
                   "pool_complet": pool_complet,
                   "delta_croisement_nul_homogene_ns_rejeu": dmax_proc,
                   "note": ("le δ de croisement au nul homogène et les comptes calculés à ce δ "
                            "dépendent de l'ensemble des ticker-mois traités ; ils ne sont comparés "
                            "que sur le pool complet, et sinon publiés en `*_hors_perimetre_pool` "
                            "hors verdict."),
               },
               "par_ticker_mois": reports,
               "constat_ordre_canonique_sous_specifie": {
                   "description": ("la clé (ts_event, sequence) n'est pas un ordre total : un message "
                                    "ITCH peut donner plusieurs enregistrements DBN de même clé. "
                                    "Variante 'groupes' (i) : permutation de l'ordre des groupes de "
                                    "même clé ; variante 'brut' (ii) : permutation enregistrement par "
                                    "enregistrement, diagnostic de sensibilité aux égalités de clé."),
                   "n_ticker_mois_brut_checksums_carnet_identiques_a_groupes": n_brut_identique,
                   "n_ticker_mois_brut_evenements_p06_identiques_a_groupes": n_brut_evts_identique,
                   "n_ticker_mois_total": len(tie_stats),
                   "par_ticker_mois": tie_stats,
               }}
    (SORTIES_REJEU / "report_t3.json").write_text(json.dumps(summary, indent=2, default=str))
    log(stage="t3", step="done", verdict_global=verdict_global,
        n_ecarts=sum(1 for r in reports.values() if r["verdict"] == "ECART"),
        elapsed=round(time.time() - t0, 1))
    return summary


# =============================================================================
# Stage T3q (primitives_t3q.py, BBO mono-bourse) — P-01 (LR_venue) / P-09
# =============================================================================

def stage_t3q(limit=None):
    import primitives_t3q as t3q

    out_dir = SORTIES_REJEU / "t3q"
    out_dir.mkdir(exist_ok=True)
    orig_dir = chemins.SORTIES / "t3q"

    strates = t3q.load_strates()
    tms = t3q.tickers_mois()
    if limit:
        tms = tms[:limit]
    # Comme pour T3 : un objet T3q par ticker-mois, ordre de traitement mélangé ; la
    # fenêtre glissante de P-09 vit dans un seul ticker-mois.
    tms_shuffled = shuffled(tms, "t3q", "tm_order")
    log(stage="t3q", step="tickers_mois", n=len(tms))

    tie_stats = {}
    reports = {}
    t0 = time.time()
    for i, tm in enumerate(tms_shuffled):
        meta = strates.get(tm, {"ticker": tm.split("_")[0], "mois": tm.split("_")[1]})
        path = t3q.DATA / f"{tm}_tbbo.dbn.zst"
        records = list(t3q.stream_records(path))

        # Dans le schéma `tbbo`, chaque mise à jour de marché est un enregistrement :
        # (ts_event, sequence) n'a présenté aucune égalité sur les ticker-mois
        # examinés. Le nombre d'enregistrements à clé dupliquée est tout de même
        # compté et publié pour chaque ticker-mois.
        keys = [(r.ts_event, r.sequence) for r in records]
        c = Counter(keys)
        n_dup = sum(v for v in c.values() if v > 1)
        tie_stats[tm] = {"n_records": len(records), "n_cles_dupliquees_enreg": n_dup}

        recs_shuffled = shuffled(records, "t3q", tm)
        recs_canon = sorted(recs_shuffled, key=lambda r: (r.ts_event, r.sequence))

        res, acc = t3q.run_tm(tm, meta, records=recs_canon)
        my_path = out_dir / f"{tm}.json"
        my_path.write_text(json.dumps(res, indent=1, default=str))
        my_json = json.loads(my_path.read_text())

        orig_path = orig_dir / f"{tm}.json"
        if orig_path.exists():
            orig = json.loads(orig_path.read_text())
            fields = ("p01", "validation_h13", "p09", "n_jours", "jours", "n_decodes",
                      "exclusions", "trades_par_jour_median")
            a = {k: my_json.get(k) for k in fields}
            b = {k: orig.get(k) for k in fields}
            all_diffs = compare_json_nested(a, b)
            # `.exclusions.desordre_fichier` compte les enregistrements reçus hors ordre
            # chronologique ; après le tri il vaut zéro, alors que la référence mesure
            # l'ordre physique du fichier. Comme pour P-14, il est exclu du verdict.
            exempt = [d for d in all_diffs if d.get("path") == ".exclusions.desordre_fichier"]
            diffs = [d for d in all_diffs if d.get("path") != ".exclusions.desordre_fichier"]
            reports[tm] = {"verdict": "IDENTIQUE" if not diffs else "ECART",
                           "n_diffs": len(diffs), "diffs_sample": diffs[:20],
                           "n_exempt_diffs": len(exempt), "exempt_diffs_sample": exempt[:3]}
        else:
            reports[tm] = {"verdict": "ERREUR", "detail": "pas de sortie de reference"}

        if (i + 1) % 5 == 0 or (i + 1) == len(tms_shuffled):
            log(stage="t3q", step="processing", i=i + 1, n=len(tms_shuffled),
                elapsed=round(time.time() - t0, 1))

    verdict_global = "IDENTIQUE" if all(r["verdict"] == "IDENTIQUE" for r in reports.values()) else "ECART"
    summary = {"verdict_global": verdict_global, "n_ticker_mois": len(reports),
               "par_ticker_mois": reports, "tie_stats": tie_stats}
    (SORTIES_REJEU / "report_t3q.json").write_text(json.dumps(summary, indent=2, default=str))
    log(stage="t3q", step="done", verdict_global=verdict_global,
        n_ecarts=sum(1 for r in reports.values() if r["verdict"] == "ECART"),
        elapsed=round(time.time() - t0, 1))
    return summary


# =============================================================================
# CLI
# =============================================================================

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--stage", required=True,
                     choices=["t1", "t1b", "p12", "t3", "t3q"])
    ap.add_argument("--limit", type=int, default=None)
    args = ap.parse_args()

    log(stage=args.stage, step="start")
    if args.stage == "t1":
        stage_t1()
    elif args.stage == "t1b":
        stage_t1b()
    elif args.stage == "p12":
        stage_p12()
    elif args.stage == "t3":
        stage_t3(limit=args.limit)
    elif args.stage == "t3q":
        stage_t3q(limit=args.limit)
    else:
        raise SystemExit(f"stage {args.stage} pas encore implemente dans ce script")


if __name__ == "__main__":
    main()
