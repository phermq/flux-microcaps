#!/usr/bin/env python3
"""Condition d'invalidation de P-16, fixée avant toute mesure, puis vérifiée.

`dp_abs` (somme des variations absolues de log-VWAP entre buckets d'une minute)
est la sortie de P-16 la plus exposée au rebond bid-ask. La condition a été
énoncée avant de regarder les données : on recalcule `dp_abs` sur le mid au
lieu du prix des trades, et si le ratio médian dp_abs(trades) / dp_abs(mid)
dépasse 1,5, `dp_abs` et `dp_efficience` qui en dérive sont considérées comme
dominées par le rebond et doivent être remplacées par une variante corrigée du
spread. Le seuil n'est pas ajusté après coup.

La comparaison est appariée : mêmes trades, mêmes buckets, mêmes poids (la
taille) ; seul le prix change (px devient (bid+ask)/2). Un trade dont le BBO est
inexploitable (absent, verrouillé, croisé) est exclu des deux séries à la fois.
Les données utilisées (TBBO Databento, BBO de la bourse d'exécution) n'ont pas
de condition codes : les prints d'enchère restent dans les deux séries, ce qui
n'affecte pas le ratio mais rend la valeur absolue de `dp_abs` différente de
celle calculée sur la tape SIP.

Usage : python3 invalidation_p16.py [--limit N]
Sortie : sorties/t3q/invalidation_p16.csv, verdict sur stdout.
"""
from __future__ import annotations

import argparse
import csv
import json
import math
import statistics
import sys
from collections import defaultdict
from pathlib import Path

CODE_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import chemins  # noqa: E402

from donnees_mbo import DATA, SessionCache, stream_records          # noqa: E402
from primitives_t3q import bbo_utilisable, BUCKET_RTH_NS          # noqa: E402
from primitives_t1 import _log_ratio, MIN_BUCKETS_P16             # noqa: E402

SEUIL_PRE_ENREGISTRE = 1.5          # fixé avant toute mesure
OUT = chemins.SORTIES / "t3q" / "invalidation_p16.csv"


def dp_abs_depuis_buckets(buckets: dict[int, list[int]]) -> float | None:
    """Σ_b |log(VWAP_b / VWAP_{b⁻})| sur les buckets actifs consécutifs.
    `buckets` : idx -> [Σ prix×taille, Σ taille] en entiers. L'ordre de sommation
    est celui de compute_p16_day, pour un résultat identique au bit près."""
    idxs = sorted(buckets)
    if len(idxs) < 2:
        return None
    total = 0.0
    for prev, cur in zip(idxs, idxs[1:]):
        n_p, s_p = buckets[prev]
        n_c, s_c = buckets[cur]
        total += abs(_log_ratio(n_c, s_c, n_p, s_p))
    return total


def mesurer_ticker_mois(tm: str) -> tuple[list[dict], dict]:
    """Une passe sur le TBBO d'un ticker-mois ; renvoie une ligne par jour et les compteurs d'exclusion."""
    sess = SessionCache()
    jour = None
    rth_t0 = rth_t1 = 0
    par_jour: dict[str, tuple[dict, dict]] = {}
    cnt = defaultdict(int)

    for rec in stream_records(DATA / f"{tm}_tbbo.dbn.zst"):
        ts = rec.ts_event
        if str(rec.action) != "T" or rec.size == 0:
            cnt["exclu_non_trade_ou_taille_nulle"] += 1
            continue
        if sess.update(ts):
            jour = str(sess.date)
            rth_t0, rth_t1 = sess.open_ns, sess.close_ns
        if not (rth_t0 <= ts < rth_t1):
            cnt["hors_rth"] += 1                    # P-16 ne porte que sur la séance régulière
            continue
        lvl = rec.levels[0]
        ok, _motif = bbo_utilisable(lvl.bid_px, lvl.ask_px)
        if not ok:
            cnt["bbo_inexploitable"] += 1           # exclu des deux séries
            continue
        idx = (ts - rth_t0) // BUCKET_RTH_NS
        b_tr, b_mid = par_jour.setdefault(jour, ({}, {}))
        sz = rec.size
        e = b_tr.setdefault(idx, [0, 0])
        e[0] += rec.price * sz                      # entiers DBN (1e-9 $)
        e[1] += sz
        m = b_mid.setdefault(idx, [0, 0])
        m[0] += (lvl.bid_px + lvl.ask_px) * sz      # 2×mid : le facteur 2 disparaît
        m[1] += sz                                  # dans les rapports de VWAP
        cnt["trades_retenus"] += 1

    lignes = []
    for j, (b_tr, b_mid) in sorted(par_jour.items()):
        if len(b_tr) < MIN_BUCKETS_P16:
            cnt["jours_buckets_insuffisants"] += 1
            continue
        a_tr, a_mid = dp_abs_depuis_buckets(b_tr), dp_abs_depuis_buckets(b_mid)
        if not a_mid:                               # None ou 0.0 -> ratio indéfini
            cnt["jours_chemin_mid_nul"] += 1
            continue
        lignes.append({"ticker_mois": tm, "date": j, "n_buckets": len(b_tr),
                       "dp_abs_trades": a_tr, "dp_abs_mid": a_mid,
                       "ratio": a_tr / a_mid})
    return lignes, dict(cnt)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--limit", type=int, default=None)
    args = ap.parse_args()

    tms = sorted({p.name[: -len("_tbbo.dbn.zst")] for p in DATA.glob("*_tbbo.dbn.zst")})
    if args.limit:
        tms = tms[: args.limit]
    toutes, cnt_total = [], defaultdict(int)
    for tm in tms:
        lignes, cnt = mesurer_ticker_mois(tm)
        toutes.extend(lignes)
        for k, v in cnt.items():
            cnt_total[k] += v
        print(f"  {tm:20s} {len(lignes):4d} jours", file=sys.stderr)

    OUT.parent.mkdir(parents=True, exist_ok=True)
    with OUT.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["ticker_mois", "date", "n_buckets",
                                          "dp_abs_trades", "dp_abs_mid", "ratio"])
        w.writeheader()
        w.writerows(toutes)

    ratios = sorted(l["ratio"] for l in toutes)
    if not ratios:
        print("Aucun titre-jour mesurable : verdict impossible", file=sys.stderr)
        sys.exit(2)
    med = statistics.median(ratios)
    qs = {f"q{q}": ratios[min(len(ratios) - 1, int(q / 100 * len(ratios)))]
          for q in (1, 5, 25, 50, 75, 95, 99)}
    verdict = "P-16 CONSERVÉE" if med <= SEUIL_PRE_ENREGISTRE else "dp_abs/dp_efficience INVALIDÉES"
    resume = {"n_titre_jours": len(ratios), "n_ticker_mois": len(tms),
              "ratio_median": med, "quantiles": qs, "seuil": SEUIL_PRE_ENREGISTRE,
              "verdict": verdict, "exclusions": dict(cnt_total)}
    print(json.dumps(resume, indent=2, ensure_ascii=False))
    print(f"\n=> ratio médian {med:.3f} vs seuil {SEUIL_PRE_ENREGISTRE} : {verdict}", file=sys.stderr)


def test_dp_abs_coherent_avec_p16():
    """dp_abs calculé ici coïncide avec celui de compute_p16_day sur une même tape synthétique."""
    from primitives_t1 import (compute_p16_day, load_tables, _p16_tr)
    prix = [10.00, 10.50, 10.20, 10.80, 10.40]
    vr = load_tables()["volume_rules"]
    ref = compute_p16_day([_p16_tr(p, 100, m) for m, p in enumerate(prix)], vr, set())
    buckets = {m: [int(round(p * 10000)) * 100, 100] for m, p in enumerate(prix)}
    assert abs(dp_abs_depuis_buckets(buckets) - ref["dp_abs"]) < 1e-12
    # indéfini avec moins de deux buckets actifs
    assert dp_abs_depuis_buckets({3: [1000, 100]}) is None


if __name__ == "__main__":
    main()
