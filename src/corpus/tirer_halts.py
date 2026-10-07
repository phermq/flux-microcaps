#!/usr/bin/env python3
"""Téléchargement des halts de cotation et construction du déclencheur T-HALT.

Source : API historique des trade halts de NYSE, qui couvre Nasdaq, NYSE, NYSE
American et Arca à partir de 2019-09 ; T-HALT est inactif avant. Les heures sont
supposées en heure de l'Est : le pic de halts tombe à 9 h, à l'ouverture, ce
qu'un horodatage UTC rendrait incohérent. Ce fuseau est déduit, non documenté.

Chaque halt est rattaché au PERMNO par le snapshot d'univers qui régit son mois,
car un même ticker peut désigner des sociétés différentes au fil du temps ; un
ticker porté par plusieurs PERMNO le même mois est rejeté. Un halt est de portée
marché si au moins 50 % des titres de l'univers présents ce jour sont arrêtés
dans la même minute ; il ne déclenche pas T-HALT et sert de covariable.

Sorties : halts_univers.csv, halts_couverture_epoque.csv, halts_rejets.csv.

Usage :  python3 tirer_halts.py [--selftest] [--depuis 2019-09] [--jusqu-a 2025-12]
"""

from __future__ import annotations

import argparse
import csv
import gzip
import io
import subprocess
import sys
import time
from collections import Counter, defaultdict
from pathlib import Path

API = "https://www.nyse.com/api/trade-halts/historical/download"
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import chemins  # noqa: E402
UNIVERS = chemins.SORTIES / "univers"
P04 = chemins.SORTIES / "rejeu" / "p04_p05_univers_titre_jour.csv.gz"
CACHE = chemins.DONNEES / "cache-halts"
SORTIES = chemins.SORTIES / "corpus"

DEBUT_COUVERTURE = "2019-09"      # début de couverture de la source
SEUIL_PORTEE_MARCHE = 0.50
EPOQUES = [("E1", "2018-05", "2019-12"), ("E2", "2020-01", "2021-12"),
           ("E3", "2022-01", "2023-12"), ("E4", "2024-01", "2025-12")]


def epoque(jour: str) -> str | None:
    m = jour[:7]
    for nom, a, b in EPOQUES:
        if a <= m <= b:
            return nom
    return None


def mois_suivants(debut: str, fin: str):
    a, m = int(debut[:4]), int(debut[5:7])
    while f"{a}-{m:02d}" <= fin:
        yield f"{a}-{m:02d}"
        a, m = (a + 1, 1) if m == 12 else (a, m + 1)


def fin_de_mois(mois: str) -> str:
    a, m = int(mois[:4]), int(mois[5:7])
    suiv = f"{a + 1}-01-01" if m == 12 else f"{a}-{m + 1:02d}-01"
    from datetime import date, timedelta
    return (date.fromisoformat(suiv) - timedelta(days=1)).isoformat()


PAUSE_ENTRE_MOIS = 1.5   # s, politesse envers l'API NYSE


def tirer_mois(mois: str, essais: int = 5) -> list[dict]:
    """Halts d'un mois calendaire, mis en cache (gzip)."""
    path = CACHE / f"halts_{mois}.csv.gz"
    if path.exists():
        with gzip.open(path, "rt", encoding="utf-8", newline="") as fh:
            return list(csv.DictReader(fh))
    url = f"{API}?haltDateFrom={mois}-01&haltDateTo={fin_de_mois(mois)}"
    # Appel via curl : l'API renvoie 429 à urllib sur la même URL.
    delai = 10.0
    for i in range(essais):
        r = subprocess.run(["curl", "-sS", "--max-time", "120", "-w", "%{http_code}", url],
                           capture_output=True, text=True)
        if r.returncode == 0 and r.stdout[-3:] == "200":
            brut = r.stdout[:-3]
            break
        if i == essais - 1:
            raise RuntimeError(f"{mois} : échec après {essais} tentatives "
                               f"(code {r.stdout[-3:]!r}, {r.stderr.strip()[:120]})")
        time.sleep(delai)
        delai *= 2
    CACHE.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    with gzip.open(tmp, "wt", encoding="utf-8", newline="") as fh:
        fh.write(brut)
    tmp.replace(path)                       # écriture atomique du cache
    return list(csv.DictReader(io.StringIO(brut)))


def mapping_date(univers: Path) -> dict[str, dict[str, str]]:
    """{mois régi -> {ticker -> permno ou None}} ; le snapshot de fin de mois M
    régit le mois M+1. None marque un ticker porté par plusieurs PERMNO."""
    out: dict[str, dict[str, str]] = {}
    for p in sorted(univers.glob("U_*.csv")):
        d = p.stem[2:]
        a, m = int(d[:4]), int(d[4:6])
        regi = f"{a + 1}-01" if m == 12 else f"{a}-{m + 1:02d}"
        par_t: dict[str, set[str]] = defaultdict(set)
        with open(p) as fh:
            for row in csv.DictReader(fh):
                par_t[row["ticker"]].add(row["permno"])
        # Ticker ambigu (plusieurs PERMNO dans le mois, ex. classes d'actions) :
        # None, et le halt est rejeté avec un compteur dédié.
        out[regi] = {t: (next(iter(ps)) if len(ps) == 1 else None)
                     for t, ps in par_t.items()}
    return out


def titres_par_jour() -> dict[str, set[str]]:
    """{jour -> PERMNO présents dans l'univers ce jour}, d'après la table P-04."""
    out: dict[str, set[str]] = defaultdict(set)
    with gzip.open(P04, "rt", newline="") as fh:
        for row in csv.DictReader(fh):
            out[row["date"]].add(row["permno"])
    return out


def selftest() -> None:
    """Vérifie, hors réseau, le seuil de portée marché et la granularité minute."""
    jour_u = {f"P{i}" for i in range(10)}
    halts = [("09:30:00", f"P{i}") for i in range(5)] + [("09:31:00", "P7")]
    par_minute: dict[str, set[str]] = defaultdict(set)
    for t, p in halts:
        par_minute[t[:5]].add(p)
    marche = {mn for mn, s in par_minute.items()
              if len(s) >= SEUIL_PORTEE_MARCHE * len(jour_u)}
    assert marche == {"09:30"}, f"seuil 50 % mal appliqué : {marche}"
    assert "09:31" not in marche, "un halt isolé ne doit pas être de portée marché"
    par_minute["09:31"].update({f"P{i}" for i in range(1, 5)})
    marche = {mn for mn, s in par_minute.items()
              if len(s) >= SEUIL_PORTEE_MARCHE * len(jour_u)}
    assert marche == {"09:30", "09:31"}, "la minute est l'unité, pas la seconde"
    print("selftest OK (seuil 50 %, granularité minute)")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--depuis", default=DEBUT_COUVERTURE)
    ap.add_argument("--jusqu-a", default="2025-12")
    ap.add_argument("--selftest", action="store_true")
    args = ap.parse_args()
    if args.selftest:
        return selftest()
    selftest()

    mapping = mapping_date(UNIVERS)
    jour_u = titres_par_jour()
    print(f"{len(mapping)} mois de mapping ticker→PERMNO, "
          f"{len(jour_u)} jours d'univers\n")

    brut: list[dict] = []
    for mois in mois_suivants(args.depuis, args.jusqu_a):
        deja = (CACHE / f"halts_{mois}.csv.gz").exists()
        rs = tirer_mois(mois)
        brut += rs
        print(f"  {mois} : {len(rs):>5d} halts{'' if deja else '  (tiré)'}", flush=True)
        if not deja:
            time.sleep(PAUSE_ENTRE_MOIS)
    print(f"\n{len(brut)} halts bruts tirés")

    # Rattachement daté et portée marché
    par_minute: dict[tuple[str, str], set[str]] = defaultdict(set)
    rattaches: list[dict] = []
    perdus = Counter()
    for h in brut:
        jour, heure, sym = h["Halt Date"], h["Halt Time"], h["Symbol"]
        if not jour or not heure or not sym:
            perdus["champ_vide"] += 1
            continue
        mois_map = mapping.get(jour[:7], {})
        if sym in mois_map and mois_map[sym] is None:
            perdus["ticker_ambigu_plusieurs_permno"] += 1
            continue
        permno = mois_map.get(sym)
        if permno is None:
            perdus["ticker_hors_univers"] += 1
            continue
        if permno not in jour_u.get(jour, ()):
            perdus["titre_absent_de_U_ce_jour"] += 1
            continue
        par_minute[(jour, heure[:5])].add(permno)
        rattaches.append({"jour": jour, "heure": heure, "minute": heure[:5],
                          "permno": permno, "ticker": sym,
                          "exchange": h.get("Exchange", ""),
                          "raison": h.get("Reason", ""),
                          "reprise_jour": h.get("Resume Date", ""),
                          "reprise_heure": h.get("NYSE Resume Time", "")})

    # La source publie parfois deux lignes par événement (sans puis avec l'heure
    # de reprise) : on garde une ligne par (jour, heure, permno, raison), la plus
    # renseignée.
    coalesce: dict[tuple, dict] = {}
    for r in rattaches:
        k = (r["jour"], r["heure"], r["permno"], r["raison"])
        prec = coalesce.get(k)
        if prec is None or (not prec["reprise_jour"] and r["reprise_jour"]):
            coalesce[k] = r
    n_avant = len(rattaches)
    rattaches = list(coalesce.values())

    minutes_marche = {k for k, s in par_minute.items()
                      if len(s) >= SEUIL_PORTEE_MARCHE * len(jour_u.get(k[0], ()) or {1})}
    for r in rattaches:
        r["portee_marche"] = int((r["jour"], r["minute"]) in minutes_marche)

    SORTIES.mkdir(parents=True, exist_ok=True)
    rattaches.sort(key=lambda r: (r["jour"], r["heure"], r["permno"]))
    with open(SORTIES / "halts_univers.csv", "w", newline="") as fh:
        w = csv.DictWriter(fh, lineterminator="\n", fieldnames=list(rattaches[0]))
        w.writeheader(); w.writerows(rattaches)

    # Couverture par époque, publiée avec C-FULL
    cov: dict[str, Counter] = defaultdict(Counter)
    for r in rattaches:
        ep = epoque(r["jour"])
        if ep:
            cov[ep]["halts"] += 1
            cov[ep]["marche" if r["portee_marche"] else "specifiques"] += 1
            cov[ep]["titres"] = 0
    for ep in cov:
        cov[ep]["titres"] = len({r["permno"] for r in rattaches
                                 if epoque(r["jour"]) == ep and not r["portee_marche"]})
    with open(SORTIES / "halts_couverture_epoque.csv", "w", newline="") as fh:
        w = csv.writer(fh, lineterminator="\n")
        w.writerow(["epoque", "source_active", "halts_rattaches",
                    "dont_portee_marche", "dont_specifiques", "titres_concernes"])
        for nom, a, b in EPOQUES:
            c = cov.get(nom, Counter())
            active = "partielle (2019-09→)" if nom == "E1" else "oui"
            w.writerow([nom, active, c["halts"], c["marche"],
                        c["specifiques"], c["titres"]])

    # Compteurs écrits sur disque pour que l'audit vérifie
    # lignes brutes = rattachées + rejetées.
    with open(SORTIES / "halts_rejets.csv", "w", newline="") as fh:
        w = csv.writer(fh, lineterminator="\n")
        w.writerow(["poste", "n"])
        w.writerow(["lignes_brutes_source", len(brut)])
        w.writerow(["lignes_rattachees_avant_coalescence", n_avant])
        w.writerow(["mises_a_jour_coalescees", n_avant - len(rattaches)])
        w.writerow(["evenements_publies", len(rattaches)])
        for k in sorted(perdus):
            w.writerow([f"rejet_{k}", perdus[k]])

    print(f"\nrattachés à l'univers : {len(rattaches)} événements "
          f"({n_avant} lignes brutes, {n_avant - len(rattaches)} mises à jour coalescées)")
    print(f"perdus : {dict(perdus)}")
    print(f"minutes de portée marché : {len(minutes_marche)} "
          f"({sum(r['portee_marche'] for r in rattaches)} halts concernés)")
    print(f"déclencheurs T-HALT (spécifiques au titre) : "
          f"{sum(1 for r in rattaches if not r['portee_marche'])}")
    print(f"\nsorties dans {SORTIES}")

    # Les sorties ne sont valides que si l'audit d'invariants passe.
    print("\n=== audit d'invariants ===")
    from audit_corpus import main as auditer
    if auditer():
        raise SystemExit("audit en échec : sorties écrites mais non validées, "
                         "à ne pas utiliser avant correction")


if __name__ == "__main__":
    main()
