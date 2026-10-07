#!/usr/bin/env python3
"""Prévalences des déclencheurs, troncatures, couverture mensuelle et couverture
P-14, à partir des corpus produits par `construire_corpus.py`.

Le dénominateur vient de `p04_p05_univers_titre_jour.csv.gz`, dont chaque ligne
est un titre-jour éligible (titre dans le snapshot d'univers qui régit le mois),
restreint à [D_MIN, D_MAX]. Deux dénominateurs sont publiés par strate :
`n_titres_jours_eligibles` (tous les titres-jours) et
`n_titres_jours_rvact_non_na` (ceux où RVact est défini), qui sert au calcul de
`prevalence_declencheur`. Pour C-FULL, numérateurs et dénominateurs sont
restreints à la période couverte par les halts (2019-09 à 2025-12).

Usage :  python3 prevalences_couverture.py [--entree <p04_p05_univers_titre_jour.csv.gz>]
"""

from __future__ import annotations

import argparse
import csv
import gzip
from collections import Counter, defaultdict
from pathlib import Path

from construire_corpus import (D_MIN, D_MAX, ENTREE, SORTIES, UNIVERS_DIR,
                                flags_univers)

DEBUT_HALTS, FIN_HALTS = "2019-09", "2025-12"     # période couverte par la source des halts
FLAGS_NOMS = ("reit", "etranger", "ads")


def lire_episodes(nom: str) -> list[dict]:
    with open(SORTIES / nom, newline="") as fh:
        return list(csv.DictReader(fh))


def lire_jours(nom: str) -> list[dict]:
    with open(SORTIES / nom, newline="") as fh:
        return list(csv.DictReader(fh))


def population(entree: Path, cles_jours: set[tuple[str, str]],
               flags: dict[str, dict[str, tuple[str, str, str]]]):
    """Calcule en un seul passage sur la table titre-jour tous les dénominateurs
    (par strate et par drapeau), chacun sur [D_MIN, D_MAX] et sur la période
    couverte par les halts, ainsi que les titres-mois éligibles.

    Retient aussi la strate propre à chaque jour déclencheur de `cles_jours`,
    qui peut différer de celle du premier déclencheur de son épisode.
    """
    denom_grid = Counter()                 # (epoque,tranche,nano_micro,exchange) -> n
    denom_grid_rv = Counter()              # idem, RVact non-NA seulement
    denom_grid_periode = Counter()         # idem, restreint à 2019-09->2025-12
    denom_grid_rv_periode = Counter()
    denom_flag = Counter()                 # (epoque,tranche,flag,valeur) -> n
    denom_flag_rv = Counter()
    denom_flag_periode = Counter()
    denom_flag_rv_periode = Counter()
    titres_mois = defaultdict(set)         # (epoque,tranche,nano_micro,exchange) -> {(permno,mois)}
    titres_mois_periode = defaultdict(set)
    strate_jour: dict[tuple[str, str], tuple] = {}   # (permno,jour) -> (epoque,tranche,nano,exch)
    n_lignes = 0
    with gzip.open(entree, "rt", newline="") as fh:
        for row in csv.DictReader(fh):
            n_lignes += 1
            d = row["date"]
            if not (D_MIN <= d <= D_MAX):
                continue
            mo = row["mois"]
            en_periode = DEBUT_HALTS <= mo <= FIN_HALTS
            cle_grid = (row["epoque"], row["tranche_prix"], row["nano_micro"], row["exchange"])
            denom_grid[cle_grid] += 1
            rv_ok = not row["rvact_na_code"]
            if rv_ok:
                denom_grid_rv[cle_grid] += 1
            titres_mois[cle_grid].add((row["permno"], mo))
            if en_periode:
                denom_grid_periode[cle_grid] += 1
                titres_mois_periode[cle_grid].add((row["permno"], mo))
                if rv_ok:
                    denom_grid_rv_periode[cle_grid] += 1

            f = flags.get(mo, {}).get(str(int(row["permno"])))
            if f:
                for nomf, val in zip(FLAGS_NOMS, f):
                    kf = (row["epoque"], row["tranche_prix"], nomf, val)
                    denom_flag[kf] += 1
                    if rv_ok:
                        denom_flag_rv[kf] += 1
                    if en_periode:
                        denom_flag_periode[kf] += 1
                        if rv_ok:
                            denom_flag_rv_periode[kf] += 1

            k = (row["permno"], d)
            if k in cles_jours:
                strate_jour[k] = cle_grid
    return {"denom_grid": denom_grid, "denom_grid_rv": denom_grid_rv,
            "denom_grid_periode": denom_grid_periode, "denom_grid_rv_periode": denom_grid_rv_periode,
            "denom_flag": denom_flag, "denom_flag_rv": denom_flag_rv,
            "denom_flag_periode": denom_flag_periode, "denom_flag_rv_periode": denom_flag_rv_periode,
            "titres_mois": titres_mois, "titres_mois_periode": titres_mois_periode,
            "strate_jour": strate_jour, "n_lignes": n_lignes}


def type_normalise(t: str) -> str:
    """Ramène "T-HALT+T-VOL" (table des jours) au libellé "les deux" (table des
    épisodes), pour que jours et épisodes d'un même type tombent sur la même ligne."""
    return "les deux" if "+" in t else t


def prevalences_grid(eps: list[dict], jours: list[dict], pop: dict,
                      restreindre_periode: bool = False) -> list[dict]:
    """Prévalences sur la grille epoque x tranche_prix x nano_micro x exchange,
    ventilées par type de déclenchement pour C-FULL.

    `restreindre_periode` limite numérateurs et dénominateurs à la période couverte
    par les halts, pour ne pas confondre absence de données et absence d'événement.
    """
    a_type = "types" in (jours[0].keys() if jours else {})

    def dans_periode(j: str) -> bool:
        return not restreindre_periode or (DEBUT_HALTS <= j[:7] <= FIN_HALTS)

    num_jours = Counter()          # (grid, type|"") -> n
    for r in jours:
        j = r["jour"]
        if not dans_periode(j):
            continue
        cle_grid = pop["strate_jour"].get((r["permno"], j))
        if cle_grid is None:
            continue    # jour hors population, signalé par main()
        typ = type_normalise(r["types"]) if a_type else ""
        num_jours[(cle_grid, typ)] += 1

    num_eps = Counter()            # (grid, type_declenchement|"") -> n
    for e in eps:
        if not dans_periode(e["premier_declencheur"]):
            continue
        cle_grid = (e["epoque_premier_declencheur"], e["tranche_prix"],
                    e["nano_micro"], e["exchange"])
        typ = type_normalise(e.get("type_declenchement", "")) if a_type else ""
        num_eps[(cle_grid, typ)] += 1

    titres_avec_ep = defaultdict(set)     # (grid, type) -> {permno}
    for e in eps:
        if not dans_periode(e["premier_declencheur"]):
            continue
        cle_grid = (e["epoque_premier_declencheur"], e["tranche_prix"],
                    e["nano_micro"], e["exchange"])
        typ = type_normalise(e.get("type_declenchement", "")) if a_type else ""
        titres_avec_ep[(cle_grid, typ)].add(e["permno"])

    if restreindre_periode:
        denom_grid_src, denom_grid_rv_src = pop["denom_grid_periode"], pop["denom_grid_rv_periode"]
        titres_mois_src = pop["titres_mois_periode"]
    else:
        denom_grid_src, denom_grid_rv_src = pop["denom_grid"], pop["denom_grid_rv"]
        titres_mois_src = pop["titres_mois"]

    types = sorted({t for _, t in num_jours} | {t for _, t in num_eps}) or [""]
    out = []
    for cle_grid, dg in sorted(denom_grid_src.items()):
        dgrv = denom_grid_rv_src.get(cle_grid, 0)
        tm = len(titres_mois_src.get(cle_grid, ()))
        for typ in types:
            nj = num_jours.get((cle_grid, typ), 0)
            ne = num_eps.get((cle_grid, typ), 0)
            if a_type and nj == 0 and ne == 0:
                continue   # combinaison type x strate inobservée
            nt = len(titres_avec_ep.get((cle_grid, typ), ()))
            out.append({
                "epoque": cle_grid[0], "tranche_prix": cle_grid[1],
                "nano_micro": cle_grid[2], "exchange": cle_grid[3],
                **({"type_declenchement": typ} if a_type else {}),
                "n_titres_jours_eligibles": dg,
                "n_titres_jours_rvact_non_na": dgrv,
                "n_jours_declencheurs": nj,
                "n_episodes": ne,
                "prevalence_declencheur": round(nj / dgrv, 6) if dgrv else 0.0,
                "n_titres_mois_eligibles": tm,
                "n_titres_avec_episode": nt,
                "prevalence_titre": round(nt / tm, 6) if tm else 0.0})
    return out


def prevalences_flags(eps: list[dict], jours: list[dict], pop: dict,
                       restreindre_periode: bool = False) -> list[dict]:
    """Prévalence des jours déclencheurs par drapeau (reit/etranger/ads), sur la
    grille epoque x tranche_prix pour limiter la taille de la sortie. Le drapeau
    est celui du snapshot d'univers qui régit le mois du jour."""

    def dans_periode(j: str) -> bool:
        return not restreindre_periode or (DEBUT_HALTS <= j[:7] <= FIN_HALTS)

    a_type = "types" in (jours[0].keys() if jours else {})
    flags = flags_univers(UNIVERS_DIR)

    def flag_ep(permno: str, mois: str):
        return flags.get(mois, {}).get(str(int(permno)))

    num_jours = Counter()
    for r in jours:
        j = r["jour"]
        if not dans_periode(j):
            continue
        cle_grid = pop["strate_jour"].get((r["permno"], j))
        if cle_grid is None:
            continue
        ep_tr = (cle_grid[0], cle_grid[1])
        f = flag_ep(r["permno"], j[:7])
        if not f:
            continue
        typ = type_normalise(r["types"]) if a_type else ""
        for nomf, val in zip(FLAGS_NOMS, f):
            num_jours[(ep_tr, nomf, val, typ)] += 1

    if restreindre_periode:
        denom_flag_src, denom_flag_rv_src = pop["denom_flag_periode"], pop["denom_flag_rv_periode"]
    else:
        denom_flag_src, denom_flag_rv_src = pop["denom_flag"], pop["denom_flag_rv"]

    types = sorted({t for _, _, _, t in num_jours}) or [""]
    out = []
    for (ep, tr, nomf, val), dg in sorted(denom_flag_src.items()):
        dgrv = denom_flag_rv_src.get((ep, tr, nomf, val), 0)
        for typ in types:
            nj = num_jours.get(((ep, tr), nomf, val, typ), 0)
            if a_type and nj == 0:
                continue
            out.append({"epoque": ep, "tranche_prix": tr, "flag": nomf, "valeur": val,
                        **({"type_declenchement": typ} if a_type else {}),
                        "n_titres_jours_eligibles": dg,
                        "n_titres_jours_rvact_non_na": dgrv,
                        "n_jours_declencheurs": nj,
                        "prevalence_declencheur": round(nj / dgrv, 6) if dgrv else 0.0})
    return out


def troncature(eps: list[dict]) -> list[dict]:
    """Part des épisodes de chaque strate (epoque x tranche_prix) par cause de
    troncature. Le dénominateur est le nombre d'épisodes de la strate, d'où le
    nom « part » plutôt que « prévalence »."""
    par_strate = defaultdict(list)
    for e in eps:
        par_strate[(e["epoque_premier_declencheur"], e["tranche_prix"])].append(e["tronque"] or "aucune")
    out = []
    for (ep, tr), causes in sorted(par_strate.items()):
        c = Counter(causes)
        total = len(causes)
        for cause, n in sorted(c.items()):
            out.append({"epoque": ep, "tranche_prix": tr, "cause_troncature": cause,
                        "n_episodes": n, "n_episodes_strate": total,
                        "part_de_la_strate": round(n / total, 6) if total else 0.0})
    return out


def couverture_mensuelle(cv: list[dict], cf: list[dict]) -> list[dict]:
    """Épisodes par mois de leur premier déclencheur, sur [D_MIN, D_MAX], avec une
    ligne pour chaque mois, y compris sans épisode. `couverture_halts_active`
    passe à 1 en 2019-09 : avant cette date, C-FULL ne contient que des T-VOL."""
    mois: list[str] = []
    a, m = int(D_MIN[:4]), int(D_MIN[5:7])
    while f"{a}-{m:02d}" <= D_MAX[:7]:
        mois.append(f"{a}-{m:02d}")
        a, m = (a + 1, 1) if m == 12 else (a, m + 1)
    n_cv = Counter(e["premier_declencheur"][:7] for e in cv)
    n_cf = Counter(e["premier_declencheur"][:7] for e in cf)
    out = []
    for mo in mois:
        out.append({"mois": mo,
                    "couverture_halts_active": int(mo >= DEBUT_HALTS),
                    "n_episodes_cvol": n_cv.get(mo, 0),
                    "n_episodes_cfull": n_cf.get(mo, 0),
                    "zero_episode_cvol": int(n_cv.get(mo, 0) == 0),
                    "zero_episode_cfull": int(n_cf.get(mo, 0) == 0)})
    return out


def couverture_p14(eps: list[dict]) -> list[dict]:
    """Part des épisodes de chaque strate pour lesquels P-14 est disponible
    (P-14 n'est calculée que sur un sous-échantillon 1/10 des PERMNO)."""
    par = defaultdict(lambda: [0, 0])
    for e in eps:
        k = (e["epoque_premier_declencheur"], e["tranche_prix"], e["nano_micro"], e["exchange"])
        par[k][0] += 1
        par[k][1] += int(e["p14_disponible"])
    out = []
    for (ep, tr, nm, ex), (n, disp) in sorted(par.items()):
        out.append({"epoque": ep, "tranche_prix": tr, "nano_micro": nm, "exchange": ex,
                    "n_episodes": n, "n_p14_disponible": disp,
                    "taux_couverture_p14": round(disp / n, 6) if n else 0.0})
    return out


def ecrire(nom: str, lignes: list[dict]) -> None:
    if not lignes:
        print(f"  avertissement : {nom} vide, rien écrit")
        return
    with open(SORTIES / nom, "w", newline="") as fh:
        w = csv.DictWriter(fh, lineterminator="\n", fieldnames=list(lignes[0]))
        w.writeheader(); w.writerows(lignes)
    print(f"  {nom} : {len(lignes)} lignes")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--entree", type=Path, default=ENTREE)
    args = ap.parse_args()

    cv = lire_episodes("corpus_cvol_episodes.csv")
    cf = lire_episodes("corpus_cfull_episodes.csv")
    jv = lire_jours("corpus_cvol_jours_declencheurs.csv")
    jf = lire_jours("corpus_cfull_jours_declencheurs.csv")
    flags = flags_univers(UNIVERS_DIR)

    print("lecture de la table titre-jour de l'univers (dénominateurs)...")
    cles = {(r["permno"], r["jour"]) for r in jv} | {(r["permno"], r["jour"]) for r in jf}
    pop = population(args.entree, cles, flags)
    print(f"  {pop['n_lignes']:,} lignes lues, {len(pop['strate_jour'])}/{len(cles)} "
          f"jours déclencheurs résolus à leur strate propre".replace(",", " "))
    manques = cles - set(pop["strate_jour"])
    if manques:
        print(f"  avertissement : {len(manques)} jours déclencheurs hors population "
              f"(hors [D_MIN, D_MAX] ou absents de la table), exclus des numérateurs, "
              f"ex. {sorted(manques)[:3]}")

    print("\n=== prévalences — grille epoque x tranche x nano_micro x exchange ===")
    ecrire("corpus_cvol_prevalences.csv", prevalences_grid(cv, jv, pop))
    ecrire("corpus_cfull_prevalences.csv", prevalences_grid(cf, jf, pop, restreindre_periode=True))

    print("\n=== prévalences par flag (reit/etranger/ads) ===")
    ecrire("corpus_cvol_prevalences_flags.csv", prevalences_flags(cv, jv, pop))
    ecrire("corpus_cfull_prevalences_flags.csv",
           prevalences_flags(cf, jf, pop, restreindre_periode=True))

    print("\n=== troncature par strate ===")
    ecrire("corpus_cvol_troncature.csv", troncature(cv))
    ecrire("corpus_cfull_troncature.csv", troncature(cf))

    print("\n=== couverture temporelle (mensuelle) ===")
    cm = couverture_mensuelle(cv, cf)
    ecrire("corpus_couverture_mensuelle.csv", cm)
    zc_v = sum(r["zero_episode_cvol"] for r in cm)
    zc_f = sum(r["zero_episode_cfull"] for r in cm)
    print(f"  mois à zéro épisode : C-VOL {zc_v}/{len(cm)}, C-FULL {zc_f}/{len(cm)}")

    print("\n=== couverture P-14 par strate ===")
    ecrire("corpus_cvol_couverture_p14.csv", couverture_p14(cv))
    ecrire("corpus_cfull_couverture_p14.csv", couverture_p14(cf))

    # Les sorties ne sont valides que si l'audit d'invariants passe.
    print("\n=== audit d'invariants ===")
    from audit_corpus import main as auditer
    if auditer():
        raise SystemExit("audit en échec : sorties écrites mais non validées, "
                         "à ne pas utiliser avant correction")


if __name__ == "__main__":
    main()
