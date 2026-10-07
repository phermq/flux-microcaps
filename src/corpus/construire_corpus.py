#!/usr/bin/env python3
"""Construction des corpus d'épisodes C-VOL et C-FULL.

Un jour déclencheur T-VOL est un titre-jour avec RVact >= 5 (sensibilité aux
seuils 3 et 10) ; C-FULL ajoute les jours de halt spécifiques au titre (T-HALT,
option --avec-halts). Chaque déclencheur ouvre une fenêtre [D-10, D+20] jours de
bourse, et les fenêtres chevauchantes d'un même PERMNO sont fusionnées. Ces
paramètres ont été fixés avant d'examiner les données. Chaque épisode porte sa
strate (époque × tranche de prix), sa cause de troncature (délisting CRSP
prioritaire sur la fin des données), les drapeaux reit/etranger/ads du snapshot
d'univers du mois, la jointure P-14 quand elle existe, et une clé de tirage
SHA-256 pour un sous-échantillonnage reproductible. Les tests de propriétés et
l'audit d'invariants sont lancés en fin d'exécution.

Usage :  python3 construire_corpus.py [--entree <p04_p05_univers_titre_jour.csv.gz>]
                                       [--delistings <univers_delists.csv>]
                                       [--p14 <primitives_conditions_titre_jour.csv>]
                                       [--avec-halts]
"""

from __future__ import annotations

import argparse
import csv
import gzip
import hashlib
import json
import statistics
import sys
from collections import Counter, defaultdict
from pathlib import Path

SEUILS = (3, 5, 10)
SEUIL_PRINCIPAL = 5
AVANT, APRES = 10, 20
FIN_DONNEES = "2026-01-30"
# Période où un jour peut déclencher. Elle est fixée ici et non déduite des
# données ; l'absence de déclencheur en 2026 vient de RVact = NA(ca_couverture).
D_MIN, D_MAX = "2018-06-01", "2025-12-31"
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import chemins  # noqa: E402
ENTREE = chemins.SORTIES / "rejeu" / "p04_p05_univers_titre_jour.csv.gz"
EARLY_CLOSES = chemins.REFERENCES / "early_closes.csv"
CALENDRIER_REF = chemins.DONNEES / "barres" / "calendrier_univers.json"
CLOTURE_NORMALE = "16:00:00"   # ET ; les séances écourtées viennent d'early_closes.csv
SORTIES = chemins.SORTIES / "corpus"
# Table CRSP des délistings, non versionnée (licence WRDS académique).
DELISTINGS = chemins.DONNEES / "crsp" / "univers_delists.csv"
UNIVERS_DIR = chemins.SORTIES / "univers"          # snapshots mensuels U_*.csv
# Sortie P-14 du runner d'univers, calculée sur un sous-échantillon 1/10 des
# PERMNO (par SHA-256) : la plupart des épisodes n'y ont pas de ligne.
P14_DEFAUT = chemins.SORTIES / "primitives-univers" / "primitives_conditions_titre_jour.csv"
COLONNES_P14 = ["p14_taux_hors_sequence_Z", "p14_taux_correction", "p14_vol_correction_part",
                "p14_n_correction_8", "p14_n_correction_10", "p14_delta_trf_part_all_810",
                "p14_delta_sp_part_all_810", "p14_delta_dvact_810", "p14_latence_mediane_ms",
                "p14_latence_p99_ms", "p14_taux_conditions_inconnues", "p14_taux_exclus",
                "p14_taux_desordre_fichier"]


def clotures() -> dict[str, str]:
    """{jour -> heure de clôture ET} des séances écourtées, d'après
    `references/early_closes.csv`."""
    out = {}
    with open(EARLY_CLOSES) as fh:
        for r in csv.DictReader(fh):
            out[r["date"]] = r["close_time_ET"] + ":00"
    return out


def horodatage(jour: str, heure: str) -> str:
    """Horodatage ISO 8601 avec offset ET.

    L'offset vaut -04:00 en heure d'été (2e dimanche de mars → 1er dimanche de
    novembre) et -05:00 sinon ; calculé sans dépendance externe.
    """
    from datetime import date, timedelta
    a = int(jour[:4])
    mars = date(a, 3, 8)
    debut = mars + timedelta(days=(6 - mars.weekday()) % 7)          # 2e dimanche de mars
    nov = date(a, 11, 1)
    fin = nov + timedelta(days=(6 - nov.weekday()) % 7)              # 1er dimanche de nov.
    d = date.fromisoformat(jour)
    return f"{jour}T{heure}{'-04:00' if debut <= d < fin else '-05:00'}"


def t_conn(permno: str, jour: str, types: set, ferm: dict, th: dict) -> str:
    """Instant où le déclencheur est connu : la clôture pour T-VOL, l'heure du
    premier halt pour T-HALT, le plus tôt des deux si le jour porte les deux.

    Un halt seul peut survenir après la clôture ; c'est alors son heure qui compte.
    """
    cl = ferm.get(jour, CLOTURE_NORMALE)
    hl = th.get((permno, jour), {}).get("heure")
    if types == {"T-HALT"}:
        return hl
    if hl is not None and "T-HALT" in types:
        return min(cl, hl)
    return cl


def sortie_univers(permno: str, e: dict, cal: list[str],
                   u_jour: dict[str, set]) -> str:
    """Premier jour de la fenêtre où le titre n'appartient plus à U, sinon "".

    La sortie d'univers est publiée comme covariable datée ; elle ne tronque pas
    l'épisode, qui reste observé jusqu'au bout de sa fenêtre.
    """
    dedans = False
    for i in range(e["debut_i"], e["fin_i"] + 1):
        present = permno in u_jour.get(cal[i], ())
        if present:
            dedans = True
        elif dedans:
            return cal[i]
    return ""


def cle_tirage(permno: str, jour: str) -> str:
    """SHA-256('{permno}|{YYYY-MM-DD}'), clé de tri d'un sous-échantillonnage
    reproductible. Le PERMNO est stable, contrairement au ticker."""
    return hashlib.sha256(f"{int(permno)}|{jour}".encode()).hexdigest()


def lire_delistings(path: Path) -> dict[str, str]:
    """{PERMNO -> delistingdt (YYYY-MM-DD)} depuis la table CRSP, qui a une ligne
    par PERMNO. Fichier absent : dictionnaire vide."""
    out: dict[str, str] = {}
    if not path.exists():
        return out
    with open(path, newline="") as fh:
        for r in csv.DictReader(fh):
            d = r.get("delistingdt", "")
            if d:
                out[str(int(r["permno"]))] = d
    return out


def flags_univers(univers: Path) -> dict[str, dict[str, tuple[str, str, str]]]:
    """{mois régi (YYYY-MM) -> {PERMNO -> (reit, etranger, ads)}}, valeurs '0'/'1'.

    Le snapshot daté de la fin du mois M régit le mois M+1, comme dans
    `tirer_halts.mapping_date` ; ici la clé est le PERMNO, unique par snapshot.
    """
    out: dict[str, dict[str, tuple[str, str, str]]] = {}
    for p in sorted(univers.glob("U_*.csv")):
        d = p.stem[2:]
        a, m = int(d[:4]), int(d[4:6])
        regi = f"{a + 1}-01" if m == 12 else f"{a}-{m + 1:02d}"
        flags: dict[str, tuple[str, str, str]] = {}
        with open(p, newline="") as fh:
            for row in csv.DictReader(fh):
                flags[str(int(row["permno"]))] = (
                    str(int(row["reit"] == "True")),
                    str(int(row["etranger"] == "True")),
                    str(int(row["ads"] == "True")))
        out[regi] = flags
    return out


def lire_p14(path: Path) -> dict[tuple[str, str], dict]:
    """{(PERMNO, jour) -> ligne P-14}. Fichier absent : dictionnaire vide, et
    l'appelant publie alors `p14_disponible=0` partout."""
    out: dict[tuple[str, str], dict] = {}
    if not path.exists():
        return out
    with open(path, newline="") as fh:
        for row in csv.DictReader(fh):
            out[(str(int(row["permno"])), row["date"])] = row
    return out


def cause_troncature(permno: str, premier_declencheur: str, fin: str,
                      tronque_fin_donnees: bool,
                      delistings: dict[str, str]) -> tuple[str, str]:
    """Cause de troncature d'un épisode : "delisting", "fin_donnees" ou "".

    Les deux causes sont exclusives et le délisting l'emporte : un délisting dans
    [premier_declencheur, fin] (bornes incluses) explique l'arrêt de la fenêtre
    même si D+20 dépasse aussi la dernière séance disponible. La table CRSP
    s'arrête fin 2025 : l'absence de délisting sur janvier 2026 traduit un manque
    de données, pas un maintien à la cote.

    Retourne (cause, delistingdt), delistingdt vide si la cause n'est pas
    "delisting". La fonction ne dépend pas du calendrier ; l'appelant en tire
    `delisting_j` et la borne publiée (`borne_delisting`).
    """
    d = delistings.get(permno)
    if d and premier_declencheur <= d <= fin:
        return "delisting", d
    return ("fin_donnees" if tronque_fin_donnees else ""), ""


def borne_delisting(fin_mecanique: str, cause: str, delistingdt: str) -> tuple[str, str, str]:
    """Borne de fin publiée une fois le délisting pris en compte.

    Retourne (fin_publiee, fin_avant_delisting, fin_raccourcie) : (fin_mecanique,
    "", "") hors délisting, sinon (delistingdt, fin_mecanique, "1" ou "0").
    Un épisode tronque=delisting n'est raccourci que si delistingdt < fin
    mécanique ; quand les deux coïncident, la fin publiée est inchangée. Comme
    delistingdt <= fin_mecanique par construction, la fin ne peut qu'avancer.
    """
    if cause != "delisting":
        return fin_mecanique, "", ""
    return delistingdt, fin_mecanique, str(int(delistingdt < fin_mecanique))


def lire(entree: Path, cles_halt: set = frozenset()) -> tuple[list[str], dict, dict]:
    """(calendrier, déclencheurs par seuil, étiquettes par (permno, jour))."""
    calendrier: set[str] = set()
    decl: dict[int, dict[str, list[str]]] = {s: defaultdict(list) for s in SEUILS}
    etiq: dict[tuple[str, str], dict] = {}
    na = Counter()
    hors_bornes = [0]
    n = 0
    with gzip.open(entree, "rt", newline="") as fh:
        for row in csv.DictReader(fh):
            n += 1
            calendrier.add(row["date"])          # tous les jours servent aux fenêtres
            if not (D_MIN <= row["date"] <= D_MAX):
                hors_bornes[0] += 1
                continue
            rv = row["rvact"]
            # Les valeurs manquantes sont codées 'NA(cause)' (histo, bar_absente,
            # ca_couverture, non_cote, med0) ; aucune ne déclenche.
            est_halt = (row["permno"], row["date"]) in cles_halt
            meta = {"ticker": row["ticker"], "exchange": row["exchange"],
                    "tranche_prix": row["tranche_prix"], "nano_micro": row["nano_micro"],
                    "epoque": row["epoque"], "so_source": row["so_source"]}
            if not rv or rv.startswith("NA("):
                na[rv or "(vide)"] += 1
                # Un jour de halt déclenche même si RVact est NA : on garde ses
                # métadonnées (époque, strate) et RVact avec son code NA.
                if est_halt:
                    etiq[(row["permno"], row["date"])] = {**meta, "rvact": rv}
                continue
            v = float(rv)
            # Métadonnées gardées pour tout jour pouvant être un premier
            # déclencheur, quel que soit le seuil ou le type.
            if v >= min(SEUILS) or (row["permno"], row["date"]) in cles_halt:
                etiq[(row["permno"], row["date"])] = {
                            "ticker": row["ticker"], "exchange": row["exchange"],
                            "tranche_prix": row["tranche_prix"], "nano_micro": row["nano_micro"],
                    "epoque": row["epoque"], "so_source": row["so_source"],
                    "rvact": v}
            for s in SEUILS:
                if v >= s:
                    decl[s][row["permno"]].append(row["date"])
    return sorted(calendrier), decl, (etiq, na, n, hors_bornes[0])


def episodes(jours: list[str], cal: list[str], idx: dict[str, int]) -> list[dict]:
    """Fenêtres [D-10, D+20] d'un titre, fusionnées transitivement."""
    out: list[dict] = []
    # Le dédoublonnage empêche un doublon d'entrée de gonfler `n_declencheurs`.
    for j in sorted(set(jours)):
        i = idx[j]
        a, b = max(0, i - AVANT), min(len(cal) - 1, i + APRES)
        # Fusion sur chevauchement seulement : deux fenêtres contiguës
        # (a == fin + 1) restent deux épisodes.
        if out and a <= out[-1]["fin_i"]:
            out[-1]["fin_i"] = max(out[-1]["fin_i"], b)
            out[-1]["declencheurs"].append(j)
        else:
            out.append({"debut_i": a, "fin_i": b, "declencheurs": [j],
                        "fin_theorique": i + APRES})
        out[-1]["fin_theorique"] = max(out[-1].get("fin_theorique", b), i + APRES)
    for e in out:
        e["debut"], e["fin"] = cal[e["debut_i"]], cal[e["fin_i"]]
        # Tronqué si et seulement si D+20 dépasse la dernière séance ; une fenêtre
        # qui finit exactement sur la dernière séance est complète.
        e["tronque"] = "fin_donnees" if e["fin_theorique"] > len(cal) - 1 else ""
    return out


def halts_marche() -> set[str]:
    """Jours portant un halt de portée marché, source de la covariable
    `halt_marche`. Sur les données actuelles l'ensemble est vide.

    C'est une propriété du jour : elle concerne tous les titres observés, pas
    seulement ceux qui ont une ligne de halt.
    """
    f = SORTIES / "halts_univers.csv"
    if not f.exists():
        return set()
    return {r["jour"] for r in csv.DictReader(open(f)) if r["portee_marche"] == "1"}


def halts_declencheurs() -> dict[tuple[str, str], dict]:
    """{(permno, jour) -> {heure du premier halt, raisons, n}} pour les halts
    spécifiques au titre. L'heure du premier halt est l'instant de connaissance
    d'un déclencheur T-HALT."""
    f = SORTIES / "halts_univers.csv"
    if not f.exists():
        raise SystemExit(f"{f} absent : lancer d'abord tirer_halts.py")
    out: dict[tuple[str, str], dict] = {}
    for r in csv.DictReader(open(f)):
        if r["portee_marche"] != "0" or not (D_MIN <= r["jour"] <= D_MAX):
            continue
        k = (r["permno"], r["jour"])
        e = out.setdefault(k, {"heure": r["heure"], "raisons": set(), "n": 0})
        e["heure"] = min(e["heure"], r["heure"])
        e["raisons"].add(r["raison"])
        e["n"] += 1
    return out


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--entree", type=Path, default=ENTREE)
    ap.add_argument("--delistings", type=Path, default=DELISTINGS,
                    help="table CRSP des délistings (univers_delists.csv)")
    ap.add_argument("--avec-halts", action="store_true",
                    help="construit aussi C-FULL = T-VOL union T-HALT")
    ap.add_argument("--p14", type=Path, default=P14_DEFAUT,
                    help="sortie P-14 du runner d'univers (sous-échantillon 1/10)")
    args = ap.parse_args()

    cles_halt: set = set()
    if args.avec_halts:
        cles_halt = set(halts_declencheurs())
    cal, decl, (etiq, na, n_lignes, hors) = lire(args.entree, cles_halt)
    ferm = clotures()
    hm = halts_marche()
    delistings = lire_delistings(args.delistings)
    if not delistings:
        print(f"avertissement : {args.delistings} absent ou vide ; "
              f"tronque=delisting et delisting_j ne seront pas renseignés "
              f"pour cette exécution")
    flags = flags_univers(UNIVERS_DIR)
    p14 = lire_p14(args.p14)
    if not p14:
        print(f"avertissement : {args.p14} absent ou vide ; "
              f"p14_disponible=0 partout et colonnes p14_* vides "
              f"pour cette exécution")
    else:
        print(f"P-14 : {len(p14):,} lignes (sous-échantillon 1/10)".replace(",", " "))

    def champs_flags_p14(p: str, d0: str) -> dict:
        """Drapeaux reit/etranger/ads du snapshot régissant le mois de d0, et
        colonnes P-14 avec leur indicateur de disponibilité."""
        f = flags.get(d0[:7], {}).get(str(int(p)))
        r14 = p14.get((str(int(p)), d0))
        out = {"reit": f[0] if f else "", "etranger": f[1] if f else "",
               "ads": f[2] if f else "", "p14_disponible": int(r14 is not None)}
        out.update({c: (r14[c] if r14 else "") for c in COLONNES_P14})
        return out
    u_jour: dict[str, set] = defaultdict(set)
    with gzip.open(args.entree, "rt", newline="") as fh:
        for row in csv.DictReader(fh):
            u_jour[row["date"]].add(row["permno"])
    # Le calendrier déduit de P-04 doit coïncider avec le calendrier de
    # référence, sinon les fenêtres [D-10, D+20] seraient décalées.
    ref = set(json.load(open(CALENDRIER_REF)))
    if ref != set(cal):
        raise SystemExit(f"calendrier divergent : {len(ref ^ set(cal))} jours d'écart "
                         f"avec {CALENDRIER_REF.name}")
    print(f"calendrier : {len(cal)} séances, identique à la référence ; "
          f"{len(ferm)} clôtures anticipées")
    idx = {d: i for i, d in enumerate(cal)}
    print(f"{n_lignes:,} tickers-jours lus, {len(cal):,} jours de bourse".replace(",", " "))
    print(f"RVact NA : {dict(na.most_common())}")
    print(f"hors période [{D_MIN}, {D_MAX}] (non éligibles au déclenchement) : "
          f"{hors:,}".replace(",", " ") + "\n")

    resume = []
    for s in SEUILS:
        eps_par_permno = {p: episodes(js, cal, idx) for p, js in decl[s].items()}
        eps = [(p, e) for p, lst in eps_par_permno.items() for e in lst]
        n_decl = sum(len(js) for js in decl[s].values())
        durees = [e["fin_i"] - e["debut_i"] + 1 for _, e in eps]
        n_par_titre = Counter(p for p, _ in eps)
        top20 = sum(c for _, c in n_par_titre.most_common(20))
        resume.append({
            "seuil_rvact": s, "principal": int(s == SEUIL_PRINCIPAL),
            "n_jours_declencheurs": n_decl, "n_episodes": len(eps),
            "n_titres": len(n_par_titre),
            "duree_mediane_jours": statistics.median(durees) if durees else 0,
            "duree_p90_jours": sorted(durees)[int(.9 * len(durees))] if durees else 0,
            "duree_max_jours": max(durees, default=0),
            "declencheurs_par_episode_moyen": round(n_decl / len(eps), 3) if eps else 0,
            # Concentration : part des 20 titres les plus déclencheurs, en jours
            # déclencheurs (métrique de référence) puis en épisodes.
            "part_top20_titres_en_jours_declencheurs": round(
                sum(c for _, c in Counter(
                    p for p, js in decl[s].items() for _ in js).most_common(20)) / n_decl, 4)
                if n_decl else 0,
            "part_top20_titres_en_episodes": round(top20 / len(eps), 4) if eps else 0,
            "n_tronques_fin_donnees": sum(1 for _, e in eps if e["tronque"])})

        if s != SEUIL_PRINCIPAL:
            continue
        SORTIES.mkdir(parents=True, exist_ok=True)
        lignes = []
        for p, e in eps:
            d0 = e["declencheurs"][0]
            m = etiq.get((p, d0), {})
            cause, ddt = cause_troncature(p, d0, e["fin"], bool(e["tronque"]), delistings)
            fin_pub, fin_avant, raccourcie = borne_delisting(e["fin"], cause, ddt)
            fin_i_pub = idx[fin_pub]
            e_pub = e if fin_pub == e["fin"] else {**e, "fin_i": fin_i_pub}
            lignes.append({
                "permno": p, "ticker": m.get("ticker", ""),
                "debut": e["debut"], "fin": fin_pub,
                "premier_declencheur": d0, "n_declencheurs": len(e["declencheurs"]),
                "duree_jours": fin_i_pub - e["debut_i"] + 1,
                "epoque_premier_declencheur": m.get("epoque", ""),
                "tranche_prix": m.get("tranche_prix", ""),
                "nano_micro": m.get("nano_micro", ""), "exchange": m.get("exchange", ""),
                "so_source": m.get("so_source", ""),
                "rvact_premier_declencheur": m.get("rvact", ""),
                "t_connaissance": horodatage(d0, ferm.get(d0, "16:00:00")),
                "t_connaissance_origine": "cloture",
                "halt_marche": int(d0 in hm),
                "fenetre_e5": int(fin_pub >= "2026-01-01"),
                "sortie_univers_j": sortie_univers(p, e_pub, cal, u_jour),
                "type_declenchement": "T-VOL", "tronque": cause,
                # Jours de bourse entre premier déclencheur et délisting, renseigné
                # seulement si tronque == "delisting" ; sortie_univers_j couvre,
                # elle, toute sortie de l'univers quelle qu'en soit la cause.
                "delisting_j": (idx[ddt] - idx[d0]) if ddt else "",
                "fin_avant_delisting": fin_avant, "fin_raccourcie": raccourcie,
                "cle_tirage": cle_tirage(p, d0), **champs_flags_p14(p, d0)})
        lignes.sort(key=lambda l: (l["permno"], l["debut"]))
        with open(SORTIES / "corpus_cvol_episodes.csv", "w", newline="") as fh:
            w = csv.DictWriter(fh, lineterminator="\n", fieldnames=list(lignes[0]))
            w.writeheader(); w.writerows(lignes)
        print("causes de troncature C-VOL : "
              + str(dict(Counter(l["tronque"] or "(aucune)" for l in lignes).most_common())))
        print(f"flags C-VOL non résolus (mois hors snapshots) : "
              f"{sum(1 for l in lignes if l['reit'] == '')}")
        print(f"P-14 disponible sur C-VOL : "
              f"{sum(l['p14_disponible'] for l in lignes)}/{len(lignes)}")

        # Table des jours : seulement les attributs propres au jour ; ceux de la
        # fenêtre (fenetre_e5, sortie_univers_j) restent dans la table des épisodes.
        jours = [{"permno": p, "jour": j, "episode_debut": e["debut"],
                  "t_connaissance": horodatage(j, ferm.get(j, CLOTURE_NORMALE)),
                  "halt_marche": int(j in hm)}
                 for p, e in eps for j in e["declencheurs"]]
        with open(SORTIES / "corpus_cvol_jours_declencheurs.csv", "w", newline="") as fh:
            w = csv.DictWriter(fh, lineterminator="\n", fieldnames=list(jours[0]))
            w.writeheader(); w.writerows(jours)

        strat = Counter((l["epoque_premier_declencheur"], l["tranche_prix"]) for l in lignes)
        with open(SORTIES / "corpus_cvol_strates.csv", "w", newline="") as fh:
            w = csv.writer(fh, lineterminator="\n")
            w.writerow(["epoque", "tranche_prix", "n_episodes", "sous_ech_k200_atteint"])
            for (ep, tp), c in sorted(strat.items()):
                w.writerow([ep, tp, c, int(c > 200)])

    # C-FULL
    if args.avec_halts:
        th = halts_declencheurs()
        tv = decl[SEUIL_PRINCIPAL]
        union: dict[str, dict[str, set[str]]] = defaultdict(lambda: defaultdict(set))
        for p, js in tv.items():
            for j in js:
                union[p][j].add("T-VOL")
        for (p, j) in th:
            union[p][j].add("T-HALT")
        eps = [(p, e) for p, jd in union.items()
               for e in episodes(sorted(jd), cal, idx)]
        lignes = []
        for p, e in eps:
            d0 = e["declencheurs"][0]
            types = set().union(*(union[p][j] for j in e["declencheurs"]))
            m = etiq.get((p, d0), {})
            cause, ddt = cause_troncature(p, d0, e["fin"], bool(e["tronque"]), delistings)
            fin_pub, fin_avant, raccourcie = borne_delisting(e["fin"], cause, ddt)
            fin_i_pub = idx[fin_pub]
            e_pub = e if fin_pub == e["fin"] else {**e, "fin_i": fin_i_pub}
            lignes.append({
                "permno": p, "ticker": m.get("ticker", ""),
                "debut": e["debut"], "fin": fin_pub, "premier_declencheur": d0,
                "n_declencheurs": len(e["declencheurs"]),
                "duree_jours": fin_i_pub - e["debut_i"] + 1,
                "epoque_premier_declencheur": m.get("epoque", ""),
                "tranche_prix": m.get("tranche_prix", ""),
                "nano_micro": m.get("nano_micro", ""), "exchange": m.get("exchange", ""),
                "so_source": m.get("so_source", ""),
                # Un jour T-HALT seul peut avoir RVact = NA(...) ; le code est publié.
                "rvact_premier_declencheur": m.get("rvact", ""),
                "halt_marche": int(d0 in hm),
                "type_declenchement": "les deux" if len(types) > 1 else types.pop(),
                "type_premier_declencheur": "+".join(sorted(union[p][d0])),
                # Instant de connaissance selon le type du jour d0 (voir t_conn).
                "t_connaissance": horodatage(d0, t_conn(p, d0, union[p][d0], ferm, th)),
                "t_connaissance_origine": (
                    "halt" if t_conn(p, d0, union[p][d0], ferm, th)
                    == th.get((p, d0), {}).get("heure") else "cloture"),
                "fenetre_e5": int(fin_pub >= "2026-01-01"),
                "sortie_univers_j": sortie_univers(p, e_pub, cal, u_jour),
                "n_halts_premier_jour": th.get((p, d0), {}).get("n", 0),
                "raisons_halt_premier_jour": "|".join(
                    sorted(th.get((p, d0), {}).get("raisons", ()))),
                "n_jours_halt_episode": sum(1 for j in e["declencheurs"] if (p, j) in th),
                "tronque": cause,
                "delisting_j": (idx[ddt] - idx[d0]) if ddt else "",
                "fin_avant_delisting": fin_avant, "fin_raccourcie": raccourcie,
                "cle_tirage": cle_tirage(p, d0), **champs_flags_p14(p, d0)})
        lignes.sort(key=lambda l: (l["permno"], l["debut"]))
        with open(SORTIES / "corpus_cfull_episodes.csv", "w", newline="") as fh:
            w = csv.DictWriter(fh, lineterminator="\n", fieldnames=list(lignes[0]))
            w.writeheader(); w.writerows(lignes)
        print("causes de troncature C-FULL : "
              + str(dict(Counter(l["tronque"] or "(aucune)" for l in lignes).most_common())))
        print(f"flags C-FULL non résolus (mois hors snapshots) : "
              f"{sum(1 for l in lignes if l['reit'] == '')}")
        print(f"P-14 disponible sur C-FULL : "
              f"{sum(l['p14_disponible'] for l in lignes)}/{len(lignes)}")
        # Table des jours déclencheurs C-FULL, seconde unité d'analyse.
        jours_cf = [{"permno": p, "jour": j,
                     "types": "+".join(sorted(union[p][j])),
                     "t_connaissance": horodatage(j, t_conn(p, j, union[p][j], ferm, th)),
                     "n_halts": th.get((p, j), {}).get("n", 0),
                     "halt_marche": int(j in hm),
                     "raisons_halt": "|".join(sorted(th.get((p, j), {}).get("raisons", ()))),
                     "episode_debut": e["debut"]}
                    for p, e in eps for j in e["declencheurs"]]
        jours_cf.sort(key=lambda l: (l["permno"], l["jour"]))
        with open(SORTIES / "corpus_cfull_jours_declencheurs.csv", "w", newline="") as fh:
            w = csv.DictWriter(fh, lineterminator="\n", fieldnames=list(jours_cf[0]))
            w.writeheader(); w.writerows(jours_cf)

        with open(SORTIES / "corpus_cfull_strates.csv", "w", newline="") as fh:
            w = csv.writer(fh, lineterminator="\n")
            w.writerow(["epoque", "tranche_prix", "type_declenchement", "n_episodes"])
            c = Counter((l["epoque_premier_declencheur"], l["tranche_prix"],
                         l["type_declenchement"]) for l in lignes)
            for k, v in sorted(c.items()):
                w.writerow([*k, v])
        n_th = len(th)                       # clés (permno, jour)
        print(f"\nC-FULL : {len(lignes):,} épisodes ".replace(",", " ") +
              f"(C-VOL en avait {resume[1]['n_episodes']:,}".replace(",", " ") + ")")
        print(f"  jours déclencheurs T-HALT : {n_th:,}".replace(",", " ") +
              f" sur {len({p for p, _ in th}):,} titres".replace(",", " "))
        print("  répartition : " + str(dict(Counter(
            l["type_declenchement"] for l in lignes).most_common())))

    with open(SORTIES / "corpus_cvol_sensibilite_seuil.csv", "w", newline="") as fh:
        w = csv.DictWriter(fh, lineterminator="\n", fieldnames=list(resume[0]))
        w.writeheader(); w.writerows(resume)

    print(f"{'seuil':>6s} {'jours décl.':>12s} {'épisodes':>10s} {'titres':>8s} "
          f"{'durée méd.':>11s} {'top20':>7s}")
    for r in resume:
        marque = " *" if r["principal"] else "  "
        print(f"{r['seuil_rvact']:>6d}{marque} {r['n_jours_declencheurs']:>10,d} "
              f"{r['n_episodes']:>10,d} {r['n_titres']:>8,d} "
              f"{r['duree_mediane_jours']:>11.0f} "
              f"{r['part_top20_titres_en_jours_declencheurs']:>7.2%}"
              .replace(",", " "))
    print(f"\nsorties dans {SORTIES}")

    # Les sorties ne sont valides que si les tests de propriétés et l'audit
    # d'invariants passent ; un échec fait échouer l'exécution.
    print("\n=== tests de propriétés (synthétiques, hors données) ===")
    from test_proprietes import main as proprietes
    if proprietes():
        raise SystemExit("tests de propriétés en échec : sorties non validées")

    print("\n=== audit d'invariants ===")
    from audit_corpus import main as auditer
    if auditer():
        raise SystemExit("audit en échec : sorties écrites mais non validées, "
                         "à ne pas utiliser avant correction")


if __name__ == "__main__":
    main()
