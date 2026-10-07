#!/usr/bin/env python3
"""Audit d'invariants des tables du corpus et des halts.

Vérifie la structure et la cohérence des tables produites par
`construire_corpus.py`, `prevalences_couverture.py` et `tirer_halts.py` :
bornes des fenêtres, troncatures, délistings recalculés depuis la source,
drapeaux, jointure P-14, prévalences et rattachement des halts. Il ne juge pas
la pertinence de la méthode. Un contrôle impossible faute de table est signalé
« SAUTÉ », jamais compté comme réussi ; --strict le compte comme un échec.

Code de retour 0 si tout passe, 1 sinon.

Usage :  python3 audit_corpus.py [--strict]
"""

from __future__ import annotations

import csv
import json
import sys
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import chemins  # noqa: E402
SORTIES = chemins.SORTIES / "corpus"
CALENDRIER = chemins.DONNEES / "barres" / "calendrier_univers.json"
EARLY_CLOSES = chemins.REFERENCES / "early_closes.csv"
CLOTURE_NORMALE = "16:00:00"

echecs: list[str] = []
sautes: list[str] = []


def check(nom: str, cond: bool, detail: str = "") -> None:
    print(f"  {'OK   ' if cond else 'ÉCHEC'} {nom}" + (f" — {detail}" if detail and not cond else ""))
    if not cond:
        echecs.append(nom)


def saute(nom: str, fichier: str) -> None:
    """Enregistre un contrôle non effectué faute de table."""
    print(f"  SAUTÉ {nom} — {fichier} absent")
    sautes.append(nom)


def lire(nom: str) -> list[dict] | None:
    f = SORTIES / nom
    if not f.exists():
        return None
    with open(f) as fh:
        return list(csv.DictReader(fh))


def main(strict: bool = False) -> int:
    cv, cf = lire("corpus_cvol_episodes.csv"), lire("corpus_cfull_episodes.csv")
    jv, jf = lire("corpus_cvol_jours_declencheurs.csv"), lire("corpus_cfull_jours_declencheurs.csv")
    h = lire("halts_univers.csv")
    if cv is None:
        print("  SAUTÉ tout — corpus_cvol_episodes.csv absent")
        return 0 if not strict else 1
    cal = sorted(set(json.load(open(CALENDRIER))))
    idx = {d: i for i, d in enumerate(cal)}
    ferm = {r["date"]: r["close_time_ET"] + ":00"
            for r in csv.DictReader(open(EARLY_CLOSES))}
    premier_halt: dict[tuple, str] = {}
    for r in (h or []):
        if r["portee_marche"] == "0":
            k = (r["permno"], r["jour"])
            premier_halt[k] = min(premier_halt.get(k, "99:99:99"), r["heure"])
    mauvais_tc: list = []

    # 1. Invariants internes
    print("=== invariants internes ===")
    for nom, rows in [("C-VOL", cv)] + ([("C-FULL", cf)] if cf else []):
        check(f"{nom} debut <= premier_declencheur <= fin",
              all(e["debut"] <= e["premier_declencheur"] <= e["fin"] for e in rows))
        check(f"{nom} duree_jours recalculable depuis le calendrier",
              all(int(e["duree_jours"]) == idx[e["fin"]] - idx[e["debut"]] + 1 for e in rows))
        # Une fenêtre complète compte 31 séances ; seuls le bord gauche du
        # calendrier et un délisting peuvent la raccourcir.
        check(f"{nom} fenêtre >= 31 séances hors bord de calendrier ou délisting",
              all(int(e["duree_jours"]) >= 31 or idx[e["premier_declencheur"]] < 10
                  or e["tronque"] == "delisting" for e in rows))
        check(f"{nom} clés de tirage distinctes",
              len({e["cle_tirage"] for e in rows}) == len(rows))
        for col in ("epoque_premier_declencheur", "tranche_prix", "nano_micro"):
            check(f"{nom} aucune valeur vide dans {col}",
                  not any(not e[col] for e in rows))
        check(f"{nom} t_connaissance horodaté, daté et zoné",
              all(len(e["t_connaissance"]) == 25 and e["t_connaissance"][10] == "T"
                  and e["t_connaissance"][19] in "+-" for e in rows))
        check(f"{nom} t_connaissance le jour du premier déclencheur",
              all(e["t_connaissance"][:10] == e["premier_declencheur"] for e in rows))
        check(f"{nom} fenetre_e5 = 1 <=> fenêtre finissant en 2026",
              all((e["fenetre_e5"] == "1") == (e["fin"] >= "2026-01-01") for e in rows))
        # t_connaissance attendu selon le type du premier jour : clôture (T-VOL),
        # premier halt, éventuellement après la clôture (T-HALT), ou le plus tôt.
        for e in rows:
            d0, tc = e["premier_declencheur"], e["t_connaissance"]
            typ = e.get("type_premier_declencheur", "T-VOL")
            cl, hl = ferm.get(d0, CLOTURE_NORMALE), premier_halt.get((e["permno"], d0))
            if typ == "T-VOL":
                attendu = cl
            elif typ == "T-HALT":
                attendu = hl
            else:                                   # T-HALT+T-VOL
                attendu = min(cl, hl) if hl else cl
            if attendu is None or tc[11:19] != attendu:
                mauvais_tc.append((nom, e["permno"], d0, typ, tc[11:19], attendu))
        check(f"{nom} t_connaissance conforme au type de déclenchement",
              not mauvais_tc, f"{len(mauvais_tc)} écarts, ex. {mauvais_tc[:1]}")
        mauvais_tc.clear()
        check(f"{nom} sortie_univers_j dans la fenêtre",
              all(not e["sortie_univers_j"] or e["debut"] <= e["sortie_univers_j"] <= e["fin"]
                  for e in rows))
        par = defaultdict(list)
        for e in rows:
            par[e["permno"]].append((e["debut"], e["fin"]))
        chev = sum(1 for l in par.values() for i, (a, b) in enumerate(sorted(l))
                   for a2, _ in sorted(l)[i + 1:] if a2 <= b)
        check(f"{nom} épisodes d'un même titre disjoints", chev == 0, f"{chev} chevauchements")

    # 2. Cohérence entre tables
    print("\n=== cohérence inter-tables ===")
    if cf is None:
        saute("C-FULL (tous contrôles)", "corpus_cfull_episodes.csv")
    for nom, eps, jours in ([("C-VOL", cv, jv)] + ([("C-FULL", cf, jf)] if cf and jf else [])):
        s = sum(int(e["n_declencheurs"]) for e in eps)
        check(f"{nom} Σ n_declencheurs = lignes de la table des jours",
              s == len(jours), f"{s} vs {len(jours)}")
    # tronque=fin_donnees si et seulement si dernier déclencheur + 20 dépasse la
    # dernière séance. Les épisodes tronque=delisting sont exclus, le délisting
    # étant prioritaire ; ils sont vérifiés dans la section 3.
    for nom, eps, jours in ([("C-VOL", cv, jv)] + ([("C-FULL", cf, jf)] if cf and jf else [])):
        dernier: dict = {}
        for r in jours:
            k = (r["permno"], r["episode_debut"])
            dernier[k] = max(dernier.get(k, ""), r["jour"])
        ecarts = [(e["permno"], e["debut"], e["tronque"])
                  for e in eps if e["tronque"] != "delisting"
                  and (e["tronque"] == "fin_donnees")
                  != (idx[dernier[(e["permno"], e["debut"])]] + 20 > len(cal) - 1)]
        check(f"{nom} tronque=fin_donnees <=> borne théorique > dernière séance (hors delisting)",
              not ecarts, f"{len(ecarts)} écarts, ex. {ecarts[:1]}")
        # Une fenêtre non tronquée qui finit sur la dernière séance doit avoir sa
        # borne théorique égale au dernier indice du calendrier.
        sur_borne = [e for e in eps if e["fin"] == cal[-1] and not e["tronque"]]
        mauvais = [e["permno"] for e in sur_borne
                   if idx[dernier[(e["permno"], e["debut"])]] + 20 != len(cal) - 1]
        check(f"{nom} fenêtre non tronquée finissant sur la dernière séance : "
              f"borne théorique exactement égale", not mauvais, f"{len(mauvais)} écarts")
        print(f"        ({len(sur_borne)} fenêtres finissant exactement sur {cal[-1]})")

    for nom, jours in ([("C-VOL", jv)] if jv else []) + ([("C-FULL", jf)] if jf else []):
        u = len({(r["permno"], r["jour"]) for r in jours})
        check(f"{nom} jours déclencheurs : couples (permno, jour) uniques",
              u == len(jours), f"{len(jours) - u} doublons")

    if jf and h:
        check("C-VOL ⊆ C-FULL (jours déclencheurs)",
              {(r["permno"], r["jour"]) for r in jv} <= {(r["permno"], r["jour"]) for r in jf})
        thj = {(r["permno"], r["jour"]) for r in h if r["portee_marche"] == "0"}
        cfh = {(r["permno"], r["jour"]) for r in jf if "T-HALT" in r["types"]}
        check("jours T-HALT de C-FULL ⊆ halts spécifiques au titre",
              cfh <= thj, f"{len(cfh - thj)} orphelins")
    else:
        saute("cohérence C-VOL/C-FULL/halts", "tables C-FULL ou halts")

    # 3. Délistings CRSP
    print("\n=== délistings ===")
    from construire_corpus import DELISTINGS as DELISTINGS_DEFAUT
    from construire_corpus import lire_delistings
    if not DELISTINGS_DEFAUT.exists():
        saute("délistings (tous contrôles)", str(DELISTINGS_DEFAUT))
    else:
        delistings = lire_delistings(DELISTINGS_DEFAUT)
        for nom, rows in [("C-VOL", cv)] + ([("C-FULL", cf)] if cf else []):
            causes = {e.get("tronque", "") for e in rows}
            check(f"{nom} tronque ∈ {{'', 'delisting', 'fin_donnees'}}",
                  causes <= {"", "delisting", "fin_donnees"}, str(causes))

            # Fin mécanique, avant raccourcissement par un délisting.
            def fin_mec(e: dict) -> str:
                return e["fin_avant_delisting"] if e["tronque"] == "delisting" else e["fin"]

            # Recalcul depuis la table CRSP : delistingdt dans
            # [premier_declencheur, fin mécanique], jour de bourse, égal à la fin publiée.
            mauvais_delisting, hors_cal_delisting, fin_pas_egale = [], [], []
            for e in rows:
                if e["tronque"] != "delisting":
                    continue
                d = delistings.get(str(int(e["permno"])))
                if not (d and e["premier_declencheur"] <= d <= fin_mec(e)):
                    mauvais_delisting.append((e["permno"], e["debut"], d))
                elif d not in idx:
                    hors_cal_delisting.append((e["permno"], d))
                if e["fin"] != d:
                    fin_pas_egale.append((e["permno"], e["debut"], e["fin"], d))
            check(f"{nom} tronque=delisting => delistingdt dans [premier_declencheur, fin_avant_delisting]",
                  not mauvais_delisting, f"{len(mauvais_delisting)} écarts, ex. {mauvais_delisting[:1]}")
            check(f"{nom} delistingdt utilisé tombe un jour de bourse (∈ calendrier)",
                  not hors_cal_delisting, f"{len(hors_cal_delisting)} cas, ex. {hors_cal_delisting[:1]}")
            check(f"{nom} tronque=delisting => fin publiée == delistingdt",
                  not fin_pas_egale, f"{len(fin_pas_egale)} écarts, ex. {fin_pas_egale[:1]}")

            # fin_raccourcie = (delistingdt < fin mécanique), et tout épisode
            # tronque=delisting est soit raccourci, soit délisté le jour même de sa fin.
            mauvais_racc = []
            n_raccourcis = n_non_raccourcis = 0
            for e in rows:
                if e["tronque"] != "delisting":
                    continue
                d = delistings.get(str(int(e["permno"])))
                attendu = "1" if d and d < fin_mec(e) else "0"
                if e["fin_raccourcie"] != attendu:
                    mauvais_racc.append((e["permno"], e["debut"], e["fin_raccourcie"], attendu))
                if e["fin_raccourcie"] == "1":
                    n_raccourcis += 1
                elif e["fin_raccourcie"] == "0":
                    n_non_raccourcis += 1
            n_qualifies = sum(1 for e in rows if e["tronque"] == "delisting")
            check(f"{nom} fin_raccourcie == (delistingdt < fin mécanique), recalculé depuis la source",
                  not mauvais_racc, f"{len(mauvais_racc)} écarts, ex. {mauvais_racc[:1]}")
            check(f"{nom} identité comptable : qualifiés ({n_qualifies}) == "
                  f"raccourcis ({n_raccourcis}) + delisting==fin ({n_non_raccourcis})",
                  n_qualifies == n_raccourcis + n_non_raccourcis)
            print(f"        {nom} : {n_qualifies} épisodes qualifiés tronque=delisting, "
                  f"dont {n_raccourcis} avec borne réellement raccourcie et "
                  f"{n_non_raccourcis} où delisting == fin mécanique (fin inchangée)")

            check(f"{nom} delisting_j renseigné <=> tronque == delisting",
                  all(bool(e["delisting_j"]) == (e["tronque"] == "delisting") for e in rows))
            mauvais_dj = []
            for e in rows:
                if not e["delisting_j"]:
                    continue
                d = delistings.get(str(int(e["permno"])))
                dj = int(e["delisting_j"])
                borne_max = idx[fin_mec(e)] - idx[e["premier_declencheur"]]
                attendu = idx[d] - idx[e["premier_declencheur"]] if d in idx else None
                if attendu is None or dj != attendu or not (0 <= dj <= borne_max):
                    mauvais_dj.append((e["permno"], e["debut"], dj, attendu, borne_max))
            check(f"{nom} delisting_j recalculable depuis la source et borné "
                  f"[0, fin_mécanique-premier_declencheur]",
                  not mauvais_dj, f"{len(mauvais_dj)} écarts, ex. {mauvais_dj[:1]}")

            # Un épisode fin_donnees ne doit pas avoir de délisting dans sa fenêtre.
            manques = [(e["permno"], e["debut"], delistings.get(str(int(e["permno"]))))
                       for e in rows if e["tronque"] == "fin_donnees"
                       and (lambda d: d and e["premier_declencheur"] <= d <= e["fin"])(
                           delistings.get(str(int(e["permno"]))))]
            check(f"{nom} priorité delisting > fin_donnees respectée (aucun fin_donnees "
                  f"avec délisting en fenêtre)", not manques, f"{len(manques)} écarts, ex. {manques[:1]}")

            # Recompte depuis la source seule, pour détecter les omissions : tout
            # épisode dont le délisting tombe dans sa fenêtre mécanique doit porter
            # tronque=delisting.
            attendus, omis = 0, []
            for e in rows:
                d = delistings.get(str(int(e["permno"])))
                borne = e["fin_avant_delisting"] or e["fin"]
                if d and e["premier_declencheur"] <= d <= borne:
                    attendus += 1
                    if e["tronque"] != "delisting":
                        omis.append((e["permno"], e["debut"], d, e["tronque"]))
            check(f"{nom} réconciliation : {n_qualifies} tronque=delisting == {attendus} attendus "
                  f"depuis la source seule", n_qualifies == attendus and not omis,
                  f"{len(omis)} omissions, ex. {omis[:1]}")

            # La table CRSP couvre jusqu'au 2025-12-31 : aucun délisting utilisé ne
            # doit être postérieur. Sur janvier 2026, les délistings sont inconnus.
            post = [(e["permno"], e["debut"], delistings.get(str(int(e["permno"]))))
                    for e in rows if e["tronque"] == "delisting"
                    and delistings.get(str(int(e["permno"])), "") > "2025-12-31"]
            check(f"{nom} aucun tronque=delisting postérieur au 2025-12-31",
                  not post, f"{len(post)} cas, ex. {post[:1]}")

    # 3 bis. Drapeaux reit/etranger/ads et jointure P-14
    print("\n=== flags et P-14 ===")
    from construire_corpus import flags_univers, lire_p14, UNIVERS_DIR, P14_DEFAUT, COLONNES_P14
    flags_src = flags_univers(UNIVERS_DIR)
    p14_src = lire_p14(P14_DEFAUT)
    if not p14_src:
        saute("P-14 (tous contrôles)", str(P14_DEFAUT))
    for nom, rows in [("C-VOL", cv)] + ([("C-FULL", cf)] if cf else []):
        check(f"{nom} reit/etranger/ads ∈ {{'0','1'}}",
              all(e["reit"] in ("0", "1") and e["etranger"] in ("0", "1")
                  and e["ads"] in ("0", "1") for e in rows))
        # Recalcul depuis le snapshot régissant le mois du premier déclencheur.
        mauvais_flags = []
        for e in rows:
            f = flags_src.get(e["premier_declencheur"][:7], {}).get(str(int(e["permno"])))
            attendu = f if f else ("", "", "")
            if (e["reit"], e["etranger"], e["ads"]) != attendu:
                mauvais_flags.append((e["permno"], e["premier_declencheur"]))
        check(f"{nom} flags recalculables depuis les snapshots U_t (source seule)",
              not mauvais_flags, f"{len(mauvais_flags)} écarts, ex. {mauvais_flags[:1]}")

        check(f"{nom} p14_disponible ∈ {{'0','1'}}",
              all(e["p14_disponible"] in ("0", "1") for e in rows))
        # p14_disponible=1 si au moins une colonne p14_* est renseignée, 0 si
        # toutes sont vides.
        incoherents = [e for e in rows
                       if (e["p14_disponible"] == "1") != any(e[c] != "" for c in COLONNES_P14)]
        check(f"{nom} p14_disponible cohérent avec le remplissage des colonnes p14_*",
              not incoherents, f"{len(incoherents)} écarts")
        # Couverture P-14 recomptée sur la source : (permno, premier_declencheur) joints.
        attendu_dispo = sum(1 for e in rows
                            if (str(int(e["permno"])), e["premier_declencheur"]) in p14_src)
        publie_dispo = sum(1 for e in rows if e["p14_disponible"] == "1")
        check(f"{nom} couverture P-14 = cardinal de l'intersection ({publie_dispo})",
              publie_dispo == attendu_dispo, f"publié {publie_dispo} vs recompté {attendu_dispo}")

    # 3 ter. Prévalences : effectifs et dénominateurs
    print("\n=== prévalences ===")
    for nom, fichier, eps in [("C-VOL", "corpus_cvol_prevalences.csv", cv)] + (
            [("C-FULL", "corpus_cfull_prevalences.csv", cf)] if cf else []):
        prev = lire(fichier)
        if prev is None:
            saute(f"{nom} prévalences", fichier)
            continue
        # Effectifs par strate sommés = épisodes de la table source ; C-FULL est
        # restreint à la période couverte par les halts (2019-09 à 2025-12).
        s = sum(int(r["n_episodes"]) for r in prev)
        base = eps if nom == "C-VOL" else [e for e in eps
                                            if "2019-09" <= e["premier_declencheur"][:7] <= "2025-12"]
        check(f"{nom} prévalences : Σ n_episodes par strate == effectif de la table source",
              s == len(base), f"{s} vs {len(base)}")
        mauvais_ratio = []
        for r in prev:
            denom = int(r["n_titres_jours_rvact_non_na"])
            num = int(r["n_jours_declencheurs"])
            attendu = round(num / denom, 6) if denom else 0.0
            if abs(float(r["prevalence_declencheur"]) - attendu) > 1e-6:
                mauvais_ratio.append((r["epoque"], r["tranche_prix"]))
        check(f"{nom} prevalence_declencheur = n_jours_declencheurs / n_titres_jours_rvact_non_na",
              not mauvais_ratio, f"{len(mauvais_ratio)} écarts, ex. {mauvais_ratio[:1]}")
        check(f"{nom} dénominateurs positifs ou nuls",
              all(int(r["n_titres_jours_eligibles"]) >= 0
                  and int(r["n_titres_jours_rvact_non_na"]) >= 0 for r in prev))
        check(f"{nom} n_titres_jours_rvact_non_na <= n_titres_jours_eligibles (sous-ensemble)",
              all(int(r["n_titres_jours_rvact_non_na"]) <= int(r["n_titres_jours_eligibles"])
                  for r in prev))

    # 4. Table des halts
    print("\n=== halts ===")
    if h is None:
        saute("halts (tous contrôles)", "halts_univers.csv")
        return bilan(strict)
    check("aucun doublon (jour, heure, permno, raison)",
          len({(r["jour"], r["heure"], r["permno"], r["raison"]) for r in h}) == len(h))
    check("tous les halts tombent un jour de bourse", all(r["jour"] in idx for r in h))
    check("portee_marche binaire", {r["portee_marche"] for r in h} <= {"0", "1"})

    # 5. Rattachement ticker -> PERMNO : un seul PERMNO par ticker dans le
    # snapshot du mois, sans résolution implicite d'ambiguïté.
    print("\n=== mapping daté et réconciliation des rejets ===")
    from tirer_halts import mapping_date, UNIVERS
    mapping = mapping_date(UNIVERS)
    amb = [r for r in h if mapping.get(r["jour"][:7], {}).get(r["ticker"]) is None]
    check("chaque halt publié a exactement un PERMNO dans le snapshot du mois",
          not amb, f"{len(amb)} halts sur ticker ambigu ou hors snapshot")
    faux = [r for r in h if mapping.get(r["jour"][:7], {}).get(r["ticker"]) not in (None, r["permno"])]
    check("aucune résolution implicite (PERMNO publié = PERMNO du snapshot daté)",
          not faux, f"{len(faux)} divergences")
    rej = lire("halts_rejets.csv")
    if rej is None:
        saute("réconciliation des rejets", "halts_rejets.csv")
    else:
        n = {r["poste"]: int(r["n"]) for r in rej}
        rejets = sum(v for k, v in n.items() if k.startswith("rejet_"))
        check("réconciliation : brutes = rattachées + rejets",
              n["lignes_brutes_source"] == n["lignes_rattachees_avant_coalescence"] + rejets,
              f"{n['lignes_brutes_source']} vs {n['lignes_rattachees_avant_coalescence']} + {rejets}")
        check("réconciliation : rattachées = publiées + coalescées",
              n["lignes_rattachees_avant_coalescence"]
              == n["evenements_publies"] + n["mises_a_jour_coalescees"])
        check("les rejets pour ambiguïté sont comptés explicitement",
              "rejet_ticker_ambigu_plusieurs_permno" in n)
        check("nombre d'événements publiés = lignes de halts_univers.csv",
              n["evenements_publies"] == len(h), f"{n['evenements_publies']} vs {len(h)}")

    return bilan(strict)


def bilan(strict: bool) -> int:
    if echecs:
        print(f"\n{len(echecs)} ÉCHEC(S) : " + ", ".join(echecs))
    elif sautes:
        print(f"\nTOUT PASSE, {len(sautes)} contrôle(s) sauté(s) : " + ", ".join(sautes)
              + ("  [strict : compté comme échec]" if strict else ""))
    else:
        print("\nTOUT PASSE")
    return 1 if echecs or (strict and sautes) else 0


if __name__ == "__main__":
    sys.exit(main("--strict" in sys.argv))
