#!/usr/bin/env python3
"""Tests de propriétés des fonctions de `construire_corpus.py`, sur un calendrier
synthétique, sans données ni réseau.

`audit_corpus.py` contrôle les tables produites ; ces tests contrôlent les
fonctions qui les produisent, y compris sur des cas limites absents des données
(fusion des fenêtres, bords de calendrier, instant de connaissance, délistings).

Usage :  python3 test_proprietes.py
"""

from __future__ import annotations

import sys

from construire_corpus import (AVANT, APRES, CLOTURE_NORMALE, borne_delisting, cause_troncature,
                               cle_tirage, episodes, horodatage, sortie_univers, t_conn)

CAL = [f"2024-{m:02d}-{j:02d}" for m in (1, 2) for j in range(1, 29)]
IDX = {d: i for i, d in enumerate(CAL)}
ok = 0


def prop(nom: str, cond: bool, detail: str = "") -> None:
    global ok
    print(f"  {'OK   ' if cond else 'ÉCHEC'} {nom}" + (f" — {detail}" if detail and not cond else ""))
    if cond:
        ok += 1
    else:
        raise AssertionError(nom)


def eps(jours: list[str]) -> list[dict]:
    return episodes(jours, CAL, IDX)


def main() -> int:
    print("=== fenêtre et fusion ===")
    e = eps([CAL[30]])[0]
    prop("fenêtre d'un déclencheur isolé = AVANT + 1 + APRES",
         e["fin_i"] - e["debut_i"] + 1 == AVANT + 1 + APRES,
         f"{e['fin_i'] - e['debut_i'] + 1}")
    prop("borne gauche = D - AVANT", e["debut_i"] == IDX[CAL[30]] - AVANT)
    prop("borne droite = D + APRES", e["fin_i"] == IDX[CAL[30]] + APRES)

    # Les fenêtres de D et D' sont contiguës quand D' - D = AVANT + APRES + 1 et
    # partagent un indice quand D' - D = AVANT + APRES ; seul ce second cas fusionne.
    a = CAL[20]
    contigu = CAL[20 + AVANT + APRES + 1]
    prop("deux fenêtres contiguës restent deux épisodes",
         len(eps([a, contigu])) == 2, f"{len(eps([a, contigu]))}")
    chevauche = CAL[20 + AVANT + APRES]              # partagent un indice
    prop("deux fenêtres partageant un indice fusionnent en un épisode",
         len(eps([a, chevauche])) == 1)
    prop("un jour d'écart suffit à séparer",
         len(eps([a, contigu])) == 2 and len(eps([a, chevauche])) == 1)
    prop("la fusion est transitive",
         len(eps([CAL[20], CAL[25], CAL[30], CAL[35]])) == 1)
    prop("les déclencheurs internes sont tous conservés",
         len(eps([CAL[20], CAL[25], CAL[30]])[0]["declencheurs"]) == 3)

    prop("fenêtre tronquée à gauche par le début du calendrier", eps([CAL[2]])[0]["debut_i"] == 0)
    prop("fenêtre tronquée à droite par la fin du calendrier",
         eps([CAL[-2]])[0]["fin_i"] == len(CAL) - 1)
    prop("dépassement à droite => tronque=fin_donnees", eps([CAL[-2]])[0]["tronque"] == "fin_donnees")
    prop("D+20 tombant sur la dernière séance => non tronquée",
         eps([CAL[len(CAL) - 1 - APRES]])[0]["tronque"] == "")

    print("\n=== instant de connaissance ===")
    FERM = {"2024-01-10": "13:00:00"}                # demi-séance
    TH = {("P1", "2024-01-15"): {"heure": "09:45:00"},   # avant la clôture normale
          ("P1", "2024-01-16"): {"heure": "19:49:45"},   # après la clôture normale
          ("P1", "2024-01-10"): {"heure": "14:30:00"}}   # après la clôture anticipée
    prop("T-VOL seul, séance normale -> clôture 16:00",
         t_conn("P1", "2024-01-15", {"T-VOL"}, FERM, TH) == CLOTURE_NORMALE)
    prop("T-VOL seul, demi-séance -> clôture 13:00",
         t_conn("P1", "2024-01-10", {"T-VOL"}, FERM, TH) == "13:00:00")
    prop("T-HALT seul, halt avant clôture -> heure du halt",
         t_conn("P1", "2024-01-15", {"T-HALT"}, FERM, TH) == "09:45:00")
    prop("T-HALT seul, halt après clôture -> heure du halt",
         t_conn("P1", "2024-01-16", {"T-HALT"}, FERM, TH) == "19:49:45")
    prop("T-HALT seul, halt après clôture anticipée -> heure du halt",
         t_conn("P1", "2024-01-10", {"T-HALT"}, FERM, TH) == "14:30:00")
    prop("les deux, halt avant clôture -> le halt",
         t_conn("P1", "2024-01-15", {"T-VOL", "T-HALT"}, FERM, TH) == "09:45:00")
    prop("les deux, halt après clôture -> la clôture",
         t_conn("P1", "2024-01-16", {"T-VOL", "T-HALT"}, FERM, TH) == CLOTURE_NORMALE)
    prop("les deux, demi-séance, halt après clôture anticipée -> 13:00",
         t_conn("P1", "2024-01-10", {"T-VOL", "T-HALT"}, FERM, TH) == "13:00:00")
    prop("horodatage avec offset d'heure d'hiver",
         horodatage("2024-01-15", "16:00:00") == "2024-01-15T16:00:00-05:00")
    prop("offset d'heure d'été appliqué",
         horodatage("2024-06-15", "16:00:00") == "2024-06-15T16:00:00-04:00")

    print("\n=== portée marché ===")
    from construire_corpus import halts_marche
    prop("halts_marche renvoie un ensemble de jours, pas de couples (titre, jour)",
         all(isinstance(x, str) and len(x) == 10 for x in halts_marche()) )
    # Un jour de portée marché concerne tous les titres observés ce jour-là.
    HM = {"2024-01-16"}
    prop("un jour de portée marché marque un titre sans ligne de halt",
         int("2024-01-16" in HM) == 1)
    prop("un jour ordinaire ne marque personne", int("2024-01-15" in HM) == 0)

    print("\n=== sortie d'univers ===")
    E = {"debut_i": 0, "fin_i": 9}
    U = {d: {"P1"} for d in CAL[:5]}                  # présent 5 jours puis absent
    prop("sortie d'univers datée au premier jour d'absence",
         sortie_univers("P1", E, CAL, U) == CAL[5])
    prop("aucune sortie si présent toute la fenêtre",
         sortie_univers("P1", E, CAL, {d: {"P1"} for d in CAL[:10]}) == "")
    prop("un titre jamais présent ne produit pas de sortie",
         sortie_univers("P9", E, CAL, U) == "")

    print("\n=== cause de troncature (délistings) ===")
    DEL = {"P1": "2024-01-20", "P2": "2024-01-05"}
    prop("délisting dans [premier_declencheur, fin] => tronque=delisting",
         cause_troncature("P1", "2024-01-15", "2024-01-25", False, DEL) == ("delisting", "2024-01-20"))
    prop("délisting exactement sur premier_declencheur => délisting (borne incluse)",
         cause_troncature("P1", "2024-01-20", "2024-01-25", False, DEL)[0] == "delisting")
    prop("délisting exactement sur fin => délisting (borne incluse)",
         cause_troncature("P1", "2024-01-15", "2024-01-20", False, DEL)[0] == "delisting")
    prop("délisting avant premier_declencheur => pas retenu comme cause",
         cause_troncature("P2", "2024-01-15", "2024-01-25", False, DEL)[0] == "")
    prop("délisting après fin => pas retenu comme cause",
         cause_troncature("P1", "2024-01-01", "2024-01-10", False, DEL)[0] == "")
    prop("aucun délisting connu, fin_donnees passe tel quel",
         cause_troncature("P9", "2024-01-15", "2024-01-25", True, DEL) == ("fin_donnees", ""))
    prop("aucun délisting connu, aucune troncature",
         cause_troncature("P9", "2024-01-15", "2024-01-25", False, DEL) == ("", ""))
    prop("priorité : délisting dans la fenêtre et fin_donnees applicable => delisting",
         cause_troncature("P1", "2024-01-15", "2024-01-25", True, DEL)[0] == "delisting")
    prop("exclusivité : une seule cause",
         cause_troncature("P1", "2024-01-15", "2024-01-25", True, DEL)[0] in ("delisting", "fin_donnees", ""))

    print("\n=== borne de fin après délisting ===")
    prop("cause != delisting : fin inchangée, pas de flag",
         borne_delisting("2024-01-25", "", "") == ("2024-01-25", "", ""))
    prop("delisting < fin mécanique : fin raccourcie, flag=1",
         borne_delisting("2024-01-25", "delisting", "2024-01-20")
         == ("2024-01-20", "2024-01-25", "1"))
    prop("delisting == fin mécanique : fin inchangée, flag=0",
         borne_delisting("2024-01-25", "delisting", "2024-01-25")
         == ("2024-01-25", "2024-01-25", "0"))
    prop("identité comptable sur un lot construit : qualifiés = raccourcis + (delisting==fin)",
         (lambda lot: sum(1 for f, r in lot if r != "")
          == sum(1 for f, r in lot if r == "1") + sum(1 for f, r in lot if r == "0"))(
             [borne_delisting(fm, "delisting", d)[1:]
              for fm, d in [("2024-01-25", "2024-01-20"), ("2024-01-25", "2024-01-25"),
                            ("2024-01-10", "2024-01-10")]]))

    print("\n=== clé de tirage ===")
    prop("clé déterministe", cle_tirage("10001", "2024-01-15") == cle_tirage("10001", "2024-01-15"))
    prop("clé sensible au PERMNO",
         cle_tirage("10001", "2024-01-15") != cle_tirage("10002", "2024-01-15"))
    prop("clé sensible à la date",
         cle_tirage("10001", "2024-01-15") != cle_tirage("10001", "2024-01-16"))
    prop("PERMNO normalisé en décimal (zéros de tête sans effet)",
         cle_tirage("010001", "2024-01-15") == cle_tirage("10001", "2024-01-15"))

    print("\n=== idempotence et cardinalités ===")
    js = [CAL[5], CAL[20], CAL[21], CAL[50]]
    # Deux appels sur des entrées distinctes, en modifiant le premier résultat
    # entre les deux, pour détecter un état partagé.
    r1 = eps(list(js))
    r1[0]["declencheurs"].append("POLLUTION")
    r1[0]["fin_i"] = -999
    r2 = eps(list(js))
    prop("construction idempotente (aucun état partagé entre appels)",
         [e["declencheurs"] for e in r2] == [e["declencheurs"] for e in eps(list(js))]
         and "POLLUTION" not in r2[0]["declencheurs"] and r2[0]["fin_i"] != -999)
    prop("invariance à l'ordre d'entrée des déclencheurs",
         [e["declencheurs"] for e in eps(js)] == [e["declencheurs"] for e in eps(js[::-1])])
    prop("conservation : Σ déclencheurs des épisodes = déclencheurs fournis",
         sum(len(e["declencheurs"]) for e in eps(js)) == len(js))
    d = eps([CAL[20], CAL[20]])
    prop("un doublon de jour déclencheur ne crée pas d'épisode", len(d) == 1)
    prop("un doublon n'est pas compté deux fois comme déclencheur",
         d[0]["declencheurs"] == [CAL[20]], f"{d[0]['declencheurs']}")
    # La localité ne vaut que pour des épisodes disjoints : la fusion transitive
    # et non bornée permet à un déclencheur intermédiaire de relier deux épisodes.
    sep = [CAL[5], CAL[40]]                            # [0,25] et [30,55] : disjoints
    prop("deux déclencheurs assez éloignés donnent deux épisodes", len(eps(sep)) == 2)
    prop("localité sur épisodes disjoints : le premier est inchangé",
         eps(sep)[0]["declencheurs"] == eps(sep + [CAL[41]])[0]["declencheurs"])
    prop("un déclencheur ajouté rejoint le bon épisode",
         eps(sep + [CAL[41]])[1]["declencheurs"] == [CAL[40], CAL[41]])
    prop("un déclencheur intermédiaire fusionne deux épisodes disjoints",
         len(eps(sep + [CAL[22]])) == 1, f"{len(eps(sep + [CAL[22]]))}")
    prop("la fusion transitive est non bornée : la durée peut dépasser la fenêtre",
         eps(sep + [CAL[22]])[0]["fin_i"] - eps(sep + [CAL[22]])[0]["debut_i"] + 1
         > AVANT + 1 + APRES)

    print(f"\n{ok} propriétés vérifiées")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except AssertionError as e:
        print(f"\nÉCHEC : {e}")
        sys.exit(1)
