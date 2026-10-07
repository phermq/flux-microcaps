#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Construction de l'univers point-in-time des micro et nanocaps US.

À chaque fin de mois t, retient dans CRSP les actions ordinaires et ADS cotées
sur NYSE, NYSE American ou Nasdaq dont la capitalisation à t est inférieure à
300 M$ et le prix de référence au moins égal à 0,10 $. Le nombre d'actions
vient, quand c'est possible, du dernier dépôt EDGAR antérieur à t, sinon du
shrout CRSP. Les règles C2 à C11 sont résumées là où le code les applique.

Entrées dans `chemins.DONNEES` (`crsp/`, `edgar-so/`, voir les constantes F_*),
sorties dans `chemins.SORTIES / "univers"` : un fichier U_AAAAMMJJ.csv par date,
journal.csv (entrées et sorties avec leur cause) et stats.md (comptages et
audits). Les sorties sont reproductibles à l'octet sur les mêmes entrées.

Unités : `shrout` CRSP est en milliers d'actions, `val` EDGAR (dei) en actions.

Usage :
  python3 src/donnees/build_univers.py --test    # autotests seuls
  python3 src/donnees/build_univers.py --build   # autotests puis construction
"""

from __future__ import annotations

import bisect
import csv
import datetime as _dt
import hashlib
import os
import shutil
import sys
from collections import Counter, defaultdict
from pathlib import Path

# --- Seuils et périmètre ---

CAP_MAX_USD = 300_000_000.0        # C5 : capitalisation à t < 300 M$
CAP_NANO_MAX_USD = 50_000_000.0    # nano < 50 M$, micro dans [50, 300) M$
PRIX_PLANCHER = 0.10               # C6 : prix de référence minimal, en dollars
FENETRE_REPORT = 5                 # C4 : report d'un prix sur au plus 5 jours de bourse

# C3 : marché principal NYSE, NYSE American ou Nasdaq, cotation régulière,
# statut actif, halté ou suspendu.
EXCHANGES_PERIMETRE = ("N", "A", "Q")
LIBELLE_EXCHANGE = {"N": "NYSE", "A": "NYSE American", "Q": "Nasdaq"}
CONDITIONALTYPE_PERIMETRE = ("RW",)
TRADINGSTATUS_PERIMETRE = ("A", "H", "S")

ISSUERTYPES_COMMON = ("CORP", "ACOR", "REIT")  # C2 : émetteurs des actions ordinaires

BANDE_COHERENCE = (1600, 2400)     # taille attendue de U_t, contrôle non bloquant
BANDE_ANNEES = ("2025",)           # années sur lesquelles la bande est vérifiée

SEUIL_ESCALADE_C9 = 0.01           # C9 : alerte au-delà de 1 % de titres-mois basculants
DECALAGES_C9 = (1, 2, 3)           # retards L du shrout, en fins de mois

SEUIL_REVERSE_SPLIT_JOURS = 80     # strate « reverse split récent » du rapport

# C11 : garde contre les erreurs d'échelle des observations dei. Chaque
# observation est comparée au shrout CRSP, converti en actions, du month-end
# le plus proche, qui ne sert que d'ordre de grandeur.
GARDE_ACCEPT = (0.5, 2.0)          # ratio val / ancre : observation acceptée
GARDE_DIV1000 = (500.0, 2000.0)    # val 1000 fois trop grande -> val / 1000
GARDE_MUL1000 = (1.0 / 2000.0, 1.0 / 500.0)  # 1000 fois trop petite -> val x 1000

# Mêmes bornes, appliquées après la sélection du dépôt applicable à t et
# comparées au shrout CRSP de t lui-même : détecte une valeur EDGAR rendue
# périmée par une émission ou un regroupement postérieur au dernier dépôt, ce
# que la garde d'échelle, ancrée près de la date de l'observation, ne voit pas.
GARDE_DIVERGENCE_SELECTION = GARDE_ACCEPT

SO_SOURCES = ("edgar", "crsp_ads", "crsp_multiclasse", "crsp_nomap",
              "crsp_nodata", "crsp_divergence")

# --- Chemins ---

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import chemins  # noqa: E402
REP_CRSP = str(chemins.DONNEES / "crsp")
REP_EDGAR = str(chemins.DONNEES / "edgar-so")
REP_SORTIE = str(chemins.SORTIES / "univers")

F_MONTHENDS = os.path.join(REP_CRSP, "univers_dsfv2_monthends.csv")
F_SECINFO = os.path.join(REP_CRSP, "univers_secinfohist.csv")
F_DELISTS = os.path.join(REP_CRSP, "univers_delists.csv")
F_DISTRIB = os.path.join(REP_CRSP, "univers_distributions.csv")
# Fichiers optionnels : calendrier de bourse, prix de repli C4 (dernière séance
# avant t, puis fenêtres de séances précédant t), données EDGAR de C11.
F_CALENDRIER = os.path.join(REP_CRSP, "calendrier_bourse.csv")
F_DLYPREV = os.path.join(REP_CRSP, "univers_dlyprev_monthends.csv")
F_FENETRES = os.path.join(REP_CRSP, "univers_fenetres_nonTR.csv")
F_FENETRES_3DATES = os.path.join(REP_CRSP, "fenetres_nonTR_3dates.csv")
F_CIKMAP = os.path.join(REP_EDGAR, "permno_cik_map.csv")             # PERMNO -> CIK
F_SOEDGAR = os.path.join(REP_EDGAR, "so_edgar.csv")                  # dei, en actions

FICHIERS_ENTREE = (F_MONTHENDS, F_SECINFO, F_DELISTS, F_DISTRIB,
                   F_CALENDRIER, F_DLYPREV, F_FENETRES, F_FENETRES_3DATES,
                   F_CIKMAP, F_SOEDGAR)

# --- Utilitaires ---


def _vide(valeur):
    return valeur is None or valeur == "" or valeur == "NA"


def _jour(chaine):
    return _dt.date(int(chaine[0:4]), int(chaine[5:7]), int(chaine[8:10]))


def _sha256(chemin):
    h = hashlib.sha256()
    with open(chemin, "rb") as flux:
        for bloc in iter(lambda: flux.read(1 << 20), b""):
            h.update(bloc)
    return h.hexdigest()


def _quantile(serie_triee, q):
    """Quantile par rang le plus proche, sans interpolation."""
    if not serie_triee:
        return None
    rang = int(q * len(serie_triee))
    if rang >= len(serie_triee):
        rang = len(serie_triee) - 1
    return serie_triee[rang]


class Calendrier:
    """Compte les jours en jours de bourse si un calendrier est fourni, en jours
    calendaires sinon (les colonnes de sortie portent alors le suffixe `_cal`)."""

    def __init__(self, dates=None):
        self.dispo = dates is not None
        self.dates = sorted(dates) if dates else []
        self.unite = "jours de bourse" if self.dispo else "jours calendaires"
        self.suffixe = "" if self.dispo else "_cal"
        self.hors_couverture = 0  # ancres anterieures au debut du calendrier

    def ecart(self, debut, fin):
        """Nombre de jours dans (debut, fin], ou None si `debut` précède le
        calendrier de bourse (ces cas sont comptés dans stats.md)."""
        if not self.dispo:
            return (_jour(fin) - _jour(debut)).days
        if debut < self.dates[0]:
            self.hors_couverture += 1
            return None
        return bisect.bisect_right(self.dates, fin) - bisect.bisect_right(self.dates, debut)


# --- Chargement des entrées ---


def charger_monthends(chemin):
    """Lignes CRSP de fin de mois groupées par t et triées par permno, plus
    l'index (permno, t) -> shrout utilisé par l'audit C9."""
    par_date = defaultdict(list)
    shrout_pit = {}
    with open(chemin, newline="") as flux:
        for ligne in csv.DictReader(flux):
            date_t = ligne["dlycaldt"]
            ligne["permno"] = int(ligne["permno"])
            par_date[date_t].append(ligne)
            shrout_pit[(ligne["permno"], date_t)] = ligne["shrout"]
    for date_t in par_date:
        par_date[date_t].sort(key=lambda l: l["permno"])
    return dict(par_date), shrout_pit


def charger_secinfohist(chemin):
    """Intervalles d'attributs par permno et date d'ancrage de l'âge de cotation.

    Les lignes « Last Known » publiées après un delisting (sharetype 'N/A' ou
    securitysubtype 'UNK') sont ignorées. L'ancre est le premier début
    d'intervalle sur un marché du périmètre : elle est unique, si bien qu'un
    passage temporaire par l'OTC ne remet pas l'âge à zéro.
    """
    intervalles = defaultdict(list)
    ancre = {}
    with open(chemin, newline="") as flux:
        for ligne in csv.DictReader(flux):
            if ligne["sharetype"] == "N/A" or ligne["securitysubtype"] == "UNK":
                continue
            permno = int(ligne["permno"])
            debut, fin = ligne["secinfostartdt"], ligne["secinfoenddt"]
            intervalles[permno].append((debut, fin, ligne["permco"]))
            if ligne["primaryexch"] in EXCHANGES_PERIMETRE:
                if permno not in ancre or debut < ancre[permno]:
                    ancre[permno] = debut
    for permno in intervalles:
        intervalles[permno].sort()
    return dict(intervalles), ancre


def charger_delists(chemin):
    """Première date de delisting par permno."""
    delistings = {}
    with open(chemin, newline="") as flux:
        for ligne in csv.DictReader(flux):
            permno = int(ligne["permno"])
            date_d = ligne["delistingdt"]
            if _vide(date_d):
                continue
            if permno not in delistings or date_d < delistings[permno]:
                delistings[permno] = date_d
    return delistings


def charger_reverse_splits(chemin):
    """Reverse splits par permno, en liste triée de (disexdt, disfacpr).

    Un reverse split est une distribution 'FRS' de facteur dans (-1, 0) ; un
    facteur de -1 correspond à une liquidation.
    """
    evenements = defaultdict(list)
    with open(chemin, newline="") as flux:
        for ligne in csv.DictReader(flux):
            if ligne["distype"] != "FRS":
                continue
            facteur = ligne["disfacpr"]
            if _vide(facteur):
                continue
            valeur = float(facteur)
            if not (-1.0 < valeur < 0.0):
                continue
            date_ex = ligne["disexdt"]
            if _vide(date_ex):
                continue
            evenements[int(ligne["permno"])].append((date_ex, valeur))
    for permno in evenements:
        evenements[permno].sort()
    return dict(evenements)


def charger_calendrier(chemin):
    """Liste triée des jours de bourse, ou None si le fichier est absent.

    La colonne de dates est la première dont le nom est reconnu, à défaut la
    première colonne du fichier.
    """
    if not os.path.exists(chemin):
        return None
    with open(chemin, newline="") as flux:
        lecteur = csv.DictReader(flux)
        colonnes = lecteur.fieldnames or []
        cible = None
        for candidate in ("caldt", "dlycaldt", "date", "tradingdt", "trading_date"):
            if candidate in colonnes:
                cible = candidate
                break
        if cible is None:
            if not colonnes:
                raise ValueError("calendrier_bourse.csv : fichier sans en-tete")
            cible = colonnes[0]
        dates = sorted({l[cible] for l in lecteur if not _vide(l[cible])})
    if not dates:
        raise ValueError("calendrier_bourse.csv : aucune date lue")
    return dates


def charger_dlyprev(chemin):
    """Prix antérieurs à t par (permno, t), en liste de (date, prix, flag).

    Lit les champs CRSP dlyprevdt / dlyprevprc / dlyprevprcflg de la ligne du
    jour t, qui ne donnent que la dernière séance cotée avant t. Ce fichier
    sert de repli pour les cibles absentes des fenêtres complètes
    (`charger_fenetres`).
    """
    if not os.path.exists(chemin):
        return None
    par_cle = defaultdict(list)
    with open(chemin, newline="") as flux:
        lecteur = csv.DictReader(flux)
        colonnes = set(lecteur.fieldnames or [])
        requis = {"permno", "dlycaldt", "dlyprevdt", "dlyprevprc", "dlyprevprcflg"}
        if not requis.issubset(colonnes):
            raise ValueError(
                "univers_dlyprev_monthends.csv : schema inattendu (colonnes "
                + ", ".join(sorted(colonnes))
                + "). Attendu au minimum : "
                + ", ".join(sorted(requis))
                + ". Revoir le chargement avant toute construction."
            )
        for ligne in lecteur:
            date_prev = ligne["dlyprevdt"]
            if _vide(date_prev) or _vide(ligne["dlyprevprc"]):
                continue
            cle = (int(ligne["permno"]), ligne["dlycaldt"])
            par_cle[cle].append((date_prev, ligne["dlyprevprc"], ligne["dlyprevprcflg"]))
    for cle in par_cle:
        par_cle[cle].sort()
    return dict(par_cle)


def charger_fenetres(chemin, chemin_complement=None):
    """Fenêtres de séances précédant t pour la règle C4, ou None si absentes.

    Une ligne par séance des 9 jours calendaires précédant t, pour chaque
    (permno, t) sans prix négocié à t. Le complément optionnel, de même schéma,
    étend la fenêtre à 15 jours calendaires pour trois dates où 9 jours ne
    contenaient que 4 séances ; les lignes sont fusionnées sans doublon sur
    (permno, t, dlycaldt), les doublons portant des valeurs identiques.

    Retourne (prix exploitables par (permno, t), ensemble des cibles couvertes).
    Une cible dont toutes les séances sont sans prix reste couverte avec une
    liste vide, et ne retombe donc pas sur `dlyprev`.
    """
    if not os.path.exists(chemin):
        return None
    par_cle = defaultdict(list)
    couvertes = set()
    vues = set()  # (permno, t, dlycaldt) déjà lus

    def _charger_un(fichier):
        with open(fichier, newline="") as flux:
            lecteur = csv.DictReader(flux)
            colonnes = set(lecteur.fieldnames or [])
            requis = {"permno", "t", "dlycaldt", "dlyprc", "dlyprcflg"}
            if not requis.issubset(colonnes):
                raise ValueError(
                    "%s : schema inattendu (colonnes " % os.path.basename(fichier)
                    + ", ".join(sorted(colonnes)) + "). Attendu au minimum : "
                    + ", ".join(sorted(requis)))
            for ligne in lecteur:
                cle = (int(ligne["permno"]), ligne["t"])
                couvertes.add(cle)
                if ligne["dlycaldt"] >= ligne["t"]:
                    raise ValueError(
                        "%s : seance %s posterieure ou egale "
                        "a t=%s (permno %s) ; la fenetre C4 doit etre strictement "
                        "anterieure a t." % (os.path.basename(fichier),
                                             ligne["dlycaldt"], ligne["t"],
                                             ligne["permno"]))
                cle_ligne = (cle[0], cle[1], ligne["dlycaldt"])
                if cle_ligne in vues:
                    continue
                vues.add(cle_ligne)
                if _vide(ligne["dlyprc"]):
                    continue
                par_cle[cle].append((ligne["dlycaldt"], ligne["dlyprc"],
                                     ligne["dlyprcflg"]))

    _charger_un(chemin)
    if chemin_complement and os.path.exists(chemin_complement):
        _charger_un(chemin_complement)
    for cle in par_cle:
        par_cle[cle].sort()
    return dict(par_cle), couvertes


# --- C11 : nombre d'actions daté par les dépôts EDGAR ---
#
# Le shrout CRSP de fin de mois peut refléter une information publiée après t.
# C11 lui substitue la valeur dei:EntityCommonStockSharesOutstanding du dernier
# dépôt EDGAR dont la date `filed` est antérieure ou égale à t, hors ADS et
# titres multi-classes, avec repli sur CRSP et une source `so_source` explicite.


def charger_cikmap(chemin):
    """Correspondance permno -> CIK et nombre de permno ambigus, ou None.

    Un permno associé à plusieurs CIK est retiré de la correspondance plutôt
    que résolu arbitrairement.
    """
    if not os.path.exists(chemin):
        return None
    vus = {}
    ambigus = set()
    with open(chemin, newline="") as flux:
        for ligne in csv.DictReader(flux):
            permno = int(ligne["permno"])
            cik = ligne["cik"]
            if permno in vus and vus[permno] != cik:
                ambigus.add(permno)
            vus[permno] = cik
    for permno in ambigus:
        del vus[permno]
    return vus, len(ambigus)


def charger_so_edgar(chemin):
    """Observations dei brutes par permno, en (filed, asof, val), ou None.

    `val` est en actions ; la garde d'échelle est appliquée ensuite.
    """
    if not os.path.exists(chemin):
        return None
    par_permno = defaultdict(list)
    with open(chemin, newline="") as flux:
        lecteur = csv.DictReader(flux)
        requis = {"permno", "val", "asof", "filed"}
        if not requis.issubset(set(lecteur.fieldnames or [])):
            raise ValueError("so_edgar.csv : schema inattendu")
        for ligne in lecteur:
            if _vide(ligne["val"]) or _vide(ligne["filed"]):
                continue
            par_permno[int(ligne["permno"])].append(
                (ligne["filed"], ligne["asof"], float(ligne["val"])))
    return dict(par_permno)


def construire_ancres_shrout(shrout_pit):
    """Shrout CRSP converti en actions par permno : (dates triées, valeurs).

    Sert d'ordre de grandeur à la garde d'échelle ; les shrout vides ou nuls
    sont ignorés.
    """
    brut = defaultdict(list)
    for (permno, date_t), shrout in shrout_pit.items():
        if _vide(shrout):
            continue
        valeur = float(shrout)
        if valeur <= 0:
            continue
        brut[permno].append((date_t, valeur * 1000.0))
    ancres = {}
    for permno, elements in brut.items():
        elements.sort()
        ancres[permno] = ([e[0] for e in elements], [e[1] for e in elements])
    return ancres


def ancre_la_plus_proche(ancres_permno, date_obs):
    """Shrout, en actions, du month-end le plus proche de `date_obs`, avant ou
    après ; à égale distance, le plus ancien."""
    if ancres_permno is None or _vide(date_obs):
        return None
    dates, valeurs = ancres_permno
    indice = bisect.bisect_left(dates, date_obs)
    meilleur = None
    for candidat in (indice - 1, indice):
        if 0 <= candidat < len(dates):
            distance = abs((_jour(dates[candidat]) - _jour(date_obs)).days)
            if meilleur is None or distance < meilleur[0]:
                meilleur = (distance, valeurs[candidat])
    return None if meilleur is None else meilleur[1]


def garde_echelle(val, ancre_actions):
    """Corrige ou rejette une observation dei selon son ratio à l'ancre CRSP.

    Retourne (valeur retenue ou None, verdict), le verdict étant 'accepte',
    'corrige_div1000', 'corrige_mul1000', 'rejete' ou 'sans_ancre'. Une valeur
    nulle ou négative est rejetée même sans ancre.
    """
    if val <= 0:
        return None, "rejete"
    if ancre_actions is None or ancre_actions <= 0:
        return val, "sans_ancre"
    ratio = val / ancre_actions
    if GARDE_ACCEPT[0] <= ratio <= GARDE_ACCEPT[1]:
        return val, "accepte"
    if GARDE_DIV1000[0] <= ratio <= GARDE_DIV1000[1]:
        return val / 1000.0, "corrige_div1000"
    if GARDE_MUL1000[0] <= ratio <= GARDE_MUL1000[1]:
        return val * 1000.0, "corrige_mul1000"
    return None, "rejete"


def indexer_sans_garde(so_brut):
    """Index des observations dei sans garde d'échelle, utilisé seulement pour
    mesurer l'effet de la garde dans stats.md."""
    index = {}
    for permno, observations in so_brut.items():
        triees = sorted(observations)
        index[permno] = ([o[0] for o in triees], triees)
    return index


def preparer_so_edgar(so_brut, ancres_shrout, date_reference="asof",
                      debut_fenetre=None, verdicts_hors=None,
                      mesure_second_passage=None, compteur_clamp=None):
    """Applique la garde d'échelle à chaque observation, puis indexe par `filed`.

    La garde ne dépend pas de t. Elle est ancrée sur `asof`, date à laquelle
    le décompte se rapporte ; l'ancrage sur `filed` sert de contrôle de
    robustesse. Quand `asof` est postérieur à `filed` (décalage de quelques
    jours ou métadonnée aberrante), l'ancrage est ramené à `filed`, date de
    disponibilité publique de la valeur, qui est conservée.

    Retourne ({permno: (dates filed, observations)}, Counter des verdicts),
    les observations étant triées par (filed, asof, val).
    """
    verdicts = Counter()
    retenues = defaultdict(list)
    for permno, observations in so_brut.items():
        ancres_permno = ancres_shrout.get(permno)
        for filed, asof, val in observations:
            date_obs = asof if date_reference == "asof" else filed
            if _vide(date_obs):
                date_obs = filed if date_reference == "asof" else asof
            if not _vide(asof) and not _vide(filed) and asof > filed:
                if compteur_clamp is not None:
                    compteur_clamp["asof_clampe_sur_filed"] += 1
                date_obs = min(date_obs, filed)
            ancre = ancre_la_plus_proche(ancres_permno, date_obs)
            valeur, verdict = garde_echelle(val, ancre)
            verdicts[verdict] += 1
            if verdicts_hors is not None and debut_fenetre is not None:
                # Une observation antérieure à la période d'étude est ancrée sur
                # le premier month-end disponible, parfois des années plus tard :
                # son ratio reflète alors l'évolution du nombre d'actions.
                periode = ("avant_fenetre" if date_obs < debut_fenetre
                           else "dans_fenetre")
                verdicts_hors[(verdict, periode)] += 1
            if verdict in ("corrige_div1000", "corrige_mul1000") \
                    and mesure_second_passage is not None:
                # Une valeur corrigée repassée dans la garde doit être acceptée.
                mesure_second_passage["total"] += 1
                _, verdict2 = garde_echelle(valeur, ancre)
                if verdict2 == "accepte":
                    mesure_second_passage["stable"] += 1
            if valeur is None:
                continue
            retenues[permno].append((filed, asof, valeur))
    index = {}
    for permno, observations in retenues.items():
        observations.sort()
        index[permno] = ([o[0] for o in observations], observations)
    return index, verdicts


def selectionner_so(index_permno, date_t):
    """Observation dei applicable à t, (val, filed) ou None : `filed` <= t
    maximal, puis `asof` maximal, puis `val` minimale."""
    if index_permno is None:
        return None
    dates_filed, observations = index_permno
    borne = bisect.bisect_right(dates_filed, date_t)
    if borne == 0:
        return None
    filed_max = dates_filed[borne - 1]
    debut = borne - 1
    while debut > 0 and dates_filed[debut - 1] == filed_max:
        debut -= 1
    bloc = observations[debut:borne]
    asof_max = max(o[1] for o in bloc)
    # Le bloc étant trié par (filed, asof, val), le premier élément d'asof
    # maximal porte la val minimale.
    for _filed, asof, val in bloc:
        if asof == asof_max:
            return val, filed_max
    return None


def resoudre_so(permno, date_t, ads, multi_classe, shrout_chaine, contexte):
    """Nombre d'actions retenu pour la capitalisation à t (C5, C11).

    Retourne (so en actions, so_source, so_filed_date). La conversion des
    milliers CRSP en actions est faite ici et nulle part ailleurs. Les ADS et
    les titres multi-classes restent sur CRSP : la couverture EDGAR des
    émetteurs étrangers est pauvre et le dei de page de garde agrège les classes.
    """
    shrout_actions = float(shrout_chaine) * 1000.0
    if not contexte["utiliser_edgar"]:
        # Variante sans EDGAR, utilisée seulement pour décomposer les effets ;
        # cette source n'est jamais écrite dans un U_t.
        return shrout_actions, "crsp_couche_desactivee", ""
    if ads:
        return shrout_actions, "crsp_ads", ""
    if multi_classe:
        return shrout_actions, "crsp_multiclasse", ""
    if permno not in contexte["cikmap"]:
        return shrout_actions, "crsp_nomap", ""
    choix = selectionner_so(contexte["so_index"].get(permno), date_t)
    if choix is None:
        return shrout_actions, "crsp_nodata", ""
    val, filed = choix
    # Le dernier dépôt peut dater de loin : une valeur hors de [0,5 ; 2] fois
    # le shrout CRSP de t est jugée périmée (par exemple après une forte
    # dilution) et remplacée par CRSP. Avec un shrout CRSP nul, le ratio n'est
    # pas défini et la valeur EDGAR est conservée.
    if shrout_actions > 0:
        ratio = val / shrout_actions
        if not (GARDE_DIVERGENCE_SELECTION[0] <= ratio <= GARDE_DIVERGENCE_SELECTION[1]):
            return shrout_actions, "crsp_divergence", ""
    return val, "edgar", filed


# --- Règles C2, C3 et C4 ---


def classer_type(ligne):
    """C2 : type de titre lu sur la ligne du jour t.

    Sont éligibles les actions ordinaires (NS / EQTY / COM, émetteur CORP, ACOR
    ou REIT) et les ADS (AD / EQTY / COM) ; ETF, fonds fermés, units et SBI
    sont exclus. Retourne (eligible, ads, reit, etranger).
    """
    sharetype = ligne["sharetype"]
    securitytype = ligne["securitytype"]
    soustype = ligne["securitysubtype"]
    issuertype = ligne["issuertype"]

    common = (
        sharetype == "NS"
        and securitytype == "EQTY"
        and soustype == "COM"
        and issuertype in ISSUERTYPES_COMMON
    )
    ads = sharetype == "AD" and securitytype == "EQTY" and soustype == "COM"
    if not (common or ads):
        return False, False, False, False
    return True, ads, issuertype == "REIT", ligne["usincflg"] == "N"


def cause_perimetre(ligne):
    """C3 : None si la ligne est dans le périmètre, sinon la cause d'exclusion."""
    if ligne["primaryexch"] not in EXCHANGES_PERIMETRE:
        return "exchange_hors_perimetre"
    if ligne["conditionaltype"] not in CONDITIONALTYPE_PERIMETRE:
        return "conditionaltype_hors_perimetre"
    if ligne["tradingstatusflg"] not in TRADINGSTATUS_PERIMETRE:
        return "tradingstatus_hors_perimetre"
    return None


def close_ref(ligne, precedents, calendrier, repli_dispo):
    """C4 : prix de référence à t.

    Par ordre de priorité : prix négocié à t ('TR') ; dernier prix négocié dans
    les 5 jours de bourse précédents ('reporte') ; prix bid/ask à t, puis le
    plus récent de la fenêtre ('bidask'). Sinon le titre est 'suspendu'. Sans
    aucune source de prix antérieurs, un titre sans prix négocié à t est mis de
    côté ('en_attente_passe3'), ni inclus ni exclu.

    `precedents` est la liste triée des (date, prix, flag) antérieurs à t.
    Retourne (prix, flag, écart en jours, cause d'échec).
    """
    if ligne["dlyprcflg"] == "TR" and not _vide(ligne["dlyprc"]):
        return ligne["dlyprc"], "TR", None, None

    if not repli_dispo:
        return None, None, None, "en_attente_passe3"

    date_t = ligne["dlycaldt"]
    fenetre = []
    for date_p, prix_p, flag_p in precedents:
        if date_p >= date_t:
            continue
        ecart = calendrier.ecart(date_p, date_t)
        if ecart is None or ecart > FENETRE_REPORT:
            continue
        fenetre.append((date_p, prix_p, flag_p, ecart))

    candidats_tr = [c for c in fenetre if c[2] == "TR"]
    if candidats_tr:
        date_p, prix_p, _, ecart = max(candidats_tr, key=lambda c: c[0])
        return prix_p, "reporte", ecart, None

    if ligne["dlyprcflg"] == "BA" and not _vide(ligne["dlyprc"]):
        return ligne["dlyprc"], "bidask", 0, None
    candidats_ba = [c for c in fenetre if c[2] == "BA"]
    if candidats_ba:
        date_p, prix_p, _, ecart = max(candidats_ba, key=lambda c: c[0])
        return prix_p, "bidask", ecart, None

    return None, None, None, "suspendu"


# --- Construction d'une date t ---


def construire_date(date_t, lignes, contexte):
    """Construit U_t. Retourne (retenus, statuts, comptages, evaluables, c4).

    `statuts` donne la cause d'exclusion des permno présents à t mais absents
    de U_t. `evaluables` liste les titres qui franchissent C2 à C4 avec un
    shrout renseigné, retenus ou écartés par C5/C6 : c'est la population de
    l'audit C9, qui doit voir les entrées comme les sorties. `c4` donne l'issue
    de la règle C4 pour chaque titre ayant franchi C2 et C3.
    """
    secinfo = contexte["secinfo"]
    ancres = contexte["ancres"]
    splits = contexte["splits"]
    calendrier = contexte["calendrier"]
    dlyprev = contexte["dlyprev"]
    fenetres = contexte["fenetres"] if contexte["utiliser_fenetres"] else None
    couvertes = contexte["fenetres_couvertes"] if contexte["utiliser_fenetres"] else ()
    repli_dispo = dlyprev is not None or fenetres is not None
    origine_fenetre = contexte["origine_fenetre"]

    statuts = {}
    comptages = Counter()
    attente_detail = Counter()
    survivants = []  # titres ayant franchi C2 et C3

    for ligne in lignes:
        permno = ligne["permno"]
        eligible, ads, reit, etranger = classer_type(ligne)
        if not eligible:
            statuts[permno] = "type_hors_perimetre"
            continue
        cause = cause_perimetre(ligne)
        if cause is not None:
            statuts[permno] = cause
            continue
        survivants.append((ligne, ads, reit, etranger))

    # C7, multi_classe : au moins deux permno d'un même permco franchissent
    # C2/C3 à t, avant tout filtre de cap ou de prix.
    permco_de = {}
    compte_permco = Counter()
    for ligne, _ads, _reit, _etr in survivants:
        permno = ligne["permno"]
        permco = ""
        for debut, fin, code_permco in secinfo.get(permno, ()):
            if debut <= date_t <= fin:
                permco = code_permco
                break
        permco_de[permno] = permco
        if permco:
            compte_permco[permco] += 1

    retenus = {}
    evaluables = []
    c4_issues = {}
    for ligne, ads, reit, etranger in survivants:
        permno = ligne["permno"]

        # Prix antérieurs pour C4 : la fenêtre complète si la cible y figure,
        # sinon la seule dernière séance fournie par `dlyprev`.
        cle = (permno, date_t)
        if fenetres is not None and cle in couvertes:
            precedents = fenetres.get(cle, ())
            if ligne["dlyprcflg"] != "TR":
                origine_fenetre["passe4"] += 1
        elif dlyprev is not None:
            precedents = dlyprev.get(cle, ())
            if ligne["dlyprcflg"] != "TR":
                origine_fenetre["dlyprev"] += 1
        else:
            precedents = ()
            if ligne["dlyprcflg"] != "TR":
                origine_fenetre["aucune"] += 1

        prix, flag_prix, ecart, echec = close_ref(
            ligne, precedents, calendrier, repli_dispo,
        )
        c4_issues[permno] = flag_prix or echec
        if echec is not None:
            statuts[permno] = echec
            if echec == "en_attente_passe3":
                attente_detail[ligne["dlyprcflg"]] += 1
                # Estimation pour stats.md seulement : le titre entrerait-il
                # dans U_t avec le prix bid/ask du jour t ?
                if ligne["dlyprcflg"] == "BA" and not _vide(ligne["dlyprc"]):
                    valeur = float(ligne["dlyprc"])
                    if not _vide(ligne["shrout"]):
                        cap = valeur * float(ligne["shrout"]) * 1000.0
                        if cap < CAP_MAX_USD and valeur >= PRIX_PLANCHER:
                            comptages["attente_p3_entrerait_si_ba"] += 1
            continue

        if _vide(ligne["shrout"]):
            statuts[permno] = "shrout_manquant"
            continue

        valeur = float(prix)
        # multi_classe détermine aussi si EDGAR peut fournir le nombre d'actions.
        multi_classe = bool(permco_de[permno]) and compte_permco[permco_de[permno]] >= 2
        so_actions, so_source, so_filed = resoudre_so(
            permno, date_t, ads, multi_classe, ligne["shrout"], contexte)
        cap = valeur * so_actions            # so_actions est en actions
        cap_crsp = valeur * float(ligne["shrout"]) * 1000.0  # pour le contrôle C10
        evaluables.append((permno, valeur, ligne["shrout"], so_source, cap))
        if cap >= CAP_MAX_USD:               # C5
            statuts[permno] = "cap_hors_borne"
            continue
        if valeur < PRIX_PLANCHER:           # C6
            statuts[permno] = "prix_plancher"
            continue

        # C7 : attributs descriptifs, sans effet sur l'appartenance.
        if valeur < 1.0:
            tranche = "[0.10-1)"
        elif valeur < 5.0:
            tranche = "[1-5)"
        else:
            tranche = "[5+)"
        nano_micro = "nano" if cap < CAP_NANO_MAX_USD else "micro"

        ancre = ancres.get(permno)
        age = calendrier.ecart(ancre, date_t) if ancre and ancre <= date_t else None

        jours_split, ratio_split = None, None
        for date_ex, facteur in reversed(splits.get(permno, ())):
            if date_ex <= date_t:
                jours_split = calendrier.ecart(date_ex, date_t)
                ratio_split = 1.0 / (1.0 + facteur)
                break

        retenus[permno] = {
            "permno": str(permno),
            "permco": permco_de[permno],
            "ticker": ligne["ticker"],
            "exchange": ligne["primaryexch"],
            "close_ref": prix,
            "close_ref_flag": flag_prix,
            "cap_usd": "%.2f" % cap,
            "shrout_milliers": ligne["shrout"],
            "tranche_prix": tranche,
            "nano_micro": nano_micro,
            "reit": str(reit),
            "etranger": str(etranger),
            "ads": str(ads),
            "multi_classe": str(multi_classe),
            "close_reporte_ecart": "" if ecart is None else str(ecart),
            "age_cotation_jours" + calendrier.suffixe: "" if age is None else str(age),
            "jours_depuis_reverse_split" + calendrier.suffixe:
                "" if jours_split is None else str(jours_split),
            "ratio_reverse_split": "" if ratio_split is None else "%.6f" % ratio_split,
            "so_source": so_source,
            "so_filed_date": so_filed,
            # champs internes, non écrits, pour le contrôle C10
            "_dlycap": ligne["dlycap"],
            "_dlycapflg": ligne["dlycapflg"],
            "_cap_crsp": cap_crsp,
            "_ads": ads,
        }

    comptages["candidats"] = len(lignes)
    comptages["survivants_c2c3"] = len(survivants)
    comptages["retenus"] = len(retenus)
    for cause, nombre in Counter(statuts.values()).items():
        comptages["exclus_" + cause] = nombre
    for flag, nombre in attente_detail.items():
        comptages["attente_p3_flg_" + flag] = nombre
    evaluables.sort()
    return retenus, statuts, comptages, evaluables, c4_issues


def cause_sortie(permno, date_prec, date_t, permno_a_t, delistings, statut_t):
    """Cause de sortie, (cause, détail), d'un permno présent à t-1 et absent à t.

    Un delisting dans (t-1, t] l'emporte, même si CRSP publie encore une ligne
    « Last Known » à t, qui recevrait sinon une autre cause d'exclusion.
    """
    date_del = delistings.get(permno)
    if date_del is not None and date_prec < date_del <= date_t:
        return "deliste", date_del
    if not permno_a_t:
        return "absent_dsf", date_del or ""
    return statut_t.get(permno, "inconnu"), ""


def colonnes_sortie(calendrier):
    return [
        "permno", "permco", "ticker", "exchange",
        "close_ref", "close_ref_flag", "cap_usd", "shrout_milliers",
        "tranche_prix", "nano_micro",
        "reit", "etranger", "ads", "multi_classe",
        "close_reporte_ecart",
        "age_cotation_jours" + calendrier.suffixe,
        "jours_depuis_reverse_split" + calendrier.suffixe,
        "ratio_reverse_split",
        # Origine du nombre d'actions ; `shrout_milliers` reste le shrout CRSP
        # brut même quand so_source vaut 'edgar'.
        "so_source",
        "so_filed_date",
    ]


def ecrire_univers(chemin, colonnes, retenus):
    with open(chemin, "w", newline="\n") as flux:
        graveur = csv.writer(flux, lineterminator="\n")
        graveur.writerow(colonnes)
        for permno in sorted(retenus):
            enreg = retenus[permno]
            # La source interne `crsp_couche_desactivee` ne doit pas être écrite.
            assert enreg["so_source"] in SO_SOURCES, (
                "so_source inattendue : %r (permno %s)"
                % (enreg["so_source"], permno))
            graveur.writerow([enreg[colonne] for colonne in colonnes])


# --- Audits C9 et C10 ---


def audit_c9(dates, membres, evaluables_par_date, shrout_pit):
    """C9 : sensibilité de l'appartenance à un shrout retardé de L fins de mois.

    Le shrout CRSP de fin de mois peut intégrer une information postérieure à t ;
    on recalcule l'appartenance avec shrout(t-L), le prix restant celui de t.
    Un titre sans ligne à t-L garde shrout(t). Les lignes dont le nombre
    d'actions vient d'EDGAR, déjà datées par leur dépôt, ne sont pas perturbées
    mais restent dans les dénominateurs : Σ|U_t|, Σ|U_t ∪ U_alt_t| et le nombre
    de candidats ayant franchi C2 à C4.

    Retourne, pour chaque L, les comptages globaux et par `so_source`.
    """
    resultats = {}
    for decalage in DECALAGES_C9:
        gagnes = 0
        perdus = 0
        base_u = 0
        base_union = 0
        base_cand = 0
        sans_ligne = 0
        par_source = defaultdict(lambda: {"entrees": 0, "sorties": 0, "base_u": 0,
                                          "base_cand": 0})
        for indice, date_t in enumerate(dates):
            if indice < decalage:
                continue  # t-L hors de la période
            date_ref = dates[indice - decalage]
            membres_t = membres[date_t]
            base_u += len(membres_t)
            base_cand += len(evaluables_par_date[date_t])
            union = set(membres_t)
            for permno, prix, shrout_t, so_source, cap_base in evaluables_par_date[date_t]:
                dans_base = permno in membres_t
                compteur = par_source[so_source]
                compteur["base_cand"] += 1
                if dans_base:
                    compteur["base_u"] += 1
                if so_source == "edgar":
                    cap_alt = cap_base
                else:
                    shrout_alt = shrout_pit.get((permno, date_ref))
                    if shrout_alt is None or _vide(shrout_alt):
                        sans_ligne += 1
                        shrout_alt = shrout_t
                    cap_alt = prix * float(shrout_alt) * 1000.0
                dans_alt = cap_alt < CAP_MAX_USD and prix >= PRIX_PLANCHER
                if dans_alt:
                    union.add(permno)
                if dans_alt and not dans_base:
                    gagnes += 1
                    compteur["entrees"] += 1
                elif dans_base and not dans_alt:
                    perdus += 1
                    compteur["sorties"] += 1
            base_union += len(union)
        resultats[decalage] = {
            "entrees": gagnes, "sorties": perdus, "bascules": gagnes + perdus,
            "base_u": base_u, "base_union": base_union, "base_cand": base_cand,
            "sans_ligne": sans_ligne,
            "par_source": {cle: dict(valeur) for cle, valeur in par_source.items()},
        }
    return resultats


def construire_variante(dates, par_date, contexte, utiliser_fenetres, utiliser_edgar):
    """Reconstruit l'appartenance avec ou sans fenêtres C4 complètes et avec ou
    sans nombre d'actions EDGAR, pour séparer leurs effets dans stats.md.

    N'écrit aucun fichier. Retourne (membres, issues C4 par (permno, t)).
    """
    ctx = dict(contexte)
    ctx["utiliser_fenetres"] = utiliser_fenetres and contexte["fenetres"] is not None
    ctx["utiliser_edgar"] = utiliser_edgar and contexte["utiliser_edgar"]
    ctx["origine_fenetre"] = Counter()
    membres = {}
    issues = {}
    for date_t in dates:
        retenus, _statuts, _comptages, _evaluables, c4 = construire_date(
            date_t, par_date[date_t], ctx)
        membres[date_t] = set(retenus)
        for permno, issue in c4.items():
            issues[(permno, date_t)] = issue
    return membres, issues


def delta_appartenance(dates, membres_a, membres_b):
    """(entrants, sortants) de A vers B, en titres-mois."""
    entrants = sum(len(membres_b[t] - membres_a[t]) for t in dates)
    sortants = sum(len(membres_a[t] - membres_b[t]) for t in dates)
    return entrants, sortants


def audit_c10(ratios):
    """C10 : distribution du ratio cap recalculé / dlycap CRSP."""
    if not ratios:
        return None
    serie = sorted(ratios)
    return {
        "n": len(serie),
        "min": serie[0],
        "p01": _quantile(serie, 0.01),
        "p25": _quantile(serie, 0.25),
        "mediane": _quantile(serie, 0.50),
        "p75": _quantile(serie, 0.75),
        "p99": _quantile(serie, 0.99),
        "max": serie[-1],
    }


# --- Pipeline ---


def executer():
    # Les sorties sont écrites dans un répertoire temporaire voisin, renommé en
    # `univers/` seulement en fin de construction : une erreur en cours de route
    # laisse les sorties précédentes intactes.
    global REP_SORTIE
    rep_sortie_final = REP_SORTIE
    rep_sortie_tmp = rep_sortie_final + ".tmp"
    rep_sortie_bak = rep_sortie_final + ".bak"
    if os.path.exists(rep_sortie_tmp):
        shutil.rmtree(rep_sortie_tmp)  # reste d'une exécution interrompue
    os.makedirs(rep_sortie_tmp)
    REP_SORTIE = rep_sortie_tmp

    print("[1/8] chargement des entrees")
    par_date, shrout_pit = charger_monthends(F_MONTHENDS)
    secinfo, ancres = charger_secinfohist(F_SECINFO)
    delistings = charger_delists(F_DELISTS)
    splits = charger_reverse_splits(F_DISTRIB)

    dates_calendrier = charger_calendrier(F_CALENDRIER)
    calendrier = Calendrier(dates_calendrier)
    dlyprev = charger_dlyprev(F_DLYPREV)
    charge_fenetres = charger_fenetres(F_FENETRES, F_FENETRES_3DATES)
    fenetres, fenetres_couvertes = charge_fenetres if charge_fenetres else (None, set())
    if (dlyprev is not None or fenetres is not None) and not calendrier.dispo:
        raise SystemExit(
            "un fichier de prix anterieurs (C4) est present mais "
            "calendrier_bourse.csv est absent : la fenetre de 5 jours de bourse "
            "ne peut pas etre evaluee."
        )

    print("[2/8] couche C11 (shares outstanding datees EDGAR)")
    charge_map = charger_cikmap(F_CIKMAP)
    cikmap, cik_ambigus = charge_map if charge_map else (None, 0)
    so_brut = charger_so_edgar(F_SOEDGAR)
    ancres_shrout = construire_ancres_shrout(shrout_pit)
    couche_edgar = cikmap is not None and so_brut is not None
    verdicts_periode = Counter()
    second_passage = Counter()
    compteur_clamp = Counter()
    index_sans_garde = {}
    if couche_edgar:
        so_index, verdicts_garde = preparer_so_edgar(
            so_brut, ancres_shrout, "asof", min(par_date), verdicts_periode,
            mesure_second_passage=second_passage, compteur_clamp=compteur_clamp)
        # Contrôle de robustesse : même garde ancrée sur `filed` au lieu d'`asof`.
        _index_filed, verdicts_filed = preparer_so_edgar(so_brut, ancres_shrout, "filed")
        index_sans_garde = indexer_sans_garde(so_brut)
    else:
        so_index, verdicts_garde, verdicts_filed = {}, Counter(), Counter()
        cikmap = {}
        print("    fichiers EDGAR absents : cap calcule sur shrout CRSP partout")

    contexte = {
        "secinfo": secinfo, "ancres": ancres, "splits": splits,
        "calendrier": calendrier, "dlyprev": dlyprev,
        "fenetres": fenetres, "fenetres_couvertes": fenetres_couvertes,
        "cikmap": cikmap, "so_index": so_index,
        "utiliser_fenetres": fenetres is not None,
        "utiliser_edgar": couche_edgar,
        "origine_fenetre": Counter(),
    }
    dates = sorted(par_date)
    colonnes = colonnes_sortie(calendrier)
    print("    %d dates de reconstitution, unite de comptage : %s"
          % (len(dates), calendrier.unite))

    print("[3/8] construction des %d univers" % len(dates))
    membres = {}
    statuts_par_date = {}
    comptages_par_date = {}
    stats_par_date = {}
    evaluables_par_date = {}
    ecarts_c4 = Counter()
    ratios_c10 = {"toutes": [], "TR": [], "non_TR": [], "ads": []}
    lignes_sans_dlycap = 0
    impact_garde = Counter()

    issues_c4_v11 = {}
    for date_t in dates:
        retenus, statuts, comptages, evaluables, c4 = construire_date(
            date_t, par_date[date_t], contexte)
        for permno, issue in c4.items():
            issues_c4_v11[(permno, date_t)] = issue
        ecrire_univers(
            os.path.join(REP_SORTIE, "U_%s.csv" % date_t.replace("-", "")),
            colonnes, retenus,
        )
        membres[date_t] = set(retenus)
        statuts_par_date[date_t] = statuts
        comptages_par_date[date_t] = comptages

        strates = Counter()
        for permno, enreg in retenus.items():
            strates[enreg["nano_micro"]] += 1
            strates["exch_" + enreg["exchange"]] += 1
            strates["prix_" + enreg["tranche_prix"]] += 1
            strates["flag_" + enreg["close_ref_flag"]] += 1
            if enreg["close_ref_flag"] != "TR":
                ecarts_c4[(enreg["close_ref_flag"], enreg["close_reporte_ecart"])] += 1
            strates["so_" + enreg["so_source"]] += 1
            # Effet de la garde d'échelle : sélection obtenue sans elle.
            if couche_edgar and permno in cikmap and enreg["so_source"] in (
                    "crsp_nodata", "edgar"):
                sans_garde = selectionner_so(index_sans_garde.get(permno), date_t)
                if enreg["so_source"] == "crsp_nodata":
                    if sans_garde is not None:
                        impact_garde["prive_par_la_garde"] += 1
                        cap_sans = float(enreg["close_ref"]) * sans_garde[0]
                        impact_garde["prive_et_serait_sorti_du_cap"
                                     if cap_sans >= CAP_MAX_USD
                                     else "prive_et_serait_reste"] += 1
                elif sans_garde is not None:
                    avec_garde = selectionner_so(so_index.get(permno), date_t)
                    if avec_garde and (sans_garde[0] != avec_garde[0]
                                       or sans_garde[1] != avec_garde[1]):
                        impact_garde["observation_deplacee"] += 1
                        impact_garde["depot_different" if sans_garde[1] != avec_garde[1]
                                     else "meme_depot_valeur_corrigee"] += 1
            if enreg["multi_classe"] == "True":
                strates["multi_classe"] += 1
            if enreg["ads"] == "True":
                strates["ads"] += 1
            if enreg["reit"] == "True":
                strates["reit"] += 1
            if enreg["etranger"] == "True":
                strates["etranger"] += 1
            if enreg["ratio_reverse_split"]:
                strates["avec_reverse_split"] += 1
                jours_split = enreg["jours_depuis_reverse_split" + calendrier.suffixe]
                if jours_split != "" and int(jours_split) <= SEUIL_REVERSE_SPLIT_JOURS:
                    strates["avec_reverse_split_le_80j"] += 1

            # C10
            if _vide(enreg["_dlycap"]) or _vide(enreg["_dlycapflg"]):
                lignes_sans_dlycap += 1
            else:
                dlycap = float(enreg["_dlycap"])
                if dlycap > 0:
                    # C10 vérifie l'unité de `dlycap` (milliers de dollars) contre
                    # close_ref x shrout x 1000 ; le cap EDGAR n'y entre pas pour
                    # ne pas mêler écart d'unité et écart de source.
                    ratio = enreg["_cap_crsp"] / (dlycap * 1000.0)
                    ratios_c10["toutes"].append(ratio)
                    # `dlycap` utilise le prix du jour t : seules les lignes TR,
                    # où close_ref est ce même prix, testent l'unité.
                    ratios_c10["TR" if enreg["close_ref_flag"] == "TR"
                               else "non_TR"].append(ratio)
                    if enreg["_ads"]:
                        ratios_c10["ads"].append(ratio)
                else:
                    lignes_sans_dlycap += 1

        stats_par_date[date_t] = strates
        evaluables_par_date[date_t] = evaluables

    print("[4/8] variantes : effets separes des fenetres C4 et d'EDGAR")
    membres_base, issues_base = construire_variante(
        dates, par_date, contexte, False, False)          # ni l'un ni l'autre
    membres_c4_seul, _ = construire_variante(
        dates, par_date, contexte, True, False)           # fenêtres C4 seules
    membres_so_seul, _ = construire_variante(
        dates, par_date, contexte, False, True)           # EDGAR seul
    decomposition = {
        "total": delta_appartenance(dates, membres_base, membres),
        "c4_seul": delta_appartenance(dates, membres_base, membres_c4_seul),
        "so_seul": delta_appartenance(dates, membres_base, membres_so_seul),
        "so_apres_c4": delta_appartenance(dates, membres_c4_seul, membres),
        "c4_apres_so": delta_appartenance(dates, membres_so_seul, membres),
        "taille_base": sum(len(membres_base[t]) for t in dates),
        "taille_v11": sum(len(membres[t]) for t in dates),
        "taille_c4_seul": sum(len(membres_c4_seul[t]) for t in dates),
        "taille_so_seul": sum(len(membres_so_seul[t]) for t in dates),
    }
    transitions_c4 = Counter()
    for cle, issue_base in issues_base.items():
        transitions_c4[(issue_base, issues_c4_v11.get(cle, "absent"))] += 1

    print("[5/8] journal des entrees / sorties")
    lignes_journal = []
    for indice, date_t in enumerate(dates):
        if indice == 0:
            for permno in sorted(membres[date_t]):
                lignes_journal.append([date_t, permno, "entree", "nouveau",
                                       "constitution_initiale"])
            continue
        date_prec = dates[indice - 1]
        precedents = membres[date_prec]
        courants = membres[date_t]
        lignes_a_t = {l["permno"] for l in par_date[date_t]}
        lignes_a_prec = {l["permno"] for l in par_date[date_prec]}

        for permno in sorted(courants - precedents):
            if permno not in lignes_a_prec:
                lignes_journal.append([date_t, permno, "entree", "nouveau", ""])
            else:
                lignes_journal.append([date_t, permno, "entree", "retour",
                                       statuts_par_date[date_prec].get(permno, "")])
        for permno in sorted(precedents - courants):
            cause, detail = cause_sortie(
                permno, date_prec, date_t, permno in lignes_a_t, delistings,
                statuts_par_date[date_t],
            )
            lignes_journal.append([date_t, permno, "sortie", cause, detail])

    with open(os.path.join(REP_SORTIE, "journal.csv"), "w", newline="\n") as flux:
        graveur = csv.writer(flux, lineterminator="\n")
        graveur.writerow(["t", "permno", "sens", "cause", "detail"])
        graveur.writerows(lignes_journal)

    print("[6/8] audit C9 (sensibilite au shrout retarde, lignes CRSP)")
    resultats_c9 = audit_c9(dates, membres, evaluables_par_date, shrout_pit)

    print("[7/8] controle C10 (cap recalcule / dlycap)")
    stats_c10 = {cle: audit_c10(valeurs) for cle, valeurs in ratios_c10.items()}

    print("[8/8] redaction de stats.md")
    profondeur_dlyprev = max((len(v) for v in dlyprev.values()), default=0) if dlyprev else 0
    profondeur_fenetres = max((len(v) for v in fenetres.values()), default=0) if fenetres else 0
    so_global = Counter()
    for evaluables in evaluables_par_date.values():
        for _permno, _prix, _shrout, so_source, _cap in evaluables:
            so_global[so_source] += 1
    edgar = {
        "actif": couche_edgar,
        "cik_ambigus": cik_ambigus,
        "permno_mappes": len(cikmap),
        "permno_avec_obs": len(so_index),
        "verdicts": verdicts_garde,
        "verdicts_filed": verdicts_filed,
        "verdicts_periode": verdicts_periode,
        "impact_garde": impact_garde,
        "so_candidats": so_global,
        "profondeur_fenetres": profondeur_fenetres,
        "origine_fenetre": contexte["origine_fenetre"],
        "fenetres_couvertes": len(fenetres_couvertes),
        "second_passage": second_passage,
        "compteur_clamp": compteur_clamp,
    }
    ecrire_stats(dates, membres, stats_par_date, comptages_par_date, lignes_journal,
                 calendrier, dlyprev, resultats_c9, stats_c10,
                 lignes_sans_dlycap, ecarts_c4, profondeur_dlyprev,
                 edgar, decomposition, transitions_c4)

    # Remplacement des sorties précédentes par renommage.
    if os.path.exists(rep_sortie_bak):
        shutil.rmtree(rep_sortie_bak)  # reste d'un remplacement interrompu
    if os.path.exists(rep_sortie_final):
        os.rename(rep_sortie_final, rep_sortie_bak)
    os.rename(rep_sortie_tmp, rep_sortie_final)
    if os.path.exists(rep_sortie_bak):
        shutil.rmtree(rep_sortie_bak)
    REP_SORTIE = rep_sortie_final

    print("termine : %d fichiers U_t, %d lignes de journal"
          % (len(dates), len(lignes_journal)))
    return {
        "dates": dates, "membres": membres, "c9": resultats_c9,
        "c10": stats_c10, "decomposition": decomposition,
        "membres_base": membres_base,
    }


def ecrire_stats(dates, membres, stats_par_date, comptages_par_date, lignes_journal,
                 calendrier, dlyprev, resultats_c9, stats_c10,
                 lignes_sans_dlycap, ecarts_c4, profondeur_dlyprev,
                 edgar, decomposition, transitions_c4):
    entrees = Counter()
    sorties = Counter()
    causes_sortie = Counter()
    for date_t, _permno, sens, cause, _detail in lignes_journal:
        if sens == "entree":
            entrees[date_t] += 1
        else:
            sorties[date_t] += 1
            causes_sortie[cause] += 1

    total_exclusions = Counter()
    total_attente_flg = Counter()
    for comptages in comptages_par_date.values():
        for cle, valeur in comptages.items():
            if cle.startswith("exclus_"):
                total_exclusions[cle[7:]] += valeur
            elif cle.startswith("attente_p3_flg_"):
                total_attente_flg[cle[15:]] += valeur

    attente_total = total_exclusions.get("en_attente_passe3", 0)
    attente_si_ba = sum(c.get("attente_p3_entrerait_si_ba", 0)
                        for c in comptages_par_date.values())

    lignes = []
    a = lignes.append
    a("# Univers point-in-time : comptages et contrôles\n")
    a("Généré par `src/donnees/build_univers.py`. Aucun horodatage n'est écrit : "
      "le fichier est reproductible à l'octet.\n")
    a("Le prix de référence (C4) utilise jusqu'à 5 jours de bourse de prix "
      "antérieurs ; le nombre d'actions vient des dépôts EDGAR quand ils sont "
      "disponibles (C11, §7). Le §8 sépare l'effet de ces deux sources.\n")

    a("## 0. Etat des entrees\n")
    a("| fichier | present | sha256 |")
    a("|---|---|---|")
    for chemin in FICHIERS_ENTREE:
        present = os.path.exists(chemin)
        a("| `%s` | %s | %s |" % (os.path.basename(chemin),
                                  "oui" if present else "NON",
                                  _sha256(chemin) if present else "—"))
    a("")

    a("### Drapeaux globaux\n")
    if not calendrier.dispo:
        a("- Calendrier de bourse absent : les jours sont comptés en jours "
          "calendaires et les colonnes correspondantes portent le suffixe `_cal` "
          "(`age_cotation_jours_cal`, `jours_depuis_reverse_split_cal`).")
    else:
        a("- Calendrier de bourse présent : jours comptés en jours de bourse. "
          "%d valeurs laissées vides (ancre antérieure au début du calendrier)."
          % calendrier.hors_couverture)
    if dlyprev is None:
        a("- Prix antérieurs `dlyprev` absents : C4 se limite au prix négocié "
          "du jour t. Un titre franchissant C2/C3 sans `dlyprcflg='TR'` à t est "
          "mis de côté avec la cause `en_attente_passe3`, ni inclus ni exclu. "
          "Aucune ligne `reporte` ou `bidask` dans cette exécution.")
    else:
        a("- Prix antérieurs disponibles : C4 complète (TR → reporte → bidask "
          "→ `suspendu`), aucun titre-mois en attente.")
        if edgar["profondeur_fenetres"]:
            a("- Fenêtres C4 complètes : au plus %d séances exploitables par "
              "cible (permno, t), %d cibles couvertes. `dlyprev` (profondeur "
              "maximale %d) ne sert que pour les cibles non couvertes."
              % (edgar["profondeur_fenetres"], edgar["fenetres_couvertes"],
                 profondeur_dlyprev))
            origine = edgar["origine_fenetre"]
            a("- Source des prix antérieurs pour les lignes non `TR` à t : "
              "fenêtres %d, `dlyprev` %d, aucune %d."
              % (origine.get("passe4", 0), origine.get("dlyprev", 0),
                 origine.get("aucune", 0)))
        else:
            a("- Fenêtres C4 complètes absentes : les prix antérieurs viennent "
              "de `dlyprev` seul (profondeur maximale %d), si bien que le "
              "report sur 5 jours de bourse ne peut pas être exploité en "
              "entier. Voir §3." % profondeur_dlyprev)
    if edgar["actif"]:
        a("- Nombre d'actions EDGAR (C11) actif : %d PERMNO associés à un CIK, "
          "%d avec au moins une observation dei retenue après garde d'échelle. "
          "Voir §7." % (edgar["permno_mappes"], edgar["permno_avec_obs"]))
    else:
        a("- Nombre d'actions EDGAR (C11) inactif (fichiers `edgar-so/` "
          "absents) : toutes les capitalisations utilisent le shrout CRSP.")
    a("")

    a("## 1. Comptages par date de reconstitution\n")
    suffixe = calendrier.suffixe
    a("Unite des colonnes de jours : %s%s.\n"
      % (calendrier.unite, " (suffixe `%s`)" % suffixe if suffixe else ""))
    a("| t | \\|U_t\\| | nano | micro | Nasdaq (Q) | NYSE (N) | NYSE Am. (A) | "
      "[0,10–1) | [1–5) | [5+) | ADS | REIT | etranger | multi_classe | "
      "attente_p3 | entrees | sorties |")
    a("|" + "---|" * 17)
    for date_t in dates:
        s = stats_par_date[date_t]
        c = comptages_par_date[date_t]
        a("| %s | %d | %d | %d | %d | %d | %d | %d | %d | %d | %d | %d | %d | %d | %d | %d | %d |"
          % (date_t, len(membres[date_t]), s["nano"], s["micro"],
             s["exch_Q"], s["exch_N"], s["exch_A"],
             s["prix_[0.10-1)"], s["prix_[1-5)"], s["prix_[5+)"],
             s["ads"], s["reit"], s["etranger"], s["multi_classe"],
             c.get("exclus_en_attente_passe3", 0),
             entrees[date_t], sorties[date_t]))
    a("")

    a("### 1bis. Titres ayant connu un reverse split\n")
    a("Membres de U_t avec `jours_depuis_reverse_split%s` renseigné, et parmi "
      "eux ceux dont le reverse split date d'au plus %d %s.\n"
      % (suffixe, SEUIL_REVERSE_SPLIT_JOURS, calendrier.unite))
    a("| t | avec_reverse_split | dont <= %d %s |"
      % (SEUIL_REVERSE_SPLIT_JOURS, calendrier.unite))
    a("|---|---|---|")
    for date_t in dates:
        s = stats_par_date[date_t]
        a("| %s | %d | %d |" % (date_t, s.get("avec_reverse_split", 0),
                                 s.get("avec_reverse_split_le_80j", 0)))
    a("")

    a("## 2. Exclusions cumulees (toutes dates)\n")
    a("| cause | titres-mois |")
    a("|---|---|")
    for cause in sorted(total_exclusions, key=lambda k: (-total_exclusions[k], k)):
        a("| `%s` | %d |" % (cause, total_exclusions[cause]))
    a("")
    a("Causes de sortie du journal (transitions, hors constitution initiale) :\n")
    a("| cause | occurrences |")
    a("|---|---|")
    for cause in sorted(causes_sortie, key=lambda k: (-causes_sortie[k], k)):
        a("| `%s` | %d |" % (cause, causes_sortie[cause]))
    a("")

    a("## 3. Regle C4 — prix de reference\n")
    total_flags = Counter()
    for strates in stats_par_date.values():
        for cle, valeur in strates.items():
            if cle.startswith("flag_"):
                total_flags[cle[5:]] += valeur
    total_u = sum(len(m) for m in membres.values())
    a("Repartition de `close_ref_flag` sur les %d titres-mois de U :\n" % total_u)
    a("| `close_ref_flag` | titres-mois | part |")
    a("|---|---|---|")
    for flag in ("TR", "reporte", "bidask"):
        nombre = total_flags.get(flag, 0)
        a("| `%s` | %d | %.3f %% |" % (flag, nombre,
                                       100.0 * nombre / max(1, total_u)))
    a("")
    if ecarts_c4:
        a("Ecart (en %s) entre la seance du prix retenu et t, pour les lignes "
          "non `TR` :\n" % calendrier.unite)  # 0 = prix bid/ask du jour t
        a("| flag | ecart | titres-mois |")
        a("|---|---|---|")
        for (flag, ecart) in sorted(ecarts_c4, key=lambda k: (k[0], int(k[1] or 0))):
            a("| `%s` | %s | %d |" % (flag, ecart or "0", ecarts_c4[(flag, ecart)]))
        a("")
        if edgar["profondeur_fenetres"]:
            a("Source des prix antérieurs : `univers_fenetres_nonTR.csv` fournit "
              "toutes les séances des 9 jours calendaires précédant t (15 pour "
              "trois dates) pour chaque cible (permno, t) sans prix négocié à t, "
              "ce qui couvre les 5 jours de bourse de C4 ; les écarts ci-dessus "
              "vont donc jusqu'à 5. Les cibles non couvertes (%d lignes non "
              "`TR`) utilisent `dlyprev`, limité à la dernière séance cotée.\n"
              % edgar["origine_fenetre"].get("dlyprev", 0))
        else:
            a("Limite de la source : `dlyprev` ne donne que la dernière séance "
              "cotée avant t. Un titre sans cotation depuis 2 à 5 jours de bourse "
              "ne peut donc pas recevoir de prix `reporte` : il prend le prix "
              "`BA` du jour t s'il existe, sinon il sort avec la cause "
              "`suspendu`.\n")
    a("### Lignes en attente de prix antérieurs\n")
    if attente_total == 0:
        a("Aucune : C4 attribue à chaque titre-mois un prix ou la cause "
          "`suspendu`.\n")
    else:
        a("- Total titres-mois `en_attente_passe3` : %d (%.2f %% des "
          "titres-mois franchissant C2/C3).\n" % (
              attente_total,
              100.0 * attente_total / max(1, sum(c["survivants_c2c3"]
                                                 for c in comptages_par_date.values()))))
        a("| `dlyprcflg` a t | titres-mois |")
        a("|---|---|")
        for flag in sorted(total_attente_flg, key=lambda k: (-total_attente_flg[k], k)):
            a("| `%s` | %d |" % (flag, total_attente_flg[flag]))
        a("")
        a("Estimation indicative, sans effet sur U_t : %d de ces titres-mois "
          "entreraient dans U_t avec le prix `BA` du jour t comme close_ref "
          "(cap < 300 M$ et prix >= 0,10 $). C4 donne la priorité au dernier "
          "prix négocié des 5 jours de bourse précédents, inconnu sans les "
          "prix antérieurs.\n" % attente_si_ba)

    a("## 4. Bornes de cohérence (contrôle, pas filtre)\n")
    a("Taille attendue de U_t en fin de période : %d–%d titres. Un écart est "
      "signalé sans modifier les règles.\n" % BANDE_COHERENCE)
    a("| t | \\|U_t\\| | dans la bande | \\|U_t\\| + attente_p3 |")
    a("|---|---|---|---|")
    for date_t in dates:
        if not date_t.startswith(BANDE_ANNEES):
            continue
        taille = len(membres[date_t])
        attente = comptages_par_date[date_t].get("exclus_en_attente_passe3", 0)
        dedans = BANDE_COHERENCE[0] <= taille <= BANDE_COHERENCE[1]
        a("| %s | %d | %s | %d |" % (date_t, taille, "oui" if dedans else "**NON**",
                                     taille + attente))
    a("")
    a("Les bandes des années antérieures ne sont pas encore établies.\n")

    a("## 5. Audit C9 : sensibilité au shrout retardé\n")
    a("Appartenance recalculée avec `shrout(t−L)` pour L ∈ {1, 2, 3} fins de "
      "mois, approximation mensuelle de retards de 30, 60 et 90 jours. Un titre "
      "sans ligne à t−L garde shrout(t), et les L premières dates sont exclues. "
      "Le prix de référence n'est pas décalé. La population évaluée comprend "
      "tous les titres franchissant C2/C3/C4, retenus ou écartés pour cap ou "
      "plancher, pour compter les entrées comme les sorties.\n")
    a("Seules les lignes dont le nombre d'actions vient de CRSP sont "
      "perturbées : une ligne `so_source='edgar'` est datée par son dépôt et "
      "garde son cap. Ces lignes restent dans les dénominateurs.\n")
    a("Trois dénominateurs sont donnés : `|U|` = Σ_t |U_t| ; `|U ∪ U_alt|` = "
      "Σ_t de l'union des deux appartenances ; `candidats` = Σ_t des titres "
      "franchissant C2/C3/C4.\n")
    a("| L | entrees | sorties | bascules | \\|U\\| | part /\\|U\\| | "
      "\\|U ∪ U_alt\\| | part /union | candidats | part /candidats | "
      "sans ligne a t−L |")
    a("|---|---|---|---|---|---|---|---|---|---|---|")
    depassement = False
    for decalage in DECALAGES_C9:
        r = resultats_c9[decalage]
        part_u = r["bascules"] / r["base_u"] if r["base_u"] else 0.0
        part_union = r["bascules"] / r["base_union"] if r["base_union"] else 0.0
        part_cand = r["bascules"] / r["base_cand"] if r["base_cand"] else 0.0
        if part_u > SEUIL_ESCALADE_C9:
            depassement = True
        a("| %d | %d | %d | %d | %d | %.3f %% | %d | %.3f %% | %d | %.3f %% | %d |"
          % (decalage, r["entrees"], r["sorties"], r["bascules"],
             r["base_u"], 100.0 * part_u,
             r["base_union"], 100.0 * part_union,
             r["base_cand"], 100.0 * part_cand, r["sans_ligne"]))
    a("")
    a("Seuil d'alerte (C9) : plus de 1,00 %% de bascules, rapportées à `|U|` : "
      "%s.\n"
      % ("seuil dépassé" if depassement else "seuil non atteint"))
    a("### 5bis. Bascules par `so_source`\n")
    a("La ligne `edgar` doit afficher 0 bascule. Les parts sont rapportées aux "
      "titres-mois de U et aux candidats de chaque source.\n")
    for decalage in DECALAGES_C9:
        r = resultats_c9[decalage]
        a("**L = %d**\n" % decalage)
        a("| `so_source` | entrees | sorties | bascules | \\|U\\| de la source | "
          "part /\\|U\\| source | candidats de la source | part /candidats source |")
        a("|---|---|---|---|---|---|---|---|")
        for source in sorted(r["par_source"]):
            c = r["par_source"][source]
            bascules = c["entrees"] + c["sorties"]
            part_u = bascules / c["base_u"] if c["base_u"] else 0.0
            part_cand = bascules / c["base_cand"] if c["base_cand"] else 0.0
            a("| `%s` | %d | %d | %d | %d | %.3f %% | %d | %.3f %% |"
              % (source, c["entrees"], c["sorties"], bascules, c["base_u"],
                 100.0 * part_u, c["base_cand"], 100.0 * part_cand))
        # Agrégat des sources CRSP, seules exposées à un shrout postérieur à t.
        crsp_entrees = sum(c["entrees"] for s, c in r["par_source"].items()
                           if s != "edgar")
        crsp_sorties = sum(c["sorties"] for s, c in r["par_source"].items()
                           if s != "edgar")
        crsp_base_u = sum(c["base_u"] for s, c in r["par_source"].items()
                          if s != "edgar")
        crsp_base_cand = sum(c["base_cand"] for s, c in r["par_source"].items()
                             if s != "edgar")
        crsp_bascules = crsp_entrees + crsp_sorties
        crsp_part_u = crsp_bascules / crsp_base_u if crsp_base_u else 0.0
        crsp_part_cand = crsp_bascules / crsp_base_cand if crsp_base_cand else 0.0
        a("| **CRSP (population à risque)** | %d | %d | %d | %d | %.3f %% | "
          "%d | %.3f %% |"
          % (crsp_entrees, crsp_sorties, crsp_bascules, crsp_base_u,
             100.0 * crsp_part_u, crsp_base_cand, 100.0 * crsp_part_cand))
        a("")

    a("## 6. Controle C10 — cap recalcule / dlycap\n")
    a("Ratio = (close_ref × shrout × 1000) / (dlycap × 1000), sur les lignes de "
      "U_t où `dlycapflg` est renseigné et `dlycap` > 0. Un ratio de 1 confirme "
      "que `dlycap` est en milliers de dollars et `shrout` en milliers "
      "d'actions.\n")
    a("| population | n | min | p01 | p25 | mediane | p75 | p99 | max |")
    a("|---|---|---|---|---|---|---|---|---|")
    for libelle, cle in (("toutes lignes de U", "toutes"),
                         ("dont `close_ref_flag = TR`", "TR"),
                         ("dont `reporte` / `bidask`", "non_TR"),
                         ("lignes `ads=True`", "ads")):
        stats = stats_c10[cle]
        if stats is None:
            a("| %s | 0 | — | — | — | — | — | — | — |" % libelle)
            continue
        a("| %s | %d | %.6f | %.6f | %.6f | %.6f | %.6f | %.6f | %.6f |"
          % (libelle, stats["n"], stats["min"], stats["p01"], stats["p25"],
             stats["mediane"], stats["p75"], stats["p99"], stats["max"]))
    a("")
    a("Lecture : `dlycap` utilise le prix du jour t. Sur les lignes `reporte` "
      "et `bidask`, close_ref est un autre prix et l'écart au ratio 1 est "
      "attendu ; l'unité se lit sur la ligne `TR`, où le ratio doit valoir 1 à "
      "l'arrondi de `dlycap` près. Sur les lignes `ads=True`, un ratio de 1 "
      "montre que CRSP applique le même calcul prix × shrout aux ADS, mais ne "
      "prouve pas que `shrout` compte des ADS plutôt que des actions "
      "sous-jacentes.\n")
    a("Lignes de U sans `dlycap` exploitable : %d.\n" % lignes_sans_dlycap)
    a("Le ratio utilise le cap CRSP (close_ref × shrout × 1000), y compris sur "
      "les lignes dont le cap publié vient d'EDGAR, pour ne pas mêler le "
      "contrôle d'unité et l'écart entre sources (mesuré au §7).\n")

    # --- §7 : nombre d'actions EDGAR ---
    a("## 7. Nombre d'actions daté par EDGAR (C11)\n")
    if not edgar["actif"]:
        a("Inactif : `edgar-so/permno_cik_map.csv` ou `edgar-so/so_edgar.csv` "
          "est absent. Toutes les capitalisations utilisent le shrout CRSP.\n")
    else:
        a("`cap = close_ref × SO`. `val` dei est en actions ; `shrout` CRSP est "
          "en milliers d'actions. La colonne `shrout_milliers` des U_t reste le "
          "shrout CRSP brut même quand `so_source='edgar'`.\n")
        a("Sélection : dernière observation dei avec `filed` ≤ t ; à `filed` "
          "égal, `asof` maximal puis `val` minimale. Les ADS (couverture "
          "20-F/6-K insuffisante) et les titres `multi_classe` (le dei de page "
          "de garde agrège les classes) restent sur CRSP.\n")
        a("Contrôle de divergence : la valeur sélectionnée est comparée au "
          "shrout CRSP du jour t ; hors de [0,5 ; 2], elle est jugée périmée "
          "(par exemple après une forte dilution postérieure au dernier dépôt) "
          "et remplacée par CRSP, avec `so_source='crsp_divergence'` et "
          "`so_filed_date` vide.\n")
        if edgar["cik_ambigus"]:
            a("PERMNO retirés de la correspondance car associés à plusieurs "
              "CIK : %d.\n" % edgar["cik_ambigus"])

        a("### 7.1 Garde contre les erreurs d'échelle\n")
        clamps = edgar["compteur_clamp"].get("asof_clampe_sur_filed", 0)
        a("L'ancrage se fait sur `asof`, date à laquelle le décompte se "
          "rapporte, ramenée à `filed`, date de publication, quand `asof > "
          "filed` ; la valeur est alors conservée. %d observations sont dans ce "
          "cas.\n" % clamps)
        a("Chaque observation dei est comparée, une fois et indépendamment de "
          "t, au shrout CRSP du month-end le plus proche, converti en actions. "
          "Ratio dans [0,5 ; 2] : acceptée ; dans [500 ; 2000] : `val` ÷ 1000 ; "
          "dans [1/2000 ; 1/500] : `val` × 1000 ; sinon rejetée.\n")
        total_obs = sum(edgar["verdicts"].values())
        a("| verdict | observations | part |")
        a("|---|---|---|")
        for verdict in ("accepte", "corrige_div1000", "corrige_mul1000",
                        "rejete", "sans_ancre"):
            nombre = edgar["verdicts"].get(verdict, 0)
            a("| `%s` | %d | %.3f %% |"
              % (verdict, nombre, 100.0 * nombre / max(1, total_obs)))
        a("| **total** | %d | |" % total_obs)
        a("")
        ecarts_garde = sum(
            abs(edgar["verdicts"].get(v, 0) - edgar["verdicts_filed"].get(v, 0))
            for v in set(edgar["verdicts"]) | set(edgar["verdicts_filed"]))
        a("Robustesse de la date d'ancrage : la même garde ancrée sur `filed` "
          "au lieu d'`asof` déplace %d verdicts (somme des écarts absolus sur "
          "les %d observations).\n" % (ecarts_garde, total_obs))
        if edgar["verdicts_periode"]:
            a("Lecture des rejets : une observation dont l'`asof` précède la "
              "période d'étude est ancrée sur le premier month-end disponible, "
              "parfois des années plus tard ; son ratio reflète alors "
              "l'évolution du nombre d'actions plutôt qu'une erreur d'échelle. "
              "Le rejet entraîne un repli sur CRSP.\n")
            a("| verdict | `asof` avant la fenetre | `asof` dans la fenetre |")
            a("|---|---|---|")
            for verdict in ("accepte", "corrige_div1000", "corrige_mul1000",
                            "rejete", "sans_ancre"):
                a("| `%s` | %d | %d |"
                  % (verdict,
                     edgar["verdicts_periode"].get((verdict, "avant_fenetre"), 0),
                     edgar["verdicts_periode"].get((verdict, "dans_fenetre"), 0)))
            a("")
        impact = edgar["impact_garde"]
        a("Effet sur l'univers, par comparaison avec une sélection sans garde "
          "(diagnostic seulement) :\n")
        a("- membres `crsp_nodata` dont la garde a écarté toutes les "
          "observations : %d (%.3f %% de U), dont %d dont le cap aurait "
          "dépassé 300 M$ sans la garde et %d qui seraient restés membres avec "
          "un autre cap ;"
          % (impact.get("prive_par_la_garde", 0),
             100.0 * impact.get("prive_par_la_garde", 0) / max(1, total_u),
             impact.get("prive_et_serait_sorti_du_cap", 0),
             impact.get("prive_et_serait_reste", 0)))
        a("- membres `edgar` dont la garde a changé l'observation retenue : "
          "%d (dont %d vers un dépôt antérieur, %d même dépôt à valeur "
          "corrigée).\n"
          % (impact.get("observation_deplacee", 0),
             impact.get("depot_different", 0),
             impact.get("meme_depot_valeur_corrigee", 0)))
        corrections_mobilisees = impact.get("meme_depot_valeur_corrigee", 0)
        a("- corrections d'échelle (`corrige_div1000`, `corrige_mul1000`) "
          "effectivement utilisées dans U : %d titres-mois (%.3f %% de U).\n"
          % (corrections_mobilisees,
             100.0 * corrections_mobilisees / max(1, total_u)))
        second = edgar.get("second_passage") or {}
        total_corrections = second.get("total", 0)
        stable_corrections = second.get("stable", 0)
        a("- stabilité des corrections : une valeur corrigée repassée dans la "
          "garde est acceptée pour %d des %d corrections (%s).\n"
          % (stable_corrections, total_corrections,
             "conforme" if stable_corrections == total_corrections
             else "écart à examiner"))

        a("### 7.2 Repartition de `so_source`\n")
        total_u = sum(len(m) for m in membres.values())
        a("Sur les %d titres-mois de U (membres retenus) :\n" % total_u)
        a("| `so_source` | titres-mois de U | part | titres-mois candidats C2-C4 |")
        a("|---|---|---|---|")
        totaux_source = Counter()
        for strates in stats_par_date.values():
            for cle, valeur in strates.items():
                if cle.startswith("so_"):
                    totaux_source[cle[3:]] += valeur
        for source in SO_SOURCES:
            nombre = totaux_source.get(source, 0)
            a("| `%s` | %d | %.3f %% | %d |"
              % (source, nombre, 100.0 * nombre / max(1, total_u),
                 edgar["so_candidats"].get(source, 0)))
        a("")
        a("### 7.3 `so_source` par date de reconstitution\n")
        a("| t | \\|U_t\\| | " + " | ".join("`%s`" % s for s in SO_SOURCES)
          + " | part edgar |")
        a("|" + "---|" * (3 + len(SO_SOURCES)))
        for date_t in dates:
            s = stats_par_date[date_t]
            taille = len(membres[date_t])
            a("| %s | %d | %s | %.1f %% |"
              % (date_t, taille,
                 " | ".join(str(s.get("so_" + source, 0)) for source in SO_SOURCES),
                 100.0 * s.get("so_edgar", 0) / max(1, taille)))
        a("")

    # --- §8 : effets séparés des fenêtres C4 et d'EDGAR ---
    a("## 8. Effets séparés des fenêtres C4 et du nombre d'actions EDGAR\n")
    a("Le pipeline est réexécuté sur les mêmes entrées avec chaque source "
      "activée isolément (sans écrire de fichier) :\n")
    a("- base : prix antérieurs `dlyprev` seuls, nombre d'actions CRSP ;")
    a("- C4 seule : fenêtres C4 complètes, nombre d'actions CRSP ;")
    a("- SO seule : nombre d'actions EDGAR, prix antérieurs `dlyprev` seuls ;")
    a("- complète : les deux, soit l'univers publié.\n")
    a("| variante | titres-mois de U | entrants vs base | sortants vs base |")
    a("|---|---|---|---|")
    a("| base | %d | — | — |" % decomposition["taille_base"])
    for libelle, cle, taille in (
            ("C4 seule (fenêtres complètes)", "c4_seul", "taille_c4_seul"),
            ("SO seule (EDGAR)", "so_seul", "taille_so_seul"),
            ("complète (les deux)", "total", "taille_v11")):
        entrants, sortants = decomposition[cle]
        a("| %s | %d | %d | %d |" % (libelle, decomposition[taille],
                                     entrants, sortants))
    a("")
    a("Décompositions séquentielles ; l'écart entre les deux ordres mesure "
      "l'interaction, un titre-mois pouvant n'entrer que s'il reçoit à la fois "
      "un close_ref des fenêtres et un nombre d'actions EDGAR :\n")
    a("| chemin | entrants | sortants |")
    a("|---|---|---|")
    for libelle, cle in (("base → C4 seule", "c4_seul"),
                         ("C4 seule → complète (effet SO)", "so_apres_c4"),
                         ("base → SO seule", "so_seul"),
                         ("SO seule → complète (effet C4)", "c4_apres_so"),
                         ("base → complète (total)", "total")):
        entrants, sortants = decomposition[cle]
        a("| %s | %d | %d |" % (libelle, entrants, sortants))
    a("")
    a("### 8.1 Changements d'issue de C4 (base -> complète)\n")
    a("Issue de C4 pour tous les titres franchissant C2/C3, avant les filtres "
      "de cap et de plancher. Seules les issues modifiées sont listées.\n")
    a("| issue base | issue complète | titres-mois |")
    a("|---|---|---|")
    for (avant, apres) in sorted(transitions_c4, key=lambda k: (-transitions_c4[k], str(k))):
        if avant == apres:
            continue
        a("| `%s` | `%s` | %d |" % (avant, apres, transitions_c4[(avant, apres)]))
    a("")
    diagonale = sum(v for (x, y), v in transitions_c4.items() if x == y)
    a("Issue inchangée : %d titres-mois.\n" % diagonale)

    with open(os.path.join(REP_SORTIE, "stats.md"), "w", newline="\n") as flux:
        flux.write("\n".join(lignes) + "\n")


# --- Autotests sur fixtures en mémoire ---


def _ligne(**kwargs):
    base = {
        "permno": 1, "dlycaldt": "2020-06-30", "dlyprc": "2.00", "dlyprcflg": "TR",
        "dlyvol": "1000", "shrout": "10000", "sharetype": "NS",
        "securitytype": "EQTY", "securitysubtype": "COM", "usincflg": "Y",
        "issuertype": "CORP", "primaryexch": "Q", "conditionaltype": "RW",
        "tradingstatusflg": "A", "dlycap": "20000", "dlycapflg": "BP",
        "ticker": "TEST",
    }
    base.update(kwargs)
    return base


def autotests():
    # C2 : types
    ok, ads, reit, etranger = classer_type(_ligne())
    assert (ok, ads, reit, etranger) == (True, False, False, False)
    assert classer_type(_ligne(issuertype="REIT"))[:3] == (True, False, True)
    assert classer_type(_ligne(usincflg="N")) == (True, False, False, True)
    assert classer_type(_ligne(sharetype="AD"))[:2] == (True, True)
    # ETF, fonds fermes, units, SBI, lignes « Last Known » : exclus
    assert classer_type(_ligne(securitytype="FUND", securitysubtype="ETF",
                               issuertype="ACOR"))[0] is False
    assert classer_type(_ligne(securitysubtype="CEF"))[0] is False
    assert classer_type(_ligne(sharetype="UG"))[0] is False
    assert classer_type(_ligne(sharetype="SB"))[0] is False
    assert classer_type(_ligne(sharetype="N/A", securitytype="N/A",
                               securitysubtype="UNK"))[0] is False
    assert classer_type(_ligne(issuertype="GOVT"))[0] is False
    assert classer_type(_ligne(sharetype="AD", securitysubtype="CEF"))[0] is False

    # C3 : périmètre
    assert cause_perimetre(_ligne()) is None
    assert cause_perimetre(_ligne(primaryexch="R")) == "exchange_hors_perimetre"
    assert cause_perimetre(_ligne(conditionaltype="NT")) == "conditionaltype_hors_perimetre"
    assert cause_perimetre(_ligne(tradingstatusflg="X")) == "tradingstatus_hors_perimetre"
    for statut in ("A", "H", "S"):
        assert cause_perimetre(_ligne(tradingstatusflg=statut)) is None

    # C4 : close_ref, sur un calendrier fictif du lundi au vendredi (juin 2020)
    jours = [d.isoformat() for d in
             (_dt.date(2020, 6, 1) + _dt.timedelta(days=i) for i in range(30))
             if d.weekday() < 5]
    cal = Calendrier(jours)
    t = "2020-06-30"

    # a) prix negocie a t
    assert close_ref(_ligne(), (), cal, True) == ("2.00", "TR", None, None)
    # b) sans prix antérieurs : mise de côté
    assert close_ref(_ligne(dlyprcflg="BA"), (), cal, False) \
        == (None, None, None, "en_attente_passe3")
    assert close_ref(_ligne(dlyprcflg="NT", dlyprc=""), (), cal, False)[3] \
        == "en_attente_passe3"
    # c) report : dernier TR dans les 5 jours de bourse précédant t
    prevs = [("2020-06-25", "1.50", "TR"), ("2020-06-26", "1.60", "TR")]
    assert close_ref(_ligne(dlyprcflg="BA", dlyprc="1.70"), prevs, cal, True) \
        == ("1.60", "reporte", 2, None)
    # d) un TR trop ancien (> 5 jours de bourse) ne sert pas de report
    vieux = [("2020-06-22", "1.40", "TR")]
    assert close_ref(_ligne(dlyprcflg="SU", dlyprc=""), vieux, cal, True) \
        == (None, None, None, "suspendu")
    # e) à défaut, bid/ask à t d'abord
    assert close_ref(_ligne(dlyprcflg="BA", dlyprc="1.70"), vieux, cal, True) \
        == ("1.70", "bidask", 0, None)
    # f) puis bid/ask dans la fenêtre
    prevs_ba = [("2020-06-26", "1.65", "BA")]
    assert close_ref(_ligne(dlyprcflg="SU", dlyprc=""), prevs_ba, cal, True) \
        == ("1.65", "bidask", 2, None)
    # g) suspension prolongée : hors U_t
    assert close_ref(_ligne(dlyprcflg="SU", dlyprc=""), (), cal, True) \
        == (None, None, None, "suspendu")
    # h) un TR antérieur l'emporte sur le BA du jour t
    assert close_ref(_ligne(dlyprcflg="BA", dlyprc="9.99"),
                     [("2020-06-24", "1.10", "TR")], cal, True) \
        == ("1.10", "reporte", 4, None)

    # Calendrier : jours de bourse et jours calendaires
    assert cal.ecart("2020-06-26", "2020-06-30") == 2      # lun 29, mar 30
    assert Calendrier(None).ecart("2020-06-26", "2020-06-30") == 4
    assert cal.ecart("2019-01-02", "2020-06-30") is None   # hors couverture

    # i) fenêtre de plusieurs séances : le TR de t-3 l'emporte sur des BA plus
    #    récents, avec un écart de 3.
    fenetre_profonde = [("2020-06-24", "1.30", "BA"), ("2020-06-25", "1.40", "TR"),
                        ("2020-06-26", "1.50", "BA"), ("2020-06-29", "1.55", "BA")]
    assert close_ref(_ligne(dlyprcflg="NT", dlyprc=""), fenetre_profonde, cal, True) \
        == ("1.40", "reporte", 3, None)
    # j) sans TR dans la fenêtre : le BA le plus récent
    assert close_ref(_ligne(dlyprcflg="NT", dlyprc=""),
                     [("2020-06-25", "1.40", "BA"), ("2020-06-29", "1.55", "BA")],
                     cal, True) == ("1.55", "bidask", 1, None)
    # k) un prix fourni au-delà de 5 jours de bourse est ignoré
    assert close_ref(_ligne(dlyprcflg="NT", dlyprc=""),
                     [("2020-06-22", "1.20", "TR")], cal, True) \
        == (None, None, None, "suspendu")

    # C11 : garde d'échelle, bornes incluses, zone morte et absence d'ancre
    ancre = 10_000_000.0                       # 10 000 milliers CRSP, en actions
    assert garde_echelle(10_000_000.0, ancre) == (10_000_000.0, "accepte")
    assert garde_echelle(20_000_000.0, ancre) == (20_000_000.0, "accepte")   # ratio 2
    assert garde_echelle(5_000_000.0, ancre) == (5_000_000.0, "accepte")     # ratio 0,5
    assert garde_echelle(1e10, ancre) == (1e7, "corrige_div1000")            # ratio 1000
    assert garde_echelle(5e9, ancre) == (5e6, "corrige_div1000")             # ratio 500
    assert garde_echelle(10_000.0, ancre) == (1e7, "corrige_mul1000")        # ratio 1/1000
    assert garde_echelle(20_000.0, ancre) == (2e7, "corrige_mul1000")        # ratio 1/500
    assert garde_echelle(1e9, ancre) == (None, "rejete")        # ratio 100 : zone morte
    assert garde_echelle(1e5, ancre) == (None, "rejete")        # ratio 1/100 : idem
    assert garde_echelle(0.0, ancre) == (None, "rejete")        # val nulle
    assert garde_echelle(1e7, None) == (1e7, "sans_ancre")      # pas d'ancre CRSP
    assert garde_echelle(1e7, 0.0) == (1e7, "sans_ancre")
    # val <= 0 est rejetée même sans ancre
    assert garde_echelle(0.0, None) == (None, "rejete")
    assert garde_echelle(-1.0, None) == (None, "rejete")

    # ancre : month-end le plus proche, avant ou après l'observation
    ancres_test = (["2020-01-31", "2020-06-30", "2020-12-31"],
                   [1e6, 2e6, 3e6])
    assert ancre_la_plus_proche(ancres_test, "2020-06-20") == 2e6
    assert ancre_la_plus_proche(ancres_test, "2020-03-01") == 1e6   # 30 j vs 121 j
    assert ancre_la_plus_proche(ancres_test, "2021-05-01") == 3e6   # apres la fin
    assert ancre_la_plus_proche(None, "2020-06-20") is None

    # garde appliquée avant l'indexation
    ancres_shrout = construire_ancres_shrout(
        {(1, "2020-06-30"): "10000", (1, "2020-05-29"): "10000",
         (2, "2020-06-30"): ""})
    index, verdicts = preparer_so_edgar(
        {1: [("2020-06-01", "2020-05-31", 1e7),      # accepte
             ("2020-06-02", "2020-05-31", 1e10),     # ÷1000
             ("2020-06-03", "2020-05-31", 1e4),      # ×1000
             ("2020-06-04", "2020-05-31", 1e9)]},    # rejete
        ancres_shrout)
    assert verdicts == Counter({"accepte": 1, "corrige_div1000": 1,
                                "corrige_mul1000": 1, "rejete": 1})
    assert [o[2] for o in index[1][1]] == [1e7, 1e7, 1e7]  # 3 obs, toutes a 1e7

    # asof > filed : l'ancrage est ramené à `filed` et la valeur conservée.
    # Ancré sur l'asof aberrant (2033), le ratio serait 1/500 ; ancré sur
    # `filed`, il vaut 1.
    ancres_shrout_i4 = {1: (["2020-06-01", "2033-06-01"], [1e7, 1e7 * 500.0])}
    compteur_clamp = Counter()
    index_i4, verdicts_i4 = preparer_so_edgar(
        {1: [("2020-06-05", "2033-01-01", 1e7)]},   # asof (2033) > filed (2020-06-05)
        ancres_shrout_i4, compteur_clamp=compteur_clamp)
    assert verdicts_i4 == Counter({"accepte": 1})
    assert compteur_clamp == Counter({"asof_clampe_sur_filed": 1})
    # l'index garde la valeur et les dates d'origine
    assert index_i4[1][1] == [("2020-06-05", "2033-01-01", 1e7)]

    # C11 : sélection à `filed` <= t et départage des égalités
    observations = [("2020-01-10", "2019-12-31", 100.0),
                    ("2020-03-10", "2020-02-29", 110.0),
                    ("2020-03-10", "2020-03-01", 115.0),
                    ("2020-03-10", "2020-03-01", 120.0),
                    ("2020-05-10", "2020-04-30", 130.0)]
    observations.sort()
    index_test = ([o[0] for o in observations], observations)
    # filed = t est admis ; égalité tranchée par asof max puis val min
    assert selectionner_so(index_test, "2020-03-10") == (115.0, "2020-03-10")
    assert selectionner_so(index_test, "2020-03-09") == (100.0, "2020-01-10")
    assert selectionner_so(index_test, "2019-12-31") is None   # aucun dépôt avant t
    assert selectionner_so(index_test, "2030-01-01") == (130.0, "2020-05-10")
    assert selectionner_so(None, "2020-03-10") is None

    # construire_date : cap, plancher, tranches, multi_classe
    contexte = {
        "secinfo": {1: [("2015-01-01", "2099-12-31", "900")],
                    2: [("2015-01-01", "2099-12-31", "900")],
                    3: [("2015-01-01", "2099-12-31", "901")]},
        "ancres": {1: "2015-01-01", 2: "2015-01-01", 3: "2019-01-01"},
        "splits": {3: [("2020-01-02", -0.75)]},
        "calendrier": Calendrier(None),
        "dlyprev": None,
        "fenetres": None, "fenetres_couvertes": set(),
        "cikmap": {}, "so_index": {},
        "utiliser_fenetres": False, "utiliser_edgar": False,
        "origine_fenetre": Counter(),
    }
    lignes = [
        _ligne(permno=1, dlyprc="2.00", shrout="10000"),    # cap 20 M$ -> nano
        _ligne(permno=2, dlyprc="40.00", shrout="10000"),   # cap 400 M$ -> exclu
        _ligne(permno=3, dlyprc="0.05", shrout="10000"),    # sous le plancher
        _ligne(permno=4, dlyprc="6.00", shrout="20000",
               primaryexch="R"),                            # hors périmètre
        _ligne(permno=5, dlyprc="6.00", shrout="20000",
               securitysubtype="ETF", securitytype="FUND"),  # hors type
    ]
    retenus, statuts, _c, evaluables, c4 = construire_date(
        "2020-06-30", lignes, contexte)
    assert set(retenus) == {1}
    assert c4[1] == "TR" and c4[3] == "TR"      # issue C4 exposée
    # population de l'audit C9 : retenus et écartés pour cap ou plancher
    assert [e[0] for e in evaluables] == [1, 2, 3]
    assert statuts[2] == "cap_hors_borne"
    assert statuts[3] == "prix_plancher"
    assert statuts[4] == "exchange_hors_perimetre"
    assert statuts[5] == "type_hors_perimetre"
    enreg = retenus[1]
    assert enreg["cap_usd"] == "%.2f" % 20_000_000.0
    assert enreg["nano_micro"] == "nano"
    assert enreg["tranche_prix"] == "[1-5)"
    assert enreg["permco"] == "900"
    # permno 2 partage le permco 900 et franchit C2/C3 (il n'est exclu que
    # par le cap) : permno 1 est donc multi-classe
    assert enreg["multi_classe"] == "True"
    assert enreg["age_cotation_jours_cal"] == str((_jour("2020-06-30")
                                                   - _jour("2015-01-01")).days)
    assert enreg["ratio_reverse_split"] == ""

    # tranches de prix et ratio de reverse split
    lignes2 = [
        _ligne(permno=3, dlyprc="0.50", shrout="1000"),
        _ligne(permno=1, dlyprc="7.00", shrout="1000"),
    ]
    retenus2, _s2, _c2, _e2, _c42 = construire_date("2020-06-30", lignes2, contexte)
    assert retenus2[3]["tranche_prix"] == "[0.10-1)"
    assert retenus2[1]["tranche_prix"] == "[5+)"
    assert retenus2[3]["ratio_reverse_split"] == "%.6f" % (1.0 / 0.25)  # 1 pour 4
    assert retenus2[3]["jours_depuis_reverse_split_cal"] == str(
        (_jour("2020-06-30") - _jour("2020-01-02")).days)
    assert retenus2[3]["multi_classe"] == "False"  # permno 2 absent à t

    # mise de côté faute de prix antérieurs
    retenus3, statuts3, comptages3, evaluables3, _c43 = construire_date(
        "2020-06-30", [_ligne(permno=1, dlyprcflg="BA", dlyprc="2.00")], contexte)
    assert retenus3 == {} and statuts3[1] == "en_attente_passe3"
    assert comptages3["attente_p3_entrerait_si_ba"] == 1
    assert evaluables3 == []  # sans close_ref, C5/C6 ne s'appliquent pas

    # C11 : unités. Le même nombre d'actions donne le même cap par les deux
    # sources : shrout CRSP = 10 000 milliers, val EDGAR = 1e7 actions, sans
    # facteur 1000, soit 20 M$ dans les deux cas.
    contexte_so = dict(contexte)
    contexte_so["utiliser_edgar"] = True
    contexte_so["cikmap"] = {1: "0000000001", 2: "0000000002", 3: "0000000003"}
    contexte_so["so_index"] = {
        1: (["2020-06-01"], [("2020-06-01", "2020-05-31", 1e7)]),
        3: (["2020-07-15"], [("2020-07-15", "2020-06-30", 1e7)]),  # filed > t
    }
    ligne_crsp = _ligne(permno=2, dlyprc="2.00", shrout="10000",
                        securitysubtype="ETF")   # écartée, sert de témoin
    retenus_so, _s, _c, evaluables_so, _c4 = construire_date(
        "2020-06-30", [_ligne(permno=1, dlyprc="2.00", shrout="10000")],
        contexte_so)
    assert retenus_so[1]["so_source"] == "edgar"
    assert retenus_so[1]["so_filed_date"] == "2020-06-01"
    assert retenus_so[1]["cap_usd"] == "%.2f" % 20_000_000.0
    assert retenus_so[1]["shrout_milliers"] == "10000"   # shrout CRSP brut conservé
    retenus_crsp, _s, _c, _e, _c4 = construire_date(
        "2020-06-30", [_ligne(permno=9, dlyprc="2.00", shrout="10000")], contexte)
    assert retenus_crsp[9]["cap_usd"] == retenus_so[1]["cap_usd"]
    assert ligne_crsp["securitysubtype"] == "ETF"        # témoin non modifié

    # ordre de résolution de `so_source`
    assert resoudre_so(1, "2020-06-30", False, False, "10000", contexte_so) \
        == (1e7, "edgar", "2020-06-01")
    assert resoudre_so(1, "2020-06-30", True, False, "10000", contexte_so) \
        == (1e7, "crsp_ads", "")            # ADS : restent sur CRSP
    assert resoudre_so(1, "2020-06-30", False, True, "10000", contexte_so) \
        == (1e7, "crsp_multiclasse", "")    # multi-classe : restent sur CRSP
    assert resoudre_so(7, "2020-06-30", False, False, "10000", contexte_so) \
        == (1e7, "crsp_nomap", "")          # permno sans CIK
    assert resoudre_so(2, "2020-06-30", False, False, "10000", contexte_so) \
        == (1e7, "crsp_nodata", "")         # CIK connu, aucune observation
    assert resoudre_so(3, "2020-06-30", False, False, "10000", contexte_so) \
        == (1e7, "crsp_nodata", "")         # CIK connu, mais filed > t
    # repli CRSP : conversion des milliers en actions
    assert resoudre_so(7, "2020-06-30", False, False, "10000", contexte_so)[0] \
        == float("10000") * 1000.0

    # C11 : contrôle de divergence entre la valeur sélectionnée et le shrout de t
    contexte_div = dict(contexte_so)
    contexte_div["cikmap"] = dict(contexte_so["cikmap"])
    contexte_div["cikmap"].update({10: "0000000010", 11: "0000000011",
                                   12: "0000000012", 84302: "0000896493"})
    contexte_div["so_index"] = dict(contexte_so["so_index"])
    contexte_div["so_index"].update({
        10: (["2020-06-01"], [("2020-06-01", "2020-05-31", 4_999_999.0)]),   # ratio < 0.5
        11: (["2020-06-01"], [("2020-06-01", "2020-05-31", 20_000_001.0)]),  # ratio > 2
        12: (["2020-06-01"], [("2020-06-01", "2020-05-31", 999.0)]),
        # Cas réel reproduit (GPUS, permno 84302) : dei d'avril 2025 de
        # 1,53 M actions, puis dilution d'un facteur 211 ; shrout CRSP de
        # 322 910 milliers au 2025-12-31.
        84302: (["2025-04-15"], [("2025-04-15", "2025-04-14", 1_529_995.0)]),
    })
    # bornes incluses : les ratios 2,0 et 0,5 restent `edgar`
    assert resoudre_so(1, "2020-06-30", False, False, "5000", contexte_div) \
        == (1e7, "edgar", "2020-06-01")              # 1e7 / 5e6   = ratio 2.0
    assert resoudre_so(1, "2020-06-30", False, False, "20000", contexte_div) \
        == (1e7, "edgar", "2020-06-01")              # 1e7 / 2e7   = ratio 0.5
    # juste hors bornes : repli sur CRSP, so_filed_date vide
    assert resoudre_so(10, "2020-06-30", False, False, "10000", contexte_div) \
        == (1e7, "crsp_divergence", "")              # ratio 0.4999999
    assert resoudre_so(11, "2020-06-30", False, False, "10000", contexte_div) \
        == (1e7, "crsp_divergence", "")              # ratio 2.0000001
    # shrout CRSP nul à t : ratio non défini, valeur EDGAR conservée
    assert resoudre_so(12, "2020-06-30", False, False, "0", contexte_div) \
        == (999.0, "edgar", "2020-06-01")
    # GPUS au 2025-12-31 : la valeur d'avril est périmée, repli sur
    # 322 910 000 actions CRSP
    assert resoudre_so(84302, "2025-12-31", False, False, "322910",
                       contexte_div) == (322_910_000.0, "crsp_divergence", "")

    # Audit C9 : permno 1 sort avec shrout(t-1) (cap 400 M$), permno 2 entre
    # (cap 20 M$) ; permno 3, dont le nombre d'actions vient d'EDGAR, n'est
    # pas perturbé bien que son shrout(t-1) le ferait sortir.
    dates_c9 = ["2020-05-29", "2020-06-30"]
    membres_c9 = {"2020-05-29": {1}, "2020-06-30": {1, 3}}
    evaluables_c9 = {"2020-05-29": [],
                     "2020-06-30": [(1, 2.0, "10000", "crsp_nomap", 20e6),
                                    (2, 2.0, "200000", "crsp_nomap", 400e6),
                                    (3, 2.0, "10000", "edgar", 20e6)]}
    shrout_c9 = {(1, "2020-05-29"): "200000", (2, "2020-05-29"): "10000",
                 (3, "2020-05-29"): "200000"}
    resultat = audit_c9(dates_c9, membres_c9, evaluables_c9, shrout_c9)
    assert (resultat[1]["entrees"], resultat[1]["sorties"]) == (1, 1)
    assert resultat[1]["par_source"]["edgar"] == {
        "entrees": 0, "sorties": 0, "base_u": 1, "base_cand": 1}
    assert resultat[1]["base_u"] == 2          # |U_t| = {1, 3}
    assert resultat[1]["base_union"] == 3      # {1,3} ∪ {2,3}
    assert resultat[1]["base_cand"] == 3
    # contre-essai : la même ligne de source CRSP bascule
    evaluables_crsp = {"2020-05-29": [],
                       "2020-06-30": [(3, 2.0, "10000", "crsp_nodata", 20e6)]}
    contre = audit_c9(dates_c9, {"2020-05-29": {1}, "2020-06-30": {3}},
                      evaluables_crsp, shrout_c9)
    assert contre[1]["sorties"] == 1

    # delta d'appartenance
    assert delta_appartenance(["a"], {"a": {1, 2}}, {"a": {2, 3}}) == (1, 1)

    # cause_sortie : un delisting dans (t-1, t] l'emporte, même si le permno a
    # encore une ligne « Last Known » à t
    date_prec, date_t = "2020-05-29", "2020-06-30"
    assert cause_sortie(1, date_prec, date_t, True,
                        {1: "2020-06-15"}, {1: "type_hors_perimetre"}) \
        == ("deliste", "2020-06-15")
    # sans ligne à t
    assert cause_sortie(1, date_prec, date_t, False,
                        {1: "2020-06-15"}, {}) == ("deliste", "2020-06-15")
    # sans ligne à t ni delisting dans l'intervalle : absent_dsf
    assert cause_sortie(1, date_prec, date_t, False, {}, {}) == ("absent_dsf", "")
    # avec ligne à t, sans delisting : cause calculée à t
    assert cause_sortie(1, date_prec, date_t, True, {},
                        {1: "cap_hors_borne"}) == ("cap_hors_borne", "")
    # delisting antérieur à t-1 : ignoré
    assert cause_sortie(1, date_prec, date_t, True,
                        {1: "2020-04-01"}, {1: "prix_plancher"}) \
        == ("prix_plancher", "")

    # charger_fenetres : fusion du complément sans doublon
    import tempfile
    with tempfile.TemporaryDirectory() as rep:
        f_principal = os.path.join(rep, "principal.csv")
        f_complement = os.path.join(rep, "complement.csv")
        with open(f_principal, "w", newline="") as flux:
            flux.write("permno,t,dlycaldt,dlyprc,dlyprcflg\n")
            flux.write("1,2020-06-30,2020-06-25,,NT\n")   # présente dans les deux fichiers
            flux.write("1,2020-06-30,2020-06-26,1.50,TR\n")
            flux.write("2,2020-06-30,2020-06-24,2.00,TR\n")  # cible hors complement
        with open(f_complement, "w", newline="") as flux:
            flux.write("permno,t,dlycaldt,dlyprc,dlyprcflg\n")
            flux.write("1,2020-06-30,2020-06-25,,NT\n")   # doublon, ignoré
            flux.write("1,2020-06-30,2020-06-15,1.10,TR\n")  # séance supplémentaire
        fenetres_f, couvertes_f = charger_fenetres(f_principal, f_complement)
        assert couvertes_f == {(1, "2020-06-30"), (2, "2020-06-30")}
        # la ligne commune, sans prix, n'apparaît pas
        assert fenetres_f[(1, "2020-06-30")] == [
            ("2020-06-15", "1.10", "TR"), ("2020-06-26", "1.50", "TR")]
        assert fenetres_f[(2, "2020-06-30")] == [("2020-06-24", "2.00", "TR")]
        # sans complément
        fenetres_seul, couvertes_seul = charger_fenetres(f_principal, None)
        assert couvertes_seul == {(1, "2020-06-30"), (2, "2020-06-30")}
        assert fenetres_seul[(1, "2020-06-30")] == [("2020-06-26", "1.50", "TR")]
        # complément inexistant : ignoré
        fenetres_absent, _ = charger_fenetres(
            f_principal, os.path.join(rep, "n_existe_pas.csv"))
        assert fenetres_absent == fenetres_seul

    print("tests : OK")


def _usage():
    print("Usage :")
    print("  python3 build_univers.py --test   # autotests unitaires seuls")
    print("  python3 build_univers.py --build  # autotests puis construction "
          "de l'univers (ecrit dans sorties/univers/)")


if __name__ == "__main__":
    # Seuls --test et --build agissent ; tout autre appel affiche l'usage.
    if len(sys.argv) == 2 and sys.argv[1] == "--test":
        autotests()
    elif len(sys.argv) == 2 and sys.argv[1] == "--build":
        autotests()
        executer()
    else:
        _usage()
        sys.exit(0 if len(sys.argv) == 2 and sys.argv[1] in ("--help", "-h") else 2)
