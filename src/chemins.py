"""Chemins du projet.

Les données de marché (CRSP, Databento, Massive) sont sous licence et ne sont
pas versionnées : elles vont dans `data/`. Les sorties de calcul, régénérables,
vont dans `sorties/`. Seules les tables de référence (bourses, conditions de
vente SIP, demi-séances) sont dans le dépôt.
"""

import sys
from pathlib import Path

SRC = Path(__file__).resolve().parent
RACINE = SRC.parent
REFERENCES = SRC / "references"
DONNEES = RACINE / "data"
SORTIES = RACINE / "sorties"

for _sous_dossier in ("donnees", "primitives", "corpus"):
    _p = str(SRC / _sous_dossier)
    if _p not in sys.path:
        sys.path.insert(0, _p)
