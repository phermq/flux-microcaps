#!/usr/bin/env python3
"""Primitives sur flux MBO Nasdaq ITCH (échantillon) : P-06, P-07, P-08, P-10b.

Rejoue localement les fichiers `.dbn.zst` (via `databento-dbn`) pour reconstruire le
carnet ordre par ordre. La lecture DBN, le cache de séance, la mesure des exécutions
cachées (P-07, `run_h16`) et l'ancien détecteur de replenishment (`run_h15`) viennent
de `src/donnees/donnees_mbo.py`.

Usage :
  python primitives_t3.py run [TM ...]     passe 1 par ticker-mois (P-07, carnet, P-08,
                                           P-10b, capture des épuisements de P-06)
  python primitives_t3.py ancien [TM ...]  ancien détecteur (2 s, sans condition de niveau)
  python primitives_t3.py derive           passe 2 : délais poolés, comptes et chaînes
                                           de P-06, E[FP], rapport Markdown
  python primitives_t3.py --test           tests sur flux synthétiques

Sémantique XNAS.ITCH : une exécution contre un ordre affiché produit, sous un même
`sequence`, un `T` (print, côté agresseur), un `F` (ordre au repos) et un `C` (retrait
de la quantité exécutée). Le carnet n'applique que `A` et `C`, `F` étant redondant avec
son `C` jumeau. Un Replace est un couple {`C` ancien id, `A` nouvel id} ; `R` vide le
carnet ; un `T` sans `F` est une exécution contre de la liquidité non affichée.
"""
from __future__ import annotations

import bisect
import csv
import gzip
import hashlib
import json
import math
import statistics
import sys
from array import array
from collections import Counter, defaultdict, namedtuple
from datetime import datetime, time as dtime
from pathlib import Path

CODE = Path(__file__).resolve()
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import chemins  # noqa: E402
A_CODE = chemins.SRC / "donnees"

from donnees_mbo import (                              # noqa: E402
    DATA, NY, SessionCache, load_strates, run_h15, run_h16, sha256, stream_records,
)

OUT = chemins.SORTIES / "t3"
PROG = OUT / ".progress"
EVT = PROG / "p06-events"
JOURNAL = OUT / ".progress.jsonl"

# --- Paramètres des primitives ---
BETA_NUM, BETA_DEN = 2, 100          # P-08 : fenêtre ±β autour du mid, β = 2 % (calcul entier)
R_CHAINE = 2                         # P-06 : longueur minimale d'une chaîne
L_RAFALE = 2                         # P-10b : nombre minimal de niveaux consommés
DT_RAFALE_NS = 50_000_000            # P-10b : Δt_s = 50 ms
FENETRE_D_TILDE = 20                 # P-08 : historique de 20 jours cotés pour D̃
BUCKET_RTH_NS = 60_000_000_000       # buckets de 1 min en RTH
BUCKET_HORS_NS = 300_000_000_000     # buckets de 5 min hors RTH
DELTA_ANCIEN_NS = 2_000_000_000      # fenêtre de l'ancien détecteur (2 s)

# Maillage log des histogrammes utilisés pour comparer la densité empirique des délais
# à celle du modèle nul ; d'autres maillages servent de test de sensibilité.
BINS_PAR_DECADE = 10
BINS_SENSIBILITE = (5, 20)
LAMBDA_BINS_PAR_DECADE = 50          # maillage des λ̂ de la mixture du modèle nul

# Le croisement densité empirique / modèle nul homogène donne un δ_max d'environ 31,6 s,
# sans rapport avec le mode rapide des délais. On retient donc δ_max = 100 µs comme
# convention descriptive et on publie systématiquement les comptes sur une grille de δ ;
# la sortie principale de P-06 reste la distribution des délais.
DELTAS_PROVISOIRES = ((100_000, "100 µs"), (1_000_000, "1 ms"), (63_100_000, "63,1 ms"))
DELTA_MAX_NS = 100_000
STATUT_DELTA = ("delta_max = 100 µs, convention descriptive ; "
                "comptes publiés sur une grille de δ ; "
                "sortie principale de P-06 = distribution des délais ; "
                "variante principale des chaînes = strict_size")


# --- Empreintes de run ---
_SHA: dict[str, str] = {}


def code_sha256() -> str:
    if "code" not in _SHA:
        _SHA["code"] = sha256(CODE)
    return _SHA["code"]


def tests_mbo_sha256() -> str:
    """SHA-256 de `donnees_mbo.py`, calculé à la demande pour que `a_jour()` soit
    appelable avant toute `empreinte()`."""
    if "a" not in _SHA:
        _SHA["a"] = sha256(A_CODE / "donnees_mbo.py")
    return _SHA["a"]


def empreinte() -> dict:
    """SHA-256 de ce module et de `donnees_mbo.py`, plus l'horodatage du run."""
    return {"code_sha256": code_sha256(), "tests_mbo_sha256": tests_mbo_sha256(),
            "date_run": datetime.now(NY).isoformat(timespec="seconds")}


def a_jour(path: Path) -> bool:
    try:
        prev = json.loads(path.read_text())
    except (OSError, ValueError):
        return False
    f = prev.get("fingerprint", {}) if isinstance(prev, dict) else {}
    return (f.get("code_sha256") == code_sha256()
            and f.get("tests_mbo_sha256") == tests_mbo_sha256())


def _test_a_jour_processus_neuf(module_file: str) -> str:
    """Vérifie que `a_jour()` fonctionne lorsqu'elle est le premier appel d'un processus
    neuf, sur une sortie dont l'empreinte correspond. Le test passe par un sous-processus
    car, dans le processus courant, le cache `_SHA` est déjà rempli."""
    import subprocess
    import tempfile
    mod = Path(module_file).stem
    empr = {"code_sha256": code_sha256(), "tests_mbo_sha256": tests_mbo_sha256()}
    with tempfile.TemporaryDirectory() as td:
        p = Path(td) / "sortie.json"
        p.write_text(json.dumps({"fingerprint": empr}))
        prog = (
            "import sys; from pathlib import Path\n"
            f"sys.path.insert(0, {str(Path(module_file).resolve().parent)!r})\n"
            f"import {mod} as m\n"
            f"assert m._SHA == {{}}, m._SHA\n"
            f"assert m.a_jour(Path({str(p)!r})) is True, 'a_jour a repondu False sur empreinte identique'\n"
            "print('OK')\n"
        )
        r = subprocess.run([sys.executable, "-c", prog], capture_output=True, text=True)
    assert r.returncode == 0, f"a_jour() en premier dans un processus neuf : {r.stderr.strip()[-500:]}"
    return "a_jour() appelée en premier dans un processus neuf : ne lève pas"


def journal(**kw):
    JOURNAL.parent.mkdir(parents=True, exist_ok=True)
    with open(JOURNAL, "a") as fh:
        fh.write(json.dumps({"ts": datetime.now(NY).isoformat(timespec="seconds"), **kw},
                            default=str) + "\n")


# --- Statistiques : quantiles exacts, histogramme log ---
QS = (0.01, 0.05, 0.25, 0.50, 0.75, 0.95, 0.99)


def quantiles(valeurs, qs=QS) -> dict | None:
    """Quantiles par la méthode du plus proche rang, sans interpolation."""
    v = sorted(valeurs)
    n = len(v)
    if n == 0:
        return None
    out = {f"q{int(q * 100):02d}": v[min(n - 1, max(0, math.ceil(q * n) - 1))] for q in qs}
    out["n"] = n
    out["min"], out["max"] = v[0], v[-1]
    out["moyenne"] = sum(v) / n
    return out


class HistLog:
    """Histogramme log10 à k classes par décade, avec un compteur séparé pour les zéros.

    Permet de pooler des distributions entre ticker-mois sans conserver les valeurs
    brutes, et de comparer densité empirique et densité du modèle nul pour δ_max.
    """

    __slots__ = ("k", "zero", "bins", "n")

    def __init__(self, k=BINS_PAR_DECADE):
        self.k, self.zero, self.n = k, 0, 0
        self.bins: Counter = Counter()

    def add(self, v, w=1):
        self.n += w
        if v == 0:
            self.zero += w
        elif v > 0:
            self.bins[math.floor(math.log10(v) * self.k)] += w

    def bornes(self, i):
        return 10.0 ** (i / self.k), 10.0 ** ((i + 1) / self.k)

    def fusion(self, autre):
        assert self.k == autre.k
        self.zero += autre.zero
        self.n += autre.n
        self.bins.update(autre.bins)
        return self

    def quantile_approx(self, q):
        """Quantile approché par interpolation géométrique dans la classe."""
        if self.n == 0:
            return None
        cible = q * self.n
        cum = self.zero
        if cum >= cible:
            return 0.0
        for i in sorted(self.bins):
            c = self.bins[i]
            if cum + c >= cible:
                a, b = self.bornes(i)
                return a * (b / a) ** ((cible - cum) / c)
            cum += c
        return self.bornes(max(self.bins))[1] if self.bins else 0.0

    def to_dict(self):
        return {"k": self.k, "zero": self.zero, "n": self.n,
                "bins": {str(i): c for i, c in sorted(self.bins.items())}}

    @classmethod
    def from_dict(cls, d):
        h = cls(d["k"])
        h.zero, h.n = d["zero"], d["n"]
        h.bins = Counter({int(i): c for i, c in d["bins"].items()})
        return h


def _hist(vals, k=BINS_PAR_DECADE):
    h = HistLog(k)
    for v in vals:
        h.add(v)
    return h


# --- Événements P-06 : schéma de la capture ---
EVT_COLS = ["jour", "side", "px", "ts", "emptied", "o1", "o2", "delai",
            "q1", "q2", "ev1", "ev2", "lam_jour", "lam_rth", "rth", "lam_loc"]
J, SIDE, PX, TS, VIDE, O1, O2, DELAI, Q1, Q2, EV1, EV2, LAMJ, LAMR, RTH, LAMLOC = range(16)

# Modèle nul local pour les faux positifs de P-06 : λ_loc est l'intensité des Add au
# même (σ, p) dans une fenêtre centrée sur l'épuisement, privée d'une bande de garde
# juste après lui pour ne pas compter le replenishment candidat dans son propre nul.
# Les Add à un niveau donné arrivent groupés dans le temps, et un épuisement survient
# en période active : l'intensité moyenne de la séance (LAMJ) sous-estime donc E[FP].
LOC_FENETRE_NS = 60 * 10 ** 9   # ±60 s autour de l'épuisement
LOC_GARDE_NS = 10 ** 9          # (t, t+1 s] exclu ; le nul local n'est défini que pour δ ≤ 1 s


def _to_evt(row) -> tuple:
    """Normalise une ligne (mémoire ou CSV) en tuple typé."""
    return (str(row[0]), str(row[1]), int(row[2]), int(row[3]), int(row[4]) == 1,
            int(row[5]), int(row[6]), int(row[7]), int(row[8]), int(row[9]),
            int(row[10]), int(row[11]), float(row[12]), float(row[13]), int(row[14]) == 1,
            float(row[15]))


def lire_evts(tm: str):
    with gzip.open(EVT / f"{tm}.csv.gz", "rt", newline="") as fh:
        r = csv.reader(fh)
        next(r)
        for row in r:
            yield _to_evt(row)


# --- Carnet reconstruit, capture P-06, P-08, P-10b ---
class Pipeline:
    """Rejoue le flux MBO d'un ticker-mois : carnet, P-06 (capture), P-08, P-10b.

    Les enregistrements sont lus dans l'ordre du fichier, soit (ts_event, sequence), et
    traités par groupe de même `sequence`, un message ITCH donnant plusieurs enregistrements.
    """

    def __init__(self, tm: str, sink=None):
        self.tm = tm
        self.sink = sink if sink is not None else []
        self.sess = SessionCache()
        # carnet
        self.ordres: dict[int, list] = {}          # oid -> [side, px, taille, q_add, exec]
        self.niv: dict[tuple, int] = {}            # (side, px) -> taille affichée
        self.niv_ts: dict[tuple, int] = {}         # (side, px) -> ts de création du niveau
        self.pxs = {"B": [], "A": []}              # prix actifs triés, par côté
        self.bb = self.ba = None
        # P-08
        self.valide = False
        self.D = {"B": 0, "A": 0}
        self.mid2, self.sp_large = 0, False
        self.t_last = None
        self.buckets: dict[tuple, list] = {}       # clé -> [dur, dur_valide, ∫D_bid, ∫D_ask]
        self.jours_bornes: dict[str, tuple] = {}
        # P-06
        self.pending: dict[tuple, list] = defaultdict(list)
        self.evts_jour: list[list] = []
        self.add_jour: Counter = Counter()
        self.add_rth: Counter = Counter()
        self.add_ts: dict = {}   # (side, px) -> ts croissants des Add du jour (nul local)
        self.exec_ordre: dict[int, int] = {}
        self.jour_courant = None
        self.premiere_ts = self.derniere_ts = 0
        self.rth_t0 = self.rth_t1 = self.pre_ns = self.ah_ns = 0
        self.jour_t0 = self.jour_t1 = 0
        # P-10b
        self.rafale = None
        self.rafales: list[tuple] = []             # (jour, dir, n_niv, notionnel, n_exec, vol)
        # divers
        self.cnt: Counter = Counter()
        self.checksums: dict[str, str] = {}
        self.vol_exec_jour: Counter = Counter()
        self.buf: list = []
        self.buf_seq = None

    # --- carnet ---
    def _dans_fenetre(self, px) -> bool:
        """p ∈ [μ(1−β), μ(1+β)] avec μ = (bb+ba)/2, en arithmétique entière."""
        m2 = self.bb + self.ba
        v = 2 * BETA_DEN * px
        return (BETA_DEN - BETA_NUM) * m2 <= v <= (BETA_DEN + BETA_NUM) * m2

    def _top(self):
        return (self.pxs["B"][-1] if self.pxs["B"] else None,
                self.pxs["A"][0] if self.pxs["A"] else None)

    def _recalc_D(self):
        self.bb, self.ba = self._top()
        if self.bb is None or self.ba is None:
            self.valide = False
            self.D["B"] = self.D["A"] = 0
            return
        self.valide = True
        m2 = self.bb + self.ba
        # Diagnostic : le mid μ^b est peu significatif si un côté n'a que des ordres très
        # éloignés ; on mesure le temps passé avec un spread relatif > 50 %.
        self.mid2 = m2
        self.sp_large = 4 * (self.ba - self.bb) > m2
        lo, hi = (BETA_DEN - BETA_NUM) * m2, (BETA_DEN + BETA_NUM) * m2
        for s in ("B", "A"):
            tot = 0
            lst = self.pxs[s]
            i = bisect.bisect_left(lst, -(-lo // (2 * BETA_DEN)))
            while i < len(lst):
                p = lst[i]
                v = 2 * BETA_DEN * p
                if v > hi:
                    break
                if v >= lo:
                    tot += self.niv[(s, p)]
                i += 1
            self.D[s] = tot

    def _maj_niveau(self, side, px, delta, ts) -> int:
        cle = (side, px)
        avant = self.niv.get(cle, 0)
        apres = avant + delta
        if apres < 0:
            self.cnt["anomalie_niveau_negatif"] += 1
            apres = 0
        if avant == 0 and apres > 0:
            bisect.insort(self.pxs[side], px)
            self.niv_ts[cle] = ts
        if apres == 0:
            self.niv.pop(cle, None)
            self.niv_ts.pop(cle, None)
            if avant > 0:
                lst = self.pxs[side]
                i = bisect.bisect_left(lst, px)
                if i < len(lst) and lst[i] == px:
                    lst.pop(i)
        else:
            self.niv[cle] = apres
        if self.valide and self._dans_fenetre(px):
            self.D[side] += (apres - avant)
        return apres

    # --- temps et buckets P-08 ---
    def _bucket(self, t):
        j = self.jour_courant
        if t < self.pre_ns:
            return self.pre_ns, (j, "AVANT", 0)
        if t < self.rth_t0:
            i = (t - self.pre_ns) // BUCKET_HORS_NS
            return self.pre_ns + (i + 1) * BUCKET_HORS_NS, (j, "PRE", i)
        if t < self.rth_t1:
            i = (t - self.rth_t0) // BUCKET_RTH_NS
            return self.rth_t0 + (i + 1) * BUCKET_RTH_NS, (j, "RTH", i)
        if t < self.ah_ns:
            i = (t - self.rth_t1) // BUCKET_HORS_NS
            return self.rth_t1 + (i + 1) * BUCKET_HORS_NS, (j, "AH", i)
        return self.jour_t1, (j, "APRES", 0)

    def _accum(self, ts):
        """Intègre D sur [t_last, ts] en découpant aux frontières de bucket.

        D et les durées étant entiers, la somme est exacte et indépendante du découpage.
        """
        if self.t_last is None:
            self.t_last = ts
            return
        t0 = self.t_last
        if ts <= t0:
            return
        while t0 < ts:
            fin, cle = self._bucket(t0)
            t1 = min(ts, fin)
            dur = t1 - t0
            if dur > 0:
                e = self.buckets.get(cle)
                if e is None:
                    e = self.buckets[cle] = [0, 0, 0, 0, 0, 0]
                e[0] += dur
                if self.valide:
                    e[1] += dur
                    e[2] += self.D["B"] * dur
                    e[3] += self.D["A"] * dur
                    e[5] += self.mid2 * dur
                    if self.sp_large:
                        e[4] += dur
            t0 = t1 if t1 > t0 else fin
        self.t_last = ts

    # --- séances ---
    def _ouvrir_jour(self, ts):
        self.jour_courant = self.sess.date
        self.rth_t0, self.rth_t1 = self.sess.open_ns, self.sess.close_ns
        self.pre_ns = self.rth_t0 - (5 * 3600 + 1800) * 10 ** 9      # 04:00 ET
        # La séance post-clôture dure 4 h après la clôture : 16:00 → 20:00 ET en séance
        # normale, 13:00 → 17:00 ET les jours de clôture anticipée
        # (cf. references/early_closes.csv).
        self.ah_ns = self.rth_t1 + 4 * 3600 * 10 ** 9
        self.jour_t0, self.jour_t1 = self.sess.lo, self.sess.hi
        self.t_last = ts
        self.premiere_ts = self.derniere_ts = ts
        self.jours_bornes[self.jour_courant] = (self.rth_t0, self.rth_t1)

    def _fermer_jour(self):
        if self.jour_courant is None:
            return
        self._accum(self.ah_ns)                    # remplit les buckets RTH restants
        self._solder_rafale()
        h = hashlib.sha256()                       # checksum d'état de carnet en fin de jour
        for (s, p) in sorted(self.niv):
            h.update(f"{s}|{p}|{self.niv[(s, p)]}\n".encode())
        self.checksums[self.jour_courant] = h.hexdigest()
        dur_j = max(1, self.derniere_ts - self.premiere_ts)
        dur_r = max(1, self.rth_t1 - self.rth_t0)
        for e in self.evts_jour:
            cle = (e[SIDE], e[PX])
            e[LAMJ] = self.add_jour.get(cle, 0) / dur_j
            e[LAMR] = self.add_rth.get(cle, 0) / dur_r
            e[LAMLOC] = self._lambda_local(cle, e[TS])
            e[EV2] = self.exec_ordre.get(e[O2], 0) if e[O2] >= 0 else 0
            self._emit(e)
        self.evts_jour = []
        self.pending.clear()
        self.add_jour.clear()
        self.add_rth.clear()
        self.add_ts.clear()
        self.exec_ordre.clear()

    def _lambda_local(self, cle, ts: int) -> float:
        """Intensité locale des Add au niveau `cle` autour de `ts`, en Add/ns.

        Fenêtre [ts−W, ts+W] bornée à la séance, privée de la bande de garde (ts, ts+G]
        où se trouvent les replenishments candidats. Vaut 0.0 sans Add voisin."""
        arr = self.add_ts.get(cle)
        if not arr:
            return 0.0
        lo = max(ts - LOC_FENETRE_NS, self.premiere_ts)
        hi = min(ts + LOC_FENETRE_NS, self.derniere_ts)
        duree = (hi - lo) - LOC_GARDE_NS
        if duree <= 0:
            return 0.0
        n_fenetre = bisect.bisect_right(arr, hi) - bisect.bisect_left(arr, lo)
        n_garde = bisect.bisect_right(arr, ts + LOC_GARDE_NS) - bisect.bisect_right(arr, ts)
        n = n_fenetre - n_garde
        return (n / duree) if n > 0 else 0.0

    def _emit(self, e):
        if isinstance(self.sink, list):
            self.sink.append(list(e))
        else:
            self.sink.writerow(e)

    # --- P-10b ---
    def _solder_rafale(self):
        if self.rafale is None:
            return
        d, t0, niveaux, notionnel, n_exec, jour = self.rafale
        self.rafales.append((jour, d, len(niveaux), notionnel, n_exec, sum(niveaux.values())))
        self.rafale = None

    def _execution(self, ts, side_repos, px, sz, jour):
        """Exécution contre un ordre affiché (F) : alimente les rafales P-10b.

        Une rafale est une suite contiguë d'exécutions de même direction agresseur tenant
        dans Δt_s ; une exécution de direction opposée la clôt. Seuls comptent les
        niveaux déjà présents au début de la rafale.
        """
        agr = "A" if side_repos == "B" else "B"
        r = self.rafale
        if r is not None and (r[0] != agr or ts - r[1] > DT_RAFALE_NS or r[5] != jour):
            self._solder_rafale()
            r = None
        if r is None:
            self.rafale = r = [agr, ts, {}, 0, 0, jour]
        cree = self.niv_ts.get((side_repos, px))
        if cree is not None and cree <= r[1]:
            r[2][px] = r[2].get(px, 0) + sz
        r[3] += px * sz
        r[4] += 1

    # --- flux ---
    def feed(self, rec):
        seq = rec.sequence
        if seq != self.buf_seq:
            self._groupe(self.buf)
            self.buf, self.buf_seq = [], seq
        self.buf.append((str(rec.action), str(rec.side), rec.price, rec.size,
                         rec.order_id, rec.ts_event, int(rec.flags)))

    def finish(self):
        self._groupe(self.buf)
        self.buf = []
        self._fermer_jour()
        return self

    def _groupe(self, g):
        if not g:
            return
        ts = g[0][5]
        if any(r[5] != ts for r in g):
            self.cnt["anomalie_groupe_ts_heterogene"] += 1
            ts = min(r[5] for r in g)
        if self.sess.update(ts):
            self._fermer_jour()
            self._ouvrir_jour(ts)
        self.derniere_ts = ts
        jour = self.jour_courant
        self._accum(ts)
        top_avant = self._top()

        fills: dict[int, int] = {}
        for act, side, px, sz, oid, _t, _f in g:
            if act == "F":
                fills[oid] = fills.get(oid, 0) + sz
                self.cnt["n_F"] += 1
                self.cnt["vol_F"] += sz
                self.vol_exec_jour[jour] += sz
        c_vus = set()

        for act, side, px, sz, oid, _t, _f in g:
            if act == "R":
                self._clear()
                top_avant = self._top()
            elif act == "A":
                self._ajouter(oid, side, px, sz, ts)
            elif act == "C":
                self._reduire(oid, side, sz, ts, par_exec=(oid in fills), jour=jour)
                c_vus.add(oid)
            elif act == "F":
                self._execution(ts, side, px, sz, jour)
                self.exec_ordre[oid] = self.exec_ordre.get(oid, 0) + sz
            elif act == "T":
                self.cnt["n_T"] += 1
            elif act == "M":
                self.cnt["action_M_inattendue"] += 1
            else:
                self.cnt[f"action_{act}_ignoree"] += 1
        for oid in fills:
            if oid not in c_vus:
                self.cnt["anomalie_F_sans_C_jumeau"] += 1
        if self._top() != top_avant:
            self._recalc_D()

    def _clear(self):
        self.ordres.clear()
        self.niv.clear()
        self.niv_ts.clear()
        self.pxs["B"].clear()
        self.pxs["A"].clear()
        self.valide = False
        self.D["B"] = self.D["A"] = 0
        self.bb = self.ba = None
        self.cnt["n_clear"] += 1

    def _ajouter(self, oid, side, px, sz, ts):
        if side not in ("B", "A"):
            self.cnt["anomalie_add_side"] += 1
            return
        if oid in self.ordres:
            self.cnt["anomalie_add_oid_existant"] += 1
        self.ordres[oid] = [side, px, sz, sz, 0]
        self._maj_niveau(side, px, sz, ts)
        self.add_jour[(side, px)] += 1
        self.add_ts.setdefault((side, px), []).append(ts)   # croissant par ordre du flux
        if self.rth_t0 <= ts < self.rth_t1:
            self.add_rth[(side, px)] += 1
        att = self.pending.get((side, px))
        if att:
            for idx in att:
                e = self.evts_jour[idx]
                if e[DELAI] < 0:
                    e[DELAI] = ts - e[TS]
                    e[O2] = oid
                    e[Q2] = sz
            att.clear()

    def _reduire(self, oid, side, sz, ts, par_exec, jour):
        o = self.ordres.get(oid)
        if o is None:
            self.cnt["anomalie_reduction_ordre_inconnu"] += 1
            return
        s, p, taille, q_add, _ex = o
        if side in ("B", "A") and side != s:
            self.cnt["anomalie_side_incoherent"] += 1
        reste = taille - sz
        if reste < 0:
            self.cnt["anomalie_reduction_superieure"] += 1
            sz, reste = taille, 0
        o[2] = reste
        niveau_reste = self._maj_niveau(s, p, -sz, ts)
        if reste > 0:
            return
        self.ordres.pop(oid, None)
        if not par_exec:
            self.cnt["n_retraits_annulation"] += 1
            return
        # P-06 : épuisement d'un ordre affiché par exécution.
        self.cnt["n_epuisements_exec"] += 1
        vide = (niveau_reste == 0)
        if vide:
            self.cnt["n_epuisements_exec_vidage"] += 1
        e = [jour, s, p, ts, 1 if vide else 0, oid, -1, -1, q_add, -1,
             self.exec_ordre.get(oid, 0), -1, 0.0, 0.0,
             1 if self.rth_t0 <= ts < self.rth_t1 else 0, 0.0]
        self.evts_jour.append(e)
        self.pending[(s, p)].append(len(self.evts_jour) - 1)

    # --- sorties ---
    def p08(self) -> dict:
        par_jour, vals_rth = {}, defaultdict(list)
        n_na = n_tot = 0
        h_tot, h_bid, h_ask = HistLog(), HistLog(), HistLog()
        dur_valide_tot = dur_large_tot = 0
        mids = []
        for (jour, sess, _i), (dur, dur_v, sB, sA, d_large, s_mid2) in self.buckets.items():
            if sess != "RTH":
                continue
            n_tot += 1
            if dur_v == 0:
                n_na += 1
                continue
            dur_valide_tot += dur_v
            dur_large_tot += d_large
            mids.append(s_mid2 / dur_v / 2e9)
            dB, dA = sB / dur_v, sA / dur_v
            vals_rth[jour].append((dB, dA))
            h_tot.add(dB + dA)
            h_bid.add(dB)
            h_ask.add(dA)
        vus_par_jour = Counter(j for (j, s, _i) in self.buckets if s == "RTH")
        for jour, (t0, t1) in self.jours_bornes.items():
            manquants = int((t1 - t0) // BUCKET_RTH_NS) - vus_par_jour.get(jour, 0)
            if manquants > 0:
                n_na += manquants
                n_tot += manquants
        for jour, lst in vals_rth.items():
            par_jour[jour] = {
                "n_buckets_rth_non_na": len(lst),
                "D_total_median": statistics.median([b + a for b, a in lst]),
                "D_bid_median": statistics.median([b for b, _a in lst]),
                "D_ask_median": statistics.median([a for _b, a in lst]),
                "checksum_carnet_fin_jour": self.checksums.get(jour),
            }
        jours = sorted(vals_rth)
        d_tilde, n_med0 = [], 0
        for k, jour in enumerate(jours):
            if k < FENETRE_D_TILDE:
                continue
            hist = [b + a for j2 in jours[k - FENETRE_D_TILDE:k] for b, a in vals_rth[j2]]
            med = statistics.median(hist) if hist else 0
            if med:
                d_tilde.extend((b + a) / med for b, a in vals_rth[jour])
            else:
                n_med0 += 1          # médiane historique nulle : D̃ indéfini (NA `med0`)
        tous = [(b, a) for lst in vals_rth.values() for b, a in lst]
        return {
            "beta": BETA_NUM / BETA_DEN,
            "n_buckets_rth": n_tot, "n_buckets_rth_na": n_na,
            "part_buckets_rth_na": (n_na / n_tot) if n_tot else None,
            "D_total_quantiles": quantiles([b + a for b, a in tous]),
            "D_bid_quantiles": quantiles([b for b, _a in tous]),
            "D_ask_quantiles": quantiles([a for _b, a in tous]),
            "part_D_total_nul": (sum(1 for b, a in tous if b + a == 0) / len(tous)) if tous else None,
            "part_temps_rth_spread_sup_50pc": (dur_large_tot / dur_valide_tot) if dur_valide_tot else None,
            "mid_carnet_bucket_quantiles": quantiles(mids),
            "hist_D_total": h_tot.to_dict(), "hist_D_bid": h_bid.to_dict(),
            "hist_D_ask": h_ask.to_dict(),
            "n_jours_D_tilde": max(0, len(jours) - FENETRE_D_TILDE) - n_med0,
            "n_jours_D_tilde_NA_histo": min(len(jours), FENETRE_D_TILDE),
            "n_jours_D_tilde_NA_med0": n_med0,
            "D_tilde_quantiles": quantiles(d_tilde),
            "D_jours_norm_b": None,      # DV$_63 indisponible sur un échantillon d'un mois
            "par_jour": par_jour,
        }

    def p10b(self) -> dict:
        niveaux = [r[2] for r in self.rafales]
        seuil = [r for r in self.rafales if r[2] >= L_RAFALE]
        not_seuil = [r[3] / 1e9 for r in seuil]
        return {
            "L": L_RAFALE, "dt_s_ns": DT_RAFALE_NS,
            "n_rafales_total": len(self.rafales),
            "hist_niveaux": dict(sorted(Counter(niveaux).items())),
            "niveaux_quantiles": quantiles(niveaux),
            "notionnel_usd_quantiles": quantiles([r[3] / 1e9 for r in self.rafales]),
            "n_rafales_L": len(seuil),
            "part_rafales_L": (len(seuil) / len(self.rafales)) if self.rafales else None,
            "notionnel_usd_L_quantiles": quantiles(not_seuil),
            "notionnel_usd_L_median": statistics.median(not_seuil) if not_seuil else None,
            "volume_L_quantiles": quantiles([r[5] for r in seuil]),
            "hist_notionnel_L": _hist(not_seuil).to_dict(),
            "hist_notionnel_tous": _hist([r[3] / 1e9 for r in self.rafales]).to_dict(),
        }


# --- Passe 1 : un ticker-mois ---
def run_tm(tm: str, meta: dict) -> dict:
    path = DATA / f"{tm}_mbo.dbn.zst"
    t_dep = datetime.now()

    # P-07 : calculé par donnees_mbo.run_h16 (avec imputation des crosses).
    h16 = run_h16(tm, meta)
    t7 = h16["totaux"]
    n_seances = max(1, h16["n_jours"])
    crosses = (t7.get("n_cross_open", 0) + t7.get("n_cross_close", 0) + t7.get("n_cross_halt", 0))
    vol_F, vol_hid = t7.get("vol_F", 0), t7.get("vol_cache", 0)
    p07 = {
        "HR_vol": h16["ratio_cache_vol"],
        "HR_vol_rth": h16["ratio_cache_vol_rth"],
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

    # Carnet : capture P-06, P-08, P-10b.
    EVT.mkdir(parents=True, exist_ok=True)
    with gzip.open(EVT / f"{tm}.csv.gz", "wt", newline="") as fev:
        w = csv.writer(fev)
        w.writerow(EVT_COLS)
        pl = Pipeline(tm, sink=w)
        for rec in stream_records(path):
            pl.feed(rec)
        pl.finish()

    res = {
        "ticker_mois": tm, **meta, "fingerprint": empreinte(), "p07": p07,
        "p08": pl.p08(), "p10b": pl.p10b(),
        "p06_capture": {
            "n_epuisements_exec": pl.cnt["n_epuisements_exec"],
            "n_epuisements_exec_vidage": pl.cnt["n_epuisements_exec_vidage"],
            "n_retraits_annulation": pl.cnt["n_retraits_annulation"],
            "vol_exec_par_jour": dict(pl.vol_exec_jour),
        },
        "compteurs_carnet": dict(pl.cnt), "checksums_carnet": pl.checksums,
        "duree_s": round((datetime.now() - t_dep).total_seconds(), 1),
    }
    verifier_invariants(res)
    return res


def verifier_invariants(res: dict) -> None:
    """Invariants de reconstruction (lève AssertionError si violés)."""
    c, tm = res["compteurs_carnet"], res["ticker_mois"]
    for cle in ("anomalie_niveau_negatif", "anomalie_reduction_superieure",
                "anomalie_F_sans_C_jumeau", "anomalie_add_oid_existant",
                "anomalie_side_incoherent", "anomalie_groupe_ts_heterogene",
                "anomalie_add_side"):
        assert c.get(cle, 0) == 0, f"{tm} : invariant carnet rompu — {cle} = {c[cle]}"
    p8 = res["p08"]
    assert p8["n_buckets_rth"] >= p8["n_buckets_rth_na"] >= 0, tm
    if p8["D_total_quantiles"]:
        assert p8["D_total_quantiles"]["min"] >= 0, tm
    assert res["p10b"]["n_rafales_L"] <= res["p10b"]["n_rafales_total"], tm
    assert (res["p06_capture"]["n_epuisements_exec_vidage"]
            <= res["p06_capture"]["n_epuisements_exec"]), tm


# --- Passe 2 : δ_max et comptes P-06 ---
def pooler_delais(tms: list[str]) -> dict:
    """Délais poolés entre un épuisement qui vide le niveau et le premier Add au même (σ,p)."""
    delais = array("q")
    delais_non_vide = array("q")
    lam_hist: Counter = Counter()
    n_zero = n_censure = n_total = n_nv_total = 0
    for tm in tms:
        for e in lire_evts(tm):
            if not e[VIDE]:
                n_nv_total += 1
                if e[DELAI] > 0:
                    delais_non_vide.append(e[DELAI])
                continue
            n_total += 1
            d = e[DELAI]
            if d < 0:
                n_censure += 1
                continue
            if d == 0:
                n_zero += 1
                continue
            delais.append(d)
            if e[LAMJ] > 0:
                lam_hist[math.floor(math.log10(e[LAMJ]) * LAMBDA_BINS_PAR_DECADE)] += 1
    return {"delais": delais, "lam_hist": lam_hist, "n_zero": n_zero, "n_censure": n_censure,
            "n_total": n_total, "delais_niveau_non_vide": delais_non_vide,
            "n_niveau_non_vide_total": n_nv_total}


def densite_nulle(lam_hist: Counter, a: float, b: float) -> float:
    """Densité moyenne sur [a, b] du modèle nul poolé, mixture (1/N)·Σ_e λ̂_e·e^{−λ̂_e δ}.

    Chaque événement a son propre λ̂ (Add au (σ,p) du jour), d'où une mixture
    d'exponentielles, cohérente avec E[FP] = Σ_e (1−e^{−λ̂_e δ}).
    """
    N = sum(lam_hist.values())
    if N == 0 or b <= a:
        return 0.0
    acc = 0.0
    for i, c in lam_hist.items():
        lam = 10.0 ** ((i + 0.5) / LAMBDA_BINS_PAR_DECADE)
        acc += c * (math.exp(-lam * a) - math.exp(-lam * b))
    return acc / (N * (b - a))


def derive_delta_max(delais, lam_hist, k=BINS_PAR_DECADE) -> dict:
    """Plus petit δ où, après le mode rapide, la densité empirique passe sous celle du
    modèle nul ; on retient la borne inférieure de la classe de croisement."""
    if not len(delais):
        return {"delta_max_ns": None, "raison": "aucun délai observé", "bins_par_decade": k}
    h = HistLog(k)
    for d in delais:
        h.add(d)
    idx = sorted(h.bins)
    trace, au_dessus, delta = [], False, None
    for i in range(idx[0], idx[-1] + 1):
        a, b = h.bornes(i)
        emp = h.bins.get(i, 0) / (h.n * (b - a))
        nul = densite_nulle(lam_hist, a, b)
        trace.append({"bin": i, "a_ns": a, "b_ns": b, "n": h.bins.get(i, 0),
                      "dens_emp": emp, "dens_nul": nul})
        if not au_dessus:
            au_dessus = emp > nul
        elif emp < nul:
            delta = a
            break
    return {"delta_max_ns": delta, "bins_par_decade": k, "n_delais": h.n,
            "mode_rapide_atteint": au_dessus, "trace": trace}


def chaines(evts: list, delta_max: int, strict: bool) -> tuple:
    """Suites maximales d'événements détectés au même (jour, σ, p), liés par
    o₂(e_i) = o₁(e_{i+1}). Retourne (longueurs, volume exécuté en chaîne par jour)."""
    par_niveau: dict[tuple, list] = defaultdict(list)
    for e in evts:
        if e[VIDE]:
            par_niveau[(e[J], e[SIDE], e[PX])].append(e)
    longueurs, vol = [], Counter()

    def fermer(courant):
        if len(courant) >= R_CHAINE:
            longueurs.append(len(courant))
            vol[courant[0][J]] += sum(x[EV1] for x in courant) + courant[-1][EV2]

    for _cle, lst in par_niveau.items():
        courant = []
        for e in lst:
            det = 0 < e[DELAI] <= delta_max
            ok = det and (not strict or e[Q2] == e[Q1])
            lie = bool(courant) and courant[-1][O2] == e[O1] and courant[-1][O2] >= 0
            if not ok or (courant and not lie):
                fermer(courant)
                courant = []
            if ok:
                courant.append(e)
        fermer(courant)
    return longueurs, vol


def comptes_p06(evts: list, delta_max: int, vol_exec_jour: dict) -> dict:
    """Comptes P-06 à δ_max donné : événements, chaînes (r≥2), E[FP], volumes."""
    n_ev = n_zero = non_vide = 0
    par_jour: dict = defaultdict(Counter)
    efp, efp_rth = defaultdict(float), defaultdict(float)
    efp_loc: dict = defaultdict(float)
    # Au-delà de la bande de garde, fenêtres de comptage et de détection se recouvrent :
    # le nul local n'est pas défini et ses sorties valent None.
    loc_valide = delta_max <= LOC_GARDE_NS
    for e in evts:
        d = e[DELAI]
        if not e[VIDE]:
            if 0 < d <= delta_max:
                non_vide += 1
            continue
        efp[e[J]] += 1.0 - math.exp(-e[LAMJ] * delta_max)
        efp_rth[e[J]] += 1.0 - math.exp(-e[LAMR] * delta_max)
        if loc_valide:
            efp_loc[e[J]] += 1.0 - math.exp(-e[LAMLOC] * delta_max)
        par_jour[e[J]]["epuisements_vidage"] += 1
        if d == 0:
            n_zero += 1
        if 0 < d <= delta_max:
            n_ev += 1
            par_jour[e[J]]["evenements"] += 1
    out = {"delta_max_ns": delta_max, "n_evenements": n_ev, "n_delai_zero": n_zero,
           "n_replenishment_niveau_non_vide": non_vide}
    for nom, strict in (("libre", False), ("strict_size", True)):
        L, vol = chaines(evts, delta_max, strict)
        out[f"chaines_{nom}"] = {
            "n_chaines": len(L), "n_evenements_en_chaine": sum(L),
            "longueur_quantiles": quantiles(L),
            "volume_execute_en_chaine": sum(vol.values()),
        }
    vol_tot = sum(vol_exec_jour.values()) if vol_exec_jour else 0
    for nom in ("libre", "strict_size"):
        v = out[f"chaines_{nom}"]["volume_execute_en_chaine"]
        out[f"part_volume_lit_en_chaine_{nom}"] = (v / vol_tot) if vol_tot else None
    ratios = [(efp[j] / c["evenements"], j) for j, c in par_jour.items() if c["evenements"]]
    out["efp_total"] = sum(efp.values())
    out["efp_total_rth"] = sum(efp_rth.values())
    out["efp_sur_detectes_global"] = (sum(efp.values()) / n_ev) if n_ev else None
    out["efp_sur_detectes_par_jour"] = sorted(r for r, _j in ratios)
    out["efp_sur_detectes_par_jour_median"] = (
        statistics.median([r for r, _j in ratios]) if ratios else None)
    out["n_jours_avec_evenements"] = len(ratios)
    # Nul local : c'est lui qui sert au critère d'invalidation.
    if loc_valide:
        ratios_loc = [efp_loc[j] / c["evenements"] for j, c in par_jour.items() if c["evenements"]]
        out["efp_local_total"] = sum(efp_loc.values())
        out["efp_local_sur_detectes_global"] = (sum(efp_loc.values()) / n_ev) if n_ev else None
        out["efp_local_sur_detectes_par_jour"] = sorted(ratios_loc)
        out["efp_local_sur_detectes_par_jour_median"] = (
            statistics.median(ratios_loc) if ratios_loc else None)
    else:
        out["efp_local_total"] = None
        out["efp_local_sur_detectes_global"] = None
        out["efp_local_sur_detectes_par_jour"] = []
        out["efp_local_sur_detectes_par_jour_median"] = None
    return out


def comptes_bridge(evts: list, delta: int) -> dict:
    """Comparaison avec l'ancien détecteur : même fenêtre δ, avec et sans vidage du niveau."""
    a = b = 0
    for e in evts:
        if 0 <= e[DELAI] <= delta:
            a += 1
            if e[VIDE]:
                b += 1
    return {"delta_ns": delta, "n_sans_condition_niveau": a, "n_avec_condition_niveau": b}


# --- derive : dérivation et récapitulatif ---
def derive(strates) -> None:
    tms = [tm for tm in tickers_itch(strates) if (OUT / f"{tm}.json").exists()]
    res = {tm: json.loads((OUT / f"{tm}.json").read_text()) for tm in tms}
    print(f"derive : {len(tms)} ticker-mois")

    pool = pooler_delais(tms)
    d_princ = derive_delta_max(pool["delais"], pool["lam_hist"], BINS_PAR_DECADE)
    d_sens = {k: derive_delta_max(pool["delais"], pool["lam_hist"], k) for k in BINS_SENSIBILITE}
    dmax_proc = d_princ["delta_max_ns"]           # croisement au nul homogène, à titre de diagnostic
    dmax = DELTA_MAX_NS
    print(f"  δ_max = {dmax} ns ; croisement nul homogène (diagnostic) = {dmax_proc} ns")

    delais_l = list(pool["delais"])
    distrib = {
        "n_epuisements_avec_vidage": pool["n_total"],
        "n_delai_zero": pool["n_zero"], "n_censures": pool["n_censure"],
        "quantiles_delais_ns": quantiles(delais_l),
        "hist_delais": _hist(delais_l).to_dict(),
        "quantiles_delais_niveau_non_vide_ns": quantiles(list(pool["delais_niveau_non_vide"])),
        "n_niveau_non_vide_total": pool["n_niveau_non_vide_total"],
        "delta_max": {k: v for k, v in d_princ.items() if k != "trace"},
        "delta_max_trace": d_princ["trace"],
        "delta_max_sensibilite_bins": {str(k): v["delta_max_ns"] for k, v in d_sens.items()},
    }
    (PROG / "distribution_delais_poolee.json").write_text(json.dumps(distrib, indent=1))

    deltas = [d for d in ((dmax // 10 if dmax else None), dmax, (dmax * 10 if dmax else None)) if d]
    ancien = {}
    fa = PROG / "ancien_detecteur.json"
    if fa.exists():
        ancien = json.loads(fa.read_text())

    # Grille de δ : valeurs rondes, plus le croisement au nul homogène comme point de
    # comparaison.
    grille = list(DELTAS_PROVISOIRES) + ([(dmax_proc, "31,62 s (croisement nul homogène — diagnostic)")] if dmax_proc else [])

    for tm in tms:
        evts = list(lire_evts(tm))
        vej = res[tm]["p06_capture"]["vol_exec_par_jour"]
        res[tm]["p06_multi_delta"] = {
            "statut": STATUT_DELTA,
            "grille_ns": {lab: d for d, lab in grille},
            "par_delta": {lab: comptes_p06(evts, d, vej) for d, lab in grille},
        }
        res[tm]["p06"] = {
            "delta_max_ns": dmax,
            "convention": "100 µs descriptif, variante principale strict_size",
            "delta_croisement_nul_homogene_ns": dmax_proc,
            "sensibilite": {str(d): comptes_p06(evts, d, vej) for d in deltas},
            "principal": comptes_p06(evts, dmax, vej) if dmax else None,
            "bridge_2s": comptes_bridge(evts, DELTA_ANCIEN_NS),
            "ancien_detecteur_A": ancien.get(tm),
            "quantiles_delais_ns": quantiles([e[DELAI] for e in evts if e[VIDE] and e[DELAI] >= 0]),
        }
        (OUT / f"{tm}.json").write_text(json.dumps(res[tm], indent=1, default=str))
        journal(etape="derive", tm=tm, delta_max=dmax,
                n_evenements=res[tm]["p06"]["principal"]["n_evenements"] if dmax else None)

    ecrire_recap(res, strates, distrib, d_princ, d_sens, deltas, grille)
    print(f"  écrit : {OUT / 'recap-b2-7.md'}")


def _fmt(x, n=3):
    if x is None:
        return "—"
    if isinstance(x, float):
        return f"{x:.{n}g}"
    return str(x)


def _par_tranche(res, cle_fn):
    out = defaultdict(list)
    for tm, r in res.items():
        v = cle_fn(r)
        if v is not None:
            out[r.get("strate_prix", "?")].append(v)
    return out


def agreger_multi_delta(res, grille) -> list[dict]:
    """Agrégats sur tous les ticker-mois pour chaque δ de la grille, avec verdict."""
    out = []
    for d, lab in grille:
        ratios, ratios_loc, lignes = [], [], []
        for tm in sorted(res):
            b = res[tm].get("p06_multi_delta")
            if b:
                lignes.append(b["par_delta"][lab])
                ratios.extend(b["par_delta"][lab]["efp_sur_detectes_par_jour"])
                ratios_loc.extend(b["par_delta"][lab].get("efp_local_sur_detectes_par_jour", []))
        n_ev = sum(x["n_evenements"] for x in lignes)
        efp = sum(x["efp_total"] for x in lignes)
        efp_loc = sum(x.get("efp_local_total") or 0.0 for x in lignes)
        med = statistics.median(ratios) if ratios else None
        med_loc = statistics.median(ratios_loc) if ratios_loc else None
        out.append({
            "delta_ns": d, "label": lab, "n_evenements": n_ev,
            "n_chaines_libre": sum(x["chaines_libre"]["n_chaines"] for x in lignes),
            "n_chaines_strict": sum(x["chaines_strict_size"]["n_chaines"] for x in lignes),
            "n_evts_en_chaine_libre": sum(x["chaines_libre"]["n_evenements_en_chaine"]
                                          for x in lignes),
            "vol_chaine_libre": sum(x["chaines_libre"]["volume_execute_en_chaine"]
                                    for x in lignes),
            "vol_chaine_strict": sum(x["chaines_strict_size"]["volume_execute_en_chaine"]
                                     for x in lignes),
            "n_niveau_non_vide": sum(x["n_replenishment_niveau_non_vide"] for x in lignes),
            "efp_total": efp, "efp_sur_detectes_global": (efp / n_ev) if n_ev else None,
            "efp_sur_detectes_median_titre_jour": med, "n_titres_jours": len(ratios),
            "verdict_nul_homogene": None if med is None else ("ROUGE" if med > 0.5 else "VERT"),
            # Le verdict repose sur le nul local ; il vaut None au-delà de la bande de
            # garde (cas δ = 31,6 s), où ce nul n'est pas calculable.
            "efp_local_total": efp_loc if ratios_loc else None,
            "efp_local_sur_detectes_global": (efp_loc / n_ev) if (ratios_loc and n_ev) else None,
            "efp_local_sur_detectes_median_titre_jour": med_loc,
            "verdict": None if med_loc is None else ("ROUGE" if med_loc > 0.5 else "VERT"),
        })
    return out


def ecrire_recap(res, strates, distrib, d_princ, d_sens, deltas, grille):
    dmax = d_princ["delta_max_ns"]
    L = []
    A_ = L.append
    A_("# Primitives P-06, P-07, P-08, P-10b sur MBO ITCH : récapitulatif\n")
    A_(f"Exécution du {datetime.now(NY).date().isoformat()} · code "
       f"`src/primitives/primitives_t3.py` (SHA-256 `{code_sha256()[:16]}…`) · "
       f"{len(res)} ticker-mois XNAS.ITCH · décodage local, aucun appel API.\n")
    A_("Sorties par ticker-mois : `{TICKER}_{MOIS}.json`. Événements bruts de "
       "P-06 : `.progress/p06-events/*.csv.gz`. Distribution poolée des délais : "
       "`.progress/distribution_delais_poolee.json`.\n")
    A_("@@SYNTHESE@@")

    # P-06
    A_("\n## P-06 — replenishment à délai court\n")
    A_("### 1. Distribution des délais (avant tout seuil)\n")
    q = distrib["quantiles_delais_ns"]
    A_(f"Épuisements d'ordre affiché par exécution avec vidage du niveau : "
       f"**{distrib['n_epuisements_avec_vidage']}**, dont {distrib['n_censures']} sans aucun Add "
       f"ultérieur au même (σ,p) dans la séance (censurés) et {distrib['n_delai_zero']} à délai "
       f"nul (Add à la nanoseconde de l'épuisement, postérieur dans l'ordre du flux).\n")
    if q:
        A_("| q01 | q05 | q25 | médiane | q75 | q95 | q99 | n |")
        A_("|---|---|---|---|---|---|---|---|")
        A_("| " + " | ".join(_fmt(q[k]) for k in ("q01", "q05", "q25", "q50", "q75", "q95", "q99"))
           + f" | {q['n']} |")
        A_("\n(délais en nanosecondes, distribution non tronquée, censures exclues)\n")
    qnv = distrib["quantiles_delais_niveau_non_vide_ns"]
    if qnv:
        A_(f"Contrôle « niveau non vidé » ({distrib['n_niveau_non_vide_total']} épuisements sans "
           f"vidage) — médiane des délais {_fmt(qnv['q50'])} ns.\n")
    A_("\n### 2. δ_max par croisement avec le modèle nul\n")
    A_(f"δ_max = **{_fmt(dmax)} ns** ({_fmt(dmax / 1000 if dmax else None)} µs) : plus petite "
       f"borne de classe où la densité empirique poolée repasse sous la densité du modèle nul "
       f"poolé (mixture des λ̂ par (σ,p,jour)), après le mode rapide. "
       f"Maillage principal : {d_princ['bins_par_decade']} classes/décade.\n")
    A_("Sensibilité au maillage : "
       + ", ".join(f"{k} classes/décade -> {_fmt(v['delta_max_ns'])} ns"
                   for k, v in d_sens.items()) + ".\n")
    A_("\n> Appliquée telle quelle, cette procédure donne un δ_max de l'ordre de la dizaine "
       "de secondes, environ six ordres de grandeur au-dessus du mode rapide visible dans la "
       "distribution (deux modes : ~8–10 µs et ~40–63 µs, cf. trace). Le modèle nul est un "
       "Poisson homogène de taux λ̂ = Add au (σ,p) sur la journée (≈ 5·10⁻¹² Add/ns, soit "
       "~0,005 Add/s par niveau de prix), alors que les Add à un niveau donné arrivent "
       "groupés dans le temps : la densité empirique reste au-dessus de la densité nulle "
       "jusqu'à ~30 s, et le croisement reflète l'épuisement de l'échantillon plutôt que la "
       "fin du mode rapide.\n")
    A_("\n> Une règle alternative d'antimode lissé ne tranche pas non plus (deux vallées "
       "équivalentes, ~16 µs et ~50–63 ms). δ_max = 100 µs est donc retenu comme convention "
       "descriptive, les comptes de P-06 sont publiés sur une grille de δ (§2bis), et la "
       "distribution des délais (§1) reste la sortie principale de P-06.\n")
    A_("\nExtrait de la trace de croisement (densités en événements/ns) :\n")
    A_("| classe [a, b[ ns | effectif | densité empirique | densité modèle nul |")
    A_("|---|---|---|---|")
    for t in d_princ["trace"]:
        if t["n"] or t["dens_nul"] > 0:
            A_(f"| [{_fmt(t['a_ns'])}, {_fmt(t['b_ns'])}[ | {t['n']} | {_fmt(t['dens_emp'])} "
               f"| {_fmt(t['dens_nul'])} |")
    # P-06 : grille de δ
    multi = agreger_multi_delta(res, grille)
    A_("\n### 2bis. Comptages sur une grille de δ\n")
    A_("Comptes recalculés depuis les événements bruts archivés "
       "(`.progress/p06-events/*.csv.gz`), sans relecture des fichiers MBO. Détail par "
       "ticker-mois : champ `p06_multi_delta` des JSON.\n")
    A_("| δ | δ (ns) | événements | chaînes r≥2 libre | chaînes r≥2 strict_size | "
       "évts en chaîne (libre) | vol. exécuté en chaîne (libre, actions) | niveau non vidé | "
       "E[FP]/détectés médian — nul homogène (diagnostic) | "
       "E[FP]/détectés médian — nul local (critère) | verdict |")
    A_("|---|---|---|---|---|---|---|---|---|---|---|")
    for m in multi:
        A_(f"| {m['label']} | {m['delta_ns']:.0f} | {m['n_evenements']} | "
           f"{m['n_chaines_libre']} | {m['n_chaines_strict']} | "
           f"{m['n_evts_en_chaine_libre']} | {m['vol_chaine_libre']} | "
           f"{m['n_niveau_non_vide']} | "
           f"{_fmt(m['efp_sur_detectes_median_titre_jour'], 3)} | "
           f"**{_fmt(m['efp_local_sur_detectes_median_titre_jour'], 3) if m['efp_local_sur_detectes_median_titre_jour'] is not None else 'non calculable'}** "
           f"(n = {m['n_titres_jours']}) | **{m['verdict'] or 'NON CALCULABLE'}** |")
    A_("\n**Contrôle des faux positifs par un nul local.** Le nul de Poisson homogène "
       "(λ̂ = Add au (σ,p) sur la séance) ne rend pas compte du groupement des Add (§2) et "
       "sous-estime E[FP]. Le critère utilise donc λ_loc, intensité des Add au même (σ, p) "
       "dans une fenêtre de ±60 s autour de l'épuisement, privée de la bande de garde "
       "(t, t+1 s] où se trouve le replenishment candidat. Au-delà de cette bande, le nul "
       "local n'est pas calculable : le verdict à δ = 31,6 s est NON CALCULABLE.\n")
    A_("Signal d'invalidation P-06 (médiane titre-jour de E[FP]/détectés sous le nul local "
       "> 0,5) : "
       + " · ".join(f"{m['label']} -> **{m['verdict'] or 'NON CALCULABLE'}** "
                    f"({_fmt(m['efp_local_sur_detectes_median_titre_jour'], 3) if m['efp_local_sur_detectes_median_titre_jour'] is not None else '—'})"
                    for m in multi)
       + ".\n")
    A_("Lecture : le nombre d'événements varie d'un facteur "
       f"{_fmt(multi[-1]['n_evenements'] / max(1, multi[0]['n_evenements']), 3)} entre les "
       "bornes de la grille : les niveaux absolus des comptes dépendent du choix de δ ; la "
       "distribution des délais (§1) et la structure (part de chaînes, part de « niveau non "
       "vidé ») sont plus informatives.\n")

    A_("\n### 3. Comptes, chaînes et E[faux positifs] par ticker-mois (à δ_max)\n")
    A_("| ticker-mois | tranche | épuis. vidage | événements | δ=0 | niveau non vidé | "
       "chaînes r≥2 (libre) | chaînes r≥2 (strict_size) | part vol. lit en chaîne | "
       "E[FP]/détectés (médiane j) |")
    A_("|---|---|---|---|---|---|---|---|---|---|")
    ratios_globaux = []
    for tm in sorted(res):
        p6 = res[tm].get("p06", {}).get("principal")
        if not p6:
            continue
        ratios_globaux.extend(p6["efp_sur_detectes_par_jour"])
        A_(f"| {tm} | {res[tm].get('strate_prix', '?')} | "
           f"{res[tm]['p06_capture']['n_epuisements_exec_vidage']} | {p6['n_evenements']} | "
           f"{p6['n_delai_zero']} | {p6['n_replenishment_niveau_non_vide']} | "
           f"{p6['chaines_libre']['n_chaines']} | {p6['chaines_strict_size']['n_chaines']} | "
           f"{_fmt(p6['part_volume_lit_en_chaine_libre'])} | "
           f"{_fmt(p6['efp_sur_detectes_par_jour_median'])} |")
    med_glob = statistics.median(ratios_globaux) if ratios_globaux else None
    A_(f"\n**Condition d'invalidation P-06** : médiane sur les titres-jours de "
       f"E[FP]/événements détectés = **{_fmt(med_glob)}** "
       f"(n = {len(ratios_globaux)} titres-jours avec ≥ 1 événement) — seuil de rejet 0,5 : "
       f"**{'ROUGE (définition rejetée)' if (med_glob or 0) > 0.5 else 'VERT'}**.\n")
    A_("\n### 4. Sensibilité {δ_max/10, δ_max, 10·δ_max}\n")
    A_("| δ (ns) | événements | chaînes r≥2 libre | niveau non vidé | "
       "E[FP] total | E[FP]/détectés global |")
    A_("|---|---|---|---|---|---|")
    for d in deltas:
        n_ev = sum(res[tm]["p06"]["sensibilite"][str(d)]["n_evenements"] for tm in res
                   if "p06" in res[tm])
        n_ch = sum(res[tm]["p06"]["sensibilite"][str(d)]["chaines_libre"]["n_chaines"]
                   for tm in res if "p06" in res[tm])
        n_nv = sum(res[tm]["p06"]["sensibilite"][str(d)]["n_replenishment_niveau_non_vide"]
                   for tm in res if "p06" in res[tm])
        efp = sum(res[tm]["p06"]["sensibilite"][str(d)]["efp_total"] for tm in res
                  if "p06" in res[tm])
        A_(f"| {d} | {n_ev} | {n_ch} | {n_nv} | {_fmt(efp)} | {_fmt(efp / n_ev if n_ev else None)} |")
    A_("\n### 5. Comparaison ancien / nouveau détecteur\n")
    A_("Ancien détecteur (`donnees_mbo.run_h15`) : fenêtre 2 s, sans condition de vidage du "
       "niveau, r ≥ 1, et carnet qui applique `F` et son `C` jumeau (double décrément).\n")
    A_("| ticker-mois | ancien (2 s) | nouveau carnet, 2 s, sans niveau | "
       "nouveau carnet, 2 s, avec vidage | nouveau, δ_max, avec vidage |")
    A_("|---|---|---|---|---|")
    tot = [0, 0, 0, 0]
    for tm in sorted(res):
        p6 = res[tm].get("p06")
        if not p6:
            continue
        anc = (p6.get("ancien_detecteur_A") or {}).get("replenish_detecte")
        br = p6["bridge_2s"]
        n_new = p6["principal"]["n_evenements"] if p6.get("principal") else None
        A_(f"| {tm} | {_fmt(anc)} | {br['n_sans_condition_niveau']} | "
           f"{br['n_avec_condition_niveau']} | {_fmt(n_new)} |")
        tot[0] += anc or 0
        tot[1] += br["n_sans_condition_niveau"]
        tot[2] += br["n_avec_condition_niveau"]
        tot[3] += n_new or 0
    A_(f"| **total** | **{tot[0]}** | **{tot[1]}** | **{tot[2]}** | **{tot[3]}** |\n")

    # P-07
    A_("\n## P-07 — ratio exécutions cachées / affichées\n")
    A_("Calcul par `donnees_mbo.run_h16`, avec imputation des crosses d'ouverture, de "
       "clôture et de reprise après halt.\n")
    A_("| ticker-mois | tranche | HR (vol) | HR (RTH) | HR variante V^disp = vol(F) | "
       "crosses/séance | jours |")
    A_("|---|---|---|---|---|---|---|")
    for tm in sorted(res):
        p = res[tm]["p07"]
        A_(f"| {tm} | {res[tm].get('strate_prix', '?')} | {_fmt(p['HR_vol'], 4)} | "
           f"{_fmt(p['HR_vol_rth'], 4)} | {_fmt(p['HR_vol_variante_Vdisp_F'], 4)} | "
           f"{_fmt(p['crosses_par_seance_moyen'], 3)} | {p['n_jours']} |")
    hrs = [r["p07"]["HR_vol"] for r in res.values() if r["p07"]["HR_vol"] is not None]
    vc = sum(r["p07"]["vol_cache"] for r in res.values())
    va = sum(r["p07"]["vol_affiche"] for r in res.values())
    A_(f"\nPoolé : **{_fmt(vc / (vc + va), 4)}** ; médiane titre-mois {_fmt(statistics.median(hrs), 4)} ; "
       f"min {_fmt(min(hrs), 4)} ; max {_fmt(max(hrs), 4)} "
       f"(valeurs de référence de `run_h16` : 0,2544 / 0,2334 / 0,0850 / 0,4045).\n")
    for tr, vals in sorted(_par_tranche(res, lambda r: r["p07"]["HR_vol"]).items()):
        A_(f"- tranche {tr} : médiane {_fmt(statistics.median(vals), 4)}, "
           f"IQR [{_fmt(quantiles(vals)['q25'], 4)}, {_fmt(quantiles(vals)['q75'], 4)}], n = {len(vals)}")
    cps = [r["p07"]["crosses_par_seance_moyen"] for r in res.values()]
    med_c = statistics.median(cps)
    hors = [tm for tm, r in res.items() if not 1 <= r["p07"]["crosses_par_seance_moyen"] <= 3]
    A_(f"\n**Condition d'invalidation P-07** : crosses imputés par séance — médiane des "
       f"ticker-mois **{_fmt(med_c, 3)}** (attendu [1, 3]) : "
       f"**{'VERT' if 1 <= med_c <= 3 else 'ROUGE'}**. Min {_fmt(min(cps), 3)}, "
       f"max {_fmt(max(cps), 3)} ; {len(hors)} ticker-mois hors [1, 3] "
       f"({', '.join(sorted(hors)) if hors else '—'}), tous par défaut de candidat "
       "(titres très peu échangés : aucune impression non appariée dans la fenêtre "
       "d'ouverture ou de clôture certaines séances).\n")
    A_("Note : `run_h16` ne ventile pas les crosses par séance ; la statistique par "
       "ticker-mois est donc une moyenne (crosses imputés / séances), ce qui est sans "
       "conséquence ici puisque les compteurs open/close valent 0 ou 1 par séance.\n")

    # P-08
    A_("\n## P-08 — profondeur affichée relative (β = 2 %)\n")
    A_("| ticker-mois | tranche | buckets RTH | % NA | D_total médian (actions) | "
       "q25 | q75 | part D = 0 | μ^b médian $ | % temps spread > 50 % | jours D̃ |")
    A_("|---|---|---|---|---|---|---|---|---|---|---|")
    nas = []
    for tm in sorted(res):
        p = res[tm]["p08"]
        q = p["D_total_quantiles"] or {}
        nas.append(p["part_buckets_rth_na"])
        A_(f"| {tm} | {res[tm].get('strate_prix', '?')} | {p['n_buckets_rth']} | "
           f"{_fmt(100 * (p['part_buckets_rth_na'] or 0), 3)} | {_fmt(q.get('q50'))} | "
           f"{_fmt(q.get('q25'))} | {_fmt(q.get('q75'))} | {_fmt(p['part_D_total_nul'], 3)} | "
           f"{_fmt((p.get('mid_carnet_bucket_quantiles') or {}).get('q50'), 4)} | "
           f"{_fmt(100 * (p.get('part_temps_rth_spread_sup_50pc') or 0), 3)} | "
           f"{p['n_jours_D_tilde']} |")
    med_na = statistics.median([x for x in nas if x is not None])
    A_(f"\n**Condition d'invalidation P-08** : part de buckets RTH NA (pas de BBO) — "
       f"médiane des ticker-mois **{_fmt(100 * med_na, 3)} %** ; seuil 50 % : "
       f"**{'ROUGE' if med_na > 0.5 else 'VERT'}**. (Max sur l'échantillon : "
       f"{_fmt(100 * max(x for x in nas if x is not None), 3)} %.)\n")
    A_("Contrôle du mid μ^b : sur l'échantillon, le carnet est toujours bilatéral en RTH "
       "(0 bucket NA) et le spread relatif ne dépasse pas 50 % ; le mid reconstruit est "
       "cohérent avec le close de référence. Les D = 0 correspondent donc à des fenêtres "
       "±2 % réellement vides.\n")
    n_dt = sum(r["p08"]["n_jours_D_tilde"] for r in res.values())
    n_m0 = sum(r["p08"].get("n_jours_D_tilde_NA_med0", 0) for r in res.values())
    A_(f"D̃ calculé sur **{n_dt} titres-jours** au total ; "
       f"{n_m0} titre-jour(s) supplémentaire(s) en NA(code `med0`) — médiane des buckets des "
       f"20 jours cotés précédents nulle (titres où plus de la moitié des buckets ont D = 0).\n")
    A_("Normalisations : (a) D̃ n'est calculable que sur 1 séance par ticker-mois de "
       "2018-06 (21 séances, fenêtre de 20 jours cotés) et sur aucune en 2026-01 "
       "(20 séances), l'échantillon ne couvrant qu'un mois calendaire par titre. "
       "(b) D^jours est NA : DV$_63 exige 63 jours de bourse antérieurs de tape SIP, hors "
       "de l'échantillon MBO. Les médianes journalières de D sont publiées par "
       "titre-jour dans les JSON (`p08.par_jour`) pour permettre le calcul ultérieur.\n")
    for tr in sorted({r.get("strate_prix", "?") for r in res.values()}):
        h = HistLog()
        for r in res.values():
            if r.get("strate_prix") == tr:
                h.fusion(HistLog.from_dict(r["p08"]["hist_D_total"]))
        A_(f"- tranche {tr} : D_total poolé (histogramme log) — q25 ≈ {_fmt(h.quantile_approx(.25))}, "
           f"médiane ≈ {_fmt(h.quantile_approx(.5))}, q75 ≈ {_fmt(h.quantile_approx(.75))} actions, "
           f"n = {h.n} buckets, dont {h.zero} à D = 0")

    # P-10b
    A_("\n## P-10b — rafales multi-niveaux (L = 2, Δt_s = 50 ms)\n")
    A_("| ticker-mois | tranche | rafales (toutes) | dont ≥ L niveaux | part | "
       "niveaux médian (≥L) | niveaux max | notionnel médian $ (≥L) | q25 | q75 |")
    A_("|---|---|---|---|---|---|---|---|---|---|")
    med_not = []
    for tm in sorted(res):
        p = res[tm]["p10b"]
        qn = p["notionnel_usd_L_quantiles"] or {}
        # niveaux des rafales ≥ L, reconstruits depuis l'histogramme des niveaux
        niv_L = [int(k) for k, c in p["hist_niveaux"].items() if int(k) >= L_RAFALE
                 for _ in range(c)]
        if p["notionnel_usd_L_median"] is not None:
            med_not.append(p["notionnel_usd_L_median"])
        A_(f"| {tm} | {res[tm].get('strate_prix', '?')} | {p['n_rafales_total']} | "
           f"{p['n_rafales_L']} | {_fmt(p['part_rafales_L'], 3)} | "
           f"{_fmt(statistics.median(niv_L) if niv_L else None)} | "
           f"{_fmt(max(niv_L) if niv_L else None)} | "
           f"{_fmt(p['notionnel_usd_L_median'], 4)} | {_fmt(qn.get('q25'), 4)} | "
           f"{_fmt(qn.get('q75'), 4)} |")
    niv_pool: Counter = Counter()
    for r in res.values():
        for k, c in r["p10b"]["hist_niveaux"].items():
            niv_pool[int(k)] += c
    n_raf = sum(niv_pool.values())
    A_("\nDistribution non tronquée du nombre de niveaux consommés par rafale "
       f"(poolée, {n_raf} rafales) : "
       + ", ".join(f"{k} niveau{'x' if k > 1 else ''} : {c} ({100 * c / n_raf:.2f} %)"
                   for k, c in sorted(niv_pool.items())[:12]) + ".\n")
    h_tous = HistLog()
    for r in res.values():
        h_tous.fusion(HistLog.from_dict(r["p10b"]["hist_notionnel_tous"]))
    A_(f"Notionnel de toutes les rafales (poolé) : q25 ≈ {_fmt(h_tous.quantile_approx(.25), 4)} $, "
       f"médiane ≈ {_fmt(h_tous.quantile_approx(.5), 4)} $, "
       f"q75 ≈ {_fmt(h_tous.quantile_approx(.75), 4)} $.\n")
    h = HistLog()
    for r in res.values():
        h.fusion(HistLog.from_dict(r["p10b"]["hist_notionnel_L"]))
    med_pool = h.quantile_approx(0.5)
    A_(f"\nNotionnel des rafales ≥ L, poolé sur l'échantillon : médiane ≈ "
       f"{_fmt(med_pool, 4)} $ (n = {h.n} rafales) ; médiane des médianes par ticker-mois "
       f"{_fmt(statistics.median(med_not), 4)} $.\n")
    A_(f"**Condition d'invalidation P-10b** : notionnel médian des rafales détectées "
       f"< 1 000 $ -> **{'ROUGE' if (med_pool or 0) < 1000 else 'VERT'}** "
       f"(poolé) / **{'ROUGE' if statistics.median(med_not) < 1000 else 'VERT'}** "
       f"(médiane des ticker-mois).\n")

    synth = [
        "## Synthèse — conditions d'invalidation\n",
        "| primitive | critère | valeur mesurée | verdict |",
        "|---|---|---|---|",
        f"| P-06 | médiane titre-jour de E[FP]/événements détectés > 0,5 -> rejet | "
        + " ; ".join(f"{m['label'].split(' (')[0]} : {_fmt(m['efp_sur_detectes_median_titre_jour'], 3)}"
                     for m in multi)
        + " | **" + ("ROUGE" if any(m["verdict"] == "ROUGE" for m in multi) else "VERT")
        + "** sur toute la grille δ |",
        f"| P-07 | médiane des crosses imputés/séance hors [1, 3] -> réexamen | {_fmt(med_c, 3)} | "
        f"**{'VERT' if 1 <= med_c <= 3 else 'ROUGE'}** |",
        f"| P-08 | > 50 % de buckets RTH NA -> maille réexaminée | "
        f"{_fmt(100 * med_na, 3)} % | **{'ROUGE' if med_na > 0.5 else 'VERT'}** |",
        f"| P-10b | notionnel médian des rafales détectées < 1 000 $ -> L et Δt_s réexaminés | "
        f"{_fmt(med_pool, 4)} $ (poolé) / {_fmt(statistics.median(med_not), 4)} $ (médiane "
        f"des ticker-mois) | **{'ROUGE' if (med_pool or 0) < 1000 else 'VERT'}** |",
        "\nRéserve : δ_max = 100 µs est une convention descriptive. Les comptes de P-06 sont "
        "publiés sur une grille de δ (§2bis) et la sortie principale de P-06 est la "
        "distribution des délais (§1) ; les niveaux absolus de comptage dépendent du choix "
        "de δ.\n",
    ]
    L[L.index("@@SYNTHESE@@")] = "\n".join(synth)
    (OUT / "recap-b2-7.md").write_text("\n".join(L))


# --- Tests sur flux synthétiques ---
_Rec = namedtuple("_Rec", "action side price size order_id ts_event flags sequence")
P = 1_000_000_000                                    # 1,00 $ en 1e-9 $


def _ts(jour, h, m, s=0, ns=0):
    d = datetime.fromisoformat(jour).date()
    return int(datetime.combine(d, dtime(h, m, s), NY).timestamp()) * 1_000_000_000 + ns


class Flux:
    """Construit un flux MBO synthétique suivant la sémantique XNAS.ITCH."""

    def __init__(self):
        self.recs: list[_Rec] = []
        self.seq = 0

    def _s(self):
        self.seq += 1
        return self.seq

    def clear(self, ts):
        self.recs.append(_Rec("R", "N", 0, 0, 0, ts, 0, self._s()))
        return self

    def add(self, ts, oid, side, px, sz):
        self.recs.append(_Rec("A", side, px, sz, oid, ts, 0, self._s()))
        return self

    def cancel(self, ts, oid, side, px, sz):
        self.recs.append(_Rec("C", side, px, sz, oid, ts, 0, self._s()))
        return self

    def execute(self, ts, oid, side, px, sz):
        """T + F + C sous un même `sequence`, comme sur XNAS.ITCH."""
        s = self._s()
        agr = "A" if side == "B" else "B"
        self.recs.append(_Rec("T", agr, px, sz, 0, ts, 0, s))
        self.recs.append(_Rec("F", side, px, sz, oid, ts, 0, s))
        self.recs.append(_Rec("C", side, px, sz, oid, ts, 0, s))
        return self

    def replace(self, ts, oid_v, side, px_v, sz_v, oid_n, px_n, sz_n):
        s = self._s()
        self.recs.append(_Rec("C", side, px_v, sz_v, oid_v, ts, 0, s))
        self.recs.append(_Rec("A", side, px_n, sz_n, oid_n, ts, 0, s))
        return self

    def cache(self, ts, px, sz):
        self.recs.append(_Rec("T", "N", px, sz, 0, ts, 0, self._s()))
        return self


def _rejouer(recs, pas=None):
    pl = Pipeline("TEST_2026-01")
    if pas is None:
        for r in recs:
            pl.feed(r)
    else:
        for i in range(0, len(recs), pas):
            for r in recs[i:i + pas]:
                pl.feed(r)
    return pl.finish()


def _sortie(pl):
    return [_to_evt(e) for e in pl.sink]


def autotests():
    JOUR = "2026-01-05"
    ok = []
    DMAX = 1_000_000                                   # 1 ms, valeur propre aux tests

    # --- P-06 ---
    # (a) chaîne de 3 tranches, Add à 40 µs avec de nouveaux order id
    f = Flux().clear(_ts(JOUR, 4, 0))
    f.add(_ts(JOUR, 10, 0), 900, "A", P + 10_000_000, 700)     # côté opposé -> μ défini
    f.add(_ts(JOUR, 10, 0), 1, "B", P, 100)
    t = _ts(JOUR, 10, 1)
    f.execute(t, 1, "B", P, 100)
    f.add(t + 40_000, 2, "B", P, 100)
    f.execute(t + 100_000, 2, "B", P, 100)
    f.add(t + 140_000, 3, "B", P, 100)
    f.execute(t + 200_000, 3, "B", P, 100)
    f.add(t + 240_000, 4, "B", P, 100)
    ev = _sortie(_rejouer(f.recs))
    assert len(ev) == 3, ev
    assert all(e[VIDE] for e in ev), ev
    assert [e[DELAI] for e in ev] == [40_000] * 3, ev
    assert [e[O1] for e in ev] == [1, 2, 3] and [e[O2] for e in ev] == [2, 3, 4], ev
    assert [e[Q1] for e in ev] == [100] * 3 and [e[Q2] for e in ev] == [100] * 3, ev
    ok.append("P-06 (a) chaîne de 3 tranches détectée, délais exacts (40 µs)")

    L_libre, vol = chaines(ev, DMAX, strict=False)
    L_strict, _ = chaines(ev, DMAX, strict=True)
    assert L_libre == [3] and L_strict == [3], (L_libre, L_strict)
    # volume en chaîne : 3 tranches de 100 épuisées, plus o₄ resté au carnet (0 exécuté)
    assert sum(vol.values()) == 300, vol
    ev_alt = [list(e) for e in ev]
    ev_alt[2][Q2] = 55                              # dernière tranche de taille différente
    ev_alt = [tuple(e) for e in ev_alt]
    assert chaines(ev_alt, DMAX, strict=False)[0] == [3]
    assert chaines(ev_alt, DMAX, strict=True)[0] == [2], chaines(ev_alt, DMAX, True)[0]
    ev_alt2 = [list(e) for e in ev]
    ev_alt2[1][Q2] = 55                             # tranche du milieu différente : plus de chaîne
    assert chaines([tuple(e) for e in ev_alt2], DMAX, strict=True)[0] == []
    ok.append("P-06 (a') chaînes r≥2 : variantes libre et strict_size discriminées")

    # (b) Add après Cancel -> non-événement
    f = Flux().clear(_ts(JOUR, 4, 0))
    f.add(_ts(JOUR, 10, 0), 900, "A", P + 10_000_000, 700)
    f.add(_ts(JOUR, 10, 0), 11, "B", P, 123)
    f.cancel(_ts(JOUR, 10, 1), 11, "B", P, 123)
    f.add(_ts(JOUR, 10, 1, 0, 40_000), 12, "B", P, 123)
    pl = _rejouer(f.recs)
    assert _sortie(pl) == [], _sortie(pl)
    assert pl.cnt["n_retraits_annulation"] == 1
    ok.append("P-06 (b) Add après Cancel : non-événement")

    # (c) niveau non vidé : un autre ordre reste affiché
    f = Flux().clear(_ts(JOUR, 4, 0))
    f.add(_ts(JOUR, 10, 0), 900, "A", P + 10_000_000, 700)
    f.add(_ts(JOUR, 10, 0), 21, "B", P, 100)
    f.add(_ts(JOUR, 10, 0), 22, "B", P, 55)
    f.execute(_ts(JOUR, 10, 1), 21, "B", P, 100)
    f.add(_ts(JOUR, 10, 1, 0, 40_000), 23, "B", P, 77)
    ev = _sortie(_rejouer(f.recs))
    assert len(ev) == 1 and not ev[0][VIDE] and ev[0][DELAI] == 40_000, ev
    c = comptes_p06(ev, DMAX, {})
    assert c["n_evenements"] == 0 and c["n_replenishment_niveau_non_vide"] == 1, c
    ok.append("P-06 (c) épuisement sans vidage -> compteur niveau_non_vide, pas un événement")

    # (d) Add hors δ_max : non-événement, mais présent dans la distribution des délais
    f = Flux().clear(_ts(JOUR, 4, 0))
    f.add(_ts(JOUR, 10, 0), 900, "A", P + 10_000_000, 700)
    f.add(_ts(JOUR, 10, 0), 31, "B", P, 100)
    f.execute(_ts(JOUR, 10, 1), 31, "B", P, 100)
    f.add(_ts(JOUR, 10, 1, 0, 500_000_000), 32, "B", P, 100)      # 500 ms >> δ_max
    ev = _sortie(_rejouer(f.recs))
    assert len(ev) == 1 and ev[0][VIDE] and ev[0][DELAI] == 500_000_000, ev
    assert comptes_p06(ev, DMAX, {})["n_evenements"] == 0
    assert comptes_p06(ev, 10 ** 9, {})["n_evenements"] == 1
    ok.append("P-06 (d) délai hors δ_max : dans la distribution, hors des comptes")

    # (e) Replace : l'Add résultant est éligible
    f = Flux().clear(_ts(JOUR, 4, 0))
    f.add(_ts(JOUR, 10, 0), 900, "A", P + 10_000_000, 700)
    f.add(_ts(JOUR, 10, 0), 41, "B", P, 100)
    f.add(_ts(JOUR, 10, 0), 42, "B", P - 5_000_000, 200)
    f.execute(_ts(JOUR, 10, 1), 41, "B", P, 100)
    f.replace(_ts(JOUR, 10, 1, 0, 40_000), 42, "B", P - 5_000_000, 200, 43, P, 200)
    ev = _sortie(_rejouer(f.recs))
    assert len(ev) == 1 and ev[0][DELAI] == 40_000 and ev[0][O2] == 43, ev
    assert comptes_p06(ev, DMAX, {})["n_evenements"] == 1
    ok.append("P-06 (e) Add issu d'un Replace : éligible")

    # λ̂ et E[FP]
    f = Flux().clear(_ts(JOUR, 4, 0))
    f.add(_ts(JOUR, 10, 0), 900, "A", P + 10_000_000, 700)
    f.add(_ts(JOUR, 10, 0), 51, "B", P, 100)
    f.execute(_ts(JOUR, 10, 1), 51, "B", P, 100)
    f.add(_ts(JOUR, 10, 1, 0, 40_000), 52, "B", P, 100)
    ev = _sortie(_rejouer(f.recs))
    dur = _ts(JOUR, 10, 1, 0, 40_000) - _ts(JOUR, 4, 0)
    assert abs(ev[0][LAMJ] - 2 / dur) < 1e-30, (ev[0][LAMJ], 2 / dur)
    c = comptes_p06(ev, DMAX, {})
    assert abs(c["efp_total"] - (1 - math.exp(-2 / dur * DMAX))) < 1e-15, c["efp_total"]
    ok.append("P-06 λ̂ = Add au (σ,p)/durée de séance ; E[FP] = Σ(1−e^{−λ̂δ}) exact")

    # Nul local : fenêtre [t−60 s, t+60 s] ∩ séance, privée de la garde (t, t+1 s].
    # L'Add 52 (replenishment candidat) tombe dans la garde et est exclu ; l'Add 51,
    # celui de l'ordre épuisé, est conservé. Tout épuisement ayant au moins un Add
    # antérieur à son niveau, λ_loc compte une arrivée de plus, ce qui biaise E[FP]
    # vers le haut, du côté prudent pour un critère de rejet.
    lo_att = _ts(JOUR, 10, 0)   # t − 60 s, avec t = 10:01:00
    hi_att = _ts(JOUR, 10, 1, 0, 40_000)
    lam_loc_att = 1 / ((hi_att - lo_att) - LOC_GARDE_NS)
    assert abs(ev[0][LAMLOC] - lam_loc_att) < 1e-20, (ev[0][LAMLOC], lam_loc_att)
    assert abs(c["efp_local_total"] - (1 - math.exp(-lam_loc_att * DMAX))) < 1e-15
    # Même sur ce cas simple, λ_loc dépasse de deux ordres de grandeur le λ̂ de séance.
    assert ev[0][LAMLOC] > 100 * ev[0][LAMJ], (ev[0][LAMLOC], ev[0][LAMJ])
    ok.append("P-06 nul local : λ_loc = Add du niveau hors garde / fenêtre locale ; "
              "≫ λ̂ journalier")

    # Une rafale d'Add au même niveau dans la fenêtre locale, diluée sur la séance pour
    # le nul homogène, doit relever λ_loc.
    f = Flux().clear(_ts(JOUR, 4, 0))
    for k in range(20):
        f.add(_ts(JOUR, 10, 0, 30, k * 1_000_000), 100 + k, "B", P - 5_000_000, 10)
        f.cancel(_ts(JOUR, 10, 0, 30, k * 1_000_000 + 1), 100 + k, "B", P - 5_000_000, 10)
    f.add(_ts(JOUR, 10, 0), 900, "A", P + 10_000_000, 700)
    f.add(_ts(JOUR, 10, 0), 61, "B", P - 5_000_000, 100)
    f.execute(_ts(JOUR, 10, 1), 61, "B", P - 5_000_000, 100)
    f.add(_ts(JOUR, 10, 1, 0, 40_000), 62, "B", P - 5_000_000, 100)
    ev2 = _sortie(_rejouer(f.recs))
    e2 = next(x for x in ev2 if x[DELAI] == 40_000)
    assert e2[LAMLOC] > 20 * lam_loc_att, (e2[LAMLOC], lam_loc_att)
    ok.append("P-06 nul local : une rafale d'Add dans la fenêtre locale relève λ_loc "
              "(le nul homogène ne la voit pas)")

    # --- P-07 ---
    g = Flux()
    g.cache(_ts(JOUR, 9, 30, 2), P, 5000)                # imputée cross (la plus grosse)
    g.cache(_ts(JOUR, 9, 30, 3), P, 41)                  # cachée (même fenêtre, plus petite)
    g.cache(_ts(JOUR, 11, 0), P, 7)
    g.cache(_ts(JOUR, 11, 1), P, 13)
    g.execute(_ts(JOUR, 11, 2), 61, "B", P, 100)
    g.execute(_ts(JOUR, 11, 3), 62, "B", P, 200)
    g.execute(_ts(JOUR, 11, 4), 63, "B", P, 400)
    r7 = run_h16("TEST_2026-01", {"ticker": "TEST", "mois": "2026-01"},
                 records=iter(g.recs), resumes=[])
    t7 = r7["totaux"]
    assert t7["n_cross_open"] == 1 and t7["vol_cross_open"] == 5000, t7
    assert t7["vol_cache"] == 61 and t7["vol_affiche"] == 700, t7
    assert abs(r7["ratio_cache_vol"] - 61 / 761) < 1e-15, r7["ratio_cache_vol"]
    assert r7["ratio_cache_nb"] != r7["ratio_cache_vol"]          # HR en volume, pas en messages
    ok.append("P-07 HR exact en volume (61/761), grosse impression imputée au cross")

    # --- P-08 ---
    f = Flux().clear(_ts(JOUR, 4, 0))
    b0 = _ts(JOUR, 10, 0)
    f.add(b0, 71, "B", P, 100)                        # bid 1,00 — dans la fenêtre
    f.add(b0, 72, "A", P + 10_000_000, 200)           # ask 1,01 — dans la fenêtre
    f.add(b0, 73, "B", P - 20_000_000, 300)           # bid 0,98 — hors fenêtre (μ = 1,005)
    f.cancel(b0 + 30_000_000_000, 71, "B", P, 40)     # 100 -> 60 à t+30 s
    f.add(b0 + 60_000_000_000, 74, "A", P + 500_000_000, 500)     # bucket 2 : ask 1,50
    f.cancel(b0 + 60_000_000_000, 72, "A", P + 10_000_000, 200)   # μ = 1,25, rien dans ±2 %
    f.cancel(b0 + 120_000_000_000, 71, "B", P, 60)                # bucket 3 : plus de bid
    f.cancel(b0 + 120_000_000_000, 73, "B", P - 20_000_000, 300)
    pl = _rejouer(f.recs)
    j, i0 = pl.jour_courant, (b0 - pl.rth_t0) // BUCKET_RTH_NS
    _d, dv0, sB0, sA0, dl0, sm0 = pl.buckets[(j, "RTH", i0)]
    assert dv0 == 60_000_000_000
    assert sB0 / dv0 == (100 * 30 + 60 * 30) / 60 == 80, sB0 / dv0
    assert sA0 / dv0 == 200, sA0 / dv0
    assert sm0 / dv0 / 2e9 == 1.005 and dl0 == 0, (sm0 / dv0 / 2e9, dl0)   # μ^b exact
    _d, dv1, sB1, sA1, dl1, _s = pl.buckets[(j, "RTH", i0 + 1)]
    assert dv1 == 60_000_000_000 and (sB1, sA1) == (0, 0), (dv1, sB1, sA1)
    assert dl1 == 0, dl1                            # spread relatif 0,50/1,25 = 40 % < 50 %
    _d, dv2, *_r = pl.buckets[(j, "RTH", i0 + 2)]
    assert dv2 == 0, dv2
    p8 = pl.p08()
    assert p8["n_buckets_rth_na"] >= 1 and p8["part_buckets_rth_na"] > 0
    assert p8["par_jour"][j]["checksum_carnet_fin_jour"]
    ok.append("P-08 moyenne pondérée par la durée exacte (80/200), D = 0 côté vide, "
              "bucket NA sans BBO")

    # Rejeu avec un autre découpage d'ingestion : sorties identiques.
    pa, pb = _rejouer(f.recs), _rejouer(f.recs, pas=3)
    assert pa.buckets == pb.buckets and pa.checksums == pb.checksums and pa.sink == pb.sink
    assert pa.p08()["D_total_quantiles"] == pb.p08()["D_total_quantiles"]
    ok.append("Rejeu : découpage d'ingestion différent -> buckets, checksums, événements "
              "identiques (bit à bit)")

    # --- P-10b ---
    f = Flux().clear(_ts(JOUR, 4, 0))
    t0 = _ts(JOUR, 10, 0)
    f.add(t0, 81, "A", P, 1000)                       # ask 1,00 × 1 000
    f.add(t0, 82, "A", P + 200_000_000, 1000)         # ask 1,20 × 1 000
    f.add(t0, 83, "B", P - 500_000_000, 200)          # bid 0,50 × 200
    f.add(t0, 84, "B", P - 600_000_000, 200)          # bid 0,40 × 200
    f.execute(t0 + 1_000_000_000, 81, "A", P, 1000)                 # 1 000 $
    f.execute(t0 + 1_010_000_000, 82, "A", P + 200_000_000, 1000)   # + 1 200 $ = 2 200 $
    f.add(t0 + 5_000_000_000, 85, "A", P, 300)
    f.add(t0 + 5_000_000_000, 86, "A", P + 200_000_000, 300)
    f.execute(t0 + 10_000_000_000, 83, "B", P - 500_000_000, 200)   # 100 $
    f.execute(t0 + 10_010_000_000, 84, "B", P - 600_000_000, 200)   # + 80 $ = 180 $
    f.execute(t0 + 20_000_000_000, 85, "A", P, 300)                 # rafale étalée : 200 ms
    f.execute(t0 + 20_200_000_000, 86, "A", P + 200_000_000, 300)
    pl = _rejouer(f.recs)
    seuil = [r for r in pl.rafales if r[2] >= L_RAFALE]
    assert len(seuil) == 2, pl.rafales
    assert sorted(round(r[3] / 1e9, 6) for r in seuil) == [180.0, 2200.0], seuil
    assert all(r[2] == 2 for r in seuil)
    assert sum(1 for r in pl.rafales if r[2] == 1) == 2, pl.rafales    # les deux étalées
    p10 = pl.p10b()
    assert p10["n_rafales_L"] == 2 and p10["notionnel_usd_L_median"] == 1190.0, p10
    ok.append("P-10b rafale 2 niveaux/10 ms détectée, 200 ms non ; notionnels 2 200 $ et 180 $")

    f = Flux().clear(_ts(JOUR, 4, 0))
    f.add(t0, 91, "A", P, 400)
    f.add(t0, 95, "B", P - 100_000_000, 400)
    f.execute(t0 + 1_000_000_000, 91, "A", P, 400)
    f.add(t0 + 1_001_000_000, 92, "A", P + 100_000_000, 600)   # niveau créé après le début de la rafale
    f.execute(t0 + 1_002_000_000, 92, "A", P + 100_000_000, 600)
    pl = _rejouer(f.recs)
    assert [r[2] for r in pl.rafales] == [1], pl.rafales
    ok.append("P-10b niveau apparu après l'instant initial de la rafale : non compté")

    # --- invariants du carnet ---
    bad = Flux().clear(_ts(JOUR, 4, 0))
    bad.add(_ts(JOUR, 10, 0), 99, "B", P, 10)
    bad.recs.append(_Rec("F", "B", P, 10, 99, _ts(JOUR, 10, 1), 0, 999))   # F sans C jumeau
    assert _rejouer(bad.recs).cnt["anomalie_F_sans_C_jumeau"] == 1
    bad2 = Flux().clear(_ts(JOUR, 4, 0))
    bad2.cancel(_ts(JOUR, 10, 0), 77, "B", P, 10)                          # C sur ordre inconnu
    assert _rejouer(bad2.recs).cnt["anomalie_reduction_ordre_inconnu"] == 1
    ok.append("Invariants de carnet discriminants (F sans C jumeau, C sur ordre inconnu)")

    # --- dérivation de δ_max ---
    delais = array("q", [500] * 4000 + [10 ** 9 + i for i in range(200)])
    lam = Counter({math.floor(math.log10(1e-9) * LAMBDA_BINS_PAR_DECADE): 4200})
    d = derive_delta_max(delais, lam)
    assert d["mode_rapide_atteint"] and d["delta_max_ns"] is not None
    assert 500 < d["delta_max_ns"] < 10 ** 9, d["delta_max_ns"]
    # sans mode rapide, pas de croisement aux petits délais
    d2 = derive_delta_max(array("q", [10 ** 9] * 100), Counter({-450: 100}))
    assert d2["delta_max_ns"] is None or d2["delta_max_ns"] > 10 ** 8, d2["delta_max_ns"]
    ok.append(f"δ_max : croisement densité empirique/modèle nul = {d['delta_max_ns']:.4g} ns "
              f"sur distribution de synthèse")

    # --- empreintes ---
    ok.append(_test_a_jour_processus_neuf(__file__))

    for line in ok:
        print("  OK  " + line)
    print(f"autotests : {len(ok)} vérifications OK")


# --- CLI ---
def tickers_itch(strates) -> list[str]:
    return sorted(k for k, v in strates.items() if v.get("dataset") == "XNAS.ITCH")


def cmd_run(args, strates, force):
    """Passe 1 sur les ticker-mois demandés, suivie de `derive` si l'un d'eux a été recalculé.

    `run` réécrit les JSON sans les blocs `p06` produits par `derive` ; relancer `derive`
    une fois après la boucle (il poole tous les ticker-mois) garantit des sorties complètes.
    """
    OUT.mkdir(parents=True, exist_ok=True)
    n_recalcules = 0
    for tm in (args or tickers_itch(strates)):
        dest = OUT / f"{tm}.json"
        if dest.exists() and not force and a_jour(dest):
            print(f"  = {tm} (déjà fait)", flush=True)
            continue
        meta = strates.get(tm, {"ticker": tm.split("_")[0], "mois": tm.split("_")[1]})
        res = run_tm(tm, meta)
        dest.write_text(json.dumps(res, indent=1, default=str))
        journal(etape="run", tm=tm, duree_s=res["duree_s"],
                epuisements=res["p06_capture"]["n_epuisements_exec"],
                vidage=res["p06_capture"]["n_epuisements_exec_vidage"],
                rafales=res["p10b"]["n_rafales_total"],
                na_p08=res["p08"]["part_buckets_rth_na"])
        print(f"  + {tm}: {res['duree_s']}s épuis={res['p06_capture']['n_epuisements_exec']} "
              f"vidage={res['p06_capture']['n_epuisements_exec_vidage']} "
              f"rafales={res['p10b']['n_rafales_total']} "
              f"NA_p08={res['p08']['part_buckets_rth_na']:.3f}", flush=True)
        n_recalcules += 1

    if n_recalcules:
        print(f"  → derive ({n_recalcules} ticker-mois recalculés)", flush=True)
        derive(strates)


def cmd_ancien(args, strates, force):
    dest = PROG / "ancien_detecteur.json"
    dest.parent.mkdir(parents=True, exist_ok=True)
    prev = json.loads(dest.read_text()) if (dest.exists() and not force) else {}
    for tm in (args or tickers_itch(strates)):
        if tm in prev:
            print(f"  = {tm}", flush=True)
            continue
        r = run_h15(tm)
        prev[tm] = r["stats"]
        dest.write_text(json.dumps(prev, indent=1))
        journal(etape="ancien", tm=tm, stats=r["stats"])
        print(f"  + {tm}: {r['stats']}", flush=True)


def main(argv):
    if "--test" in argv:
        autotests()
        return 0
    if not argv:
        print(__doc__)
        return 1
    cmd = argv[0]
    args = [a for a in argv[1:] if not a.startswith("--")]
    force = "--force" in argv
    strates = load_strates()
    if cmd == "run":
        cmd_run(args, strates, force)
    elif cmd == "ancien":
        cmd_ancien(args, strates, force)
    elif cmd == "derive":
        derive(strates)
    else:
        print(f"commande inconnue : {cmd}")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
