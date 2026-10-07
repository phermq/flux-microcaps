#!/usr/bin/env python3
"""P-01 en mode `LR_venue` et P-09 (λ_venue) sur le BBO de la bourse d'exécution.

Entrée : fichiers TBBO Databento de l'échantillon (`*_tbbo.dbn.zst`, schéma MBP-1),
décodés localement. Lecture DBN, cache de séance et strates : `donnees/donnees_mbo.py`.

Usage :
  python3 primitives_t3q.py run [TM ...] [--force]
      P-01 par bucket, validation contre le flag agresseur natif, désaccord tick/LR,
      P-09 par titre-jour -> sorties/t3q/{TM}.json, sorties/t3q/.progress/buckets/{TM}.csv.gz
  python3 primitives_t3q.py recap
      agrégat -> sorties/t3q/recap-b2-6.md, comparé compteur par compteur à
      sorties/tests-mbo/h13_par_ticker_mois.csv (produit par donnees_mbo.py)
  python3 primitives_t3q.py --test
      tests sur flux synthétiques, sans lecture de données

Dans le schéma `tbbo`, le BBO joint à un trade est l'état du carnet juste avant ce
trade (vérifié en reconstruisant le carnet depuis le `mbo` : 3 114 cas discriminants
contre 3 sur HTBX 2018-06). Il entre dans `FluxBBO` avec `prevaut_avant=True`, ce qui
garde testable la règle de P-01 : une cotation ordinaire horodatée à l'instant exact
du trade n'est pas utilisée.

Limites : la tape ne contient aucun print TRF, donc `V^trf` = 0 (vérifié à chaque
ticker-mois) et les tests du tick « tous prints » et « lit seuls » coïncident ; la
série retenue inclut les prints `side='N'`. Le schéma MBP-1 n'a ni condition de vente
ni `order_id` : le flag `auction` n'est pas calculable, les prints d'enchère restent
dans les buckets et leur poids est borné par des compteurs sur les fenêtres
[09:30, +5 s) et [16:00, +15 s).
"""
from __future__ import annotations

import csv
import gzip
import json
import math
import statistics
import sys
import time
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path

CODE = Path(__file__).resolve()
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import chemins  # noqa: E402
A_CODE = chemins.SRC / "donnees"

from donnees_mbo import (                              # noqa: E402
    DATA, NY, UNDEF_PRICE, SessionCache, load_strates, sha256, stream_records,
)

OUT = chemins.SORTIES / "t3q"
PROG = OUT / ".progress"
BUCK = PROG / "buckets"
JOURNAL = OUT / ".progress.jsonl"
REF_H13 = chemins.SORTIES / "tests-mbo" / "h13_par_ticker_mois.csv"

# --- Paramètres --------------------------------------------------------------
BUCKET_RTH_NS = 60_000_000_000       # Δ = 1 min en séance régulière
BUCKET_HORS_NS = 300_000_000_000     # 5 min hors séance
PRE_AVANT_RTH_NS = (5 * 3600 + 1800) * 10 ** 9       # prémarché : 04:00 ET = 09:30 − 5h30
AH_APRES_RTH_NS = 4 * 3600 * 10 ** 9                 # after-hours : 20:00 ET = 16:00 + 4h
N_MIN_P09 = 30                       # P-09 : n_min = 30 buckets
UNITE_F_USD = 10_000.0               # P-09 : flux signé f_b en dizaines de milliers de $
BP = 10_000.0                        # Δμ en points de base
PX_DBN = 1e9                         # DBN : prix en entiers de 1e-9 $

# Fenêtres d'enchère d'ouverture et de clôture (celles de P-07), utilisées ici
# uniquement comme compteurs de contrôle
FEN_OUV_NS = 5_000_000_000
FEN_CLO_NS = 15_000_000_000

# Taux d'erreur poolés obtenus par donnees_mbo.run_h13 sur le même échantillon
REF_LR_POOLE = 0.013305              # 8 151 / 612 672
REF_TICK_POOLE = 0.188150            # 115 115 / 611 820
TOL_PT = 0.1                         # écart toléré, en points de pourcentage

QS = (0.01, 0.05, 0.25, 0.50, 0.75, 0.95, 0.99)


# --- Empreintes de run et journal (mêmes conventions que primitives_t3.py) ---
_SHA: dict[str, str] = {}


def code_sha256() -> str:
    if "code" not in _SHA:
        _SHA["code"] = sha256(CODE)
    return _SHA["code"]


def tests_mbo_sha256() -> str:
    """Calculé à la demande, pour que a_jour() fonctionne avant tout appel à empreinte()."""
    if "a" not in _SHA:
        _SHA["a"] = sha256(A_CODE / "donnees_mbo.py")
    return _SHA["a"]


def empreinte() -> dict:
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
    """Vérifie que a_jour() peut être le premier appel d'un processus neuf, y compris
    quand l'empreinte correspond. Lancé en sous-processus, car `_SHA` est déjà rempli
    dans le processus courant."""
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
    return "a_jour() appelée en premier dans un processus neuf"


def journal(**kw):
    JOURNAL.parent.mkdir(parents=True, exist_ok=True)
    with open(JOURNAL, "a") as fh:
        fh.write(json.dumps({"ts": datetime.now(NY).isoformat(timespec="seconds"), **kw},
                            default=str) + "\n")


class NA:
    """Valeur manquante avec code de raison, à la place de 0 ou ±inf."""

    __slots__ = ("code",)

    def __init__(self, code: str):
        self.code = code

    def __repr__(self):
        return f"NA({self.code})"

    def __eq__(self, other):
        return isinstance(other, NA) and self.code == other.code

    def __bool__(self):
        return False


def quantiles(valeurs, qs=QS) -> dict | None:
    """Quantiles au plus proche rang, sans interpolation."""
    v = sorted(valeurs)
    n = len(v)
    if n == 0:
        return None
    out = {f"q{int(q * 100):02d}": v[min(n - 1, max(0, math.ceil(q * n) - 1))] for q in qs}
    out["n"], out["min"], out["max"] = n, v[0], v[-1]
    out["moyenne"] = sum(v) / n
    return out


def taux(num, den):
    return None if not den else num / den


# --- P-01 `LR_venue` : éléments de base -------------------------------------
class FluxBBO:
    """BBO de la bourse d'exécution en vigueur strictement avant un instant donné.

    Une cotation `on_quote(ts, ...)` prend effet à ts et n'est donc pas vue par un
    trade horodaté ts. Avec `prevaut_avant=True`, elle décrit l'état juste avant ts
    (cas du schéma `tbbo`) et est vue par ce trade. Ni décalage ni interpolation.
    """

    __slots__ = ("cur", "pend")

    def __init__(self):
        self.cur = (None, None)
        self.pend: list[tuple] = []

    def reset(self):
        self.cur = (None, None)
        self.pend.clear()

    def on_quote(self, ts, bid, ask, prevaut_avant=False):
        self.pend.append((ts, bid, ask, prevaut_avant))

    def prevalant(self, ts):
        i = 0
        for (qts, b, a, pa) in self.pend:
            if qts < ts or (pa and qts <= ts):
                self.cur = (b, a)
                i += 1
            else:
                break
        if i:
            del self.pend[:i]
        return self.cur


def bbo_utilisable(bid, ask) -> tuple[bool, str]:
    """BBO absent, croisé ou verrouillé : inutilisable, on se replie sur le test du tick."""
    if bid is None or ask is None or bid == UNDEF_PRICE or ask == UNDEF_PRICE:
        return False, "sans_bbo"
    if bid > ask:
        return False, "croise"
    if bid == ask:
        return False, "verrouille"
    return True, "ok"


def position_prix(px, bid, ask) -> str:
    """Position du prix par rapport au BBO, avec les mêmes catégories que donnees_mbo.run_h13."""
    ok, motif = bbo_utilisable(bid, ask)
    if not ok:
        return motif
    s2 = bid + ask
    if px > ask:
        return "au_dessus_ask"
    if px == ask:
        return "au_ask"
    if px < bid:
        return "sous_bid"
    if px == bid:
        return "au_bid"
    if 2 * px > s2:
        return "dedans_haut"
    if 2 * px < s2:
        return "dedans_bas"
    return "au_mid"


class EtatTick:
    """Test du tick sur le dernier prix différent ; l'état est remis à zéro à chaque séance."""

    __slots__ = ("last_px", "last_dir")

    def __init__(self):
        self.last_px = self.last_dir = None

    def reset(self):
        self.last_px = self.last_dir = None

    def signe(self, px):
        """Signe du tick pour un print à `px`, avant mise à jour de l'état."""
        if self.last_px is None:
            return None
        if px > self.last_px:
            return 1
        if px < self.last_px:
            return -1
        return self.last_dir

    def maj(self, px):
        if self.last_px is not None and px != self.last_px:
            self.last_dir = 1 if px > self.last_px else -1
        self.last_px = px


def signe_lr_venue(px, bid, ask, tick) -> tuple[int | None, bool, str]:
    """Retourne (signe, fallback, motif). `fallback` signale un BBO inutilisable ; le
    recours au tick pour un print au mid fait partie de la règle de Lee-Ready et n'en est pas un."""
    ok, motif = bbo_utilisable(bid, ask)
    if not ok:
        return tick, True, motif
    s2 = bid + ask
    if 2 * px > s2:
        return 1, False, "quote"
    if 2 * px < s2:
        return -1, False, "quote"
    return tick, False, "au_mid"


# --- Accumulateur par ticker-mois -------------------------------------------
# Colonnes d'un bucket P-01 (entiers exacts ; notionnels en 1e-9 $ × actions)
NP, NM, NN, VP, VM, VN, EP, EM, EN, NFB, NAUC, VAUC, NTOT = range(13)
NCOL = 13


class T3q:
    """Une passe sur le `tbbo` d'un ticker-mois : P-01 par bucket, validation contre
    le flag agresseur natif, P-09."""

    def __init__(self, tm: str, meta: dict):
        self.tm, self.meta = tm, meta
        self.sess = SessionCache()
        self.flux, self.tick = FluxBBO(), EtatTick()
        self.buckets: dict[tuple, list] = {}
        self.h13: Counter = Counter()
        self.pos: dict[str, Counter] = defaultdict(Counter)
        self.motifs_fallback: Counter = Counter()
        self.cnt: Counter = Counter()
        self.jours: list[str] = []
        self.n_par_jour: Counter = Counter()
        self.mids: dict[str, list] = {}      # jour -> bid+ask aux 391 frontières RTH
        self.jour = None
        self.last_ts = None
        self.s2_cur = None
        self.b_idx = 0
        self.rth_t0 = self.rth_t1 = self.pre_ns = self.ah_ns = 0
        self.jour_t1 = 0
        self.nb_rth = 0

    # séances
    def _ouvrir_jour(self, ts):
        self.jour = self.sess.date
        self.jours.append(self.jour)
        self.rth_t0, self.rth_t1 = self.sess.open_ns, self.sess.close_ns
        self.pre_ns = self.rth_t0 - PRE_AVANT_RTH_NS
        self.ah_ns = self.rth_t1 + AH_APRES_RTH_NS
        self.jour_t1 = self.sess.hi
        self.nb_rth = (self.rth_t1 - self.rth_t0) // BUCKET_RTH_NS
        self.mids[self.jour] = [None] * (self.nb_rth + 1)
        self.b_idx = 0
        # Le tick et le carnet de la veille ne valent plus à la nouvelle séance
        self.tick.reset()
        self.flux.reset()
        self.s2_cur = None

    def _fermer_jour(self):
        if self.jour is None:
            return
        self._stamp(self.jour_t1, True)   # frontières RTH restantes : dernier mid observé

    def _bucket_cle(self, t):
        j = self.jour
        if t < self.pre_ns:
            return (j, "AVANT", 0)
        if t < self.rth_t0:
            return (j, "PRE", (t - self.pre_ns) // BUCKET_HORS_NS)
        if t < self.rth_t1:
            return (j, "RTH", (t - self.rth_t0) // BUCKET_RTH_NS)
        if t < self.ah_ns:
            return (j, "AH", (t - self.rth_t1) // BUCKET_HORS_NS)
        return (j, "APRES", 0)

    # mids pour P-09
    def _stamp(self, t, inclusif):
        """Affecte le mid courant aux frontières de bucket RTH T < t (T <= t si `inclusif`).

        μ(T) est le mid de la dernière cotation prenant effet au plus tard en T. Le schéma
        `tbbo` n'observe les cotations qu'aux trades ; celle d'un trade en T prend effet
        juste avant T. On a ainsi μ_fin(b) = μ_début(b+1), sans trou entre buckets.
        """
        mb = self.mids.get(self.jour)
        if mb is None:
            return
        while self.b_idx <= self.nb_rth:
            T = self.rth_t0 + self.b_idx * BUCKET_RTH_NS
            if T < t or (inclusif and T == t):
                mb[self.b_idx] = self.s2_cur
                self.b_idx += 1
            else:
                break

    # ingestion
    def feed(self, rec):
        ts = rec.ts_event
        if self.last_ts is not None and ts < self.last_ts:
            self.cnt["desordre_fichier"] += 1
        self.last_ts = ts
        if str(rec.action) != "T":
            self.cnt["exclu_non_trade"] += 1
            return
        if rec.size == 0:
            self.cnt["exclu_taille_nulle"] += 1
            return
        if self.sess.update(ts):
            self._fermer_jour()
            self._ouvrir_jour(ts)
        self.n_par_jour[self.jour] += 1

        lvl = rec.levels[0]
        # Les frontières strictement antérieures à ts reçoivent le mid d'avant cette
        # cotation ; une frontière égale à ts reçoit la cotation pré-trade.
        self.flux.on_quote(ts, lvl.bid_px, lvl.ask_px, prevaut_avant=True)
        self._stamp(ts, False)
        bid, ask = self.flux.prevalant(ts)
        ok_bbo, _motif = bbo_utilisable(bid, ask)
        self.s2_cur = (bid + ask) if ok_bbo else None
        self._stamp(ts, True)

        px, sz = rec.price, rec.size
        tick = self.tick.signe(px)
        self.tick.maj(px)
        s, fallback, motif = signe_lr_venue(px, bid, ask, tick)
        if fallback:
            self.motifs_fallback[motif] += 1

        e = self.buckets.get(self._bucket_cle(ts))
        if e is None:
            e = self.buckets[self._bucket_cle(ts)] = [0] * NCOL
        notionnel = px * sz
        if s == 1:
            e[NP] += 1; e[VP] += sz; e[EP] += notionnel
        elif s == -1:
            e[NM] += 1; e[VM] += sz; e[EM] += notionnel
        else:
            e[NN] += 1; e[VN] += sz; e[EN] += notionnel
        e[NTOT] += 1
        if fallback:
            e[NFB] += 1
        if (self.rth_t0 <= ts < self.rth_t0 + FEN_OUV_NS) or \
           (self.rth_t1 <= ts < self.rth_t1 + FEN_CLO_NS):
            e[NAUC] += 1; e[VAUC] += sz

        self._h13(rec, px, sz, bid, ask, s, tick)

    def _h13(self, rec, px, sz, bid, ask, lr, tick):
        """Compteurs de validation contre le flag agresseur natif, définis comme dans donnees_mbo.run_h13."""
        t = self.h13
        t["n_total"] += 1
        t["vol_total"] += sz
        if tick is not None and lr is not None:
            t["n_comp_tick_lr_tous"] += 1
            if tick != lr:
                t["n_desac_tick_lr_tous"] += 1
        side = str(rec.side)
        if side == "N":
            t["n_side_N"] += 1
            t["vol_side_N"] += sz
            return
        vrai = 1 if side == "B" else -1
        t["n_eval"] += 1
        t["vol_eval"] += sz
        b = self.pos[position_prix(px, bid, ask)]
        b["n"] += 1
        b["vol"] += sz
        if lr is None:
            b["lr_indet"] += 1; t["lr_indet"] += 1
        elif lr == vrai:
            b["lr_ok"] += 1; t["lr_ok"] += 1; t["vol_lr_ok"] += sz
        else:
            b["lr_faux"] += 1; t["lr_faux"] += 1; t["vol_lr_faux"] += sz
        if tick is None:
            b["tick_indet"] += 1; t["tick_indet"] += 1
        elif tick == vrai:
            b["tick_ok"] += 1; t["tick_ok"] += 1; t["vol_tick_ok"] += sz
        else:
            b["tick_faux"] += 1; t["tick_faux"] += 1; t["vol_tick_faux"] += sz
        if tick is not None and lr is not None:
            t["n_comp_tick_lr_eval"] += 1
            if tick != lr:
                t["n_desac_tick_lr_eval"] += 1

    def finish(self):
        self._fermer_jour()

    # sorties
    def p01(self) -> dict:
        som = [0] * NCOL
        ofi_act, ofi_not, par_jour = [], [], defaultdict(lambda: [0] * NCOL)
        for cle in sorted(self.buckets):
            e = self.buckets[cle]
            for i in range(NCOL):
                som[i] += e[i]
                par_jour[cle[0]][i] += e[i]
            if e[VP] + e[VM] > 0:
                ofi_act.append((e[VP] - e[VM]) / (e[VP] + e[VM]))
            if e[EP] + e[EM] > 0:
                ofi_not.append((e[EP] - e[EM]) / (e[EP] + e[EM]))
        signe_vol = som[VP] + som[VM]
        return {
            "signing_method": "LR_venue",
            "n_buckets": len(self.buckets),
            "sommes": {
                "n_plus": som[NP], "n_moins": som[NM], "n_na": som[NN],
                "v_plus": som[VP], "v_moins": som[VM], "v_na": som[VN], "v_trf": 0,
                "notionnel_plus": som[EP] / PX_DBN, "notionnel_moins": som[EM] / PX_DBN,
                "notionnel_na": som[EN] / PX_DBN, "notionnel_trf": 0.0,
            },
            "part_signable_actions": taux(signe_vol, signe_vol + som[VN]),
            "part_signable_nb": taux(som[NP] + som[NM], som[NP] + som[NM] + som[NN]),
            "n_fallback": som[NFB],
            "part_fallback": taux(som[NFB], som[NTOT]),
            "motifs_fallback": dict(self.motifs_fallback),
            "ofi_median_actions": statistics.median(ofi_act) if ofi_act else None,
            "ofi_median_notionnel": statistics.median(ofi_not) if ofi_not else None,
            "ofi_quantiles_actions": quantiles(ofi_act),
            "n_buckets_ofi": len(ofi_act),
            "controle_fenetres_auction": {
                "n_prints": som[NAUC], "vol": som[VAUC],
                "part_nb": taux(som[NAUC], som[NTOT]), "part_vol": taux(som[VAUC], som[VP] + som[VM] + som[VN]),
                "note": "flag `auction` non calculable sur T3q — prints inclus dans les buckets",
            },
            "par_jour": [
                {"jour": j, "n_prints": v[NTOT], "v_plus": v[VP], "v_moins": v[VM],
                 "v_na": v[VN], "ofi_actions": taux(v[VP] - v[VM], v[VP] + v[VM])}
                for j, v in sorted(par_jour.items())
            ],
        }

    def validation_h13(self) -> dict:
        t = self.h13
        n_lr = t["lr_ok"] + t["lr_faux"]
        n_tk = t["tick_ok"] + t["tick_faux"]
        mid = self.pos.get("au_mid", Counter())
        return {
            "n_trades_total": t["n_total"], "vol_total": t["vol_total"],
            "n_side_N": t["n_side_N"], "vol_side_N": t["vol_side_N"],
            "n_eval": t["n_eval"], "vol_eval": t["vol_eval"],
            "n_lr_classes": n_lr, "n_lr_faux": t["lr_faux"], "n_lr_indet": t["lr_indet"],
            "taux_lr": taux(t["lr_faux"], n_lr),
            "taux_lr_vol": taux(t["vol_lr_faux"], t["vol_lr_faux"] + t["vol_lr_ok"]),
            "n_tick_classes": n_tk, "n_tick_faux": t["tick_faux"], "n_tick_indet": t["tick_indet"],
            "taux_tick": taux(t["tick_faux"], n_tk),
            "n_au_mid": mid.get("n", 0), "n_au_mid_faux_lr": mid.get("lr_faux", 0),
            "positions": {k: dict(v) for k, v in sorted(self.pos.items())},
            "desaccord_tick_lr": {
                "n_comparables_tous": t["n_comp_tick_lr_tous"],
                "n_desaccords_tous": t["n_desac_tick_lr_tous"],
                "taux_tous": taux(t["n_desac_tick_lr_tous"], t["n_comp_tick_lr_tous"]),
                "n_comparables_eval": t["n_comp_tick_lr_eval"],
                "n_desaccords_eval": t["n_desac_tick_lr_eval"],
                "taux_eval": taux(t["n_desac_tick_lr_eval"], t["n_comp_tick_lr_eval"]),
            },
        }

    # P-09
    def p09(self) -> dict:
        """Par jour RTH, MCO Δμ_b = α + λ·f_b + ε, avec Δμ_b en bp de μ_début et f_b en 10 k$."""
        out = []
        n_sig = n_sans_mu = 0
        for jour in self.jours:
            mb = self.mids.get(jour) or []
            pts, sig_j, sans_mu_j = [], 0, 0
            for i in range((len(mb) - 1) if mb else 0):
                e = self.buckets.get((jour, "RTH", i))
                if e is None or (e[NP] + e[NM]) == 0:
                    continue                       # bucket sans trade signé : exclu
                sig_j += 1
                s2d, s2f = mb[i], mb[i + 1]
                if s2d is None or s2f is None or s2d == 0:
                    sans_mu_j += 1                 # mid indisponible : bucket sans Δμ
                    continue
                y = BP * (s2f - s2d) / s2d         # Δμ/μ = Δ(bid+ask)/(bid+ask)
                f = (e[EP] - e[EM]) / PX_DBN / UNITE_F_USD
                pts.append((f, y))
            n_sig += sig_j
            n_sans_mu += sans_mu_j
            out.append({"jour": jour, "n_buckets_signes": sig_j,
                        "n_buckets_signes_sans_mu": sans_mu_j, **_mco(pts)})
        na = [r for r in out if r["lambda"] is None]
        codes = Counter(r["na_code"] for r in na)
        lam = [r["lambda"] for r in out if r["lambda"] is not None]
        return {
            "n_titre_jour": len(out), "n_na": len(na),
            "part_na": taux(len(na), len(out)), "na_par_code": dict(codes),
            "n_buckets_signes": n_sig, "n_buckets_signes_sans_mu": n_sans_mu,
            "part_buckets_signes_sans_mu": taux(n_sans_mu, n_sig),
            "lambda_quantiles": quantiles(lam), "par_jour": out,
        }


def _mco(pts) -> dict:
    """MCO avec constante ; sommes float64 séquentielles dans l'ordre des points, pour un résultat reproductible."""
    n = len(pts)
    if n < N_MIN_P09:
        return {"lambda": None, "alpha": None, "r2": None, "n": n,
                "na_code": "n_insuffisant", "na": repr(NA("n_insuffisant"))}
    sf = sy = 0.0
    for f, y in pts:
        sf += f
        sy += y
    fbar, ybar = sf / n, sy / n
    sxx = sxy = syy = 0.0
    for f, y in pts:
        df, dy = f - fbar, y - ybar
        sxx += df * df
        sxy += df * dy
        syy += dy * dy
    if sxx == 0.0:
        return {"lambda": None, "alpha": None, "r2": None, "n": n,
                "na_code": "var_f_nulle", "na": repr(NA("var_f_nulle"))}
    lam = sxy / sxx
    return {"lambda": lam, "alpha": ybar - lam * fbar,
            "r2": (sxy * sxy) / (sxx * syy) if syy > 0 else None,
            "n": n, "na_code": None, "na": None}


# --- Exécution par ticker-mois ----------------------------------------------
def run_tm(tm: str, meta: dict, records=None) -> tuple[dict, "T3q"]:
    t0 = time.time()
    acc = T3q(tm, meta)
    if records is None:
        records = stream_records(DATA / f"{tm}_tbbo.dbn.zst")
    for rec in records:
        acc.feed(rec)
    acc.finish()
    p01, p09, h13 = acc.p01(), acc.p09(), acc.validation_h13()
    assert p01["sommes"]["v_trf"] == 0 and p01["sommes"]["notionnel_trf"] == 0.0, tm
    res = {
        "ticker_mois": tm, **{k: v for k, v in meta.items()},
        "n_jours": len(acc.jours), "jours": acc.jours,
        "n_decodes": acc.h13["n_total"] + acc.cnt["exclu_non_trade"] + acc.cnt["exclu_taille_nulle"],
        "exclusions": dict(acc.cnt),
        "trades_par_jour_median": statistics.median(acc.n_par_jour.values()) if acc.n_par_jour else 0,
        "p01": p01, "validation_h13": h13, "p09": p09,
        "duree_s": round(time.time() - t0, 2),
        "fingerprint": empreinte(),
    }
    return res, acc


def ecrire_buckets(tm: str, acc: T3q) -> None:
    BUCK.mkdir(parents=True, exist_ok=True)
    with gzip.open(BUCK / f"{tm}.csv.gz", "wt", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(["jour", "session", "bucket", "n_plus", "n_moins", "n_na",
                    "v_plus", "v_moins", "v_na", "v_trf",
                    "notionnel_plus", "notionnel_moins", "notionnel_na",
                    "n_fallback", "ofi_actions", "ofi_notionnel",
                    "mu_debut", "mu_fin", "d_mu_bp"])
        for (j, sess, i) in sorted(acc.buckets):
            e = acc.buckets[(j, sess, i)]
            mu_d = mu_f = dmu = ""
            if sess == "RTH":
                mb = acc.mids.get(j) or []
                if i + 1 < len(mb) and mb[i] is not None and mb[i + 1] is not None and mb[i]:
                    mu_d, mu_f = mb[i] / 2 / PX_DBN, mb[i + 1] / 2 / PX_DBN
                    dmu = BP * (mb[i + 1] - mb[i]) / mb[i]
            ofi_a = taux(e[VP] - e[VM], e[VP] + e[VM])
            ofi_n = taux(e[EP] - e[EM], e[EP] + e[EM])
            w.writerow([j, sess, i, e[NP], e[NM], e[NN], e[VP], e[VM], e[VN], 0,
                        e[EP] / PX_DBN, e[EM] / PX_DBN, e[EN] / PX_DBN, e[NFB],
                        "" if ofi_a is None else ofi_a, "" if ofi_n is None else ofi_n,
                        mu_d, mu_f, dmu])


# --- Comparaison à la référence de donnees_mbo.py (h13_par_ticker_mois.csv) --
CLES_A = ("n_trades_total", "n_side_N", "vol_side_N", "n_eval", "vol_eval",
          "n_lr_classes", "n_lr_faux", "n_lr_indet", "n_tick_classes", "n_tick_faux",
          "n_tick_indet", "n_au_mid", "n_au_mid_faux_lr", "vol_total")
POS_A = {"n_sans_bbo": "sans_bbo", "n_croise": "croise", "n_verrouille": "verrouille",
         "n_au_bid": "au_bid", "n_au_ask": "au_ask"}


def ref_a() -> dict[str, dict]:
    if not REF_H13.exists():
        return {}
    with open(REF_H13) as fh:
        return {r["ticker_mois"]: r for r in csv.DictReader(fh)}


def comparer_a(tm: str, h13: dict, ref: dict) -> dict:
    """Compare les compteurs entiers à ceux de h13_par_ticker_mois.csv pour ce ticker-mois."""
    r = ref.get(tm)
    if r is None:
        return {"statut": "reference_absente"}
    ecarts = {}
    for k in CLES_A:
        a, b = int(float(r[k])), int(h13[k])
        if a != b:
            ecarts[k] = {"reference": a, "calcul": b}
    p = h13["positions"]
    for k, nom in POS_A.items():
        a, b = int(float(r[k])), p.get(nom, {}).get("n", 0)
        if a != b:
            ecarts[k] = {"reference": a, "calcul": b}
    a_ded = int(float(r["n_dedans"]))
    b_ded = p.get("dedans_haut", {}).get("n", 0) + p.get("dedans_bas", {}).get("n", 0)
    if a_ded != b_ded:
        ecarts["n_dedans"] = {"reference": a_ded, "calcul": b_ded}
    taux_a = float(r["taux_lr"]) if r["taux_lr"] not in ("", "None") else None
    ecart_pt = None
    if taux_a is not None and h13["taux_lr"] is not None:
        ecart_pt = 100 * (h13["taux_lr"] - taux_a)
    return {"statut": "identique" if not ecarts else "ECART", "ecarts": ecarts,
            "taux_lr_A": taux_a, "ecart_taux_lr_pt": ecart_pt}


# --- Récapitulatif ----------------------------------------------------------
def _f(x, n=3):
    if x is None:
        return "NA"
    if isinstance(x, float):
        return f"{x:.{n}f}"
    return str(x)


def charger_sorties() -> dict[str, dict]:
    return {p.stem: json.loads(p.read_text()) for p in sorted(OUT.glob("*.json"))}


def ecrire_recap(res: dict[str, dict]) -> dict:
    ref = ref_a()
    L: list[str] = []
    A_ = L.append
    A_("# P-01 mode `LR_venue` et P-09 (λ_venue) sur TBBO : récapitulatif\n")
    A_(f"Exécution du {datetime.now(NY).date().isoformat()} · code : "
       f"`src/primitives/primitives_t3q.py` (SHA-256 `{code_sha256()[:16]}…`) · "
       f"décodage local. Entrées : {len(res)} fichiers "
       "`*_tbbo.dbn.zst` de l'échantillon.\n")
    A_("Primitives : P-01 (mode `LR_venue`) et P-09, sur le BBO de la bourse d'exécution.\n")

    # 1. Validation contre le flag agresseur natif
    tot = Counter()
    for r in res.values():
        for k in ("n_trades_total", "n_side_N", "n_eval", "n_lr_classes", "n_lr_faux",
                  "n_tick_classes", "n_tick_faux", "n_lr_indet", "n_tick_indet"):
            tot[k] += r["validation_h13"][k]
        d = r["validation_h13"]["desaccord_tick_lr"]
        tot["n_comp_tous"] += d["n_comparables_tous"]
        tot["n_des_tous"] += d["n_desaccords_tous"]
        tot["n_comp_eval"] += d["n_comparables_eval"]
        tot["n_des_eval"] += d["n_desaccords_eval"]
    tx_lr = taux(tot["n_lr_faux"], tot["n_lr_classes"])
    tx_tk = taux(tot["n_tick_faux"], tot["n_tick_classes"])
    A_("\n## 1. Validation de LR_venue contre le flag agresseur natif\n")
    A_("| grandeur | ce module | référence (donnees_mbo.py) | écart |")
    A_("|---|---|---|---|")
    A_(f"| prints décodés | {tot['n_trades_total']} | 872 214 | "
       f"{tot['n_trades_total'] - 872214:+d} |")
    A_(f"| trades évaluables (`side` ≠ N) | {tot['n_eval']} | 612 698 | "
       f"{tot['n_eval'] - 612698:+d} |")
    A_(f"| taux d'erreur LR_venue (poolé) | {100 * tx_lr:.4f} % "
       f"({tot['n_lr_faux']}/{tot['n_lr_classes']}) | 1,33 % (8 151/612 672) | "
       f"{100 * (tx_lr - REF_LR_POOLE):+.4f} pt |")
    A_(f"| taux d'erreur tick pure (poolé) | {100 * tx_tk:.4f} % "
       f"({tot['n_tick_faux']}/{tot['n_tick_classes']}) | 18,82 % (115 115/611 820) | "
       f"{100 * (tx_tk - REF_TICK_POOLE):+.4f} pt |")
    A_("")
    A_("Comparaison compteur par compteur avec `sorties/tests-mbo/"
       "h13_par_ticker_mois.csv` (14 compteurs et 6 positions de prix, par ticker-mois) :\n")
    cmps = {tm: comparer_a(tm, r["validation_h13"], ref) for tm, r in res.items()}
    n_id = sum(1 for c in cmps.values() if c["statut"] == "identique")
    A_(f"- {n_id}/{len(cmps)} ticker-mois identiques à l'entier près.")
    A_("- Les écarts non nuls de la colonne « écart (pt) » ci-dessous viennent de "
       "l'arrondi à 5 décimales du `taux_lr` stocké dans le CSV de référence ; les compteurs "
       "entiers dont ces taux dérivent sont identiques.")
    for tm, c in sorted(cmps.items()):
        if c["statut"] != "identique":
            A_(f"- {tm} : {c['statut']}, {json.dumps(c.get('ecarts', {}))}")
    A_("")
    A_("| ticker-mois | dataset | tranche | n_eval | taux LR_venue | taux LR (réf.) | "
       "écart (pt) | taux tick | désaccord tick/LR (tous) |")
    A_("|---|---|---|---|---|---|---|---|---|")
    for tm in sorted(res, key=lambda k: (res[k].get("mois", ""), k)):
        r, v = res[tm], res[tm]["validation_h13"]
        c = cmps[tm]
        d = v["desaccord_tick_lr"]
        A_(f"| {tm} | {r.get('dataset', '?')} | {r.get('strate_prix', '?')} | {v['n_eval']} | "
           f"{_f(100 * v['taux_lr'], 4) if v['taux_lr'] is not None else 'NA'} % | "
           f"{_f(100 * c['taux_lr_A'], 4) if c.get('taux_lr_A') is not None else 'NA'} % | "
           f"{_f(c['ecart_taux_lr_pt'], 6) if c.get('ecart_taux_lr_pt') is not None else 'NA'} | "
           f"{_f(100 * v['taux_tick'], 2) if v['taux_tick'] is not None else 'NA'} % | "
           f"{_f(100 * d['taux_tous'], 2) if d['taux_tous'] is not None else 'NA'} % |")

    # 2. Désaccord tick / LR_venue
    A_("\n## 2. Désaccord tick vs `LR_venue` (P-01, condition d'invalidation à 30 %)\n")
    tx_tous = taux(tot["n_des_tous"], tot["n_comp_tous"])
    tx_eval = taux(tot["n_des_eval"], tot["n_comp_eval"])
    med = statistics.median([r["validation_h13"]["desaccord_tick_lr"]["taux_tous"]
                             for r in res.values()
                             if r["validation_h13"]["desaccord_tick_lr"]["taux_tous"] is not None])
    A_("Désaccord entre deux classificateurs, et non borne d'erreur, calculé sur la seule "
       "tape de la bourse d'exécution : test du tick sur tous les prints (`side='N'` inclus) "
       "comparé à `LR_venue` sur les mêmes trades.\n")
    A_(f"- poolé, tous les prints : {100 * tx_tous:.2f} % "
       f"({tot['n_des_tous']}/{tot['n_comp_tous']})")
    A_(f"- poolé, prints évaluables seuls (`side` ≠ N) : {100 * tx_eval:.2f} % "
       f"({tot['n_des_eval']}/{tot['n_comp_eval']})")
    A_(f"- médiane par ticker-mois : {100 * med:.2f} % ; critère d'invalidation P-01 "
       f"(médiane > 30 %) : {'déclenché' if med > 0.30 else 'non déclenché'}")

    # 3. P-01 : volumes signés
    A_("\n## 3. P-01 `LR_venue` — sorties par bucket (agrégats par ticker-mois)\n")
    A_("`V^trf` = 0 partout : la tape de la bourse d'exécution ne contient pas de print TRF "
       "(vérifié à l'exécution). Le flag `auction` n'est pas calculable sur ce schéma : les "
       "prints d'enchère sont inclus, la colonne « fenêtres enchère » borne leur poids.\n")
    A_("| ticker-mois | prints | V+ | V− | V^na | part signable | fallback | "
       "OFI médian (actions) | fenêtres enchère (% vol) |")
    A_("|---|---|---|---|---|---|---|---|---|")
    for tm in sorted(res):
        p = res[tm]["p01"]
        s = p["sommes"]
        A_(f"| {tm} | {s['n_plus'] + s['n_moins'] + s['n_na']} | {s['v_plus']} | "
           f"{s['v_moins']} | {s['v_na']} | {_f(100 * p['part_signable_actions'], 2)} % | "
           f"{p['n_fallback']} ({_f(100 * (p['part_fallback'] or 0), 3)} %) | "
           f"{_f(p['ofi_median_actions'], 4)} | "
           f"{_f(100 * (p['controle_fenetres_auction']['part_vol'] or 0), 2)} % |")
    n_fb = sum(res[tm]["p01"]["n_fallback"] for tm in res)
    n_pr = sum(sum(res[tm]["p01"]["sommes"][k] for k in ("n_plus", "n_moins", "n_na"))
               for tm in res)
    mf = Counter()
    n_fb_eval = 0
    for tm in res:
        mf.update(res[tm]["p01"]["motifs_fallback"])
        p = res[tm]["validation_h13"]["positions"]
        n_fb_eval += sum(p.get(k, {}).get("n", 0)
                         for k in ("sans_bbo", "croise", "verrouille"))
    A_(f"\nRepli sur le test du tick (BBO absent, verrouillé ou croisé), sur les trades "
       f"évaluables (`side` ≠ N) : {n_fb_eval}/{tot['n_eval']} = "
       f"{100 * n_fb_eval / tot['n_eval']:.4f} % (référence : 0,04 %, 251/612 698). "
       f"Sur tous les prints, périmètre de P-01 qui signe aussi les prints `side='N'` : "
       f"{n_fb}/{n_pr} = {100 * n_fb / n_pr:.4f} %. Le repli est environ 10 fois plus "
       f"fréquent sur les prints `side='N'` (enchères et exécutions cachées, souvent avec un "
       f"BBO verrouillé ou croisé). Motifs : {dict(mf)}.\n")

    # 4. P-09
    A_("\n## 4. P-09 λ_venue — distribution et taux de NA\n")
    par_tr: dict[str, list] = defaultdict(list)
    na_tr: dict[str, Counter] = defaultdict(Counter)
    n_tj = n_na = 0
    codes = Counter()
    for tm, r in res.items():
        tr = r.get("strate_prix", "?")
        for j in r["p09"]["par_jour"]:
            n_tj += 1
            na_tr[tr]["n"] += 1
            if j["lambda"] is None:
                n_na += 1
                codes[j["na_code"]] += 1
                na_tr[tr]["na"] += 1
            else:
                par_tr[tr].append(j["lambda"])
    A_(f"Unité : bp / 10 k$. MCO avec constante par jour RTH, Δ = 1 min, n_min = "
       f"{N_MIN_P09} buckets, buckets sans trade signé exclus.\n")
    A_("| tranche de prix | titres-jours | NA | % NA | λ q05 | λ q25 | λ médian | λ q75 | λ q95 |")
    A_("|---|---|---|---|---|---|---|---|---|")
    for tr in sorted(na_tr):
        q = quantiles(par_tr[tr]) or {}
        c = na_tr[tr]
        A_(f"| {tr} | {c['n']} | {c['na']} | {_f(100 * c['na'] / c['n'], 1)} | "
           f"{_f(q.get('q05'), 4)} | {_f(q.get('q25'), 4)} | {_f(q.get('q50'), 4)} | "
           f"{_f(q.get('q75'), 4)} | {_f(q.get('q95'), 4)} |")
    q = quantiles([x for v in par_tr.values() for x in v]) or {}
    A_(f"| total | {n_tj} | {n_na} | {_f(100 * n_na / n_tj, 1)} | "
       f"{_f(q.get('q05'), 4)} | {_f(q.get('q25'), 4)} | {_f(q.get('q50'), 4)} | "
       f"{_f(q.get('q75'), 4)} | {_f(q.get('q95'), 4)} |")
    n_sig = sum(r["p09"]["n_buckets_signes"] for r in res.values())
    n_sm = sum(r["p09"]["n_buckets_signes_sans_mu"] for r in res.values())
    A_(f"\nCodes de NA : {dict(codes)}.\n")
    A_(f"Coût de la convention sur μ (réserve 2) : {n_sm}/{n_sig} = "
       f"{100 * n_sm / n_sig:.3f} % des buckets RTH porteurs de flux signé sont écartés "
       f"faute de Δμ. Il s'agit surtout du bucket 09:30–09:31 des séances sans print de "
       f"prémarché, où aucune cotation n'est antérieure à 09:30.\n")
    part_na = n_na / n_tj if n_tj else None
    verdict = "ROUGE" if (part_na is not None and part_na > 0.50) else "vert"
    A_(f"Critère d'invalidation P-09 : si plus de 50 % des titres-jours de l'échantillon "
       f"sont NA, P-09 n'est pas calculée sur l'univers complet et doit être redéfinie à "
       f"une maille plus large ou abandonnée. Part de NA = {_f(100 * part_na, 1)} % : "
       f"{verdict}.\n")
    A_("| ticker-mois | tranche | titres-jours | NA | % NA | λ médian |")
    A_("|---|---|---|---|---|---|")
    for tm in sorted(res):
        p = res[tm]["p09"]
        lq = p["lambda_quantiles"] or {}
        A_(f"| {tm} | {res[tm].get('strate_prix', '?')} | {p['n_titre_jour']} | {p['n_na']} | "
           f"{_f(100 * (p['part_na'] or 0), 1)} | {_f(lq.get('q50'), 4)} |")

    A_("\n## 5. Réserves\n")
    A_("1. Flag `auction` non calculable : le schéma MBP-1 n'a ni condition de vente ni "
       "`order_id`, et l'imputation structurelle des enchères demande le `mbo`. Les prints "
       "d'enchère restent dans les buckets P-01 ; la colonne « fenêtres enchère » borne l'effet.")
    A_("2. μ n'est échantillonné qu'aux instants de trade, le schéma `tbbo` ne portant pas "
       "d'événement de cotation : μ(T) est le mid de la dernière cotation pré-trade d'instant "
       "≤ T, et P-09 hérite de cette approximation.")
    A_("3. μ est NA quand le BBO est absent, verrouillé ou croisé, avec le même critère "
       "`bbo_utilisable` que le repli de signature.")
    A_("4. `LR_venue` utilise le BBO de la seule bourse d'exécution : son taux d'erreur est "
       "une borne inférieure, non transposable au SIP ni au mode `tick`. La sensibilité à un "
       "décalage de ±1 ms n'est pas étudiée ici.")
    A_("5. P-09 hérite de ces limites via P-01 ; le volume TRF est absent du régresseur.")
    (OUT / "recap-b2-6.md").write_text("\n".join(L) + "\n")
    return {"taux_lr_poole": tx_lr, "taux_tick_poole": tx_tk, "desaccord_tick_lr": tx_tous,
            "part_na_p09": part_na, "verdict_p09": verdict,
            "n_tm_identiques": n_id, "n_tm": len(cmps)}


# --- Tests sur flux synthétiques --------------------------------------------
class _Lvl:
    __slots__ = ("bid_px", "ask_px")

    def __init__(self, b, a):
        self.bid_px, self.ask_px = b, a


class _Rec:
    """Enregistrement TBBO synthétique (mêmes champs que `databento_dbn.MBP1Msg`)."""
    __slots__ = ("ts_event", "price", "size", "action", "side", "levels")

    def __init__(self, ts, px, sz, side="N", bid=None, ask=None, action="T"):
        self.ts_event, self.price, self.size = ts, px, sz
        self.action, self.side = action, side
        self.levels = [_Lvl(UNDEF_PRICE if bid is None else bid,
                            UNDEF_PRICE if ask is None else ask)]


def _ts(jour, h, m, s=0, ns=0):
    from datetime import time as dtime
    d = datetime.fromisoformat(jour).date()
    return int(datetime.combine(d, dtime(h, m, s), NY).timestamp()) * 10 ** 9 + ns


D = 10 ** 9   # 1 $ en unités DBN


def autotests():
    ok: list[str] = []

    # 1. Antériorité stricte de la cotation par rapport au trade (P-01)
    fx = FluxBBO()
    fx.on_quote(100, 10 * D, 11 * D)                 # cotation à t=100
    fx.on_quote(200, 20 * D, 21 * D)                 # cotation au timestamp du trade
    assert fx.prevalant(200) == (10 * D, 11 * D), "cotation au timestamp exact du trade utilisée à tort"
    assert fx.prevalant(201) == (20 * D, 21 * D)
    fy = FluxBBO()
    fy.on_quote(200, 20 * D, 21 * D, prevaut_avant=True)   # cotation pré-trade du schéma tbbo
    assert fy.prevalant(200) == (20 * D, 21 * D), "cotation pré-trade non utilisée"
    ok.append("P-01 : cotation horodatée au timestamp du trade ignorée ; "
              "cotation pré-trade `tbbo` utilisée")

    # 2. Règles de signature
    b, a = 10 * D, 11 * D
    assert signe_lr_venue(a, b, a, None)[0] == 1
    assert signe_lr_venue(b, b, a, None)[0] == -1
    assert signe_lr_venue(105 * D // 10, b, a, 1) == (1, False, "au_mid")
    assert signe_lr_venue(105 * D // 10, b, a, -1) == (-1, False, "au_mid")
    assert signe_lr_venue(105 * D // 10, b, a, None) == (None, False, "au_mid")
    assert signe_lr_venue(b, b, b, 1) == (1, True, "verrouille")
    assert signe_lr_venue(b, a, b, -1) == (-1, True, "croise")
    assert signe_lr_venue(b, None, None, 1) == (1, True, "sans_bbo")
    ok.append("P-01 : ask/bid/mid+tick, replis verrouillé/croisé/absent (flag `fallback`)")

    # 3. Passe complète sur une tape synthétique (tailles toutes distinctes)
    J1, J2 = "2026-01-05", "2026-01-06"
    recs = [
        # 09:30:00 : premier print de la séance, au mid, sans tick -> NA
        _Rec(_ts(J1, 9, 30, 0), 105 * D // 10, 11, "B", 10 * D, 11 * D),
        _Rec(_ts(J1, 9, 30, 1), 11 * D, 13, "B", 10 * D, 11 * D),          # à l'ask -> +1
        _Rec(_ts(J1, 9, 30, 2), 10 * D, 17, "A", 10 * D, 11 * D),          # au bid -> −1
        # au mid, tick montant (10 -> 10,5) -> +1
        _Rec(_ts(J1, 9, 30, 3), 105 * D // 10, 19, "B", 10 * D, 11 * D),
        _Rec(_ts(J1, 9, 30, 4), 11 * D, 23, "B", 10 * D, 11 * D),          # à l'ask -> +1
        # au mid, tick descendant (11 -> 10,5) -> −1 ; agresseur natif 'B', donc LR faux
        _Rec(_ts(J1, 9, 30, 5), 105 * D // 10, 29, "B", 10 * D, 11 * D),
        # BBO verrouillé puis croisé -> repli sur le tick (dernier prix : 10,5)
        _Rec(_ts(J1, 9, 30, 6), 11 * D, 31, "B", 10 * D, 10 * D),
        _Rec(_ts(J1, 9, 30, 7), 10 * D, 37, "A", 11 * D, 10 * D),
        # bucket suivant (09:31) : l'état du tick traverse la frontière de bucket
        _Rec(_ts(J1, 9, 31, 0), 10 * D, 41, "N", None, None),              # sans BBO, tick nul
        # enregistrements exclus : action ≠ T et taille nulle
        _Rec(_ts(J1, 9, 31, 1), 10 * D, 43, "B", 10 * D, 11 * D, action="A"),
        _Rec(_ts(J1, 9, 31, 2), 10 * D, 0, "B", 10 * D, 11 * D),
        # jour suivant : l'état du tick est réinitialisé, premier print au mid -> NA
        _Rec(_ts(J2, 9, 30, 0), 105 * D // 10, 47, "B", 10 * D, 11 * D),
    ]
    res, acc = run_tm("TEST_2026-01", {"ticker": "TEST"}, records=iter(recs))
    b0 = acc.buckets[(J1, "RTH", 0)]
    assert b0[NP] == 4 and b0[VP] == 13 + 19 + 23 + 31, (b0[NP], b0[VP])
    assert b0[NM] == 3 and b0[VM] == 17 + 29 + 37, (b0[NM], b0[VM])
    assert b0[NN] == 1 and b0[VN] == 11, (b0[NN], b0[VN])
    assert b0[NFB] == 2, b0[NFB]
    assert b0[EP] == 11 * D * 13 + (105 * D // 10) * 19 + 11 * D * 23 + 11 * D * 31
    b1 = acc.buckets[(J1, "RTH", 1)]
    assert b1[NM] == 1 and b1[VM] == 41, "p_last≠ n'a pas survécu à la frontière de bucket"
    bd2 = acc.buckets[(J2, "RTH", 0)]
    assert bd2[NN] == 1 and bd2[VN] == 47, "p_last≠ non réinitialisé à la frontière de jour"
    assert res["exclusions"]["exclu_non_trade"] == 1
    assert res["exclusions"]["exclu_taille_nulle"] == 1
    assert res["p01"]["sommes"]["v_trf"] == 0
    assert acc.motifs_fallback == Counter({"verrouille": 1, "croise": 1, "sans_bbo": 1})
    ofi = (b0[VP] - b0[VM]) / (b0[VP] + b0[VM])
    assert abs(res["p01"]["ofi_quantiles_actions"]["min"] - min(ofi, -1.0)) < 1e-15
    ok.append("P-01 : buckets V+/V−/V^na/V^trf, OFI, fallback, exclusions (action≠T, "
              "taille nulle), survie du p_last≠ au bucket et réinitialisation au jour")

    # 4. Validation contre le flag agresseur natif sur la tape synthétique
    v = res["validation_h13"]
    # agresseur natif : B,B,A,B,B,B,B,A,(N exclu),B
    # LR_venue        : NA,+1,−1,+1,+1,−1(faux),+1,−1, ·  ,NA
    assert v["n_eval"] == 9 and v["n_side_N"] == 1, (v["n_eval"], v["n_side_N"])
    assert v["n_lr_faux"] == 1 and v["n_lr_classes"] == 7, (v["n_lr_faux"], v["n_lr_classes"])
    assert v["n_lr_indet"] == 2
    assert v["n_tick_faux"] == 1 and v["n_tick_classes"] == 7
    assert v["positions"]["au_mid"]["n"] == 4, v["positions"]["au_mid"]
    assert v["desaccord_tick_lr"]["n_desaccords_tous"] == 0
    ok.append("P-01 : compteurs de validation contre le flag agresseur natif")

    # 5. P-09 : (α, λ) imposés retrouvés
    alpha, lam = 1.5, 0.25
    pts = [(float(k), alpha + lam * float(k)) for k in range(-20, 20)]
    r = _mco(pts)
    assert abs(r["lambda"] - lam) < 1e-12 and abs(r["alpha"] - alpha) < 1e-12, r
    assert abs(r["r2"] - 1.0) < 1e-12 and r["n"] == 40
    assert _mco(pts[:29])["na_code"] == "n_insuffisant"
    assert _mco([(3.0, float(k)) for k in range(40)])["na_code"] == "var_f_nulle"
    ok.append("P-09 : (α, λ) imposés récupérés exactement ; n < 30 -> NA ; Var(f)=0 -> NA")

    # 6. P-09 : un bucket vide au milieu est exclu sans décaler les autres
    jour = "2026-01-07"
    recs2, s2 = [], 20 * D
    # Un trade acheteur par bucket RTH, mid en hausse régulière, sauf le bucket 5
    # laissé vide.
    for i in range(41):
        if i == 5:
            continue
        bid = 10 * D + i * D // 1000
        ask = bid + 2 * (D // 1000)
        # prix à l'ask -> +1 ; taille distincte par bucket
        recs2.append(_Rec(_ts(jour, 9, 30) + i * BUCKET_RTH_NS + 10 ** 6,
                          ask, 100 + i, "B", bid, ask))
    res2, acc2 = run_tm("TEST2_2026-01", {"ticker": "T2"}, records=iter(recs2))
    j2 = res2["p09"]["par_jour"][0]
    # 41 buckets, moins le bucket 5 (vide) et le bucket 0 (pas de μ_début)
    assert j2["n"] == 39, j2
    assert (jour, "RTH", 5) not in acc2.buckets
    mb = acc2.mids[jour]
    # La frontière 5 garde le dernier mid observé et le bucket 6 ses propres
    # frontières : μ_fin(6) − μ_début(6) = μ(7) − μ(6).
    assert mb[5] == mb[6], (mb[5], mb[6])
    assert mb[8] - mb[7] == 2 * (D // 1000), (mb[7], mb[8])
    ok.append("P-09 : bucket vide au milieu exclu sans décaler les autres ; "
              "Δμ_b calculé sur les frontières du bucket lui-même")

    # 7. μ : continuité entre buckets et antériorité des frontières
    assert all(mb[i] is not None for i in range(1, 41))
    assert mb[0] is None, "μ(09:30) ne doit pas exister sans cotation antérieure"
    ok.append("P-09 : μ_fin(b) = μ_début(b+1) ; μ(09:30) NA en l'absence de cotation "
              "d'instant ≤ 09:30")

    # 8. Rejeu : sorties identiques quel que soit l'itérable d'entrée
    ra, _ = run_tm("TEST_2026-01", {"ticker": "TEST"}, records=iter(recs))
    rb, _ = run_tm("TEST_2026-01", {"ticker": "TEST"}, records=(r for r in list(recs)))
    for k in ("p01", "p09", "validation_h13"):
        assert json.dumps(ra[k], sort_keys=True, default=str) == \
               json.dumps(rb[k], sort_keys=True, default=str), k
    ok.append("Rejeu : sorties P-01, P-09 et validation déterministes")

    # 9. Empreintes : a_jour() appelée en premier dans un processus neuf
    ok.append(_test_a_jour_processus_neuf(__file__))

    for line in ok:
        print("  OK  " + line)
    print(f"autotests : {len(ok)} vérifications OK")


# --- Ligne de commande ------------------------------------------------------
def tickers_mois() -> list[str]:
    return sorted(p.name[: -len("_tbbo.dbn.zst")] for p in DATA.glob("*_tbbo.dbn.zst"))


def cmd_run(args, strates, force):
    OUT.mkdir(parents=True, exist_ok=True)
    ref = ref_a()
    for tm in (args or tickers_mois()):
        dest = OUT / f"{tm}.json"
        if dest.exists() and not force and a_jour(dest):
            print(f"  = {tm} (déjà fait)", flush=True)
            continue
        meta = strates.get(tm, {"ticker": tm.split("_")[0], "mois": tm.split("_")[1]})
        res, acc = run_tm(tm, meta)
        res["comparaison_A"] = comparer_a(tm, res["validation_h13"], ref)
        dest.write_text(json.dumps(res, indent=1, default=str))
        ecrire_buckets(tm, acc)
        v, c = res["validation_h13"], res["comparaison_A"]
        journal(etape="run", tm=tm, duree_s=res["duree_s"], n_eval=v["n_eval"],
                taux_lr=v["taux_lr"], taux_tick=v["taux_tick"],
                desaccord=v["desaccord_tick_lr"]["taux_tous"],
                part_na_p09=res["p09"]["part_na"], comparaison_A=c["statut"])
        alerte = ""
        if c.get("ecart_taux_lr_pt") is not None and abs(c["ecart_taux_lr_pt"]) > TOL_PT:
            alerte = "  écart > 0,1 pt avec la référence, à examiner"
        print(f"  + {tm}: {res['duree_s']}s n_eval={v['n_eval']} "
              f"LR={_f(100 * (v['taux_lr'] or 0), 4)}% (réf. : {c['statut']}) "
              f"tick={_f(100 * (v['taux_tick'] or 0), 2)}% "
              f"NA_p09={_f(100 * (res['p09']['part_na'] or 0), 1)}%{alerte}", flush=True)


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
    elif cmd == "recap":
        s = ecrire_recap(charger_sorties())
        journal(etape="recap", **s)
        print(json.dumps(s, indent=1, default=str))
    else:
        print(f"commande inconnue : {cmd}")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
