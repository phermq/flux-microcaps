#!/usr/bin/env python3
"""Mesures sur l'échantillon order-by-order (MBO) Databento, décodé localement.

Lit les fichiers .dbn.zst (schémas tbbo, mbo, status) sans appel API et calcule,
par ticker-mois, trois mesures de microstructure :

  h13 [TICKER_MOIS ...]   exactitude des règles de signature (tick, cotation, Lee-Ready)
                          contre le côté agresseur natif du schéma tbbo
  h16 [TICKER_MOIS ...]   part du volume exécuté contre de la liquidité cachée (schéma mbo)
  h15 TICKER_MOIS         exploration du réapprovisionnement d'un niveau épuisé
  agg                     agrégation en CSV et empreintes SHA-256 des entrées
  --test                  autotests sur flux synthétiques (aucune donnée lue)

Les résultats intermédiaires sont des JSON par ticker-mois sous tests-mbo/.progress/.
Chacun porte le SHA-256 du présent fichier : un ticker-mois n'est sauté à la relance
que si ce code n'a pas changé (--force recalcule tout).
"""
from __future__ import annotations

import bisect
import csv
import hashlib
import json
import statistics
import sys
from collections import Counter, defaultdict, namedtuple
from datetime import datetime, time as dtime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import databento_dbn as dbn
import zstandard

# Chemins
CODE = Path(__file__).resolve()                     # haché pour l'empreinte de run
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import chemins  # noqa: E402
DATA = chemins.DONNEES / "mbo-echantillon"
OUT = chemins.SORTIES / "tests-mbo"
PROG = OUT / ".progress"
ECH = chemins.DONNEES / "echantillon-calibration" / "echantillon.csv"
DEVIS = chemins.DONNEES / "mbo-devis" / "devis_detail.csv"

UNDEF_PRICE = 9223372036854775807
NY = ZoneInfo("America/New_York")

# Fenêtres (ns) après l'ouverture, la clôture ou une reprise de halt dans lesquelles
# on cherche l'impression du cross. ITCH n'émet qu'un message Cross Trade par enchère :
# seule la plus grosse impression non appariée de la fenêtre est imputée au cross,
# les autres sont comptées comme cachées.
OPEN_WINDOW_NS = 5_000_000_000
CLOSE_WINDOW_NS = 15_000_000_000
HALT_WINDOW_NS = 5_000_000_000


# Utilitaires
def stream_records(path: Path):
    """Décode un .dbn.zst en flux, par blocs de 1 Mo."""
    dec = dbn.DBNDecoder()
    with open(path, "rb") as fh:
        with zstandard.ZstdDecompressor().stream_reader(fh) as reader:
            while True:
                chunk = reader.read(1 << 20)
                if not chunk:
                    break
                dec.write(chunk)
                for rec in dec.decode():
                    if isinstance(rec, dbn.Metadata):
                        continue
                    yield rec


class SessionCache:
    """Séance courante (heure ET) : bornes du jour calendaire, ouverture 9 h 30, clôture 16 h."""

    __slots__ = ("lo", "hi", "date", "open_ns", "close_ns")

    def __init__(self):
        self.lo, self.hi = 1, 0
        self.date = None
        self.open_ns = self.close_ns = 0

    def update(self, ts_ns: int) -> bool:
        """Retourne True si on a changé de séance."""
        if self.lo <= ts_ns < self.hi:
            return False
        d = datetime.fromtimestamp(ts_ns / 1e9, NY).date()
        self.lo = int(datetime.combine(d, dtime(0, 0), NY).timestamp()) * 1_000_000_000
        self.hi = int(
            datetime.combine(d + timedelta(days=1), dtime(0, 0), NY).timestamp()
        ) * 1_000_000_000
        self.open_ns = int(datetime.combine(d, dtime(9, 30), NY).timestamp()) * 1_000_000_000
        self.close_ns = int(datetime.combine(d, dtime(16, 0), NY).timestamp()) * 1_000_000_000
        self.date = d.isoformat()
        return True


def ticker_mois_list() -> list[str]:
    return sorted({p.name.rsplit("_", 1)[0] for p in DATA.glob("*.dbn.zst")})


def load_strates() -> dict[str, dict]:
    """ticker_mois -> strate de l'échantillon de calibration et dataset Databento."""
    out = {}
    with open(ECH) as fh:
        for row in csv.DictReader(fh):
            d = datetime.fromisoformat(row["date_t"]).date()
            mois = (d + timedelta(days=1)).strftime("%Y-%m")
            key = f"{row['ticker']}_{mois}"
            out[key] = {
                "ticker": row["ticker"],
                "mois": mois,
                "epoque": mois,
                "strate_cap": row["strate_cap"],
                "strate_prix": row["strate_prix"],
                "rang": row["rang_dans_strate"],
                "exchange": row["exchange"],
                "close_ref": float(row["close_ref"]),
                "cap_usd": float(row["cap_usd"]),
                "mediane_vol_mois": float(row["mediane_vol_mois"]),
            }
    with open(DEVIS) as fh:
        for row in csv.DictReader(fh):
            key = f"{row['ticker']}_{row['mois_start'][:7]}"
            if key in out:
                out[key]["dataset"] = row["dataset"]
    return out


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for blk in iter(lambda: fh.read(1 << 20), b""):
            h.update(blk)
    return h.hexdigest()


_CODE_SHA: list[str] = []


def code_sha256() -> str:
    """SHA-256 du présent fichier, calculé une fois par processus."""
    if not _CODE_SHA:
        _CODE_SHA.append(sha256(CODE))
    return _CODE_SHA[0]


def empreinte() -> dict:
    """Empreinte écrite dans chaque JSON de sortie : un JSON produit par une autre
    version du code est recalculé à la relance au lieu d'être réutilisé."""
    return {"code_sha256": code_sha256(),
            "date_run": datetime.now(NY).isoformat(timespec="seconds")}


def empreinte_a_jour(path: Path) -> bool:
    """True si `path` existe et porte l'empreinte de code du fichier courant."""
    try:
        prev = json.loads(path.read_text())
    except (OSError, ValueError):
        return False
    return isinstance(prev, dict) and \
        prev.get("fingerprint", {}).get("code_sha256") == code_sha256()


# Signature des transactions (schéma tbbo)
#
# Référence : le champ `side` du trade est le côté agresseur (convention Databento ;
# dans le schéma mbo, chaque T porte bien le côté opposé au F de l'ordre exécuté).
# Les trades sont répartis selon la position du prix par rapport au BBO.
BUCKETS = (
    "sans_bbo", "croise", "verrouille",
    "au_dessus_ask", "au_ask", "dedans_haut", "au_mid", "dedans_bas", "au_bid", "sous_bid",
)


def run_h13(tm: str, meta: dict) -> dict:
    """Taux d'erreur des règles du tick, de la cotation et de Lee-Ready contre le côté
    agresseur natif, ventilés par position du prix dans le BBO. Les trades de côté N
    (indéterminé) sont exclus de l'évaluation."""
    path = DATA / f"{tm}_tbbo.dbn.zst"
    sess = SessionCache()
    last_px = None          # dernier prix imprimé de la séance
    last_dir = None         # dernier tick non nul de la séance ('B'/'S')

    c = defaultdict(Counter)        # bucket -> compteurs
    tot = Counter()
    spreads_rel = []
    jours = set()
    n_par_jour = Counter()
    exemples_desaccord = []

    for rec in stream_records(path):
        ts = rec.ts_event
        if sess.update(ts):
            last_px = last_dir = None
        jours.add(sess.date)
        n_par_jour[sess.date] += 1

        px, sz = rec.price, rec.size
        side = str(rec.side)                 # 'B' (achat agresseur) / 'A' (vente) / 'N'
        lvl = rec.levels[0]
        bid, ask = lvl.bid_px, lvl.ask_px

        # Position du prix par rapport au BBO ; spread relatif mesuré en séance régulière.
        if bid == UNDEF_PRICE or ask == UNDEF_PRICE:
            bucket, quote_ok = "sans_bbo", False
        elif bid > ask:
            bucket, quote_ok = "croise", False
        elif bid == ask:
            bucket, quote_ok = "verrouille", False
        else:
            quote_ok = True
            s2 = bid + ask
            if px > ask:
                bucket = "au_dessus_ask"
            elif px == ask:
                bucket = "au_ask"
            elif px < bid:
                bucket = "sous_bid"
            elif px == bid:
                bucket = "au_bid"
            elif 2 * px > s2:
                bucket = "dedans_haut"
            elif 2 * px < s2:
                bucket = "dedans_bas"
            else:
                bucket = "au_mid"
            if rec.ts_event >= sess.open_ns and rec.ts_event < sess.close_ns:
                spreads_rel.append(2.0 * (ask - bid) / s2)

        # Règle du tick, évaluée avant la mise à jour de l'état ; un zero-tick reprend
        # la direction du dernier tick non nul de la séance.
        if last_px is None:
            tick = None
        elif px > last_px:
            tick = "B"
        elif px < last_px:
            tick = "S"
        else:
            tick = last_dir
        if last_px is not None and px != last_px:
            last_dir = "B" if px > last_px else "S"
        last_px = px

        # Règle de la cotation (comparaison au mid) ; Lee-Ready retombe sur le tick au mid
        # ou sans BBO exploitable.
        if quote_ok:
            s2 = bid + ask
            quote = "B" if 2 * px > s2 else ("S" if 2 * px < s2 else None)
        else:
            quote = None
        lr = quote if quote is not None else tick

        tot["n_total"] += 1
        tot["vol_total"] += sz
        if side == "N":
            tot["n_side_N"] += 1
            tot["vol_side_N"] += sz
            continue
        vrai = "B" if side == "B" else "S"
        tot["n_eval"] += 1
        tot["vol_eval"] += sz
        b = c[bucket]
        b["n"] += 1
        b["vol"] += sz
        if lr is None:
            b["lr_indet"] += 1
            tot["lr_indet"] += 1
        elif lr == vrai:
            b["lr_ok"] += 1
            tot["lr_ok"] += 1
            tot["vol_lr_ok"] += sz
        else:
            b["lr_faux"] += 1
            tot["lr_faux"] += 1
            tot["vol_lr_faux"] += sz
            if len(exemples_desaccord) < 3:
                exemples_desaccord.append(
                    {"ts_event": ts, "px": px / 1e9, "sz": sz, "side_natif": side,
                     "lee_ready": lr, "bid": None if bid == UNDEF_PRICE else bid / 1e9,
                     "ask": None if ask == UNDEF_PRICE else ask / 1e9, "bucket": bucket}
                )
        if tick is None:
            b["tick_indet"] += 1
            tot["tick_indet"] += 1
        elif tick == vrai:
            b["tick_ok"] += 1
            tot["tick_ok"] += 1
            tot["vol_tick_ok"] += sz
        else:
            b["tick_faux"] += 1
            tot["tick_faux"] += 1
            tot["vol_tick_faux"] += sz
        if quote is None:
            b["quote_indet"] += 1
            tot["quote_indet"] += 1

    res = {
        "ticker_mois": tm, **{k: v for k, v in meta.items()},
        "n_jours": len(jours),
        "trades_par_jour_median": statistics.median(n_par_jour.values()) if n_par_jour else 0,
        "spread_rel_median": statistics.median(spreads_rel) if spreads_rel else None,
        "spread_rel_n": len(spreads_rel),
        "totaux": dict(tot),
        "buckets": {k: dict(v) for k, v in c.items()},
        "exemples_desaccord": exemples_desaccord,
    }
    return res


# Part cachée (schéma mbo)
def _halt_resume_ts(tm: str) -> list[int]:
    """Instants de reprise de cotation après une suspension (schéma status)."""
    path = DATA / f"{tm}_status.dbn.zst"
    if not path.exists():
        return []
    events = []
    for rec in stream_records(path):
        events.append((rec.ts_event, str(rec.action)))
    out, halted = [], False
    for ts, act in events:
        a = act.split(".")[-1].split(":")[0]
        if a in ("PAUSE", "HALT", "SUSPEND"):
            halted = True
        elif a in ("TRADING", "QUOTING") and halted:
            out.append(ts)
            halted = False
    return out


def run_h16(tm: str, meta: dict, records=None, resumes=None) -> dict:
    """Part du volume exécuté contre de la liquidité cachée.

    Un T accompagné d'un F de même numéro de séquence exécute un ordre affiché ; un T
    de côté N sans F est une impression de cross s'il tombe dans une fenêtre d'enchère
    (ouverture, clôture, reprise de halt), sinon une exécution cachée. Compte aussi
    les motifs d'actions par groupe de séquence. `records` et `resumes` permettent
    d'injecter un flux synthétique (autotests).
    """
    path = DATA / f"{tm}_mbo.dbn.zst"
    sess = SessionCache()
    if resumes is None:
        resumes = _halt_resume_ts(tm)
    resumes_set = sorted(resumes)
    if records is None:
        records = stream_records(path)

    actions = Counter()
    tot = Counter()
    seq_pattern = Counter()
    exemples_caches = []
    exemples_affiches = []
    jours = set()

    buf: list = []          # enregistrements du groupe de séquence courant
    buf_seq = None
    # Candidats cross de la séance en cours : une fenêtre par séance pour l'ouverture et
    # la clôture, une fenêtre par reprise de halt (clé = instant de la reprise), afin
    # que chaque enchère de reprise d'une même séance ait son impression de cross.
    fen = {"open": [], "close": []}
    fen_halt: dict[int, list] = {}
    fenetres_halt_vues: set[int] = set()   # reprises ayant reçu au moins un candidat
    seance = [None]

    def in_halt_window(ts):
        """Instant de la reprise dont la fenêtre contient `ts`, sinon None.

        Si deux fenêtres se chevauchent, le candidat est rattaché à la reprise la plus
        récente, dont il est le plus plausiblement l'impression.
        """
        i = bisect.bisect_right(resumes_set, ts) - 1
        if i >= 0 and ts < resumes_set[i] + HALT_WINDOW_NS:
            return resumes_set[i]
        return None

    def compte_cache(ts, px, sz, side, oid, flags, has_fill, taille_groupe, rth):
        tot["n_cache"] += 1
        tot["vol_cache"] += sz
        if rth:
            tot["n_cache_rth"] += 1
            tot["vol_cache_rth"] += sz
        if px % 10_000_000:          # prix non multiple d'un cent
            tot["n_cache_sous_penny"] += 1
            tot["vol_cache_sous_penny"] += sz
        if len(exemples_caches) < 3:
            exemples_caches.append(
                {"ts_event": ts, "px": px / 1e9, "sz": sz, "side": side,
                 "order_id": oid, "flags": flags,
                 "F_dans_le_groupe": has_fill, "taille_groupe": taille_groupe})

    def imputer_fenetre(nom, cands):
        """Impute au cross la plus grosse impression de la fenêtre ; les autres sont cachées."""
        if not cands:
            return
        cands.sort(key=lambda x: -x[0])
        sz, ts, px, rth = cands[0]
        tot[f"n_cross_{nom}"] += 1
        tot[f"vol_cross_{nom}"] += sz
        for sz2, ts2, px2, rth2 in cands[1:]:
            compte_cache(ts2, px2, sz2, "N", 0, 0, False, 1, rth2)
        cands.clear()

    def solde_seance():
        """Arbitrage de fin de séance : une impression de cross par enchère (ouverture,
        clôture et chaque reprise de halt ayant reçu un candidat)."""
        for nom, cands in fen.items():
            imputer_fenetre(nom, cands)
        for r_ts in sorted(fen_halt):
            imputer_fenetre("halt", fen_halt[r_ts])
        fen_halt.clear()

    def flush(group):
        if not group:
            return
        has_fill = any(r[0] == "F" for r in group)
        for act, side, px, sz, oid, ts, flags in group:
            if act != "T":
                continue
            tot["n_T"] += 1
            tot["vol_T"] += sz
            if sess.update(ts):
                solde_seance()
            jours.add(sess.date)
            rth = sess.open_ns <= ts < sess.close_ns
            if has_fill:
                # exécution contre un ordre affiché du carnet
                tot["n_affiche"] += 1
                tot["vol_affiche"] += sz
                if rth:
                    tot["n_affiche_rth"] += 1
                    tot["vol_affiche_rth"] += sz
                if side == "N":
                    tot["anomalie_T_N_apparie"] += 1
                if len(exemples_affiches) < 3:
                    exemples_affiches.append(
                        {"ts_event": ts, "px": px / 1e9, "sz": sz, "side": side,
                         "order_id": oid, "flags": flags, "n_F_du_groupe":
                             sum(1 for r in group if r[0] == "F")})
                continue
            # T sans F : un côté B/A est une anomalie, exclue du ratio.
            if side != "N":
                tot["anomalie_T_AB_non_apparie"] += 1
                tot["vol_anomalie_T_AB_non_apparie"] += sz
                continue
            if oid != 0:
                tot["n_T_N_non_apparie_oid_non_nul"] += 1
                tot["vol_T_N_non_apparie_oid_non_nul"] += sz
            # Les candidats des fenêtres d'enchère sont arbitrés en fin de séance.
            do, dc = ts - sess.open_ns, ts - sess.close_ns
            if 0 <= do < OPEN_WINDOW_NS:
                fen["open"].append((sz, ts, px, rth))
            elif 0 <= dc < CLOSE_WINDOW_NS:
                fen["close"].append((sz, ts, px, rth))
            elif (r_ts := in_halt_window(ts)) is not None:
                fen_halt.setdefault(r_ts, []).append((sz, ts, px, rth))
                fenetres_halt_vues.add(r_ts)
            else:
                compte_cache(ts, px, sz, side, oid, flags, has_fill, len(group), rth)
        # Motifs d'actions par groupe, p. ex. pour voir si un remplacement apparaît en C+A.
        if len(group) > 1:
            acts = "".join(sorted({r[0] for r in group}))
            seq_pattern[acts] += 1

    for rec in records:
        act = str(rec.action)
        actions[act] += 1
        if act == "F":
            tot["n_F"] += 1
            tot["vol_F"] += rec.size
        seq = rec.sequence
        if seq != buf_seq:
            flush(buf)
            buf, buf_seq = [], seq
        buf.append((act, str(rec.side), rec.price, rec.size, rec.order_id,
                    rec.ts_event, int(rec.flags)))
    flush(buf)
    solde_seance()
    # Compté indépendamment de l'arbitrage, pour l'invariant n_cross_halt.
    tot["n_fenetres_halt_avec_candidat"] = len(fenetres_halt_vues)

    vol_cont = tot["vol_affiche"] + tot["vol_cache"]
    vol_cont_rth = tot["vol_affiche_rth"] + tot["vol_cache_rth"]
    res = {
        "ticker_mois": tm, **meta,
        "n_jours": len(jours),
        "actions": dict(actions),
        "motifs_sequence": dict(seq_pattern.most_common(12)),
        "totaux": dict(tot),
        "ratio_cache_vol": (tot["vol_cache"] / vol_cont) if vol_cont else None,
        "ratio_cache_vol_rth": (tot["vol_cache_rth"] / vol_cont_rth) if vol_cont_rth else None,
        "ratio_cache_nb": (tot["n_cache"] / (tot["n_affiche"] + tot["n_cache"]))
        if (tot["n_affiche"] + tot["n_cache"]) else None,
        "exemples_caches": exemples_caches,
        "exemples_affiches": exemples_affiches,
    }
    verifier_invariants_h16(res)
    return res


def verifier_invariants_h16(res: dict) -> None:
    """Invariants de comptage de run_h16 (AssertionError si violés).

    Chaque T, en nombre et en volume, tombe dans exactement une catégorie : affichée,
    cachée, cross (ouverture, clôture, halt) ou exclue. Il y a un cross de halt par
    fenêtre de reprise ayant reçu au moins un candidat.
    """
    t = res["totaux"]
    g = t.get
    # les compteurs d'exclusion ne suivent pas le préfixe n_/vol_
    exclus = {"n": "anomalie_T_AB_non_apparie", "vol": "vol_anomalie_T_AB_non_apparie"}
    for unite in ("n", "vol"):
        parts = (g(f"{unite}_affiche", 0) + g(f"{unite}_cache", 0)
                 + g(f"{unite}_cross_open", 0) + g(f"{unite}_cross_close", 0)
                 + g(f"{unite}_cross_halt", 0) + g(exclus[unite], 0))
        assert parts == g(f"{unite}_T", 0), (
            f"{res.get('ticker_mois')} : partition {unite} rompue "
            f"({parts} != {g(f'{unite}_T', 0)})")
    assert g("n_cross_halt", 0) == g("n_fenetres_halt_avec_candidat", 0), (
        f"{res.get('ticker_mois')} : {g('n_cross_halt', 0)} cross halt imputés pour "
        f"{g('n_fenetres_halt_avec_candidat', 0)} fenêtres de reprise avec candidat")


# Réapprovisionnement (schéma mbo)
def run_h15(tm: str, n_max: int = 12) -> dict:
    """Après l'exécution complète d'un ordre affiché, compte les nouveaux ordres posés
    au même prix et du même côté dans les 2 s, et si leur order id est le même."""
    path = DATA / f"{tm}_mbo.dbn.zst"
    live: dict[int, tuple] = {}          # order_id -> (side, price, size)
    events = []
    pending: list[tuple] = []            # (ts, side, price, order_id_epuise, taille)
    stats = Counter()
    WIN = 2_000_000_000                  # 2 s

    for rec in stream_records(path):
        act, oid = str(rec.action), rec.order_id
        ts, px, side, sz = rec.ts_event, rec.price, str(rec.side), rec.size
        if act == "A":
            live[oid] = (side, px, sz)
            for i, (t0, s0, p0, o0, z0) in enumerate(pending):
                if ts - t0 <= WIN and s0 == side and p0 == px:
                    stats["replenish_detecte"] += 1
                    stats["meme_order_id" if oid == o0 else "order_id_different"] += 1
                    if len(events) < n_max:
                        events.append({
                            "ts_epuisement": t0, "ts_reapparition": ts,
                            "delai_us": (ts - t0) / 1e3, "side": side, "px": px / 1e9,
                            "order_id_epuise": o0, "order_id_nouveau": oid,
                            "taille_epuisee": z0, "taille_nouvelle": sz,
                        })
                    pending.pop(i)
                    break
        elif act == "M":
            stats["action_M"] += 1
            live[oid] = (side, px, sz)
        elif act == "C":
            st = live.get(oid)
            if st and sz >= st[2]:
                live.pop(oid, None)
            elif st:
                live[oid] = (st[0], st[1], st[2] - sz)
        elif act == "F":
            st = live.get(oid)
            if st:
                rest = st[2] - sz
                if rest <= 0:
                    live.pop(oid, None)
                    stats["ordre_affiche_epuise"] += 1
                    pending.append((ts, st[0], st[1], oid, sz))
                    pending[:] = [p for p in pending if ts - p[0] <= WIN][-50:]
                else:
                    live[oid] = (st[0], st[1], rest)
        elif act == "R":
            live.clear()
    return {"ticker_mois": tm, "stats": dict(stats), "exemples": events}


# Agrégation
def _taux(num, den):
    return round(num / den, 6) if den else None


def aggregate():
    strates = load_strates()
    OUT.mkdir(parents=True, exist_ok=True)

    # Signature : une ligne par ticker-mois
    rows13 = []
    for f in sorted((PROG / "h13").glob("*.json")):
        r = json.loads(f.read_text())
        t = r["totaux"]
        bk = r["buckets"]
        mid = bk.get("au_mid", {})
        nomid = {k: sum(v.get(k, 0) for b, v in bk.items() if b != "au_mid")
                 for k in ("n", "lr_faux", "lr_ok", "tick_faux", "tick_ok", "lr_indet", "tick_indet")}
        n_lr = t.get("lr_ok", 0) + t.get("lr_faux", 0)
        n_tk = t.get("tick_ok", 0) + t.get("tick_faux", 0)
        rows13.append({
            "ticker_mois": r["ticker_mois"], "ticker": r["ticker"], "mois": r["mois"],
            "dataset": r.get("dataset", ""), "strate_cap": r["strate_cap"],
            "strate_prix": r["strate_prix"], "rang": r["rang"], "epoque": r["epoque"],
            "close_ref": r["close_ref"], "n_jours": r["n_jours"],
            "spread_rel_median": round(r["spread_rel_median"], 6) if r["spread_rel_median"] else None,
            "n_trades_total": t.get("n_total", 0), "vol_total": t.get("vol_total", 0),
            "n_side_N": t.get("n_side_N", 0), "vol_side_N": t.get("vol_side_N", 0),
            "part_side_N_nb": _taux(t.get("n_side_N", 0), t.get("n_total", 0)),
            "part_side_N_vol": _taux(t.get("vol_side_N", 0), t.get("vol_total", 0)),
            "n_eval": t.get("n_eval", 0), "vol_eval": t.get("vol_eval", 0),
            "n_lr_classes": n_lr, "n_lr_faux": t.get("lr_faux", 0),
            "taux_lr": _taux(t.get("lr_faux", 0), n_lr),
            "taux_lr_vol": _taux(t.get("vol_lr_faux", 0), t.get("vol_lr_faux", 0) + t.get("vol_lr_ok", 0)),
            "n_lr_indet": t.get("lr_indet", 0),
            "n_tick_classes": n_tk, "n_tick_faux": t.get("tick_faux", 0),
            "taux_tick": _taux(t.get("tick_faux", 0), n_tk),
            "taux_tick_vol": _taux(t.get("vol_tick_faux", 0), t.get("vol_tick_faux", 0) + t.get("vol_tick_ok", 0)),
            "n_tick_indet": t.get("tick_indet", 0),
            "n_au_mid": mid.get("n", 0),
            "part_au_mid": _taux(mid.get("n", 0), t.get("n_eval", 0)),
            "n_au_mid_classes": mid.get("lr_ok", 0) + mid.get("lr_faux", 0),
            "n_au_mid_faux_lr": mid.get("lr_faux", 0),
            "taux_lr_au_mid": _taux(mid.get("lr_faux", 0), mid.get("lr_ok", 0) + mid.get("lr_faux", 0)),
            "n_hors_mid": nomid["n"],
            "n_hors_mid_classes": nomid["lr_ok"] + nomid["lr_faux"],
            "n_hors_mid_faux_lr": nomid["lr_faux"],
            "taux_lr_hors_mid": _taux(nomid["lr_faux"], nomid["lr_ok"] + nomid["lr_faux"]),
            "n_sans_bbo": bk.get("sans_bbo", {}).get("n", 0),
            "n_croise": bk.get("croise", {}).get("n", 0),
            "n_verrouille": bk.get("verrouille", {}).get("n", 0),
            "n_au_bid": bk.get("au_bid", {}).get("n", 0),
            "n_au_ask": bk.get("au_ask", {}).get("n", 0),
            "n_dedans": bk.get("dedans_haut", {}).get("n", 0) + bk.get("dedans_bas", {}).get("n", 0),
        })
    rows13.sort(key=lambda r: (r["epoque"], r["strate_cap"], r["strate_prix"], r["rang"]))
    _write_csv(OUT / "h13_par_ticker_mois.csv", rows13)

    # Signature : taux poolés trade par trade, par strate
    def pool(rows, label, valeur):
        n_lr = sum(r["n_lr_classes"] for r in rows)
        n_tk = sum(r["n_tick_classes"] for r in rows)
        nmid = sum(r["n_au_mid"] for r in rows)
        nmid_lr = sum(r["n_au_mid_faux_lr"] for r in rows)
        nmid_cl = sum(r["n_au_mid_classes"] for r in rows)   # trades au mid effectivement classés
        taux_par_tm = [r["taux_lr"] for r in rows if r["taux_lr"] is not None]
        return {
            "dimension": label, "valeur": valeur, "n_ticker_mois": len(rows),
            "n_trades_total": sum(r["n_trades_total"] for r in rows),
            "n_side_N": sum(r["n_side_N"] for r in rows),
            "part_side_N_nb": _taux(sum(r["n_side_N"] for r in rows), sum(r["n_trades_total"] for r in rows)),
            "part_side_N_vol": _taux(sum(r["vol_side_N"] for r in rows), sum(r["vol_total"] for r in rows)),
            "n_eval": sum(r["n_eval"] for r in rows),
            "n_lr_classes": n_lr, "n_lr_faux": sum(r["n_lr_faux"] for r in rows),
            "taux_lr_poole": _taux(sum(r["n_lr_faux"] for r in rows), n_lr),
            "taux_lr_median_tm": round(statistics.median(taux_par_tm), 6) if taux_par_tm else None,
            "taux_lr_min_tm": round(min(taux_par_tm), 6) if taux_par_tm else None,
            "taux_lr_max_tm": round(max(taux_par_tm), 6) if taux_par_tm else None,
            "n_tick_classes": n_tk, "n_tick_faux": sum(r["n_tick_faux"] for r in rows),
            "taux_tick_poole": _taux(sum(r["n_tick_faux"] for r in rows), n_tk),
            "n_au_mid": nmid, "part_au_mid": _taux(nmid, sum(r["n_eval"] for r in rows)),
            "taux_lr_au_mid_poole": _taux(nmid_lr, nmid_cl),
            "taux_lr_hors_mid_poole": _taux(
                sum(r["n_lr_faux"] for r in rows) - nmid_lr, n_lr - nmid_cl),
        }

    agg = [pool(rows13, "global", "tous")]
    for dim, key in (("epoque", "epoque"), ("strate_cap", "strate_cap"),
                     ("strate_prix", "strate_prix"), ("dataset", "dataset")):
        for v in sorted({r[key] for r in rows13}):
            agg.append(pool([r for r in rows13 if r[key] == v], dim, v))
    for ep in sorted({r["epoque"] for r in rows13}):
        for cap in sorted({r["strate_cap"] for r in rows13}):
            for pr in sorted({r["strate_prix"] for r in rows13}):
                sub = [r for r in rows13 if r["epoque"] == ep and r["strate_cap"] == cap
                       and r["strate_prix"] == pr]
                if sub:
                    agg.append(pool(sub, "epoque x cap x prix", f"{ep} | {cap} | {pr}"))
    _write_csv(OUT / "h13_agrege.csv", agg)

    # Part cachée : une ligne par ticker-mois
    rows16 = []
    h13_by_tm = {r["ticker_mois"]: r for r in rows13}
    for f in sorted((PROG / "h16").glob("*.json")):
        r = json.loads(f.read_text())
        t = r["totaux"]
        h13 = h13_by_tm.get(r["ticker_mois"], {})
        rows16.append({
            "ticker_mois": r["ticker_mois"], "ticker": r["ticker"], "mois": r["mois"],
            "dataset": r.get("dataset", ""), "strate_cap": r["strate_cap"],
            "strate_prix": r["strate_prix"], "rang": r["rang"], "epoque": r["epoque"],
            "close_ref": r["close_ref"], "n_jours": r["n_jours"],
            "spread_rel_median": h13.get("spread_rel_median"),
            "vol_mediane_mois_crsp": None,
            "n_T": t.get("n_T", 0), "vol_T": t.get("vol_T", 0),
            "n_affiche": t.get("n_affiche", 0), "vol_affiche": t.get("vol_affiche", 0),
            "n_cache": t.get("n_cache", 0), "vol_cache": t.get("vol_cache", 0),
            "n_cross_open": t.get("n_cross_open", 0), "vol_cross_open": t.get("vol_cross_open", 0),
            "n_cross_close": t.get("n_cross_close", 0), "vol_cross_close": t.get("vol_cross_close", 0),
            "n_cross_halt": t.get("n_cross_halt", 0), "vol_cross_halt": t.get("vol_cross_halt", 0),
            "ratio_cache_vol": round(r["ratio_cache_vol"], 6) if r["ratio_cache_vol"] is not None else None,
            "ratio_cache_vol_rth": round(r["ratio_cache_vol_rth"], 6) if r["ratio_cache_vol_rth"] is not None else None,
            "ratio_cache_nb": round(r["ratio_cache_nb"], 6) if r["ratio_cache_nb"] is not None else None,
            "n_cache_sous_penny": t.get("n_cache_sous_penny", 0),
            "n_F_sans_T": t.get("n_F", 0) - t.get("n_affiche", 0),
            "anomalie_T_AB_non_apparie": t.get("anomalie_T_AB_non_apparie", 0),
            "anomalie_T_N_apparie": t.get("anomalie_T_N_apparie", 0),
            "n_T_N_non_apparie_oid_non_nul": t.get("n_T_N_non_apparie_oid_non_nul", 0),
            "action_M": r["actions"].get("M", 0),
            "action_A": r["actions"].get("A", 0),
            "action_C": r["actions"].get("C", 0),
        })
    rows16.sort(key=lambda r: (r["epoque"], r["strate_cap"], r["strate_prix"], r["rang"]))
    _write_csv(OUT / "h16_par_ticker_mois.csv", rows16)

    # Part cachée par strate (Nasdaq ITCH seul) et corrélations avec la liquidité
    itch = [r for r in rows16 if r["dataset"] == "XNAS.ITCH"]
    strate_rows = []

    def pool16(rows, label, valeur):
        vc = sum(r["vol_cache"] for r in rows)
        va = sum(r["vol_affiche"] for r in rows)
        ratios = [r["ratio_cache_vol"] for r in rows if r["ratio_cache_vol"] is not None]
        return {
            "dimension": label, "valeur": valeur, "n_ticker_mois": len(rows),
            "n_T": sum(r["n_T"] for r in rows),
            "vol_affiche": va, "vol_cache": vc,
            "ratio_cache_vol_poole": _taux(vc, va + vc),
            "ratio_median_tm": round(statistics.median(ratios), 6) if ratios else None,
            "ratio_min_tm": round(min(ratios), 6) if ratios else None,
            "ratio_max_tm": round(max(ratios), 6) if ratios else None,
        }

    strate_rows.append(pool16(itch, "global_ITCH", "tous"))
    for dim in ("epoque", "strate_cap", "strate_prix", "rang"):
        for v in sorted({r[dim] for r in itch}):
            strate_rows.append(pool16([r for r in itch if r[dim] == v], dim, v))
    for ep in sorted({r["epoque"] for r in itch}):
        for cap in sorted({r["strate_cap"] for r in itch}):
            for pr in sorted({r["strate_prix"] for r in itch}):
                sub = [r for r in itch if r["epoque"] == ep and r["strate_cap"] == cap
                       and r["strate_prix"] == pr]
                if sub:
                    strate_rows.append(pool16(sub, "epoque x cap x prix", f"{ep} | {cap} | {pr}"))
    # Spearman (rangs moyens en cas d'ex aequo) entre ratio caché et proxys de liquidité
    def spearman(xs, ys):
        n = len(xs)
        if n < 4:
            return None
        def ranks(v):
            order = sorted(range(n), key=lambda i: v[i])
            rk = [0.0] * n
            i = 0
            while i < n:
                j = i
                while j + 1 < n and v[order[j + 1]] == v[order[i]]:
                    j += 1
                moy = (i + j) / 2 + 1
                for k in range(i, j + 1):
                    rk[order[k]] = moy
                i = j + 1
            return rk
        rx, ry = ranks(xs), ranks(ys)
        mx, my = sum(rx) / n, sum(ry) / n
        num = sum((a - mx) * (b - my) for a, b in zip(rx, ry))
        den = (sum((a - mx) ** 2 for a in rx) * sum((b - my) ** 2 for b in ry)) ** 0.5
        return round(num / den, 4) if den else None

    for label, sel in (("ITCH tous", itch),
                       ("ITCH n_T>=1000", [r for r in itch if r["n_T"] >= 1000])):
        y = [r["ratio_cache_vol"] for r in sel]
        strate_rows.append({
            "dimension": "correlation_rang", "valeur": f"{label} : rho(ratio / volume echange)",
            "n_ticker_mois": len(sel), "n_T": sum(r["n_T"] for r in sel),
            "vol_affiche": None, "vol_cache": None,
            "ratio_cache_vol_poole": spearman([r["vol_affiche"] + r["vol_cache"] for r in sel], y),
            "ratio_median_tm": None, "ratio_min_tm": None, "ratio_max_tm": None,
        })
        sel2 = [r for r in sel if r["spread_rel_median"]]
        strate_rows.append({
            "dimension": "correlation_rang", "valeur": f"{label} : rho(ratio / spread relatif median)",
            "n_ticker_mois": len(sel2), "n_T": sum(r["n_T"] for r in sel2),
            "vol_affiche": None, "vol_cache": None,
            "ratio_cache_vol_poole": spearman([float(r["spread_rel_median"]) for r in sel2],
                                              [r["ratio_cache_vol"] for r in sel2]),
            "ratio_median_tm": None, "ratio_min_tm": None, "ratio_max_tm": None,
        })
    _write_csv(OUT / "h16_par_strate.csv", strate_rows)

    # empreintes des fichiers d'entrée utilisés
    manifest = OUT / "entrees_sha256.txt"
    used = set()
    for r in rows13:
        used.add(f"{r['ticker_mois']}_tbbo.dbn.zst")
    for r in rows16:
        used.add(f"{r['ticker_mois']}_mbo.dbn.zst")
        used.add(f"{r['ticker_mois']}_status.dbn.zst")
    with open(manifest, "w") as fh:
        for name in sorted(used):
            p = DATA / name
            if p.exists():
                fh.write(f"{sha256(p)}  {name}  {p.stat().st_size}\n")
    print(f"écrit : {OUT}/h13_par_ticker_mois.csv ({len(rows13)}), h13_agrege.csv ({len(agg)}), "
          f"h16_par_ticker_mois.csv ({len(rows16)}), entrees_sha256.txt ({len(used)})")


def _write_csv(path: Path, rows: list[dict]):
    if not rows:
        print(f"(rien à écrire pour {path.name})")
        return
    keys = list(rows[0].keys())
    with open(path, "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=keys)
        w.writeheader()
        w.writerows(rows)


# Autotests : flux MBO synthétiques injectés dans run_h16, sans lecture de données
_Rec = namedtuple("_Rec", "action side price size order_id ts_event flags sequence")

_PX = 1_000_000_000          # 1,00 $ en unités de 1e-9 $ (pas de sous-penny)
_META = {"ticker": "TEST", "mois": "2026-01"}


def _ts(jour: str, h: int, m: int, s: int = 0, ns: int = 0) -> int:
    d = datetime.fromisoformat(jour).date()
    return int(datetime.combine(d, dtime(h, m, s), NY).timestamp()) * 1_000_000_000 + ns


class _Flux:
    """Constructeur de flux MBO synthétique (numéros de séquence auto-incrémentés)."""

    def __init__(self):
        self.recs: list[_Rec] = []
        self.seq = 0

    def non_apparie(self, ts, sz, side="N"):
        """T seule dans son groupe : cross, cachée ou anomalie selon le côté et l'instant."""
        self.seq += 1
        self.recs.append(_Rec("T", side, _PX, sz, 0, ts, 0, self.seq))
        return self

    def affiche(self, ts, sz):
        """Groupe F+T de même séquence : exécution contre un ordre affiché."""
        self.seq += 1
        self.recs.append(_Rec("F", "B", _PX, sz, 4242, ts, 0, self.seq))
        self.recs.append(_Rec("T", "A", _PX, sz, 0, ts, 0, self.seq))
        return self


def _leve(fn) -> bool:
    try:
        fn()
    except AssertionError:
        return True
    return False


def autotests():
    import tempfile

    J1, J2 = "2026-01-02", "2026-01-05"

    # Deux reprises de halt la même séance : chacune reçoit son impression de cross.
    r1, r2 = _ts(J1, 10, 0), _ts(J1, 11, 0)
    r3 = _ts(J2, 14, 30)                                    # 2e séance : 1 reprise
    f = _Flux()
    f.non_apparie(r1 + 1_000_000_000, 5000)                 # principal fenêtre 1
    f.non_apparie(r1 + 2_000_000_000, 700)                  # secondaire fenêtre 1
    f.non_apparie(r2 + 500_000_000, 3000)                   # principal fenêtre 2
    f.non_apparie(r2 + 3_000_000_000, 200)                  # secondaire fenêtre 2
    f.non_apparie(_ts(J1, 12, 0), 42)                       # caché ordinaire
    f.affiche(_ts(J1, 12, 0, 1), 100)
    f.non_apparie(r3 + 1_000_000_000, 900)                  # principal fenêtre 3 (J2)
    f.non_apparie(r3 + 4_000_000_000, 30)                   # secondaire fenêtre 3
    res = run_h16("TEST_2026-01", _META, records=iter(f.recs), resumes=[r1, r2, r3])
    t = res["totaux"]
    assert t["n_cross_halt"] == 3, t                 # une imputation par reprise
    assert t["vol_cross_halt"] == 5000 + 3000 + 900, t
    assert t["n_cache"] == 4 and t["vol_cache"] == 700 + 200 + 42 + 30, t
    assert t["n_affiche"] == 1 and t["vol_affiche"] == 100, t
    assert t.get("n_cross_open", 0) == 0 and t.get("n_cross_close", 0) == 0, t
    # Le test distingue bien ce résultat d'une imputation unique par séance.
    ancien = {"n_cross_halt": 2, "vol_cross_halt": 5900,
              "n_cache": 5, "vol_cache": 3972}
    assert {k: t[k] for k in ancien} != ancien
    assert res["ratio_cache_vol"] == 972 / (972 + 100)

    # Reprises séparées de 2 s : un candidat dans les deux fenêtres va à la plus récente.
    a1 = _ts(J1, 10, 0)
    a2 = a1 + 2_000_000_000
    g = _Flux()
    g.non_apparie(a1 + 500_000_000, 100)                    # fenêtre a1 seule
    g.non_apparie(a2 + 500_000_000, 900)                    # a1 ∩ a2 -> a2
    g.non_apparie(a2 + 1_000_000_000, 50)                   # a1 ∩ a2 -> a2
    res_b = run_h16("TEST_2026-01", _META, records=iter(g.recs), resumes=[a1, a2])
    tb = res_b["totaux"]
    assert tb["n_cross_halt"] == 2 and tb["vol_cross_halt"] == 100 + 900, tb
    assert tb["n_cache"] == 1 and tb["vol_cache"] == 50, tb

    # Fenêtres d'ouverture et de clôture, et exclusion d'une T de côté B sans F.
    h = _Flux()
    h.non_apparie(_ts(J1, 9, 30, 0, 500_000_000), 2000)     # principal open
    h.non_apparie(_ts(J1, 9, 30, 1), 300)                   # secondaire open
    h.non_apparie(_ts(J1, 11, 0), 77, side="B")             # T B non appariée : exclue
    h.non_apparie(_ts(J1, 16, 0, 2), 1500)                  # principal close
    h.non_apparie(_ts(J1, 16, 0, 5), 60)                    # secondaire close
    res_c = run_h16("TEST_2026-01", _META, records=iter(h.recs), resumes=[])
    tc = res_c["totaux"]
    assert tc["n_cross_open"] == 1 and tc["vol_cross_open"] == 2000, tc
    assert tc["n_cross_close"] == 1 and tc["vol_cross_close"] == 1500, tc
    assert tc.get("n_cross_halt", 0) == 0 and tc["vol_cache"] == 360, tc
    assert tc["anomalie_T_AB_non_apparie"] == 1, tc
    assert tc["vol_anomalie_T_AB_non_apparie"] == 77, tc

    # Les invariants lèvent bien une erreur sur des totaux altérés.
    for casse in ("vol_cache", "n_cross_halt"):
        faux = json.loads(json.dumps(res))
        faux["totaux"][casse] += 1
        assert _leve(lambda: verifier_invariants_h16(faux)), casse
    faux = json.loads(json.dumps(res))
    faux["totaux"]["n_fenetres_halt_avec_candidat"] = 99
    assert _leve(lambda: verifier_invariants_h16(faux))

    # Empreinte de run et décision de reprise.
    emp = empreinte()
    assert set(emp) == {"code_sha256", "date_run"}
    assert emp["code_sha256"] == sha256(CODE) and len(emp["code_sha256"]) == 64
    with tempfile.TemporaryDirectory() as d:
        p = Path(d) / "x.json"
        p.write_text(json.dumps({"fingerprint": emp}))
        assert empreinte_a_jour(p)                          # empreinte courante -> saut
        p.write_text(json.dumps({"fingerprint": {"code_sha256": "0" * 64}}))
        assert not empreinte_a_jour(p)                      # code différent -> recalcul
        p.write_text(json.dumps({"totaux": {}}))
        assert not empreinte_a_jour(p)                      # JSON sans empreinte
        assert not empreinte_a_jour(Path(d) / "absent.json")

    print("autotests : OK")


# CLI
def main(argv):
    if "--test" in argv:
        autotests()
        return 0
    if not argv:
        print(__doc__)
        return 1
    cmd, args = argv[0], argv[1:]
    force = "--force" in args
    args = [a for a in args if not a.startswith("--")]
    strates = load_strates()

    if cmd == "agg":
        aggregate()
        return 0

    if cmd == "h15":
        for tm in args:
            res = run_h15(tm)
            res["fingerprint"] = empreinte()
            (PROG / "h15").mkdir(parents=True, exist_ok=True)
            (PROG / "h15" / f"{tm}.json").write_text(json.dumps(res, indent=1))
            print(tm, res["stats"])
        return 0

    if cmd not in ("h13", "h16"):
        print(f"commande inconnue : {cmd}")
        return 1

    dest = PROG / cmd
    dest.mkdir(parents=True, exist_ok=True)
    targets = args or ticker_mois_list()
    for tm in targets:
        out = dest / f"{tm}.json"
        if out.exists() and not force:
            if empreinte_a_jour(out):
                print(f"  = {tm} (déjà fait)")
                continue
            print(f"  ~ {tm} (empreinte de code absente ou obsolète -> recalcul)")
        meta = strates.get(tm, {"ticker": tm.split("_")[0], "mois": tm.split("_")[1]})
        fn = run_h13 if cmd == "h13" else run_h16
        res = fn(tm, meta)
        res["fingerprint"] = empreinte()
        out.write_text(json.dumps(res, indent=1, default=str))
        t = res["totaux"]
        if cmd == "h13":
            n = t.get("lr_ok", 0) + t.get("lr_faux", 0)
            print(f"  + {tm}: {t.get('n_total',0)} trades, N={t.get('n_side_N',0)}, "
                  f"eval={n}, taux_LR={t.get('lr_faux',0)/n:.4f}" if n else f"  + {tm}: aucun trade évaluable")
        else:
            print(f"  + {tm}: T={t.get('n_T',0)} affiche={t.get('n_affiche',0)} "
                  f"cache={t.get('n_cache',0)} ratio_vol={res['ratio_cache_vol']}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
