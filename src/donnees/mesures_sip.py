#!/usr/bin/env python3
"""Mesures sur la tape SIP historique d'un titre-jour, via l'API Massive (ex-Polygon).

Télécharge tous les trades (v3/trades) et calcule, globalement et par session ET
(pré-ouverture, séance régulière, after-hours), la part du volume exécutée hors
bourse (TRF), à un prix sous-penny et en odd lots, ainsi que le décompte des
corrections. Le volume suit les règles `updates_volume` des conditions SIP, lues
dans `references/p2-conditions.json` ; les venues viennent de `p1-exchanges.json`.
La clé API est lue dans un fichier .env (MASSIVE_API_KEY). Bibliothèque standard seule.

  python3 donnees/mesures_sip.py TICKER AAAA-MM-JJ [--out fichier.json]
  python3 donnees/mesures_sip.py --selfcheck
"""

from __future__ import annotations

import argparse
import json
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, time as dtime, timezone
from decimal import Decimal
from pathlib import Path
from zoneinfo import ZoneInfo

API_BASE = "https://api.polygon.io"
CODE_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import chemins  # noqa: E402
REFERENCES_DIR = chemins.REFERENCES
ET = ZoneInfo("America/New_York")

# Deux entrées typées "TRF" par l'API sont des venues défuntes (6 = ISE Stocks,
# 16 = CBSX), fermées avant la période étudiée (2018 et après) : exclues des TRF.
DEFUNCT_TRF_IDS = {6, 16}

# Bornes de session en heure de l'Est (ET).
SESSION_BOUNDS = [
    ("premarket", dtime(4, 0), dtime(9, 30)),
    ("rth", dtime(9, 30), dtime(16, 0)),
    ("afterhours", dtime(16, 0), dtime(20, 0)),
]
SESSION_NAMES = [name for name, _, _ in SESSION_BOUNDS] + ["hors_session"]

TICK_HIGH = Decimal("0.01")
TICK_LOW = Decimal("0.0001")
ODD_LOT_CONDITION_ID = 37
STANDALONE_BUDGET_SECONDS = 300.0  # budget de temps de main() en appel direct


# Tables de référence des venues et des conditions, lues dans src/references/.

def load_json(path: Path) -> dict:
    with path.open(encoding="utf-8") as f:
        return json.load(f)


def load_active_trf_ids(p1: dict) -> dict[int, str]:
    """id -> nom pour les venues TRF actives (type=='TRF', défuntes exclues)."""
    out = {}
    for entry in p1["results"]:
        if entry.get("type") == "TRF" and entry["id"] not in DEFUNCT_TRF_IDS:
            out[entry["id"]] = entry.get("name", str(entry["id"]))
    return out


def load_exchange_names(p1: dict) -> dict[int, str]:
    return {e["id"]: e.get("name", str(e["id"])) for e in p1["results"]}


def load_volume_rules(p2: dict) -> dict[int, bool]:
    """id -> updates_volume (règle consolidée) pour chaque condition de trade.

    Retient toute condition dont `data_types` contient 'trade' et qui a des
    `update_rules`, quelle que soit sa catégorie. Les ids sont réutilisés d'une
    catégorie à l'autre (12 = « Form T » en vente, « Manual Bid and Ask » en
    cotation) ; le filtre sur 'trade' lève l'ambiguïté. Si deux entrées éligibles
    partagent encore un id, celui-ci est exclu de la table (traité comme inconnu)
    et signalé sur stderr.
    """
    candidates: dict[int, list[dict]] = {}
    for entry in p2["results"]:
        if "trade" not in entry.get("data_types", []):
            continue
        rules = entry.get("update_rules", {}).get("consolidated")
        if rules is None or "updates_volume" not in rules:
            continue
        candidates.setdefault(entry["id"], []).append(entry)

    out = {}
    for cid, entries in candidates.items():
        if len(entries) > 1:
            cats = [e.get("type") for e in entries]
            print(
                f"[mesures_sip] conflit id={cid} entre catégories {cats} "
                f"(data_types incluant 'trade' et update_rules dans chacune) : "
                f"id exclu de la table, traité comme condition inconnue.",
                file=sys.stderr,
            )
            continue
        out[cid] = entries[0]["update_rules"]["consolidated"]["updates_volume"]
    return out


# Classification par trade

def is_subpenny(price) -> bool:
    """Prix hors de l'échelon de cotation de la Rule 612 (0,01 $ au-dessus de 1 $,
    0,0001 $ en dessous), testé en Decimal pour éviter les erreurs de flottant."""
    d = Decimal(str(price))
    tick = TICK_HIGH if d >= 1 else TICK_LOW
    return d % tick != 0


def classify_session(sip_timestamp_ns: int) -> str:
    """sip_timestamp (ns Unix) -> session en heure ET, heure d'été comprise."""
    seconds = sip_timestamp_ns // 1_000_000_000
    dt_et = datetime.fromtimestamp(seconds, tz=timezone.utc).astimezone(ET)
    t = dt_et.time()
    for name, start, end in SESSION_BOUNDS:
        if start <= t < end:
            return name
    return "hors_session"


def is_hors_bourse(trade: dict) -> bool:
    return trade.get("trf_id") is not None


def is_odd_lot(trade: dict) -> bool:
    return ODD_LOT_CONDITION_ID in (trade.get("conditions") or [])


def counts_to_volume(trade: dict, volume_rules: dict[int, bool], unknown_ids: set[int]) -> bool:
    """Un trade compte au volume si et seulement si toutes ses conditions ont
    updates_volume vrai. Une condition absente de la table est supposée compter
    et son id est ajouté à `unknown_ids`."""
    for cid in trade.get("conditions") or []:
        rule = volume_rules.get(cid)
        if rule is None:
            unknown_ids.add(cid)
            continue
        if not rule:
            return False
    return True


# Téléchargement (v3/trades, pagination par next_url)

def build_first_url(ticker: str, date: str) -> str:
    q = urllib.parse.urlencode({"timestamp": date, "limit": 50000})
    return f"{API_BASE}/v3/trades/{ticker}?{q}"


def strip_apikey(url: str) -> str:
    """Retire un éventuel paramètre apiKey de l'URL (par exemple dans next_url) :
    l'authentification passe par l'en-tête Authorization."""
    parts = urllib.parse.urlsplit(url)
    q = [(k, v) for k, v in urllib.parse.parse_qsl(parts.query) if k.lower() != "apikey"]
    new_query = urllib.parse.urlencode(q)
    return urllib.parse.urlunsplit(parts._replace(query=new_query))


def fetch_page(url: str, api_key: str, deadline: float, max_retries: int = 5) -> dict:
    """Télécharge une page v3/trades, avec reprises et backoff exponentiel sur 429/5xx.

    `deadline` est une échéance absolue en `time.monotonic()`, fixée par l'appelant.
    Chaque tentative a un timeout de min(30 s, temps restant), et aucune tentative
    ni attente n'est entamée une fois l'échéance passée (TimeoutError) : le budget
    de l'appelant est donc respecté quel que soit le nombre de reprises."""
    url = strip_apikey(url)
    req = urllib.request.Request(url, headers={"Authorization": f"Bearer {api_key}"})
    delay = 1.0
    for attempt in range(max_retries):
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError(
                f"fetch_page: budget de temps épuisé avant la tentative "
                f"{attempt + 1}/{max_retries} ({url})"
            )
        try:
            with urllib.request.urlopen(req, timeout=min(30, remaining)) as resp:
                return json.loads(resp.read())
        except urllib.error.HTTPError as e:
            if e.code in (429, 500, 502, 503, 504) and attempt < max_retries - 1:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise TimeoutError(
                        f"fetch_page: budget de temps épuisé avant nouvelle "
                        f"tentative ({url})"
                    ) from e
                time.sleep(min(delay, remaining))
                delay *= 2
                continue
            raise
    raise RuntimeError("fetch_page: retries épuisés")  # pragma: no cover


def fetch_all_trades(ticker: str, date: str, api_key: str, deadline: float) -> list[dict]:
    trades: list[dict] = []
    url = build_first_url(ticker, date)
    page = 0
    while url:
        page += 1
        data = fetch_page(url, api_key, deadline)
        trades.extend(data.get("results") or [])
        print(f"[mesures_sip] page {page} : {len(trades)} trades cumulés", file=sys.stderr)
        url = data.get("next_url")
    return trades


# Agrégation

def new_bucket() -> dict:
    return {
        "n_trades": 0,
        "volume_total": 0,
        "hors_bourse_volume": 0,
        "hors_bourse_par_venue": {},
        "sous_penny_volume": 0,
        "sous_penny_par_tranche": {"ge_1": 0, "lt_1": 0},
        "sous_penny_par_venue": {},
        "odd_lot_volume": 0,
        "correction_par_valeur": {},
        "n_conditions_inconnues": 0,
        "conditions_inconnues_ids": set(),
    }


def accumulate(bucket: dict, trade: dict, volume_rules: dict[int, bool], active_trf_ids: dict[int, str]) -> None:
    bucket["n_trades"] += 1

    unknown = set()
    ok_volume = counts_to_volume(trade, volume_rules, unknown)
    bucket["conditions_inconnues_ids"] |= unknown
    bucket["n_conditions_inconnues"] += len(unknown)

    if "correction" in trade:
        key = str(trade["correction"])
        bucket["correction_par_valeur"][key] = bucket["correction_par_valeur"].get(key, 0) + 1

    if not ok_volume:
        # Le volume total est le dénominateur commun de toutes les parts : un trade
        # qui n'y compte pas n'entre dans aucun numérateur.
        return

    size = trade.get("size", 0)
    bucket["volume_total"] += size

    exch = trade.get("exchange")
    if is_hors_bourse(trade):
        bucket["hors_bourse_volume"] += size
        # La ventilation se fait par trf_id (202 = Carteret, etc.) : le champ exchange
        # vaut 4 (ADF) pour tous les prints hors bourse observés.
        trf = trade.get("trf_id")
        venue_key = str(trf) if trf in active_trf_ids else "autre"
        d = bucket["hors_bourse_par_venue"]
        d[venue_key] = d.get(venue_key, 0) + size

    if is_subpenny(trade["price"]):
        bucket["sous_penny_volume"] += size
        tranche = "ge_1" if Decimal(str(trade["price"])) >= 1 else "lt_1"
        bucket["sous_penny_par_tranche"][tranche] += size
        d = bucket["sous_penny_par_venue"]
        vkey = str(exch)
        d[vkey] = d.get(vkey, 0) + size

    if is_odd_lot(trade):
        bucket["odd_lot_volume"] += size


def _part(volume: int, total: int):
    return (volume / total) if total else None


def _venue_block(volumes: dict[str, int], total: int, exchange_names: dict[int, str]) -> dict:
    out = {}
    for k, v in volumes.items():
        name = None
        if k != "autre":
            name = exchange_names.get(int(k))
        out[k] = {"nom": name, "volume": v, "part": _part(v, total)}
    return out


def finalize(bucket: dict, exchange_names: dict[int, str]) -> dict:
    vt = bucket["volume_total"]
    return {
        "n_trades": bucket["n_trades"],
        "volume_total": vt,
        "hors_bourse": {
            "volume": bucket["hors_bourse_volume"],
            "part": _part(bucket["hors_bourse_volume"], vt),
            "par_venue": _venue_block(bucket["hors_bourse_par_venue"], vt, exchange_names),
        },
        "sous_penny": {
            "volume": bucket["sous_penny_volume"],
            "part": _part(bucket["sous_penny_volume"], vt),
            "par_tranche_prix": {
                k: {"volume": v, "part": _part(v, vt)}
                for k, v in bucket["sous_penny_par_tranche"].items()
            },
            "par_venue": _venue_block(bucket["sous_penny_par_venue"], vt, exchange_names),
        },
        "odd_lot": {
            "volume": bucket["odd_lot_volume"],
            "part": _part(bucket["odd_lot_volume"], vt),
        },
        "correction": {"n_trades_par_valeur": bucket["correction_par_valeur"]},
        "n_conditions_inconnues": bucket["n_conditions_inconnues"],
        "conditions_inconnues_ids": sorted(bucket["conditions_inconnues_ids"]),
    }


def compute_measures(trades: list[dict], volume_rules: dict[int, bool],
                      active_trf_ids: dict[int, str], exchange_names: dict[int, str],
                      ticker: str, date: str) -> dict:
    session_buckets = {name: new_bucket() for name in SESSION_NAMES}
    global_bucket = new_bucket()

    for trade in trades:
        session = classify_session(trade["sip_timestamp"])
        accumulate(session_buckets[session], trade, volume_rules, active_trf_ids)
        accumulate(global_bucket, trade, volume_rules, active_trf_ids)

    all_unknown = set()
    for b in list(session_buckets.values()) + [global_bucket]:
        all_unknown |= b["conditions_inconnues_ids"]
    if all_unknown:
        print(f"[mesures_sip] AVERTISSEMENT : ids de conditions inconnus rencontrés : "
              f"{sorted(all_unknown)}", file=sys.stderr)

    return {
        "ticker": ticker,
        "date": date,
        "n_trades_total": len(trades),
        "global": finalize(global_bucket, exchange_names),
        "par_session": {name: finalize(b, exchange_names) for name, b in session_buckets.items()},
    }


# Clé API, lue dans le premier .env trouvé depuis le répertoire courant ou ceux du script

def find_env_file() -> Path | None:
    candidates = [Path.cwd()] + list(CODE_DIR.parents)
    for d in candidates:
        p = d / ".env"
        if p.is_file():
            return p
    return None


def load_api_key(var_name: str = "MASSIVE_API_KEY") -> str:
    env_path = find_env_file()
    if env_path is None:
        raise RuntimeError(".env introuvable (recherché depuis le cwd et les parents du script)")
    with env_path.open(encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, _, v = line.partition("=")
            if k.strip() == var_name:
                return v.strip().strip('"').strip("'")
    raise RuntimeError(f"{var_name} introuvable dans {env_path}")


# Vérifications sans appel réseau

def selfcheck() -> None:
    p1 = load_json(REFERENCES_DIR / "p1-exchanges.json")
    p2 = load_json(REFERENCES_DIR / "p2-conditions.json")
    volume_rules = load_volume_rules(p2)
    active_trf = load_active_trf_ids(p1)

    # 1. sous-penny, de part et d'autre de 1 $
    assert is_subpenny(296.34) is False
    assert is_subpenny(296.3401) is True
    assert is_subpenny(0.9999) is False
    assert is_subpenny(0.99995) is True

    # 2. heure d'été : la même heure UTC (13:35) tombe dans deux sessions différentes
    ts_jan = int(datetime(2026, 1, 15, 13, 35, 0, tzinfo=timezone.utc).timestamp()) * 1_000_000_000
    ts_jul = int(datetime(2026, 7, 15, 13, 35, 0, tzinfo=timezone.utc).timestamp()) * 1_000_000_000
    assert classify_session(ts_jan) == "premarket"  # EST : 08:35 ET
    assert classify_session(ts_jul) == "rth"        # EDT : 09:35 ET
    assert classify_session(ts_jan) != classify_session(ts_jul)

    # 3. trade sans conditions compte au volume
    unknown: set[int] = set()
    t_sans_cond = {"price": 10.0, "size": 100}
    assert counts_to_volume(t_sans_cond, volume_rules, unknown) is True

    # 4. odd-lot (id 37) : compte au volume et classé odd_lot
    t_odd = {"price": 10.0, "size": 5, "conditions": [37]}
    assert counts_to_volume(t_odd, volume_rules, unknown) is True
    assert is_odd_lot(t_odd) is True

    # 5. condition volume=false exclut le trade (id 38 = Corrected Consolidated Close)
    t_excl = {"price": 10.0, "size": 5, "conditions": [38]}
    assert counts_to_volume(t_excl, volume_rules, unknown) is False

    # 6. trf_id présent : hors bourse, ventilé par trf_id
    t_trf = {"price": 10.0, "size": 5, "trf_id": 202, "exchange": 4}
    assert is_hors_bourse(t_trf) is True
    assert 4 in active_trf and 202 in active_trf  # table P1 cohérente
    b = new_bucket()
    accumulate(b, t_trf, volume_rules, active_trf)
    assert list(b["hors_bourse_par_venue"]) == ["202"]

    # 7. condition inconnue (id fictif) : comptée au volume et signalée
    unknown2: set[int] = set()
    t_inconnu = {"price": 10.0, "size": 5, "conditions": [999999]}
    assert counts_to_volume(t_inconnu, volume_rules, unknown2) is True
    assert 999999 in unknown2

    # 8. id 41 (Trade Thru Exempt, hors catégorie sale_condition) : présent dans la
    #    table et compté au volume par sa propre règle
    unknown3: set[int] = set()
    t_41 = {"price": 10.0, "size": 5, "conditions": [41]}
    assert 41 in volume_rules
    assert counts_to_volume(t_41, volume_rules, unknown3) is True
    assert 41 not in unknown3

    print("selfcheck : OK (8 groupes de vérifications)", file=sys.stderr)


# CLI

def main() -> None:
    parser = argparse.ArgumentParser(description="Mesures SIP par titre-jour.")
    parser.add_argument("ticker", nargs="?", help="Ticker (ex. AAPL)")
    parser.add_argument("date", nargs="?", help="Date YYYY-MM-DD")
    parser.add_argument("--out", help="Fichier de sortie JSON (défaut : stdout)")
    parser.add_argument("--selfcheck", action="store_true", help="Suite d'asserts, aucun appel réseau")
    args = parser.parse_args()

    if args.selfcheck:
        selfcheck()
        return

    if not args.ticker or not args.date:
        parser.error("ticker et date requis (sauf --selfcheck)")

    p1 = load_json(REFERENCES_DIR / "p1-exchanges.json")
    p2 = load_json(REFERENCES_DIR / "p2-conditions.json")
    volume_rules = load_volume_rules(p2)
    active_trf_ids = load_active_trf_ids(p1)
    exchange_names = load_exchange_names(p1)

    api_key = load_api_key()
    # En appel direct, aucun budget n'est imposé de l'extérieur : échéance large,
    # pour que fetch_page dispose toujours d'un temps restant fini.
    deadline = time.monotonic() + STANDALONE_BUDGET_SECONDS
    trades = fetch_all_trades(args.ticker, args.date, api_key, deadline)
    result = compute_measures(trades, volume_rules, active_trf_ids, exchange_names, args.ticker, args.date)

    output = json.dumps(result, indent=2, ensure_ascii=False)
    if args.out:
        Path(args.out).write_text(output, encoding="utf-8")
        print(f"[mesures_sip] écrit dans {args.out}", file=sys.stderr)
    else:
        print(output)


if __name__ == "__main__":
    main()
