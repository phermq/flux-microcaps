# flux-microcaps

Order flow and liquidity states in US micro and nano caps.

I started this project with a simple question: when a small cap moves on heavy volume, can you tell from the tape *what kind* of flow is behind it? Forums are full of stories about whales, campaigns and hidden sellers. I wanted to see how much of that can actually be measured, and how much is narrative.
The short answer so far: a lot less than the stories suggest (not exactly big news, admittedly)

This started as a personal project. I'm sharing it in case it's useful to anyone working on similar questions, and because I'd be glad to hear from people who know this better than I do.

## What the data says about observability

Before building anything, I measured how much of the flow is visible at all. On a sample of 29 stock-months with full Nasdaq order-by-order data (ITCH, via Databento), matched against the consolidated tape:

- the median share of volume executed off-exchange (TRF) is 54%;
- once hidden executions observed on Nasdaq are added, about 60% of volume (median) trades with no displayed pre-trade trace. This is a lower bound, since hidden liquidity on other venues is not observed. The displayed book describes 40% of what actually trades, at best;
- the classic Lee-Ready trade classification works well on Nasdaq executions (1.3% error against the native aggressor side, over 612k trades), while the tick rule alone is wrong 18.8% of the time. The real problem is the TRF half of the volume, where there is no ground truth to check against.

This changed the design. Anything built only on the displayed book measures a minority of the activity, so the project describes market *states* rather than trying to identify *participants*.

## How it is organised

The structure comes from my training in mathematics, where you learn to define each object precisely before using it, state the assumptions, and never let a conclusion slip into its own definition.

Four layers, each one only allowed to read the one below:

1. **Primitives.** Objective measurements, each with a written definition, a unit and unit tests. No interpretation.
2. **Market states.** Persistent one-sided pressure, absorption (strong effort, weak result), liquidity vacuum, compression, halt regime, neutral. Defined from the primitives through an effort / result / reaction grid.
3. **Interpretations.** Statements about participants ("probable structural seller") live here, always as probabilities with an invalidation condition. They never feed decisions directly.
4. **Setups.** Locked until layers 1 and 2 are validated.

Only layer 1 is built for the moment. Layer 2 is the next step, and I'm currently working on it (while doing my degree so I don't have much time) so there are no trading signals and no backtests here for now.

## The primitives

| | Primitive | Source |
|---|---|---|
| P-01 | Signed flow and order flow imbalance on lit venues, plus off-exchange volume | SIP trades (tick test), venue BBO (Lee-Ready) |
| P-02 | Off-exchange (TRF) share | SIP trades |
| P-03 | Sub-penny prints | SIP trades |
| P-04 | Relative volume | SIP trades, CRSP |
| P-05 | Turnover of shares outstanding | SIP trades, EDGAR |
| P-06 | Fast replenishment of displayed liquidity | ITCH order-by-order |
| P-07 | Hidden vs displayed executions | ITCH order-by-order |
| P-08 | Relative displayed depth | ITCH order-by-order |
| P-09 | Contemporaneous slope between midquote and flow (local impact) | Databento TBBO |
| P-10 | Sweeps: intermarket sweep orders (P-10a) and multi-level bursts in the book (P-10b) | SIP, ITCH |
| P-12 | Halts, LULD pauses, short sale restriction, session regime | NYSE halt history, SIP |
| P-13 | Acceleration of activity | SIP trades |
| P-14 | Tape quality (out-of-sequence prints, corrections, unknown conditions) | SIP trades |
| P-15 | Share of volume with no displayed trace | ITCH, SIP |
| P-16 | Price displacement: net move, path length, efficiency, range | SIP trades |

P-11 (distance to LULD bands) was dropped: it cannot be computed without quotes, and the code for it is kept only for reference.

A few details I care about:

- Notional is kept in exact integers (units of $0.0001) and VWAP ratios go through a single correctly rounded division. Results do not depend on the order trades are read in, and a price that returns to its start gives a net move of exactly 0.
- A replay harness (`rejeu.py`) recomputes the primitives on a randomly permuted and split input and checks that the outputs match the original run.
- For P-16, I checked the tests against five deliberately broken versions of the code (auction prints not excluded, a trailing window that leaks one day of future, and so on). Each must make the tests fail. The first version of the tests let one through, so I added the case that catches it.

## A pre-registered check

P-16 measures the path a price takes during the day. Path length on trade prices is exposed to bid-ask bounce: a stock bouncing between bid and ask looks busy without going anywhere. Before computing anything, I fixed a rule: recompute the path on the midquote, and if the median ratio path(trades) / path(mid) is above 1.5, drop the measure.

On 597 stock-days (793k trades, run by `invalidation_p16.py` on the licensed sample), the median ratio is 0.98, so P-16 stays. The tail is real though (1.73 at the 95th percentile), so the time-weighted spread is a mandatory covariate wherever P-16 is used.

On the same sample, 77% of stock-days have an efficiency (net move / path) below 0.1 in absolute value, meaning that the intraday path is usually tens of times the net move. So "absorption" cannot be defined as low efficiency alone, since that is the normal case.

## Universe and corpus

The universe is rebuilt every month from May 2018 to December 2025, point-in-time: Nasdaq, NYSE and NYSE American primary listings, common stocks and ADSs, market cap under $300M, price at least $0.10, no activity filter.

One trap: CRSP (the Center for Research in Security Prices, at the University of Chicago, which is a huge academic database of US stock history) shares outstanding are dated at the end of the reporting period, not when the filing became public, which leaks future information (9 days at the median) and so shares outstanding come from EDGAR, dated by filing date, wherever they are available (about 69% of the universe). In total the universe covers about 4 million stock-days.

The case corpus is selected mechanically, by a rule written before looking at the data, to avoid cherry-picking episodes that happen to work. Any stock-day with relative volume of at least 5 opens a window from 10 trading days before to 20 after, and overlapping windows merge. This gives 37,615 episodes (37,972 with stock-specific halts added), stratified by period and price bucket.

## Repository layout

```
src/donnees/     data access: SIP tape, ITCH sample, universe construction
src/primitives/  layer 1 primitives, replay harness, universe runner
src/corpus/      episode corpus, audits, property tests
src/references/  reference tables: exchanges, SIP sale conditions, NYSE early closes
```

Module suffixes give the data source: `t1` is the SIP trade tape, `t3` the Nasdaq order book (ITCH), `t3q` the venue BBO. The code and comments are in French, which is the language I worked in but i will switch to English to make it easier to follow.

## Running it

Python 3.10 or later.

```
pip install -e ".[dev]"
python -m pytest -q
```

The 37 tests run offline in under a second, on synthetic inputs.

The market data (CRSP via WRDS, Databento, and Massive, formerly Polygon, for the SIP tape) is licensed and cannot be redistributed. The scripts that run on real data expect it under `data/` and write to `sorties/`; neither is in the repository, and the figures quoted above come from those runs.

## Contact

My name is Simon Chemama and I'm a first-year master's student (M1) in mathematics at Université Paris-Dauphine. 
Don't hesitate to contact me if you have questions or advice about my project.
[LinkedIn](https://www.linkedin.com/in/simon-chemama-4abb103a7/)
