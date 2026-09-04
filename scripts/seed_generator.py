#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import os
import random
import sys
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from typing import Dict, List, Optional, Tuple

import yaml

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DEFAULT_CONFIG = os.path.join(REPO_ROOT, "conf", "recon_config.yml")

# Answer-key classes (the shared contract)
MATCHED = "MATCHED"
AMOUNT_MISMATCH = "AMOUNT_MISMATCH"
MISSING_IN_PROCESSOR = "MISSING_IN_PROCESSOR"
MISSING_IN_BANK = "MISSING_IN_BANK"
DUPLICATE = "DUPLICATE"
TIMING_DIFFERENCE = "TIMING_DIFFERENCE"

LEG_PROCESSOR = "PROCESSOR"
LEG_BANK = "BANK"

INTERNAL_COLS = ["txn_uid", "business_date", "txn_ref", "amount_minor",
                 "currency", "side", "source_system", "event_ts"]
PROCESSOR_COLS = ["txn_uid", "business_date", "txn_ref", "gross_amount_minor",
                  "fee_minor", "currency", "side", "source_system", "event_ts"]
BANK_COLS = ["txn_uid", "business_date", "txn_ref", "net_amount_minor",
             "currency", "side", "source_system", "event_ts"]
ANSWER_KEY_COLS = ["txn_uid", "source_system", "leg", "expected_class",
                   "expected_counterpart_ref", "expects_hungarian",
                   "greedy_differs", "trap_group_id", "trap_assert",
                   "dense_block_id"]



def load_config(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as fh:
        return yaml.safe_load(fh)


def fee_minor(gross_minor: int, currency: str, cfg: dict) -> int:
    """Integer fee. floor(gross * rate_bps / 10000), capped. No float, ever."""
    sched = cfg["reference_data"]["fee_schedule"][currency]
    raw = (gross_minor * sched["rate_bps"]) // 10000
    return min(raw, sched["max_fee_minor"])


def total_tolerance(currency: str, cfg: dict) -> int:
    """max_fee + fee_jitter_max + fx_rounding + epsilon. The engine derives the
    identical value from reference_data on Day 3; both read this config block."""
    rd = cfg["reference_data"]
    return (rd["fee_schedule"][currency]["max_fee_minor"]
            + rd.get("fee_jitter_max", 0)
            + rd["fx_precision"][currency]["fx_rounding_minor"]
            + rd["epsilon_minor"])


def expected_net(gross_minor: int, currency: str, cfg: dict) -> int:
    """What the fee MODEL predicts the bank will deposit. The matcher can compute
    this too, from the same reference data — which is the point."""
    return gross_minor - fee_minor(gross_minor, currency, cfg)


def actual_fee(gross_minor: int, currency: str, cfg: dict, jitter: int) -> int:
    """What the processor ACTUALLY charged: the modelled fee plus settlement
    noise the model cannot predict (rounding, tiering, FX). The residual left
    over after the model is applied is exactly this jitter, and it is what makes
    tolerant matching a real problem instead of an arithmetic identity."""
    return fee_minor(gross_minor, currency, cfg) + jitter

@dataclass
class TruthTxn:
    seq: int
    merchant: str
    currency: str
    gross_minor: int
    biz_date: date
    event_ts: str
    txn_ref: str
    fate: str = MATCHED                 # perturbation applied to this txn
    bank_date_shift: int = 0            # days, TIMING_DIFFERENCE only
    corrupt_delta: int = 0              # minor units, AMOUNT_MISMATCH only
    trap_group_id: str = ""             # m2m trap membership
    dense_block_id: str = ""            # dense ambiguity block membership
    dense_role: str = ""                # BAND | CHAIN_A | CHAIN_B
    fee_jitter: int = 0                 # settlement noise on top of the modelled fee
    origin: str = "MAIN"                # MAIN | TRAP | DENSE


@dataclass
class Emitted:
    internal: List[dict] = field(default_factory=list)
    processor: List[dict] = field(default_factory=list)
    bank: List[dict] = field(default_factory=list)
    answer_key: List[dict] = field(default_factory=list)


def hex8(rng: random.Random) -> str:
    return "".join(rng.choice("0123456789abcdef") for _ in range(8))


def make_ref(merchant: str, rng: random.Random) -> str:
    """Merchant-prefixed. The first 4 chars ARE the merchant, which is what makes
    the Day-4 whale-skew demo physically real: blocking prefix == counterparty."""
    return f"{merchant}-{hex8(rng)}"


def make_event_ts(biz_date: date, rng: random.Random) -> str:
    secs = rng.randrange(0, 86400)
    return (datetime(biz_date.year, biz_date.month, biz_date.day)
            + timedelta(seconds=secs)).strftime("%Y-%m-%d %H:%M:%S")



def build_main_population(n: int, base_date: date, cfg: dict,
                          rng: random.Random) -> List[TruthTxn]:
    g = cfg["generator"]
    merchants = g["main_merchants"]
    currencies = g["currencies"]
    lo, hi = g["amount_min_minor"], g["amount_max_minor"]
    rates = g["rates"]

    # cumulative fate thresholds — order is fixed, so the draw is reproducible
    order = [(AMOUNT_MISMATCH, rates["amount_mismatch"]),
             (MISSING_IN_PROCESSOR, rates["missing_in_processor"]),
             (MISSING_IN_BANK, rates["missing_in_bank"]),
             (DUPLICATE, rates["duplicate"]),
             (TIMING_DIFFERENCE, rates["timing_difference"])]

    out: List[TruthTxn] = []
    for i in range(n):
        merchant = rng.choice(merchants)
        currency = rng.choice(currencies)
        gross = rng.randrange(lo, hi + 1)
        t = TruthTxn(seq=i, merchant=merchant, currency=currency,
                     gross_minor=gross, biz_date=base_date,
                     event_ts=make_event_ts(base_date, rng),
                     txn_ref=make_ref(merchant, rng))
        u = rng.random()
        acc = 0.0
        for fate, rate in order:
            acc += rate
            if u < acc:
                t.fate = fate
                break
        if t.fate == TIMING_DIFFERENCE:
            t.bank_date_shift = rng.choice([1, 2])
        t.fee_jitter = rng.randrange(0, int(cfg["reference_data"].get("fee_jitter_max", 0)) + 1)
        if t.fate == AMOUNT_MISMATCH:
            # MUST land beyond tolerance, or the exact/tolerant pass would match
            # it and the answer key would be wrong. Tolerance comes from the same
            # config the engine reads.
            tol = total_tolerance(currency, cfg)
            magnitude = tol + rng.randrange(100, 5000)
            sign = rng.choice([-1, 1])
            if sign < 0 and gross - magnitude < 1:
                sign = 1
            t.corrupt_delta = sign * magnitude
        out.append(t)
    return out


def build_trap_groups(n_groups: int, base_date: date, cfg: dict,
                      rng: random.Random, seq_start: int) -> List[TruthTxn]:
    """M:N traps: two internal transactions that are INDISTINGUISHABLE to the
    matcher (same merchant, currency, week, amount and date), each with a bank
    settlement of the same net amount.

    ORACLE HONESTY NOTE (deviation from the plan, documented in daily_log):
    the plan wanted the answer key to name 'the intended pairing'. It cannot.
    Two identical transactions and two identical settlements carry no signal
    that could distinguish them, so demanding a specific pairing would be an
    oracle asserting information the matcher provably does not have. The
    property that IS guaranteed — and the one finance actually needs — is:
    one-to-one, no fan-out, both matched, identical assignment across runs.
    That is what trap_assert=ONE_TO_ONE_STABLE encodes.
    """
    g = cfg["generator"]
    merchants = g["trap_merchants"]
    currencies = ["USD", "EUR"]
    out: List[TruthTxn] = []
    seq = seq_start
    for grp in range(n_groups):
        merchant = merchants[grp % len(merchants)]
        currency = currencies[(grp // len(merchants)) % len(currencies)]
        # spaced far apart so no candidate edges form BETWEEN groups
        gross = 20000 + 10000 * grp
        gid = f"TRAP-{grp:03d}"
        # ONE jitter for the whole group: if the two members had different
        # settlement noise the matcher could tell them apart and the trap would
        # stop being a trap.
        jitter = rng.randrange(0, int(cfg["reference_data"].get("fee_jitter_max", 0)) + 1)
        for _ in range(2):
            out.append(TruthTxn(seq=seq, merchant=merchant, currency=currency,
                                gross_minor=gross, biz_date=base_date,
                                event_ts=make_event_ts(base_date, rng),
                                txn_ref=make_ref(merchant, rng),
                                trap_group_id=gid, origin="TRAP",
                                fee_jitter=jitter))
            seq += 1
    return out


def build_dense_blocks(n_blocks: int, base_date: date, cfg: dict,
                       rng: random.Random, seq_start: int) -> List[TruthTxn]:
    """Blocks where GREEDY PROVABLY LOSES to the optimal assignment.

    The matcher ranks bank-leg candidates on what the FEE MODEL CANNOT EXPLAIN:

        residual(i, j) = | bank_net_j - (gross_i - modelled_fee(gross_i)) |

    For a true pair that residual is exactly the settlement jitter. So ambiguity
    is only possible where the jitter is large enough to make a wrong pair look
    better than a right one, and that is what these blocks construct.

      band  — six transactions spaced 150 minor units apart. Cross-pair
              residuals are ~150 while true-pair residuals are the jitter (<= 8),
              so greedy and the optimal assignment agree here. The band exists
              only to push candidate density above the threshold.

      chain — two transactions, A and B, both carrying the maximum jitter J.
              B sits DELTA minor units above A, chosen so that:

                residual(A, b) = |DELTA - J|   < J = residual(A, a)
                    -> the wrong edge outscores both true edges, so greedy
                       takes it first
                raw(B, a) = DELTA + max_fee + J > total_tolerance
                    -> B's own settlement is not even a candidate for it once
                       a is gone, so greedy strands B and ends the block a pair
                       short
                1/(1+J) + 1/(1+J) > 1/(1+|DELTA-J|)
                    -> the optimal assignment, which is the truth, wins. A
                       non-candidate cell costs a prohibitive 10.0 against at
                       most 1.0 for any real edge, so the assignment maximises
                       matched pairs first and only then minimises cost: two
                       true pairs beat one wrong pair plus a stranded record

    Every one of those is asserted below, and then the whole block is re-checked
    against scipy in verify_dense_blocks() before a single CSV is written.
    """
    g = cfg["generator"]
    merchants = g["dense_merchants"]
    currency = "USD"
    jitter_max = int(cfg["reference_data"].get("fee_jitter_max", 0))
    out: List[TruthTxn] = []
    seq = seq_start
    for b in range(n_blocks):
        merchant = merchants[b % len(merchants)]
        bid = f"DENSE-{b:02d}"
        tol = total_tolerance(currency, cfg)
        max_fee = cfg["reference_data"]["fee_schedule"][currency]["max_fee_minor"]

        spacing = 150
        band_base = 100000                      # large enough that the fee caps
        assert fee_minor(band_base, currency, cfg) == max_fee, \
            "band amounts must sit above the fee cap so cross residuals are the spacing"
        for k in range(6):
            gross = band_base + spacing * k
            out.append(TruthTxn(seq=seq, merchant=merchant, currency=currency,
                                gross_minor=gross, biz_date=base_date,
                                event_ts=make_event_ts(base_date, rng),
                                txn_ref=make_ref(merchant, rng),
                                dense_block_id=bid, dense_role="BAND",
                                origin="DENSE", fee_jitter=k % 3))
            seq += 1

        j = jitter_max
        delta = 2 * j - 3                       # -> residual(A,b) = |delta-j| = j-3
        g_a = 200000
        g_b = g_a + delta
        assert fee_minor(g_a, currency, cfg) == max_fee, "chain must sit above the cap"
        r_true = j                              # residual(A,a) = residual(B,b) = j
        r_wrong = abs(delta - j)                # residual(A,b)
        raw_true = max_fee + j                  # |gross - net| for a true pair
        raw_ba = delta + max_fee + j            # |gross_B - net_a|
        assert r_wrong < r_true, "greedy would not be tempted by the wrong edge"
        assert raw_true <= tol, "a true pair would fall outside tolerance"
        assert raw_ba > tol, "B would keep a second candidate and never be stranded"
        assert 2.0 / (1 + r_true) > 1.0 / (1 + r_wrong), \
            "the optimal assignment would not beat greedy"
        for role, gross in (("CHAIN_A", g_a), ("CHAIN_B", g_b)):
            out.append(TruthTxn(seq=seq, merchant=merchant, currency=currency,
                                gross_minor=gross, biz_date=base_date,
                                event_ts=make_event_ts(base_date, rng),
                                txn_ref=make_ref(merchant, rng),
                                dense_block_id=bid, dense_role=role,
                                origin="DENSE", fee_jitter=j))
            seq += 1
    return out



def derive(truth: List[TruthTxn], cfg: dict, rng: random.Random) -> Emitted:
    em = Emitted()
    bank_ref_mode = cfg["generator"]["bank_ref_mode"]
    blind_rate = float(cfg["generator"].get("bank_ref_blind_rate", 0.0))

    for t in truth:
        iu = f"INT-{t.seq:07d}"
        pu = f"PRC-{t.seq:07d}"
        bu = f"BNK-{t.seq:07d}"
        fee = actual_fee(t.gross_minor, t.currency, cfg, t.fee_jitter)
        net = t.gross_minor - fee
        bank_date = t.biz_date + timedelta(days=t.bank_date_shift)
        # Ref blinding. Real bank statements often carry a settlement/batch
        # reference rather than the processor's transaction id, so a share of
        # bank rows cannot be matched by ref at all and must be reconciled on
        # economic attributes (amount + date). That population is where the
        # tolerant matcher, the m:n traps and the Hungarian fallback live.
        # Trap and dense-block rows are ALWAYS blinded: if they kept the ref,
        # the ref-anchored pass would resolve them and the demo would prove
        # nothing. The merchant prefix is preserved so blocking still works.
        blind = (bank_ref_mode == "batch"
                 or bool(t.trap_group_id) or bool(t.dense_block_id)
                 or rng.random() < blind_rate)
        bank_ref = (f"{t.merchant}-B{hex8(rng)[:7]}" if blind else t.txn_ref)

        em.internal.append({
            "txn_uid": iu, "business_date": t.biz_date.isoformat(),
            "txn_ref": t.txn_ref, "amount_minor": str(t.gross_minor),
            "currency": t.currency, "side": "DEBIT",
            "source_system": "INTERNAL", "event_ts": t.event_ts})

        proc_class = MATCHED
        if t.fate == MISSING_IN_PROCESSOR:
            proc_class = MISSING_IN_PROCESSOR
        else:
            amt = t.gross_minor + (t.corrupt_delta if t.fate == AMOUNT_MISMATCH else 0)
            if t.fate == AMOUNT_MISMATCH:
                proc_class = AMOUNT_MISMATCH
            em.processor.append({
                "txn_uid": pu, "business_date": t.biz_date.isoformat(),
                "txn_ref": t.txn_ref, "gross_amount_minor": str(amt),
                "fee_minor": str(fee), "currency": t.currency, "side": "DEBIT",
                "source_system": "PROCESSOR", "event_ts": t.event_ts})
            em.answer_key.append(ak(pu, "PROCESSOR", LEG_PROCESSOR, proc_class,
                                    t.txn_ref, t))
            if t.fate == DUPLICATE:
                # same txn_ref, DISTINCT txn_uid, so the key can address the copy
                em.processor.append({
                    "txn_uid": f"{pu}-D1", "business_date": t.biz_date.isoformat(),
                    "txn_ref": t.txn_ref, "gross_amount_minor": str(amt),
                    "fee_minor": str(fee), "currency": t.currency, "side": "DEBIT",
                    "source_system": "PROCESSOR", "event_ts": t.event_ts})
                em.answer_key.append(ak(f"{pu}-D1", "PROCESSOR", LEG_PROCESSOR,
                                        DUPLICATE, t.txn_ref, t))
        em.answer_key.append(ak(iu, "INTERNAL", LEG_PROCESSOR, proc_class,
                                t.txn_ref, t))

        if t.fate == MISSING_IN_BANK:
            bank_class = MISSING_IN_BANK
        else:
            bank_class = TIMING_DIFFERENCE if t.bank_date_shift else MATCHED
            em.bank.append({
                "txn_uid": bu, "business_date": bank_date.isoformat(),
                "txn_ref": bank_ref, "net_amount_minor": str(net),
                "currency": t.currency, "side": "CREDIT",
                "source_system": "BANK", "event_ts": make_event_ts(bank_date, rng)})
            em.answer_key.append(ak(bu, "BANK", LEG_BANK, bank_class,
                                    t.txn_ref, t))
        em.answer_key.append(ak(iu, "INTERNAL", LEG_BANK, bank_class,
                                bank_ref, t))
    return em


def ak(txn_uid: str, source: str, leg: str, klass: str,
       counterpart_ref: str, t: TruthTxn) -> dict:
    """One answer-key row.

    DAY-1 DECISION #1: the key is grained by (source_system, txn_uid, LEG).
    An internal row participates in BOTH legs and therefore has two expected
    states. The plan's one-row-per-(source, txn_uid) contract cannot express
    that, and the same grain decision governs the output table: one row per
    (source_system, txn_uid, leg). Without it, control totals double-count the
    internal spine and can never balance.
    """
    expects_hungarian = bool(t.dense_block_id) and leg == LEG_BANK
    greedy_differs = t.dense_role.startswith("CHAIN") and leg == LEG_BANK
    return {
        "txn_uid": txn_uid, "source_system": source, "leg": leg,
        "expected_class": klass, "expected_counterpart_ref": counterpart_ref,
        "expects_hungarian": "true" if expects_hungarian else "false",
        "greedy_differs": "true" if greedy_differs else "false",
        "trap_group_id": t.trap_group_id,
        "trap_assert": "ONE_TO_ONE_STABLE" if t.trap_group_id else "",
        "dense_block_id": t.dense_block_id,
    }


def _greedy(edges: List[Tuple[str, str, int]]) -> List[Tuple[str, str]]:
    """Same rule as spark/recon/resolver.py: score desc, then BOTH refs asc,
    stable sort, first-come-first-served."""
    ranked = sorted(edges, key=lambda e: (-(1.0 / (1 + e[2])), e[0], e[1]))
    tl, tr, keep = set(), set(), []
    for l, r, _ in ranked:
        if l in tl or r in tr:
            continue
        tl.add(l); tr.add(r); keep.append((l, r))
    return sorted(keep)


def _optimal(edges: List[Tuple[str, str, int]]) -> List[Tuple[str, str]]:
    import numpy as np
    from scipy.optimize import linear_sum_assignment
    ls = sorted({e[0] for e in edges})
    rs = sorted({e[1] for e in edges})
    li = {u: i for i, u in enumerate(ls)}
    ri = {u: i for i, u in enumerate(rs)}
    cost = np.full((len(ls), len(rs)), 10.0)
    for l, r, d in edges:
        cost[li[l], ri[r]] = 1.0 - (1.0 / (1 + d))
    rows, cols = linear_sum_assignment(cost)
    return sorted((ls[i], rs[j]) for i, j in zip(rows, cols) if cost[i, j] < 10.0)


def verify_dense_blocks(truth: List[TruthTxn], cfg: dict) -> List[dict]:
    """Rebuild the exact candidate set the engine will build for each dense
    block — raw-diff FILTER, fee-residual SCORE — then assert: density above the
    threshold, optimal == truth, greedy != truth. A demo is only a demo if
    greedy loses, so the generator refuses to emit data where it doesn't."""
    threshold = cfg["matching"]["ambiguity_density_threshold"]
    report = []
    blocks: Dict[str, List[TruthTxn]] = {}
    for t in truth:
        if t.dense_block_id:
            blocks.setdefault(t.dense_block_id, []).append(t)

    for bid, rows in sorted(blocks.items()):
        ccy = rows[0].currency
        tol = total_tolerance(ccy, cfg)
        internals = [(t.txn_ref, t.gross_minor, expected_net(t.gross_minor, ccy, cfg))
                     for t in rows]
        banks = [(t.txn_ref,
                  t.gross_minor - actual_fee(t.gross_minor, ccy, cfg, t.fee_jitter))
                 for t in rows]
        edges = []
        for lr, lg, len_ in internals:
            for rr, rn in banks:
                if abs(lg - rn) <= tol:                 # FILTER: raw gross/net gap
                    edges.append((lr, rr, abs(rn - len_)))  # SCORE: fee residual
        density = len(edges) / max(min(len(internals), len(banks)), 1)
        truth_pairs = sorted((r, r) for r, _, _ in internals)
        g = _greedy(edges)
        o = _optimal(edges)
        assert density > threshold, f"{bid}: density {density} <= {threshold}"
        assert o == truth_pairs, f"{bid}: optimal assignment != truth"
        assert g != truth_pairs, f"{bid}: greedy already gets it right — not a trap"
        report.append({"block": bid, "density": round(density, 3),
                       "edges": len(edges), "records": len(internals),
                       "greedy_pairs": len(g), "optimal_pairs": len(o)})
    return report


def verify_traps(truth: List[TruthTxn], cfg: dict) -> List[dict]:
    """Each trap group must present exactly 2x2 candidates, all four within
    tolerance and all four carrying an IDENTICAL score, and must not trip the
    ambiguity threshold (density is exactly 2.0, and the threshold is a strict
    greater-than). Identical scores are the point: the group is decided by the
    tie-break rule, not by the data."""
    threshold = cfg["matching"]["ambiguity_density_threshold"]
    groups: Dict[str, List[TruthTxn]] = {}
    for t in truth:
        if t.trap_group_id:
            groups.setdefault(t.trap_group_id, []).append(t)
    report = []
    for gid, rows in sorted(groups.items()):
        assert len(rows) == 2, f"{gid}: expected 2 internal rows"
        ccy = rows[0].currency
        tol = total_tolerance(ccy, cfg)
        scores = set()
        edges = 0
        for a in rows:
            for b in rows:
                net = b.gross_minor - actual_fee(b.gross_minor, ccy, cfg, b.fee_jitter)
                if abs(a.gross_minor - net) <= tol:
                    edges += 1
                    scores.add(abs(net - expected_net(a.gross_minor, ccy, cfg)))
        assert edges == 4, f"{gid}: expected 4 candidate edges, got {edges}"
        assert len(scores) == 1, f"{gid}: members are distinguishable — not a trap"
        density = edges / 2
        assert not density > threshold, f"{gid}: would trip Hungarian"
        report.append({"group": gid, "edges": edges, "density": density})
    return report



def write_csv(path: str, cols: List[str], rows: List[dict]) -> None:
    with open(path, "w", newline="\n", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=cols, lineterminator="\n")
        w.writeheader()
        for r in rows:
            w.writerow(r)


def control_line(em: Emitted) -> dict:
    out: Dict[str, Dict[str, dict]] = {}
    for name, rows, amt_col in (("internal", em.internal, "amount_minor"),
                                ("processor", em.processor, "gross_amount_minor"),
                                ("bank", em.bank, "net_amount_minor")):
        per: Dict[str, dict] = {}
        for r in rows:
            c = per.setdefault(r["currency"], {"rows": 0, "sum_amount_minor": 0})
            c["rows"] += 1
            c["sum_amount_minor"] += int(r[amt_col])
        out[name] = {k: per[k] for k in sorted(per)}
    return out


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description="Deterministic reconciliation oracle")
    p.add_argument("--date", required=True)
    p.add_argument("--rows", type=int, required=True)
    p.add_argument("--seed", type=int, required=True)
    p.add_argument("--out", required=True)
    p.add_argument("--config", default=DEFAULT_CONFIG)
    p.add_argument("--trap-groups", type=int, default=None)
    p.add_argument("--dense-blocks", type=int, default=None)
    p.add_argument("--whale", default=None, help="merchant to overweight (Day 4)")
    p.add_argument("--whale-share", type=float, default=0.0)
    a = p.parse_args(argv)

    cfg = load_config(a.config)
    if a.whale:                       # Day-4 skew injection reuses this generator
        others = [m for m in cfg["generator"]["main_merchants"] if m != a.whale]
        s = min(max(a.whale_share, 0.0), 0.95)
        copies = max(1, int(round(s * len(others) / (1.0 - s)))) if s > 0 else 1
        cfg["generator"]["main_merchants"] = [a.whale] * copies + others
    n_traps = a.trap_groups if a.trap_groups is not None else cfg["generator"]["m2m_trap_groups"]
    n_dense = a.dense_blocks if a.dense_blocks is not None else cfg["generator"]["dense_blocks"]

    base_date = date.fromisoformat(a.date)
    rng = random.Random(a.seed)

    special = n_traps * 2 + n_dense * 8
    if a.rows <= special:
        print(f"ERROR: --rows {a.rows} must exceed {special} reserved trap/dense rows",
              file=sys.stderr)
        return 2

    truth = build_main_population(a.rows - special, base_date, cfg, rng)
    truth += build_trap_groups(n_traps, base_date, cfg, rng, seq_start=len(truth))
    truth += build_dense_blocks(n_dense, base_date, cfg, rng, seq_start=len(truth))

    dense_report = verify_dense_blocks(truth, cfg)
    trap_report = verify_traps(truth, cfg)

    em = derive(truth, cfg, rng)

    os.makedirs(a.out, exist_ok=True)
    em.internal.sort(key=lambda r: r["txn_uid"])
    em.processor.sort(key=lambda r: r["txn_uid"])
    em.bank.sort(key=lambda r: r["txn_uid"])
    em.answer_key.sort(key=lambda r: (r["txn_uid"], r["leg"]))

    write_csv(os.path.join(a.out, f"internal_{a.date}.csv"), INTERNAL_COLS, em.internal)
    write_csv(os.path.join(a.out, f"processor_{a.date}.csv"), PROCESSOR_COLS, em.processor)
    write_csv(os.path.join(a.out, f"bank_{a.date}.csv"), BANK_COLS, em.bank)
    write_csv(os.path.join(a.out, f"answer_key_{a.date}.csv"), ANSWER_KEY_COLS, em.answer_key)

    cl = {"business_date": a.date, "seed": a.seed, "sources": control_line(em)}
    with open(os.path.join(a.out, f"control_line_{a.date}.json"), "w",
              encoding="utf-8") as fh:
        json.dump(cl, fh, indent=2, sort_keys=True)
        fh.write("\n")

    counts: Dict[str, int] = {}
    for t in truth:
        counts[t.fate] = counts.get(t.fate, 0) + 1
    seen_internal_refs = {r["txn_ref"] for r in em.internal}
    print("=" * 72)
    print(f"SEED GENERATOR — date={a.date} seed={a.seed} rows={a.rows}")
    print("=" * 72)
    for name in ("internal", "processor", "bank"):
        per = cl["sources"][name]
        total = sum(v["rows"] for v in per.values())
        detail = " | ".join(f"{c} {v['rows']}r {v['sum_amount_minor']}m"
                            for c, v in per.items())
        print(f"{name:<10} {total:>6} rows | {detail}")
    print("-" * 72)
    print("seeded classes (per truth txn):")
    for k in (MATCHED, AMOUNT_MISMATCH, MISSING_IN_PROCESSOR, MISSING_IN_BANK,
              DUPLICATE, TIMING_DIFFERENCE):
        n = counts.get(k, 0)
        print(f"  {k:<22} {n:>5}  ({n / len(truth) * 100:5.2f}%)")
    print("-" * 72)
    print(f"m2m trap groups : {len(trap_report)} "
          f"(each 2x2 candidates, density 2.0 = greedy path, assert ONE_TO_ONE_STABLE)")
    for r in dense_report:
        print(f"dense block     : {r['block']} density={r['density']} "
              f"edges={r['edges']} records={r['records']} "
              f"greedy={r['greedy_pairs']} pairs / optimal={r['optimal_pairs']} pairs")
    blinded = sum(1 for r in em.bank if r["txn_ref"] not in seen_internal_refs)
    print(f"bank ref blinding: {blinded}/{len(em.bank)} bank rows carry a settlement "
          f"ref (tier-3 population: amount+date matching only)")
    print(f"answer key rows : {len(em.answer_key)} (grain: source_system x txn_uid x leg)")
    print(f"tolerances      : " + " ".join(
        f"{c}={total_tolerance(c, cfg)}" for c in cfg["generator"]["currencies"]))
    print(f"ORACLE VERIFIED : greedy loses in all {len(dense_report)} dense block(s); "
          f"optimal == truth   seed={a.seed}")
    print("=" * 72)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
