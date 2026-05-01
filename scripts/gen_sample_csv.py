#!/usr/bin/env python3
"""Generate a sample transactions CSV for the payments engine.

Output columns: type, client, tx, amount

Invariants enforced:
  - tx IDs are globally unique u32 values.
  - client IDs are u16.
  - amounts have up to 4 decimal places, and are only set for deposit/withdrawal.
  - dispute/resolve/chargeback reference a real prior deposit's tx ID.
  - resolve/chargeback only fire against a tx that is currently disputed.
  - rows are emitted in chronological order (file order == time order).

A client whose account becomes locked (chargeback) is excluded from subsequent
deposits/withdrawals so the dataset stays realistic; downstream disputes are
still allowed against pre-lock deposits to exercise the locked-flag behaviour.
"""

from __future__ import annotations

import argparse
import csv
import random
import sys
from dataclasses import dataclass, field
from decimal import Decimal, ROUND_HALF_EVEN
from pathlib import Path

U16_MAX = 0xFFFF
U32_MAX = 0xFFFF_FFFF


@dataclass
class ClientState:
    client_id: int
    available: Decimal = Decimal("0")
    # tx IDs of this client's deposits that are not currently disputed and
    # have not been charged back. These are eligible for new disputes.
    open_deposits: dict[int, Decimal] = field(default_factory=dict)
    # tx IDs currently under dispute -> amount.
    disputed: dict[int, Decimal] = field(default_factory=dict)
    locked: bool = False


def quantize(amount: Decimal) -> Decimal:
    return amount.quantize(Decimal("0.0001"), rounding=ROUND_HALF_EVEN)


def random_amount(rng: random.Random) -> Decimal:
    # Bias toward smaller amounts but keep some variance.
    whole = rng.randint(1, 10_000)
    frac = rng.randint(0, 9999)
    return quantize(Decimal(whole) + Decimal(frac) / Decimal(10_000))


def write_row(writer: csv.writer, kind: str, client: int, tx: int, amount: Decimal | None) -> None:
    amount_str = format(amount, "f") if amount is not None else ""
    writer.writerow([kind, client, tx, amount_str])


def generate(
    out_path: Path,
    rows: int,
    num_clients: int,
    seed: int,
    weights: dict[str, float],
) -> None:
    if num_clients < 1 or num_clients > U16_MAX:
        raise ValueError(f"num_clients must be in 1..={U16_MAX}")
    if rows < 1:
        raise ValueError("rows must be >= 1")

    rng = random.Random(seed)
    clients: dict[int, ClientState] = {
        cid: ClientState(client_id=cid) for cid in range(1, num_clients + 1)
    }
    next_tx = 1

    kinds = list(weights.keys())
    kind_weights = list(weights.values())

    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["type", "client", "tx", "amount"])

        emitted = 0
        # Force the first few rows to be deposits so disputes/resolves have
        # something to reference. Otherwise the generator can produce a long
        # tail of ignored dispute rows at the start.
        warmup = min(rows, max(num_clients * 2, 10))

        while emitted < rows:
            if next_tx > U32_MAX:
                print("tx ID space exhausted; stopping early", file=sys.stderr)
                break

            if emitted < warmup:
                kind = "deposit"
            else:
                kind = rng.choices(kinds, weights=kind_weights, k=1)[0]

            client_id = rng.randint(1, num_clients)
            client = clients[client_id]

            if kind == "deposit":
                if client.locked:
                    continue
                amount = random_amount(rng)
                tx = next_tx
                next_tx += 1
                client.available += amount
                client.open_deposits[tx] = amount
                write_row(writer, "deposit", client_id, tx, amount)

            elif kind == "withdrawal":
                if client.locked or client.available <= 0:
                    continue
                # Pick an amount we can actually cover most of the time so
                # the dataset isn't dominated by failed withdrawals.
                cap = min(client.available, Decimal("5000"))
                amount = quantize(Decimal(rng.uniform(0.01, float(cap))))
                if amount <= 0 or amount > client.available:
                    continue
                tx = next_tx
                next_tx += 1
                client.available -= amount
                write_row(writer, "withdrawal", client_id, tx, amount)

            elif kind == "dispute":
                if not client.open_deposits:
                    continue
                tx = rng.choice(list(client.open_deposits.keys()))
                amount = client.open_deposits.pop(tx)
                client.disputed[tx] = amount
                write_row(writer, "dispute", client_id, tx, None)

            elif kind == "resolve":
                if not client.disputed:
                    continue
                tx = rng.choice(list(client.disputed.keys()))
                amount = client.disputed.pop(tx)
                # A resolved deposit is eligible to be disputed again — the
                # spec doesn't forbid it, and partner re-disputes are realistic.
                client.open_deposits[tx] = amount
                write_row(writer, "resolve", client_id, tx, None)

            elif kind == "chargeback":
                if not client.disputed:
                    continue
                tx = rng.choice(list(client.disputed.keys()))
                client.disputed.pop(tx)
                client.locked = True
                write_row(writer, "chargeback", client_id, tx, None)

            else:
                raise AssertionError(f"unknown kind: {kind}")

            emitted += 1


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "-o", "--output",
        type=Path,
        default=Path("crates/tests/tests/data/test_input_large.csv"),
        help="output CSV path (default: %(default)s)",
    )
    parser.add_argument("-n", "--rows", type=int, default=10_000, help="rows to generate")
    parser.add_argument("-c", "--clients", type=int, default=100, help="distinct client IDs")
    parser.add_argument("-s", "--seed", type=int, default=42, help="RNG seed for reproducibility")
    parser.add_argument("--w-deposit", type=float, default=0.55)
    parser.add_argument("--w-withdrawal", type=float, default=0.30)
    parser.add_argument("--w-dispute", type=float, default=0.08)
    parser.add_argument("--w-resolve", type=float, default=0.05)
    parser.add_argument("--w-chargeback", type=float, default=0.02)
    return parser.parse_args(argv)


def main(argv: list[str]) -> int:
    args = parse_args(argv)
    weights = {
        "deposit": args.w_deposit,
        "withdrawal": args.w_withdrawal,
        "dispute": args.w_dispute,
        "resolve": args.w_resolve,
        "chargeback": args.w_chargeback,
    }
    generate(
        out_path=args.output,
        rows=args.rows,
        num_clients=args.clients,
        seed=args.seed,
        weights=weights,
    )
    print(f"wrote {args.rows} rows to {args.output}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
