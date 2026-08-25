"""
Trades (fills) generator — append-only stock executions, the load driver.

Emits immutable BUY/SELL fills into the `trades` table at a configurable rate
(TRADES_PER_SEC), batched for throughput so it can sustain 1k / 10k+ fills/sec.
Each fill references an account and a symbol; positions are DERIVED downstream by
folding signed quantities per (account_id, symbol).

Env:
  TRADES_PER_SEC     target fills/sec (default 1000)
  TRADES_BATCH_SIZE  rows per INSERT/commit (default 500)
  MAX_ACCOUNT_ID     account id range (default 1000, matches the accounts seed)
"""
import os
import random
import time

from psycopg2.extras import execute_values
from common import get_conn, wait_for_postgres, log

MAX_ACCOUNT_ID = int(os.environ.get("MAX_ACCOUNT_ID", "1000"))
TRADES_PER_SEC = int(os.environ.get("TRADES_PER_SEC", "1000"))
BATCH_SIZE = int(os.environ.get("TRADES_BATCH_SIZE", "500"))
BUY_BIAS = float(os.environ.get("TRADES_BUY_BIAS", "0.55"))  # >0.5 → positions tend to open

# symbol -> rough base price
SYMBOLS = {
    "AAPL": 185, "MSFT": 420, "TSLA": 250, "AMZN": 180, "GOOGL": 170,
    "NVDA": 120, "META": 500, "NFLX": 650, "AMD": 160, "INTC": 40,
    "JPM": 200, "BAC": 40, "XOM": 110, "WMT": 70, "DIS": 110,
    "BA": 180, "KO": 60, "PEP": 170, "CSCO": 50, "ORCL": 130,
}
SYM_LIST = list(SYMBOLS)


def make_batch(n):
    rows = []
    for _ in range(n):
        sym = random.choice(SYM_LIST)
        base = SYMBOLS[sym]
        price = round(base * (1 + random.uniform(-0.03, 0.03)), 4)
        side = "BUY" if random.random() < BUY_BIAS else "SELL"
        rows.append((random.randint(1, MAX_ACCOUNT_ID), sym, side,
                     random.randint(1, 1000), price))
    return rows


def run():
    wait_for_postgres()
    conn = get_conn()
    conn.autocommit = False
    cur = conn.cursor()
    interval = BATCH_SIZE / TRADES_PER_SEC  # seconds between batches
    log.info(f"Trades generator: target {TRADES_PER_SEC} fills/s, batch {BATCH_SIZE} "
             f"(~{interval*1000:.0f}ms/batch)")

    emitted = 0
    t_report = time.monotonic()
    while True:
        t0 = time.monotonic()
        try:
            execute_values(cur,
                "INSERT INTO trades (account_id, symbol, side, quantity, price) VALUES %s",
                make_batch(BATCH_SIZE))
            conn.commit()
            emitted += BATCH_SIZE
        except Exception as e:
            conn.rollback()
            log.warning(f"DB error (will retry): {e}")

        now = time.monotonic()
        if now - t_report >= 10:
            log.info(f"emitted ~{emitted} fills in 10s (~{emitted/(now-t_report):.0f}/s)")
            emitted = 0
            t_report = now

        sleep_for = interval - (now - t0)
        if sleep_for > 0:
            time.sleep(sleep_for)


if __name__ == "__main__":
    run()
