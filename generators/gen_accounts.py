"""
Accounts dimension generator (SCD1) — trading account holders.

Seeds a fixed base of accounts, then trickles slow-changing-dimension updates
(country / tier changes, occasional new account). Low rate — it's a dimension.
Drives Debezium CDC on the `accounts` table.
"""
import os
import random
import time

from psycopg2.extras import execute_values
from common import get_conn, wait_for_postgres, get_rate, log

MAX_ACCOUNT_ID = int(os.environ.get("MAX_ACCOUNT_ID", "1000"))
SEED_ACCOUNTS = int(os.environ.get("SEED_ACCOUNTS", "1000"))

COUNTRIES = ["GB", "US", "DE", "FR", "ES", "IE", "IN", "SG"]
TIERS = ["retail", "premier", "business", "wealth"]
FIRST = ["Ava", "Leo", "Mia", "Noah", "Zoe", "Kai", "Ivy", "Max", "Ada", "Sam"]
LAST = ["Stone", "Cole", "Vale", "Ray", "Frost", "Hale", "Reed", "Bond", "Lux", "Wren"]


def random_name() -> str:
    return f"{random.choice(FIRST)} {random.choice(LAST)}"


def seed(conn, cur):
    cur.execute("SELECT count(*) FROM accounts")
    if cur.fetchone()[0] == 0:
        rows = [(i, random_name(), random.choice(COUNTRIES), random.choice(TIERS))
                for i in range(1, SEED_ACCOUNTS + 1)]
        execute_values(cur,
            "INSERT INTO accounts (account_id, name, country, tier) VALUES %s "
            "ON CONFLICT (account_id) DO NOTHING", rows)
        conn.commit()
        log.info(f"Seeded {SEED_ACCOUNTS} accounts.")


def run():
    wait_for_postgres()
    conn = get_conn()
    conn.autocommit = False
    cur = conn.cursor()
    seed(conn, cur)

    rate = get_rate()
    interval = 1.0 / rate
    log.info(f"Accounts (SCD1) generator at {rate} evt/s")

    while True:
        t0 = time.monotonic()
        aid = random.randint(1, MAX_ACCOUNT_ID)
        r = random.random()
        try:
            if r < 0.15:  # rare new account
                cur.execute("INSERT INTO accounts (account_id, name, country, tier) "
                            "VALUES (%s,%s,%s,%s) ON CONFLICT (account_id) DO NOTHING",
                            (aid, random_name(), random.choice(COUNTRIES), random.choice(TIERS)))
            elif r < 0.60:  # country change
                cur.execute("UPDATE accounts SET country=%s, updated_at=now() WHERE account_id=%s",
                            (random.choice(COUNTRIES), aid))
            else:  # tier change
                cur.execute("UPDATE accounts SET tier=%s, updated_at=now() WHERE account_id=%s",
                            (random.choice(TIERS), aid))
            conn.commit()
        except Exception as e:
            conn.rollback()
            log.warning(f"DB error (will retry): {e}")

        sleep_for = interval - (time.monotonic() - t0)
        if sleep_for > 0:
            time.sleep(sleep_for)


if __name__ == "__main__":
    run()
