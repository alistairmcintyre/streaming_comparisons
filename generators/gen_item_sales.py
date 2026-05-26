"""
Generates continuous item_sales changes in Postgres.
Debezium CDC will stream these as Kafka events.

Rates:
  70% UPDATE  — quantity or total_price changes (new sale recorded)
  20% UPSERT  — new item or re-insert after delete
  10% DELETE  — sale record removed

Default rate: 5 events/second (set via EVENTS_PER_SEC env var).
"""

import random
import time
import logging

from common import get_conn, wait_for_postgres, get_rate

log = logging.getLogger(__name__)

MAX_ITEM_ID = 1000


def run():
    wait_for_postgres()
    conn = get_conn()
    conn.autocommit = False
    cur = conn.cursor()

    rate = get_rate()
    interval = 1.0 / rate
    log.info(f"Starting item_sales generator at {rate} evt/s")

    while True:
        t0 = time.monotonic()
        item_id = random.randint(1, MAX_ITEM_ID)
        r = random.random()

        try:
            if r < 0.70:
                quantity    = random.randint(1, 200)
                total_price = round(quantity * random.uniform(0.99, 499.99), 2)
                cur.execute(
                    "UPDATE item_sales SET quantity = %s, total_price = %s, updated_at = now() WHERE item_id = %s",
                    (quantity, total_price, item_id),
                )
            elif r < 0.90:
                quantity    = random.randint(1, 200)
                total_price = round(quantity * random.uniform(0.99, 499.99), 2)
                cur.execute(
                    """
                    INSERT INTO item_sales (item_id, quantity, total_price)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (item_id) DO UPDATE
                        SET quantity    = EXCLUDED.quantity,
                            total_price = EXCLUDED.total_price,
                            updated_at  = now()
                    """,
                    (item_id, quantity, total_price),
                )
            else:
                cur.execute("DELETE FROM item_sales WHERE item_id = %s", (item_id,))

            conn.commit()
        except Exception as e:
            conn.rollback()
            log.warning(f"DB error (will retry): {e}")

        elapsed = time.monotonic() - t0
        sleep_for = max(0.0, interval - elapsed)
        if sleep_for > 0:
            time.sleep(sleep_for)


if __name__ == "__main__":
    run()
