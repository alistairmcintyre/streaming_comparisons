"""
Generates continuous item_inventory changes in Postgres.
Debezium CDC will stream these as Kafka events.

Rates:
  80% UPDATE  — qty_on_hand or location changes
  15% UPSERT  — new item or re-insert after delete
   5% DELETE  — item removed from inventory

Default rate: 5 events/second (set via EVENTS_PER_SEC env var).
"""

import random
import time
import logging

from common import get_conn, wait_for_postgres, get_rate

log = logging.getLogger(__name__)

LOCATIONS = ["A", "B", "C", "D", "E"]
MAX_ITEM_ID = 1000


def run():
    wait_for_postgres()
    conn = get_conn()
    conn.autocommit = False
    cur = conn.cursor()

    rate = get_rate()
    interval = 1.0 / rate
    log.info(f"Starting item_inventory generator at {rate} evt/s")

    while True:
        t0 = time.monotonic()
        item_id = random.randint(1, MAX_ITEM_ID)
        r = random.random()

        try:
            if r < 0.80:
                # UPDATE qty and/or location
                cur.execute(
                    "UPDATE item_inventory SET qty_on_hand = %s, updated_at = now() WHERE item_id = %s",
                    (random.randint(0, 500), item_id),
                )
            elif r < 0.95:
                # UPSERT — handles both new items and re-inserts
                cur.execute(
                    """
                    INSERT INTO item_inventory (item_id, qty_on_hand, location)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (item_id) DO UPDATE
                        SET qty_on_hand = EXCLUDED.qty_on_hand,
                            location    = EXCLUDED.location,
                            updated_at  = now()
                    """,
                    (item_id, random.randint(0, 500), random.choice(LOCATIONS)),
                )
            else:
                # DELETE
                cur.execute("DELETE FROM item_inventory WHERE item_id = %s", (item_id,))

            conn.commit()
        except Exception as e:
            conn.rollback()
            log.warning(f"DB error (will retry): {e}")

        # Sleep remainder of interval to maintain target rate
        elapsed = time.monotonic() - t0
        sleep_for = max(0.0, interval - elapsed)
        if sleep_for > 0:
            time.sleep(sleep_for)


if __name__ == "__main__":
    run()
