"""
Generates continuous item_attributes changes in Postgres.
Attributes change less frequently than inventory.

Rates:
  70% UPDATE price
  20% UPDATE category
  10% UPDATE name

Default rate: 1 event/second (set via EVENTS_PER_SEC env var).
"""

import random
import time
import logging

from common import get_conn, wait_for_postgres, get_rate

log = logging.getLogger(__name__)

CATEGORIES = ["electronics", "clothing", "food", "tools", "toys", "books", "sports"]
MAX_ITEM_ID = 1000


def run():
    wait_for_postgres()
    conn = get_conn()
    conn.autocommit = False
    cur = conn.cursor()

    rate = get_rate()
    interval = 1.0 / rate
    log.info(f"Starting item_attributes generator at {rate} evt/s")

    while True:
        t0 = time.monotonic()
        item_id = random.randint(1, MAX_ITEM_ID)
        r = random.random()

        try:
            if r < 0.70:
                cur.execute(
                    "UPDATE item_attributes SET price = %s, updated_at = now() WHERE item_id = %s",
                    (round(random.uniform(0.99, 999.99), 2), item_id),
                )
            elif r < 0.90:
                cur.execute(
                    "UPDATE item_attributes SET category = %s, updated_at = now() WHERE item_id = %s",
                    (random.choice(CATEGORIES), item_id),
                )
            else:
                cur.execute(
                    "UPDATE item_attributes SET name = %s, updated_at = now() WHERE item_id = %s",
                    (f"Item-{item_id}-v{random.randint(2, 99)}", item_id),
                )
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
