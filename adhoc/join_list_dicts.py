from datetime import datetime

sales = [
    {"store_id": 1, "item_id": 1, "sales_event_ts": "2026-03-01T12:00:00", "qty": 1, "price": 1.00, "tx_id": 1},
    {"store_id": 1, "item_id": 2, "sales_event_ts": "2026-03-01T12:00:00", "qty": 2, "price": 4.00, "tx_id": 2},
    {"store_id": 2, "item_id": 3, "sales_event_ts": "2026-03-01T13:00:00", "qty": 1, "price": 2.00, "tx_id": 3},
    {"store_id": 2, "item_id": 3, "sales_event_ts": "2026-03-01T14:00:00", "qty": 1, "price": 2.00, "tx_id": 4},
]

transactions = [
    {"tx_id": 1, "tx_event_ts": "2026-03-01T12:02:00", "clubcard_id": 1, "points": 2},
    {"tx_id": 2, "tx_event_ts": "2026-03-01T12:03:00", "clubcard_id": 2, "points": 3},
    {"tx_id": 3, "tx_event_ts": "2026-03-01T13:20:00", "clubcard_id": 3, "points": 4},
    {"tx_id": 4, "tx_event_ts": "2026-03-01T13:59:00", "clubcard_id": 4, "points": 5},
]

tx_by_id = {t['tx_id']: t for t in transactions}
print(f"{tx_by_id=}")

points_used = 0
for s in sales:
    tx_details = tx_by_id.get(s['tx_id'])
    if tx_details is None:
        continue
    tx_ts: str = tx_details.get("tx_event_ts")
    sale_ts: str = s.get("sales_event_ts")
    delta_secs = abs(datetime.fromisoformat(tx_ts) - datetime.fromisoformat(sale_ts)).total_seconds()
    if delta_secs <= 300:
        points_used += tx_details.get('points', 0)


print(f"{points_used=}")




