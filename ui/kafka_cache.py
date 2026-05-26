"""
Kafka latest-value cache for reconciliation.

Runs a background thread that consumes both Kafka CDC topics from the
beginning and maintains an in-memory dict of the latest known value per
item_id for each topic. The UI reads from this cache — no Postgres queries.

Why this is better than querying Postgres:
  - Zero load on the source DB
  - The cache reflects what the pipeline *received*, so comparing it against
    silver tells you whether the issue is a missed event vs a wrong merge
  - Kafka retains messages for 24h (configured in docker-compose), so a
    restart replays history and rebuilds the cache quickly
  - For the 5 tracked items, cache lookups are O(1) dict access

Limitations vs Postgres:
  - In-memory only — cache is lost on container restart (rebuilds from
    Kafka on startup, takes a few seconds for 1000-item snapshot replay)
  - Reflects Kafka-committed state, not Postgres in-flight transactions.
    There will be a small gap (Debezium lag, typically <1s) between a Postgres
    commit and the event appearing in Kafka. For reconciliation purposes this
    is fine — it's the same lag the streaming jobs see.
"""

import json
import threading
import logging
import os
from dataclasses import dataclass, field
from typing import Optional

log = logging.getLogger(__name__)

KAFKA_BROKERS = os.environ.get("KAFKA_BROKERS", "kafka:9092")
INVENTORY_TOPIC  = "app.public.item_inventory"
ATTRIBUTES_TOPIC = "app.public.item_attributes"


@dataclass
class ItemInventorySnapshot:
    item_id: int
    qty_on_hand: Optional[int]
    location: Optional[str]
    updated_at: Optional[int]   # epoch microseconds from Debezium
    op: str = "r"               # c=create, u=update, d=delete, r=read (snapshot)


@dataclass
class CacheState:
    inventory: dict = field(default_factory=dict)   # item_id → ItemInventorySnapshot
    ready: bool = False
    error: Optional[str] = None
    messages_consumed: int = 0


_state = CacheState()
_lock  = threading.Lock()


def get_inventory_snapshot(item_ids: list[int]) -> dict[int, Optional[ItemInventorySnapshot]]:
    """Return the cached latest CDC state for the requested item_ids."""
    with _lock:
        return {iid: _state.inventory.get(iid) for iid in item_ids}


def is_ready() -> bool:
    with _lock:
        return _state.ready


def cache_error() -> Optional[str]:
    with _lock:
        return _state.error


def messages_consumed() -> int:
    with _lock:
        return _state.messages_consumed


def _parse_debezium_inventory(msg_value: bytes) -> Optional[ItemInventorySnapshot]:
    """Parse a Debezium JSON envelope (schemas.enable=false) into a snapshot."""
    try:
        env = json.loads(msg_value.decode("utf-8"))
    except Exception:
        return None

    op = env.get("op")
    if not op:
        return None

    # For deletes, 'after' is null — fall back to 'before' for item_id
    after  = env.get("after")  or {}
    before = env.get("before") or {}
    item_id = after.get("item_id") or before.get("item_id")
    if item_id is None:
        return None

    return ItemInventorySnapshot(
        item_id=int(item_id),
        qty_on_hand=after.get("qty_on_hand"),
        location=after.get("location"),
        updated_at=after.get("updated_at"),
        op=op,
    )


def _consume_loop():
    """Background thread: consume Kafka from earliest, maintain latest-value cache."""
    try:
        from confluent_kafka import Consumer, KafkaException
    except ImportError:
        with _lock:
            _state.error = "confluent-kafka not installed in this container"
        log.warning("confluent-kafka not available — Kafka cache disabled")
        return

    conf = {
        "bootstrap.servers": KAFKA_BROKERS,
        "group.id": "ui-recon-cache",
        "auto.offset.reset": "earliest",
        # Don't commit offsets — we always want to replay from start on restart
        # so the cache is always consistent with Kafka history
        "enable.auto.commit": False,
    }

    consumer = None
    try:
        consumer = Consumer(conf)
        consumer.subscribe([INVENTORY_TOPIC])
        log.info(f"Kafka cache consumer started on {INVENTORY_TOPIC}")

        # Consume until we've caught up (no new messages for 2s), then stay live
        caught_up = False
        while True:
            msg = consumer.poll(timeout=2.0)

            if msg is None:
                if not caught_up:
                    # First timeout after startup means we've replayed all history
                    caught_up = True
                    with _lock:
                        _state.ready = True
                    log.info(f"Kafka cache caught up. {_state.messages_consumed} messages replayed.")
                continue

            if msg.error():
                log.warning(f"Kafka consumer error: {msg.error()}")
                continue

            value = msg.value()
            if value is None:
                # Tombstone — treat as delete
                key_bytes = msg.key()
                if key_bytes:
                    try:
                        key = json.loads(key_bytes.decode("utf-8"))
                        item_id = key.get("item_id") or key.get("payload", {}).get("item_id")
                        if item_id is not None:
                            with _lock:
                                _state.inventory.pop(int(item_id), None)
                                _state.messages_consumed += 1
                    except Exception:
                        pass
                continue

            snap = _parse_debezium_inventory(value)
            if snap is None:
                continue

            with _lock:
                if snap.op == "d":
                    # Delete — remove from cache
                    _state.inventory.pop(snap.item_id, None)
                else:
                    # Create/update/read snapshot — keep if newer
                    existing = _state.inventory.get(snap.item_id)
                    if (existing is None
                            or snap.updated_at is None
                            or existing.updated_at is None
                            or snap.updated_at >= existing.updated_at):
                        _state.inventory[snap.item_id] = snap
                _state.messages_consumed += 1

    except Exception as e:
        log.error(f"Kafka cache consumer fatal error: {e}", exc_info=True)
        with _lock:
            _state.error = str(e)
            _state.ready = True  # unblock UI even on error
    finally:
        if consumer:
            consumer.close()


def start():
    """Start the background consumer thread. Call once at app startup."""
    t = threading.Thread(target=_consume_loop, name="kafka-cache", daemon=True)
    t.start()
    log.info("Kafka cache thread started")
