"""Discrete-event simulator for a fictional food-delivery app ("QuickBite").

It produces two correlated event streams:

* ``clickstream``  - JSON app events (app_open, search, view_restaurant,
  add_to_cart, checkout_started) grouped into user sessions.
* ``order_events`` - Avro order lifecycle events
  (PLACED -> ACCEPTED -> PICKED_UP -> DELIVERED, or CANCELLED).

A checkout in the clickstream leads (with some probability) to a PLACED order
with the same ``session_id`` a few seconds later, so the two streams can be
joined downstream.

The simulator is pure Python with an injectable clock and random generator,
so it is fully deterministic under test. It knows nothing about Kafka.
"""

from __future__ import annotations

import heapq
import itertools
import random
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any

ORDER_TOPIC = "order_events"
CLICK_TOPIC = "clickstream"

ZONES = [
    "KORAMANGALA",
    "INDIRANAGAR",
    "HSR_LAYOUT",
    "WHITEFIELD",
    "JAYANAGAR",
    "MARATHAHALLI",
    "ELECTRONIC_CITY",
    "HEBBAL",
]
DEVICES = ["android", "ios", "web"]
PAYMENT_METHODS = ["UPI", "CARD", "COD", "WALLET"]

ORDER_STATUSES = ("PLACED", "ACCEPTED", "PICKED_UP", "DELIVERED", "CANCELLED")


@dataclass(frozen=True)
class Emission:
    """One message the producer should send.

    ``value`` is a dict for well-formed events (serialised by the producer as
    Avro or JSON depending on ``kind``) or raw ``bytes`` for garbage payloads.
    """

    topic: str
    key: str
    value: Any
    kind: str  # "order" (Avro) | "click" (JSON) | "raw" (bytes as-is)
    tag: str = "normal"  # normal | duplicate | late | malformed:<reason>
    expected_in_silver: bool = True


@dataclass
class SimConfig:
    sessions_per_sec: float = 5.0
    browse_steps: tuple[int, int] = (1, 5)
    add_to_cart_prob: float = 0.55
    checkout_given_cart: float = 0.70
    order_given_checkout: float = 0.75
    cancel_prob: float = 0.05
    stuck_prob: float = 0.03
    # Lifecycle delays in seconds. Time is compressed so a demo shows full
    # lifecycles within a few minutes.
    click_gap_s: tuple[float, float] = (1.0, 10.0)
    checkout_to_order_s: tuple[float, float] = (2.0, 25.0)
    accept_delay_s: tuple[float, float] = (3.0, 15.0)
    pickup_delay_s: tuple[float, float] = (15.0, 60.0)
    deliver_delay_s: tuple[float, float] = (20.0, 100.0)
    n_customers: int = 5_000
    n_restaurants: int = 120


@dataclass
class _Session:
    session_id: str
    customer_id: str
    zone: str
    device: str
    steps_left: int
    restaurant_id: str | None = None
    stage: str = "browse"  # browse -> cart -> checkout -> done


@dataclass
class _Order:
    order_id: str
    customer_id: str
    session_id: str | None
    restaurant_id: str
    zone: str
    amount: Decimal
    payment_method: str
    fate: str  # delivered | cancelled | stuck_accepted | stuck_picked_up
    status: str | None = None


@dataclass(order=True)
class _Scheduled:
    due_ms: int
    seq: int
    action: str = field(compare=False)
    obj: Any = field(compare=False)


class Simulator:
    def __init__(self, config: SimConfig, rng: random.Random, start_ms: int):
        self.cfg = config
        self.rng = rng
        self._heap: list[_Scheduled] = []
        self._seq = itertools.count()
        self._last_ms = start_ms
        self._session_debt = 0.0
        self.schema_version = 2
        self._customers = [
            (f"C{idx:06d}", rng.choice(ZONES)) for idx in range(config.n_customers)
        ]
        self._restaurants_by_zone: dict[str, list[str]] = {z: [] for z in ZONES}
        for idx in range(config.n_restaurants):
            self._restaurants_by_zone[ZONES[idx % len(ZONES)]].append(f"R{idx:04d}")

    # ------------------------------------------------------------------ ids
    def _id(self, prefix: str) -> str:
        return f"{prefix}-{self.rng.getrandbits(64):016x}"

    # ------------------------------------------------------------ scheduling
    def _schedule(self, due_ms: int, action: str, obj: Any) -> None:
        heapq.heappush(self._heap, _Scheduled(due_ms, next(self._seq), action, obj))

    def _delay_ms(self, bounds: tuple[float, float]) -> int:
        return int(self.rng.uniform(*bounds) * 1000)

    @property
    def pending(self) -> int:
        return len(self._heap)

    # ------------------------------------------------------------------ tick
    def tick(self, now_ms: int) -> list[Emission]:
        """Advance the simulation to ``now_ms`` and return what happened."""
        out: list[Emission] = []
        elapsed_s = max(0, now_ms - self._last_ms) / 1000.0
        self._last_ms = now_ms

        self._session_debt += self.cfg.sessions_per_sec * elapsed_s
        while self._session_debt >= 1.0:
            self._session_debt -= 1.0
            self._start_session(now_ms, out)

        while self._heap and self._heap[0].due_ms <= now_ms:
            item = heapq.heappop(self._heap)
            if item.action == "session":
                self._advance_session(item.obj, item.due_ms, out)
            elif item.action == "order":
                self._advance_order(item.obj, item.due_ms, out)
        return out

    # -------------------------------------------------------------- sessions
    def _start_session(self, now_ms: int, out: list[Emission]) -> None:
        customer_id, zone = self.rng.choice(self._customers)
        session = _Session(
            session_id=self._id("S"),
            customer_id=customer_id,
            zone=zone,
            device=self.rng.choice(DEVICES),
            steps_left=self.rng.randint(*self.cfg.browse_steps),
        )
        out.append(self._click(session, "app_open", now_ms))
        self._schedule(now_ms + self._delay_ms(self.cfg.click_gap_s), "session", session)

    def _advance_session(self, s: _Session, ts: int, out: list[Emission]) -> None:
        nxt = ts + self._delay_ms(self.cfg.click_gap_s)
        if s.stage == "browse":
            if s.steps_left > 0:
                s.steps_left -= 1
                if self.rng.random() < 0.4:
                    out.append(self._click(s, "search", ts))
                else:
                    s.restaurant_id = self.rng.choice(self._restaurants_by_zone[s.zone])
                    out.append(self._click(s, "view_restaurant", ts))
                self._schedule(nxt, "session", s)
            elif s.restaurant_id and self.rng.random() < self.cfg.add_to_cart_prob:
                s.stage = "cart"
                out.append(self._click(s, "add_to_cart", ts))
                self._schedule(nxt, "session", s)
            else:
                s.stage = "done"
        elif s.stage == "cart":
            if self.rng.random() < self.cfg.checkout_given_cart:
                s.stage = "checkout"
                out.append(self._click(s, "checkout_started", ts))
                if self.rng.random() < self.cfg.order_given_checkout:
                    order = self._new_order(s)
                    self._schedule(
                        ts + self._delay_ms(self.cfg.checkout_to_order_s), "order", order
                    )
            s.stage = "done"

    def _click(self, s: _Session, event_type: str, ts: int) -> Emission:
        value = {
            "event_id": self._id("E"),
            "session_id": s.session_id,
            "customer_id": s.customer_id,
            "event_type": event_type,
            "restaurant_id": s.restaurant_id if event_type != "app_open" else None,
            "zone": s.zone,
            "device": s.device,
            "event_ts": ts,
        }
        return Emission(topic=CLICK_TOPIC, key=s.session_id, value=value, kind="click")

    # ---------------------------------------------------------------- orders
    def _new_order(self, s: _Session) -> _Order:
        r = self.rng.random()
        if r < self.cfg.cancel_prob:
            fate = "cancelled"
        elif r < self.cfg.cancel_prob + self.cfg.stuck_prob:
            fate = self.rng.choice(["stuck_accepted", "stuck_picked_up"])
        else:
            fate = "delivered"
        amount = Decimal(self.rng.randint(9_900, 150_000)) / Decimal(100)
        return _Order(
            order_id=self._id("O"),
            customer_id=s.customer_id,
            session_id=s.session_id,
            restaurant_id=s.restaurant_id or self.rng.choice(self._restaurants_by_zone[s.zone]),
            zone=s.zone,
            amount=amount.quantize(Decimal("0.01")),
            payment_method=self.rng.choice(PAYMENT_METHODS),
            fate=fate,
        )

    def _advance_order(self, o: _Order, ts: int, out: list[Emission]) -> None:
        transitions = {
            None: ("PLACED", self.cfg.accept_delay_s),
            "PLACED": ("ACCEPTED", self.cfg.pickup_delay_s),
            "ACCEPTED": ("PICKED_UP", self.cfg.deliver_delay_s),
            "PICKED_UP": ("DELIVERED", None),
        }
        if o.status == "PLACED" and o.fate == "cancelled":
            o.status = "CANCELLED"
            out.append(self._order_event(o, ts))
            return
        new_status, next_delay = transitions[o.status]
        o.status = new_status
        out.append(self._order_event(o, ts))

        stuck_here = (o.fate == "stuck_accepted" and new_status == "ACCEPTED") or (
            o.fate == "stuck_picked_up" and new_status == "PICKED_UP"
        )
        if next_delay is not None and not stuck_here:
            self._schedule(ts + self._delay_ms(next_delay), "order", o)

    def order_event_value(self, o: _Order, ts: int, status: str | None = None) -> dict:
        value = {
            "event_id": self._id("E"),
            "order_id": o.order_id,
            "customer_id": o.customer_id,
            "session_id": o.session_id,
            "restaurant_id": o.restaurant_id,
            "zone": o.zone,
            "status": status or o.status,
            "amount": o.amount,
            "event_ts": ts,
        }
        if self.schema_version >= 2:
            value["payment_method"] = o.payment_method
        return value

    def _order_event(self, o: _Order, ts: int) -> Emission:
        return Emission(
            topic=ORDER_TOPIC, key=o.order_id, value=self.order_event_value(o, ts), kind="order"
        )
