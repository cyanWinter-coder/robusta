"""Incoming alert deduplication.

AlertManager redelivers the same alert on every group flush, so identical
(fingerprint, status, startsAt, endsAt) payloads arrive repeatedly. Deduplication
intentionally lives in the core, before any playbook runs:

* playbooks stay stateless and keep their existing trigger interface - every
  playbook can assume it is invoked once per distinct alert occurrence;
* the rule is independent of playbook/sink configuration reloads, so a config
  reload in the middle of an alert storm cannot change dedup semantics.

Two states are tracked per alert hash:

* ``processed`` - the alert was handed to the alert queue. The entry is kept for
  a TTL so late redeliveries are suppressed.
* ``in_flight`` - the alert was queued but not yet processed. The marker is only
  released once processing finishes, so if the queue rejects the task (full
  queue, worker crash) a redelivered copy is accepted and retried instead of
  being silently dropped for the remainder of the TTL window.
"""

import enum
import hashlib
import logging
import threading
import time
from typing import Dict, List

import prometheus_client
from cachetools import TTLCache

from robusta.core.model.env_vars import PROCESSED_ALERTS_CACHE_MAX_SIZE, PROCESSED_ALERTS_CACHE_TTL
from robusta.integrations.prometheus.models import PrometheusAlert

alerts_dedup_counter = prometheus_client.Counter(
    "alerts_dedup_decisions",
    "Number of incoming alerts per dedup decision.",
    labelnames=("decision",),
)
alerts_in_flight_gauge = prometheus_client.Gauge(
    "alerts_in_flight",
    "Number of alerts queued for processing but not yet completed.",
)


class AlertDedupDecision(enum.Enum):
    ACCEPT = "accept"  # a new alert occurrence, enqueue it
    DUPLICATE = "duplicate"  # already processed within the TTL window
    IN_FLIGHT = "in_flight"  # an identical alert is currently being processed


class AlertDeduplicator:
    """Thread-safe dedup of incoming AlertManager alerts.

    The lifecycle of an accepted alert is::

        decision = deduplicator.register(alert)   # ACCEPT, marks it in-flight
        if queue.add_task(...):
            deduplicator.complete(alert)          # processed -> TTL cache
        else:
            deduplicator.requeue_failed(alert)    # release the in-flight marker

    If the alert could not be queued, :meth:`requeue_failed` releases the
    in-flight marker without recording the alert as processed, so the next
    redelivery is retried.
    """

    def __init__(self, ttl: int = PROCESSED_ALERTS_CACHE_TTL, max_size: int = PROCESSED_ALERTS_CACHE_MAX_SIZE):
        self._ttl = ttl
        self._max_size = max_size
        self._processed_cache: TTLCache = TTLCache(maxsize=max_size, ttl=ttl)
        # alert hash -> insertion timestamp. Bounded and self-healing: an entry
        # whose worker died before completing expires after ttl, so its
        # redelivery is eventually accepted instead of being suppressed forever.
        self._in_flight: Dict[bytes, float] = {}
        self._lock = threading.Lock()

    def _prune_stale_in_flight(self, now_ts: float) -> None:
        stale = [alert_hash for alert_hash, ts in self._in_flight.items() if now_ts - ts > self._ttl]
        for alert_hash in stale:
            del self._in_flight[alert_hash]
            logging.warning("Expiring stale in-flight alert %s; its redelivery will be retried", alert_hash.hex())

    @staticmethod
    def alert_hash(alert: PrometheusAlert) -> bytes:
        """Stable identity of one AlertManager alert occurrence.

        fingerprint identifies the alerting rule/labels; status plus startsAt
        and endsAt distinguish the firing occurrence from its resolution and
        from re-fires with a new start time.
        """
        return AlertDeduplicator.compute_hash(
            [
                alert.fingerprint.encode("ascii", errors="replace"),
                alert.status.encode("utf-8"),
                str(alert.startsAt.timestamp()).encode("ascii"),
                str(alert.endsAt.timestamp()).encode("ascii"),
            ]
        )

    @staticmethod
    def compute_hash(data: List[bytes]) -> bytes:
        hash_value = hashlib.sha1()
        for item in data:
            hash_value.update(hashlib.sha1(item).digest())
        return hash_value.digest()

    def register(self, alert: PrometheusAlert) -> AlertDedupDecision:
        """Decide whether an incoming alert should be processed.

        ACCEPT reserves an in-flight slot for the alert. The caller must
        eventually call :meth:`complete` or :meth:`requeue_failed`.
        """
        alert_hash = self.alert_hash(alert)
        with self._lock:
            now_ts = time.time()
            self._prune_stale_in_flight(now_ts)
            if alert_hash in self._processed_cache:
                alerts_dedup_counter.labels(AlertDedupDecision.DUPLICATE.value).inc()
                return AlertDedupDecision.DUPLICATE
            if alert_hash in self._in_flight:
                alerts_dedup_counter.labels(AlertDedupDecision.IN_FLIGHT.value).inc()
                return AlertDedupDecision.IN_FLIGHT
            self._in_flight[alert_hash] = now_ts
            alerts_in_flight_gauge.set(len(self._in_flight))
            alerts_dedup_counter.labels(AlertDedupDecision.ACCEPT.value).inc()
            return AlertDedupDecision.ACCEPT

    def complete(self, alert: PrometheusAlert) -> None:
        """Record successful hand-off to the alert queue and clear in-flight.

        No-op if the alert is no longer in flight (e.g. its enqueue already
        failed and the slot was released via :meth:`requeue_failed`), so a late
        completion callback cannot block a legitimate redelivery retry.
        """
        alert_hash = self.alert_hash(alert)
        with self._lock:
            if alert_hash not in self._in_flight:
                return
            self._processed_cache[alert_hash] = True
            del self._in_flight[alert_hash]
            alerts_in_flight_gauge.set(len(self._in_flight))

    def requeue_failed(self, alert: PrometheusAlert) -> None:
        """Release the in-flight marker after a failed enqueue/processing attempt.

        The next redelivery of this alert will be accepted and retried.
        """
        alert_hash = self.alert_hash(alert)
        with self._lock:
            removed = self._in_flight.pop(alert_hash, None)
            alerts_in_flight_gauge.set(len(self._in_flight))
            if removed is not None:
                logging.warning(
                    "Alert %s could not be queued for processing; its redelivery will be retried",
                    alert.fingerprint,
                )

    def is_in_flight(self, alert: PrometheusAlert) -> bool:
        with self._lock:
            return self.alert_hash(alert) in self._in_flight

    def is_processed(self, alert: PrometheusAlert) -> bool:
        with self._lock:
            return self.alert_hash(alert) in self._processed_cache
