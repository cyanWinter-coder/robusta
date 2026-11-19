import threading
from datetime import datetime

import pytest

from robusta.core.alerts.alert_deduplicator import AlertDedupDecision, AlertDeduplicator
from robusta.integrations.prometheus.models import PrometheusAlert


def _make_alert(
    fingerprint: str = "fp-1234",
    status: str = "firing",
    starts_at: str = "2026-01-01T10:00:00Z",
    ends_at: str = "0001-01-01T00:00:00Z",
) -> PrometheusAlert:
    return PrometheusAlert(
        fingerprint=fingerprint,
        status=status,
        startsAt=datetime.fromisoformat(starts_at.replace("Z", "+00:00")),
        endsAt=datetime.fromisoformat(ends_at.replace("Z", "+00:00")),
        labels={"alertname": "TestAlert", "severity": "warning"},
        annotations={"summary": "test alert"},
        generatorURL="http://prometheus/graph?g0.expr=up",
    )


@pytest.fixture
def deduplicator():
    return AlertDeduplicator(ttl=3600, max_size=100)


class TestAlertDeduplicator:
    def test_first_alert_is_accepted_and_marked_in_flight(self, deduplicator):
        alert = _make_alert()
        assert deduplicator.register(alert) == AlertDedupDecision.ACCEPT
        assert deduplicator.is_in_flight(alert)
        assert not deduplicator.is_processed(alert)

    def test_redelivery_while_in_flight_is_suppressed(self, deduplicator):
        alert = _make_alert()
        assert deduplicator.register(alert) == AlertDedupDecision.ACCEPT
        # AlertManager redelivers before processing finished - must not run playbooks twice
        assert deduplicator.register(alert) == AlertDedupDecision.IN_FLIGHT
        assert deduplicator.register(alert) == AlertDedupDecision.IN_FLIGHT

    def test_redelivery_after_completion_is_duplicate(self, deduplicator):
        alert = _make_alert()
        deduplicator.register(alert)
        deduplicator.complete(alert)
        assert not deduplicator.is_in_flight(alert)
        assert deduplicator.is_processed(alert)
        assert deduplicator.register(alert) == AlertDedupDecision.DUPLICATE

    def test_failed_enqueue_allows_redelivery_retry(self, deduplicator):
        """Queue-full must not poison the dedup state: next redelivery is accepted."""
        alert = _make_alert()
        assert deduplicator.register(alert) == AlertDedupDecision.ACCEPT
        deduplicator.requeue_failed(alert)
        assert not deduplicator.is_in_flight(alert)
        assert not deduplicator.is_processed(alert)
        assert deduplicator.register(alert) == AlertDedupDecision.ACCEPT

    def test_different_statuses_are_distinct_occurrences(self, deduplicator):
        firing = _make_alert(status="firing")
        resolved = _make_alert(status="resolved")
        assert deduplicator.register(firing) == AlertDedupDecision.ACCEPT
        assert deduplicator.register(resolved) == AlertDedupDecision.ACCEPT

    def test_different_fingerprints_are_distinct(self, deduplicator):
        alert_a = _make_alert(fingerprint="fp-a")
        alert_b = _make_alert(fingerprint="fp-b")
        assert deduplicator.register(alert_a) == AlertDedupDecision.ACCEPT
        assert deduplicator.register(alert_b) == AlertDedupDecision.ACCEPT

    def test_restarted_alert_with_new_startsat_is_new_occurrence(self, deduplicator):
        first = _make_alert(starts_at="2026-01-01T10:00:00Z")
        second = _make_alert(starts_at="2026-01-01T11:00:00Z")
        assert deduplicator.register(first) == AlertDedupDecision.ACCEPT
        deduplicator.complete(first)
        assert deduplicator.register(second) == AlertDedupDecision.ACCEPT

    def test_hash_is_stable_across_alert_instances(self, deduplicator):
        alert1 = _make_alert()
        alert2 = _make_alert()
        assert AlertDeduplicator.alert_hash(alert1) == AlertDeduplicator.alert_hash(alert2)

    def test_concurrent_identical_alerts_only_one_accepted(self, deduplicator):
        alert = _make_alert()
        decisions = []
        barrier = threading.Barrier(8)

        def worker():
            barrier.wait()
            decisions.append(deduplicator.register(alert))

        threads = [threading.Thread(target=worker) for _ in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert decisions.count(AlertDedupDecision.ACCEPT) == 1
        assert decisions.count(AlertDedupDecision.IN_FLIGHT) == 7

    def test_complete_after_requeue_failed_is_noop(self, deduplicator):
        """A late completion callback for an alert whose enqueue failed must not
        resurrect a stale processed entry and block a legitimate retry."""
        alert = _make_alert()
        deduplicator.register(alert)
        deduplicator.requeue_failed(alert)
        deduplicator.complete(alert)
        # requeue_failed released the marker, so complete() has nothing to clear;
        # the alert stays unprocessed and can be retried
        assert deduplicator.register(alert) == AlertDedupDecision.ACCEPT

    def test_processed_entries_expire_after_ttl(self):
        deduplicator = AlertDeduplicator(ttl=1, max_size=100)
        alert = _make_alert()
        deduplicator.register(alert)
        deduplicator.complete(alert)
        assert deduplicator.register(alert) == AlertDedupDecision.DUPLICATE

        import time

        time.sleep(1.1)
        assert deduplicator.register(alert) == AlertDedupDecision.ACCEPT

    def test_stale_in_flight_entry_self_heals(self):
        """A worker that dies after enqueue must not suppress the alert forever."""
        deduplicator = AlertDeduplicator(ttl=1, max_size=100)
        alert = _make_alert()
        assert deduplicator.register(alert) == AlertDedupDecision.ACCEPT
        # simulate a crashed worker: complete()/requeue_failed() never run
        assert deduplicator.register(alert) == AlertDedupDecision.IN_FLIGHT

        import time

        time.sleep(1.1)
        assert deduplicator.register(alert) == AlertDedupDecision.ACCEPT
        assert len(deduplicator._in_flight) == 1  # re-registered fresh, not duplicated
