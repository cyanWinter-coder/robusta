import pytest

from robusta.core.alerts.alert_deduplicator import AlertDeduplicator, AlertDedupDecision
from robusta.runner import web as web_module
from robusta.runner.web import Web


def _alertmanager_payload(fingerprint: str = "fp-abc", status: str = "firing") -> dict:
    return {
        "receiver": "robusta",
        "status": status,
        "externalURL": "http://alertmanager:9093",
        "version": "4",
        "groupKey": "{}:{}",
        "alerts": [
            {
                "status": status,
                "labels": {"alertname": "TestAlert", "severity": "warning"},
                "annotations": {"summary": "test"},
                "startsAt": "2026-01-01T10:00:00Z",
                "endsAt": "0001-01-01T00:00:00Z",
                "generatorURL": "http://prometheus/graph?g0.expr=up",
                "fingerprint": fingerprint,
            }
        ],
    }


class _FakeQueue:
    def __init__(self, accept: bool = True):
        self.accept = accept
        self.tasks = []

    def add_task(self, task, *args, **kwargs) -> bool:
        if not self.accept:
            return False
        self.tasks.append((task, args, kwargs))
        return True


class _FakeTelemetry:
    def __init__(self):
        self.last_alert_at = None


class _FakeEventHandler:
    def __init__(self):
        self.relabel = []
        self.trigger_events = []
        self.telemetry = _FakeTelemetry()

    def get_relabel_config(self):
        return self.relabel

    def handle_trigger(self, trigger_event):
        self.trigger_events.append(trigger_event)

    def get_telemetry(self):
        return self.telemetry


@pytest.fixture
def client(monkeypatch):
    fresh_deduplicator = AlertDeduplicator(ttl=3600, max_size=100)
    monkeypatch.setattr(Web, "alert_deduplicator", fresh_deduplicator)
    event_handler = _FakeEventHandler()
    fake_queue = _FakeQueue()
    monkeypatch.setattr(Web, "event_handler", event_handler)
    monkeypatch.setattr(Web, "alerts_queue", fake_queue)
    web_module.app.config.update(TESTING=True)
    return web_module.app.test_client(), event_handler, fake_queue, fresh_deduplicator


class TestAlertIngress:
    def test_new_alert_is_queued_and_marked_processed(self, client):
        test_client, event_handler, queue, deduplicator = client
        resp = test_client.post("/api/alerts", json=_alertmanager_payload())
        assert resp.status_code == 200
        assert len(queue.tasks) == 1
        assert len(event_handler.trigger_events) == 0  # only queued, not executed
        # state moved from in-flight to processed
        assert len(deduplicator._in_flight) == 0
        assert len(deduplicator._processed_cache) == 1

    def test_duplicate_redelivery_is_not_queued(self, client):
        test_client, event_handler, queue, _ = client
        test_client.post("/api/alerts", json=_alertmanager_payload())
        test_client.post("/api/alerts", json=_alertmanager_payload())
        test_client.post("/api/alerts", json=_alertmanager_payload())
        assert len(queue.tasks) == 1  # playbooks run exactly once for this alert occurrence

    def test_resolved_alert_is_a_separate_occurrence(self, client):
        test_client, event_handler, queue, _ = client
        test_client.post("/api/alerts", json=_alertmanager_payload(status="firing"))
        test_client.post("/api/alerts", json=_alertmanager_payload(status="resolved"))
        assert len(queue.tasks) == 2

    def test_queue_full_keeps_alert_retriable(self, client, monkeypatch):
        test_client, event_handler, _, deduplicator = client
        full_queue = _FakeQueue(accept=False)
        monkeypatch.setattr(Web, "alerts_queue", full_queue)

        resp = test_client.post("/api/alerts", json=_alertmanager_payload())
        assert resp.status_code == 200
        assert len(full_queue.tasks) == 0
        # neither processed nor in-flight: AlertManager's redelivery will be retried
        assert len(deduplicator._in_flight) == 0
        assert len(deduplicator._processed_cache) == 0

        # once the queue drains, the redelivered alert goes through
        recovering_queue = _FakeQueue(accept=True)
        monkeypatch.setattr(Web, "alerts_queue", recovering_queue)
        test_client.post("/api/alerts", json=_alertmanager_payload())
        assert len(recovering_queue.tasks) == 1
        assert len(deduplicator._processed_cache) == 1

    def test_relabel_runs_before_dedup(self, client):
        from robusta.model.alert_relabel_config import AlertRelabel, AlertRelabelOp

        test_client, event_handler, queue, _ = client
        event_handler.relabel = [
            AlertRelabel(source="old_label", target="new_label", operation=AlertRelabelOp.Add)
        ]
        payload = _alertmanager_payload()
        payload["alerts"][0]["labels"]["old_label"] = "value"
        test_client.post("/api/alerts", json=payload)
        assert len(queue.tasks) == 1
