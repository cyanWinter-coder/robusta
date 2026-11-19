from collections import defaultdict
from unittest.mock import Mock

import pytest

from robusta.core.model.events import ExecutionBaseEvent
from robusta.core.playbooks.playbooks_event_handler_impl import PlaybooksEventHandlerImpl
from robusta.core.reporting import Finding
from robusta.core.sinks.sink_dispatcher import sink_send_counter
from robusta.runner.telemetry import SinkInfo, Telemetry


class _FakeSink:
    def __init__(self, name: str, failures: int = 0, accepts: bool = True, stop: bool = False):
        self.sink_name = name
        self.failures_remaining = failures
        self.writes = 0
        self._accepts = accepts
        self.params = Mock(stop=stop)

    def accepts(self, finding):
        return self._accepts

    def write_finding(self, finding, platform_enabled):
        self.writes += 1
        if self.failures_remaining > 0:
            self.failures_remaining -= 1
            raise RuntimeError(f"channel {self.sink_name} down")


class _FakeSinksRegistry:
    def __init__(self, sinks):
        self.sinks = sinks
        self.platform_enabled = False


def _handler(sinks):
    telemetry = Telemetry(runner_version="test")
    telemetry.sinks_info = defaultdict(SinkInfo)
    registry = Mock()
    registry.get_sinks.return_value = _FakeSinksRegistry(sinks)
    registry.get_telemetry.return_value = telemetry
    return PlaybooksEventHandlerImpl(registry), telemetry


def _event(sink_name: str) -> ExecutionBaseEvent:
    finding = Finding(title="alert", aggregation_key="TestAlert")
    event = ExecutionBaseEvent()
    event.sink_findings = defaultdict(list)
    event.sink_findings[sink_name].append(finding)
    return event


def _counter(name: str, result: str) -> float:
    return sink_send_counter.labels(name, result)._value.get()


class TestHandleFindingsDispatch:
    def test_finding_delivered_and_telemetry_incremented(self):
        sink = _FakeSink("slack-main")
        handler, telemetry = _handler({"slack-main": sink})
        handler._PlaybooksEventHandlerImpl__handle_findings(_event("slack-main"))

        assert sink.writes == 1
        assert telemetry.sinks_info["slack-main"].findings_count == 1
        assert telemetry.sinks_info["slack-main"].type == "_FakeSink"

    def test_transient_channel_failure_retried_and_counted_on_success(self):
        sink = _FakeSink("slack-main", failures=2)
        handler, telemetry = _handler({"slack-main": sink})
        with pytest.MonkeyPatch.context() as mp:
            mp.setattr(
                "robusta.core.playbooks.playbooks_event_handler_impl.sink_dispatcher.retry_backoff_sec",
                0,
            )
            handler._PlaybooksEventHandlerImpl__handle_findings(_event("slack-main"))

        assert sink.writes == 3
        assert telemetry.sinks_info["slack-main"].findings_count == 1

    def test_persistent_failure_does_not_increment_telemetry_or_raise(self):
        sink = _FakeSink("slack-bad", failures=99)
        handler, telemetry = _handler({"slack-bad": sink})
        before = _counter("slack-bad", "failure")
        with pytest.MonkeyPatch.context() as mp:
            mp.setattr(
                "robusta.core.playbooks.playbooks_event_handler_impl.sink_dispatcher.max_attempts",
                2,
            )
            mp.setattr(
                "robusta.core.playbooks.playbooks_event_handler_impl.sink_dispatcher.retry_backoff_sec",
                0,
            )
            handler._PlaybooksEventHandlerImpl__handle_findings(_event("slack-bad"))

        assert sink.writes == 2
        assert telemetry.sinks_info["slack-bad"].findings_count == 0
        assert _counter("slack-bad", "failure") == before + 1

    def test_missing_sink_after_rule_reload_is_metered(self):
        handler, telemetry = _handler({})
        before = _counter("removed-sink", "sink_not_found")
        # must not raise even though the sink registry was swapped mid-flight
        handler._PlaybooksEventHandlerImpl__handle_findings(_event("removed-sink"))
        assert _counter("removed-sink", "sink_not_found") == before + 1

    def test_stop_sink_short_circuits_remaining_sinks(self):
        first = _FakeSink("first", stop=True)
        second = _FakeSink("second")
        handler, _ = _handler({"first": first, "second": second})

        event = ExecutionBaseEvent()
        event.sink_findings = defaultdict(list)
        event.sink_findings["first"].append(Finding(title="a", aggregation_key="A"))
        event.sink_findings["second"].append(Finding(title="b", aggregation_key="B"))

        handler._PlaybooksEventHandlerImpl__handle_findings(event)
        assert first.writes == 1
        assert second.writes == 0
