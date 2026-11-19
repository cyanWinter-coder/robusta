import pytest

from robusta.core.reporting import Finding
from robusta.core.sinks.sink_dispatcher import SinkDispatcher, sink_send_counter
from robusta.core.sinks.sink_base import SinkBase


def _make_finding(aggregation_key: str = "TestAlert") -> Finding:
    return Finding(title="test", aggregation_key=aggregation_key)


class _RecordingSink:
    """Minimal sink stand-in: only the attributes SinkDispatcher touches."""

    def __init__(self, name: str, failures: int = 0):
        self.sink_name = name
        self.failures_remaining = failures
        self.calls = 0
        self.received = []

    def write_finding(self, finding, platform_enabled):
        self.calls += 1
        self.received.append(finding)
        if self.failures_remaining > 0:
            self.failures_remaining -= 1
            raise RuntimeError(f"simulated channel failure in {self.sink_name}")


class TestSinkDispatcher:
    def test_successful_send_counts_one_attempt(self):
        sink = _RecordingSink("slack-main")
        sleeps = []
        dispatcher = SinkDispatcher(max_attempts=3, retry_backoff_sec=0.1, sleep_fn=sleeps.append)
        finding = _make_finding()

        assert dispatcher.send_to_sink(sink, finding, platform_enabled=False) is True
        assert sink.calls == 1
        assert sleeps == []

    def test_transient_failure_is_retried_then_succeeds(self):
        sink = _RecordingSink("slack-main", failures=2)
        sleeps = []
        dispatcher = SinkDispatcher(max_attempts=3, retry_backoff_sec=0.01, sleep_fn=sleeps.append)
        finding = _make_finding()

        assert dispatcher.send_to_sink(sink, finding, platform_enabled=False) is True
        assert sink.calls == 3
        # linear backoff: 0.01, 0.02
        assert sleeps == [pytest.approx(0.01), pytest.approx(0.02)]
        # the same finding object is retried - no duplicate notifications, retries only
        assert len(sink.received) == 3
        assert all(f is finding for f in sink.received)

    def test_persistent_failure_returns_false_without_raising(self):
        sink = _RecordingSink("broken-teams", failures=99)
        dispatcher = SinkDispatcher(max_attempts=3, retry_backoff_sec=0, sleep_fn=lambda _: None)
        finding = _make_finding()

        assert dispatcher.send_to_sink(sink, finding, platform_enabled=False) is False
        assert sink.calls == 3

    def test_failing_sink_does_not_affect_other_sinks(self):
        """One channel failing must not prevent delivery to the remaining sinks."""
        good_sink = _RecordingSink("slack-good")
        bad_sink = _RecordingSink("slack-bad", failures=99)
        dispatcher = SinkDispatcher(max_attempts=2, retry_backoff_sec=0, sleep_fn=lambda _: None)
        finding = _make_finding()

        assert dispatcher.send_to_sink(bad_sink, finding, platform_enabled=False) is False
        assert dispatcher.send_to_sink(good_sink, finding, platform_enabled=False) is True
        assert good_sink.calls == 1
        assert bad_sink.calls == 2

    def test_success_and_failure_metrics_are_recorded(self):
        good_sink = _RecordingSink("metric-good")
        bad_sink = _RecordingSink("metric-bad", failures=99)
        dispatcher = SinkDispatcher(max_attempts=2, retry_backoff_sec=0, sleep_fn=lambda _: None)

        def _value(sink_name: str, result: str) -> float:
            return sink_send_counter.labels(sink_name, result)._value.get()

        before_success = _value("metric-good", "success")
        before_failure = _value("metric-bad", "failure")

        dispatcher.send_to_sink(good_sink, _make_finding(), False)
        dispatcher.send_to_sink(bad_sink, _make_finding(), False)

        assert _value("metric-good", "success") == before_success + 1
        assert _value("metric-bad", "failure") == before_failure + 1

    def test_missing_sink_is_metered_not_silent(self):
        SinkDispatcher.report_missing_sink("removed-sink", 2)
        value = sink_send_counter.labels("removed-sink", "sink_not_found")._value.get()
        assert value >= 1

    def test_max_attempts_floored_to_one(self):
        sink = _RecordingSink("once", failures=99)
        dispatcher = SinkDispatcher(max_attempts=0, retry_backoff_sec=0, sleep_fn=lambda _: None)
        assert dispatcher.send_to_sink(sink, _make_finding(), False) is False
        assert sink.calls == 1
