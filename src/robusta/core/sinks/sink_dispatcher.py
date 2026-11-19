"""Dispatch findings to sinks with bounded failure handling.

Sending a finding is best-effort per sink: one sink failing must never prevent
delivery to the others. Previously a sink exception was only logged and the
finding was lost, with no metric and no retry for transient failures (rate
limits, short network blips). This centralizes the delivery policy so every
sink gets:

* a bounded number of attempts with small backoff (configurable via
  SINK_SEND_MAX_ATTEMPTS / SINK_SEND_RETRY_BACKOFF_SEC);
* Prometheus counters for success/failure, making silent channel failures
  visible;
* the same per-sink exception isolation the runner relied on before.

Grouping sinks (e.g. Slack with ``grouping``) update in-memory notification
accounting inside ``write_finding``. Retries therefore call the same
``write_finding`` again - the sink's own group lock keeps the accounting
consistent, and a finding whose every attempt fails is not counted as a
delivered notification.
"""

import logging
import time

import prometheus_client

from robusta.core.model.env_vars import SINK_SEND_MAX_ATTEMPTS, SINK_SEND_RETRY_BACKOFF_SEC
from robusta.core.reporting.base import Finding
from robusta.core.sinks.sink_base import SinkBase

sink_send_counter = prometheus_client.Counter(
    "sink_findings_send",
    "Number of finding delivery attempts to sinks, by sink name and result.",
    labelnames=("sink_name", "result"),
)


class SinkDispatcher:
    def __init__(
        self,
        max_attempts: int = SINK_SEND_MAX_ATTEMPTS,
        retry_backoff_sec: float = SINK_SEND_RETRY_BACKOFF_SEC,
        sleep_fn=time.sleep,
    ):
        self.max_attempts = max(1, max_attempts)
        self.retry_backoff_sec = retry_backoff_sec
        self._sleep = sleep_fn

    def send_to_sink(self, sink: SinkBase, finding: Finding, platform_enabled: bool) -> bool:
        """Send one finding to one sink with bounded retries.

        Returns True on success, False if every attempt failed. Never raises:
        a broken sink cannot break dispatch to the remaining sinks.
        """
        for attempt in range(1, self.max_attempts + 1):
            try:
                sink.write_finding(finding, platform_enabled)
                sink_send_counter.labels(sink.sink_name, "success").inc()
                return True
            except Exception:
                if attempt >= self.max_attempts:
                    logging.exception(
                        "Failed to send finding %s to sink %s after %d attempt(s)",
                        finding.aggregation_key,
                        sink.sink_name,
                        attempt,
                    )
                    sink_send_counter.labels(sink.sink_name, "failure").inc()
                    return False
                logging.warning(
                    "Sending finding %s to sink %s failed (attempt %d/%d); retrying",
                    finding.aggregation_key,
                    sink.sink_name,
                    attempt,
                    self.max_attempts,
                    exc_info=True,
                )
                if self.retry_backoff_sec > 0:
                    self._sleep(self.retry_backoff_sec * attempt)
        return False

    @staticmethod
    def report_missing_sink(sink_name: str, num_findings: int) -> None:
        """Record findings routed to a sink absent from the registry.

        Happens when a config reload removes/renames a sink while findings for
        it are already in flight. Logged and metered instead of silently lost.
        """
        logging.error(
            "sink %s not found. Skipping %d finding(s) (removed or reloaded while in flight?)",
            sink_name,
            num_findings,
        )
        sink_send_counter.labels(sink_name, "sink_not_found").inc()
