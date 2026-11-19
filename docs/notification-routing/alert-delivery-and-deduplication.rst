.. _alert-delivery-and-deduplication:

Alert Deduplication, Channel Failures and Rule Reloads
=========================================================

This page explains what happens inside the runner between an AlertManager
webhook and a sink notification, and how Robusta avoids duplicate or lost
notifications when redelivered alerts, failing channels and configuration
reloads overlap.

The Alert Pipeline
------------------

Every alert posted to ``/api/alerts`` goes through these stages::

    AlertManager webhook
        |
        v
    label relabeling (alert_relabel)
        |
        v
    core ingress deduplication (AlertDeduplicator)   <-- one playbook run per distinct alert
        |
        v
    alerts queue (worker pool)
        |
        v
    playbooks / triggers (PrometheusAlertTrigger)
        |
        v
    findings routed to named sinks
        |
        v
    sink dispatch with bounded retries (SinkDispatcher)

Deduplication happens **in the core, before playbooks run** - not inside
individual playbooks. This keeps playbooks stateless: a playbook can assume it
is invoked exactly once per distinct alert occurrence, and the guarantee does
not change when playbook or sink rules are reloaded.

How duplicate alerts are suppressed
-----------------------------------

AlertManager redelivers every alert on each group flush, even when nothing
changed. An alert occurrence is identified by a hash of:

* the AlertManager ``fingerprint`` (the alerting rule and its label set),
* ``status`` (``firing`` vs ``resolved``),
* ``startsAt`` and ``endsAt``.

Two states are tracked per hash:

* **in-flight** - the alert was accepted and handed to the alert queue but has
  not finished processing. A redelivery arriving in this window is suppressed
  (decision ``in_flight``), so the same alert can never run its playbooks
  concurrently.
* **processed** - the alert was queued successfully. Its hash is kept in a TTL
  cache and further redeliveries are suppressed (decision ``duplicate``).

If the alert queue is full and the alert cannot be queued, the in-flight marker
is released *without* recording the alert as processed. The next AlertManager
redelivery is then accepted and retried, instead of the alert being silently
dropped for the remainder of the TTL window.

A firing alert and its resolution, or a re-fired alert with a new
``startsAt``, produce different hashes and are always processed as separate
occurrences.

Channel failures
----------------

Each finding is delivered to its sinks through a shared dispatcher that:

* retries transient sink failures a bounded number of times with linear
  backoff (the same finding is retried - retries never create extra
  notifications);
* isolates sinks from each other - one broken channel never blocks delivery to
  the other sinks;
* records a Prometheus counter for every delivery, so silent channel failures
  are observable instead of being only logged.

The relevant counters (exposed on ``/metrics``) are:

.. list-table::
   :header-rows: 1
   :widths: 40 60

   * - Metric
     - Meaning
   * - ``alerts_dedup_decisions_total{decision}``
     - Incoming alerts per decision: ``accept``, ``duplicate`` or ``in_flight``.
   * - ``alerts_in_flight``
     - Alerts queued for processing but not yet completed.
   * - ``sink_findings_send_total{sink_name,result}``
     - Sink deliveries by result: ``success``, ``failure`` or ``sink_not_found``.

A ``sink_not_found`` result means a finding was routed to a sink that a
concurrent configuration reload removed or renamed. The finding is metered and
logged rather than silently dropped.

Rule reloads
------------

Reloading playbooks or sink configuration (via ``/api/playbooks/reload`` or a
config file change) swaps the sink registry while alert workers keep running.
Two rules keep the state consistent:

1. Ingress dedup state is independent of the configuration. Reloading rules
   never clears the processed/in-flight cache, so an alert storm straddling a
   reload is still deduplicated exactly once.
2. When a sink is rebuilt because an unrelated parameter changed, its in-memory
   notification grouping state (``grouping`` counters, summary tables and
   thread identifiers) is migrated onto the new sink instance **as long as the
   grouping configuration itself is unchanged**. ``ignore_first`` thresholds
   therefore survive a reload instead of resetting and re-notifying. If the
   grouping configuration changed, the state intentionally starts fresh
   because the old thresholds no longer apply.

Core-side aggregation vs. playbook-side deduplication
-----------------------------------------------------

Robusta deliberately splits responsibilities this way:

* **Core (ingress) deduplication** answers "is this alert occurrence new?". It
  is global, configuration-independent and happens before any playbook code
  runs, so every playbook - built-in or custom - gets exactly-once invocation
  semantics without having to maintain its own state.
* **Sink-side grouping** answers "how should notifications for this alert be
  presented?". Thresholds such as ``ignore_first`` and summary threads are
  per-sink presentation policy (see :doc:`notification-grouping`) and stay on
  the sink, which is also why grouping state is preserved across reloads.

Putting dedup logic inside playbooks instead would duplicate state across every
playbook, couple it to reloads, and make custom playbooks re-implement the same
concurrency handling. Playbooks remain focused on enrichment and routing; they
consume the unchanged :class:`~robusta.integrations.prometheus.trigger.PrometheusTriggerEvent`
/ :class:`~robusta.integrations.prometheus.models.PrometheusKubernetesAlert`
interface and need no changes.

Tuning
------

The deduplication window and sink retry behavior can be tuned with runner
environment variables:

.. list-table::
   :header-rows: 1
   :widths: 32 48 12

   * - Variable
     - Purpose
     - Default
   * - ``PROCESSED_ALERT_CACHE_TTL``
     - Seconds to remember a processed alert hash.
     - ``7200``
   * - ``PROCESSED_ALERTS_CACHE_MAX_SIZE``
     - Max processed-alert hashes kept in memory.
     - ``100000``
   * - ``SINK_SEND_MAX_ATTEMPTS``
     - Total delivery attempts per finding per sink.
     - ``3``
   * - ``SINK_SEND_RETRY_BACKOFF_SEC``
     - Linear backoff base in seconds (attempt N waits ``backoff * N`` before retrying).
     - ``1.0``

Like all in-memory runner state, the caches reset if the runner restarts;
AlertManager redeliveries after a restart are processed normally.
