from unittest.mock import Mock, patch

import pytest

from robusta.core.reporting import Finding
from robusta.core.sinks.sink_base import SinkBase
from robusta.core.sinks.sink_base_params import GroupingParams, SinkBaseParams
from robusta.model.config import SinksRegistry


class _TestSinkParams(SinkBaseParams):
    @classmethod
    def _supports_grouping(cls):
        return True

    @classmethod
    def _get_sink_type(cls):
        return "test_sink"


class _TestSink(SinkBase):
    def write_finding(self, finding: Finding, platform_enabled: bool):
        pass


def _registry():
    return Mock(get_global_config=lambda: {"account_id": "", "cluster_name": "", "signing_key": ""})


def _sink(name: str, grouping: GroupingParams = None) -> _TestSink:
    params = _TestSinkParams(name=name, grouping=grouping)
    return _TestSink(sink_params=params, registry=_registry())


def _regular_grouping(ignore_first: int = 3) -> GroupingParams:
    return GroupingParams(
        group_by=["cluster"],
        interval=300,
        notification_mode={"regular": {"ignore_first": ignore_first}},
    )


def _summary_grouping() -> GroupingParams:
    return GroupingParams(
        group_by=["cluster"],
        interval=300,
        notification_mode={"summary": {"threaded": True, "by": ["identifier"]}},
    )


class TestGroupingStateMigration:
    def test_regular_mode_state_migrated_when_grouping_unchanged(self):
        old_sink = _sink("slack-main", _regular_grouping(ignore_first=3))
        group_key = ("cluster-1",)
        # two notifications already accounted, below the ignore_first threshold
        old_sink.groups[group_key].register_notification(interval=300, threshold=3)
        old_sink.groups[group_key].register_notification(interval=300, threshold=3)

        new_sink = _sink("slack-main", _regular_grouping(ignore_first=3))
        assert old_sink.migrate_grouping_state(new_sink) is True
        assert new_sink.groups[group_key].timestamps == old_sink.groups[group_key].timestamps

        # threshold accounting survives the reload: the third notification reaches the limit
        should_send = new_sink.groups[group_key].register_notification(interval=300, threshold=3)
        assert should_send is True

    def test_summary_mode_state_migrated(self):
        old_sink = _sink("slack-main", _summary_grouping())
        new_sink = _sink("slack-main", _summary_grouping())

        assert old_sink.migrate_grouping_state(new_sink) is True
        assert new_sink.summaries is old_sink.summaries
        assert new_sink.grouping_summary_mode is True

    def test_state_not_migrated_when_grouping_config_changes(self):
        old_sink = _sink("slack-main", _regular_grouping(ignore_first=3))
        old_sink.groups[("cluster-1",)].register_notification(interval=300, threshold=3)

        new_sink = _sink("slack-main", _regular_grouping(ignore_first=5))
        assert old_sink.migrate_grouping_state(new_sink) is False
        # fresh state: thresholds changed, old accounting must not leak through
        assert all(group.timestamps == [] for group in new_sink.groups.values())

    def test_no_migration_between_different_grouping_modes(self):
        old_sink = _sink("slack-main", _regular_grouping())
        new_sink = _sink("slack-main", _summary_grouping())
        assert old_sink.migrate_grouping_state(new_sink) is False

    def test_no_migration_when_grouping_disabled(self):
        old_sink = _sink("slack-main", None)
        new_sink = _sink("slack-main", None)
        assert old_sink.migrate_grouping_state(new_sink) is False

    def test_migrated_sink_keeps_lock_guarded_state(self):
        """Migration must not break concurrent register_notification on the new sink."""
        import threading

        old_sink = _sink("slack-main", _regular_grouping(ignore_first=100))
        new_sink = _sink("slack-main", _regular_grouping(ignore_first=100))
        assert old_sink.migrate_grouping_state(new_sink) is True

        errors = []

        def hammer():
            try:
                for _ in range(50):
                    with new_sink.finding_group_lock:
                        new_sink.groups[("c",)].register_notification(interval=300, threshold=100)
            except Exception as e:  # pragma: no cover - failure path
                errors.append(e)

        threads = [threading.Thread(target=hammer) for _ in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert not errors
        assert len(new_sink.groups[("c",)].timestamps) == 200


class _FakeSinkConfigWrapper:
    """Stand-in for a SinkConfigBase in SinksRegistry.construct_new_sinks."""

    def __init__(self, name: str, params: SinkBaseParams):
        self._name = name
        self._params = params

    def get_name(self) -> str:
        return self._name

    def get_params(self) -> SinkBaseParams:
        return self._params


class TestSinkRegistryReload:
    def test_reload_preserves_grouping_state_across_sink_rebuild(self):
        grouping = _regular_grouping(ignore_first=3)
        old_params = _TestSinkParams(name="routing-sink", grouping=grouping, match={"scope": "v1"})
        old_sink = _TestSink(sink_params=old_params, registry=_registry())
        old_sink.groups[("cluster-1",)].register_notification(interval=300, threshold=3)

        # same grouping, different unrelated param -> sink is rebuilt, state must survive
        new_params = _TestSinkParams(name="routing-sink", grouping=grouping, match={"scope": "v2"})
        new_sink = _TestSink(sink_params=new_params, registry=_registry())
        wrapper = _FakeSinkConfigWrapper("routing-sink", new_params)

        with patch("robusta.model.config.SinkFactory.create_sink", return_value=new_sink):
            rebuilt, has_errors = SinksRegistry.construct_new_sinks(
                new_sinks_config=[wrapper],
                existing_sinks={"routing-sink": old_sink},
                registry=_registry(),
            )

        assert has_errors is False
        assert rebuilt["routing-sink"] is new_sink
        assert len(rebuilt["routing-sink"].groups[("cluster-1",)].timestamps) == 1

    def test_reload_with_changed_grouping_resets_state(self):
        old_sink = _sink("routing-sink", _regular_grouping(ignore_first=3))
        old_sink.groups[("cluster-1",)].register_notification(interval=300, threshold=3)

        new_params = _TestSinkParams(name="routing-sink", grouping=_regular_grouping(ignore_first=5))
        new_sink = _TestSink(sink_params=new_params, registry=_registry())
        wrapper = _FakeSinkConfigWrapper("routing-sink", new_params)

        with patch("robusta.model.config.SinkFactory.create_sink", return_value=new_sink):
            rebuilt, _ = SinksRegistry.construct_new_sinks(
                new_sinks_config=[wrapper],
                existing_sinks={"routing-sink": old_sink},
                registry=_registry(),
            )

        assert all(group.timestamps == [] for group in rebuilt["routing-sink"].groups.values())

    def test_reload_migration_failure_does_not_abort_reload(self):
        grouping = _regular_grouping(ignore_first=3)
        old_sink = _TestSink(
            sink_params=_TestSinkParams(name="routing-sink", grouping=grouping, match={"scope": "v1"}),
            registry=_registry(),
        )
        # unrelated param change forces the rebuild path while grouping stays identical
        new_params = _TestSinkParams(name="routing-sink", grouping=grouping, match={"scope": "v2"})
        new_sink = _TestSink(sink_params=new_params, registry=_registry())
        wrapper = _FakeSinkConfigWrapper("routing-sink", new_params)

        with patch("robusta.model.config.SinkFactory.create_sink", return_value=new_sink), \
             patch.object(SinkBase, "migrate_grouping_state", side_effect=RuntimeError("boom")):
            rebuilt, has_errors = SinksRegistry.construct_new_sinks(
                new_sinks_config=[wrapper],
                existing_sinks={"routing-sink": old_sink},
                registry=_registry(),
            )

        # reload still completes with the fresh sink; grouping state simply starts empty
        assert rebuilt["routing-sink"] is new_sink
        assert has_errors is False
