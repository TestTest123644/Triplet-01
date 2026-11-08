"""Decision rules: one parsed message in, the MCP actions to take out.

Pure apart from the repeat counter, so it is tested without Kafka or MCP. The
rules table is the one in issue #116, mapped onto Guardian's ``Anomalies`` labels.
"""

from collections import defaultdict
from collections.abc import Callable
from datetime import datetime, timedelta

from guardian_layer.consumer_producer.action_taker.schema import (
    Action,
    AnomalyRecord,
    DlqRecord,
    ToolName,
)
from guardian_layer.utils.logger import get_logger
from guardian_layer.utils.schema import Anomalies

logger = get_logger(__name__)

DlqRule = Callable[[DlqRecord], list[Action]]
AnomalyRule = Callable[[AnomalyRecord], list[Action]]


class RepeatCounter:
    """How often one device hit one signal within a sliding time window.

    shortcut: kept in memory, so it is lost on restart and counted per instance;
    move it to shared state if more than one action_taker instance ever runs.
    """

    def __init__(self, threshold: int, window: timedelta):
        self._threshold = threshold
        self._window = window
        self._hits: defaultdict[tuple[str, str], list[datetime]] = defaultdict(list)

    def hit(self, key: tuple[str, str], at: datetime) -> bool:
        """Record one hit at ``at`` (message time); True once the threshold is met.

        The window ends at the newest hit seen, so a late message with an old
        timestamp neither revives expired hits nor counts itself.
        """
        hits = self._hits[key]
        hits.append(at)
        cutoff = max(hits) - self._window
        hits[:] = [moment for moment in hits if moment >= cutoff]
        return len(hits) >= self._threshold


class Rules:
    def __init__(self, counter: RepeatCounter):
        self._counter = counter
        # DLQ codes by class (first two characters): E2xx forged or unknown
        # source, E3xx schema errors (E300, E320), E4xx validation errors
        self._dlq_rules: dict[str, DlqRule] = {
            "E2": self._forged_source,
            "E3": self._repeated_invalid,
            "E4": self._repeated_invalid,
        }
        self._anomaly_rules: dict[Anomalies | None, AnomalyRule] = {
            Anomalies.SPOOFED_DATA: self._spoofed,
            Anomalies.NOT_SENSOR_DATA: self._spoofed,
            Anomalies.OUT_OF_RANGE: self._faulty,
            Anomalies.SENSOR_MALFUNCTION: self._faulty,
            Anomalies.SENSOR_IS_DOWN: self._faulty,
            None: self._recovered,
        }

    def decide(self, record: DlqRecord | AnomalyRecord) -> list[Action]:
        """The actions for one message; [] when no rule matches."""
        match record:
            case DlqRecord():
                dlq_rule = self._dlq_rules.get(record.error_code[:2])
                if dlq_rule is None:
                    logger.debug("no rule for DLQ code %s", record.error_code)
                    return []
                return dlq_rule(record)
            case AnomalyRecord():
                anomaly_rule = self._anomaly_rules.get(record.label)
                if anomaly_rule is None:
                    logger.debug("no rule for anomaly label %s", record.label)
                    return []
                return anomaly_rule(record)

    def _forged_source(self, record: DlqRecord) -> list[Action]:
        if record.device_id is None:
            return []
        actions = [Action(ToolName.ISOLATE_DEVICE, record.device_id)]
        if record.reading_id is not None:
            actions.append(Action(ToolName.ROLLBACK_READING, record.reading_id))
        return actions

    def _repeated_invalid(self, record: DlqRecord) -> list[Action]:
        if record.device_id is None:
            return []
        if self._counter.hit(("invalid", record.device_id), record.failed_at):
            return [Action(ToolName.QUARANTINE_DEVICE, record.device_id)]
        return []

    def _spoofed(self, record: AnomalyRecord) -> list[Action]:
        actions: list[Action] = []
        if self._counter.hit(("spoofed", record.device_id), record.detected_at):
            actions.append(Action(ToolName.ISOLATE_DEVICE, record.device_id))
        if record.reading_id is not None:
            actions.append(Action(ToolName.ROLLBACK_READING, record.reading_id))
        return actions

    def _faulty(self, record: AnomalyRecord) -> list[Action]:
        return [Action(ToolName.QUARANTINE_DEVICE, record.device_id)]

    def _recovered(self, record: AnomalyRecord) -> list[Action]:
        # the executor checks the device is isolated or quarantined before calling
        return [Action(ToolName.REINSTATE_DEVICE, record.device_id)]
