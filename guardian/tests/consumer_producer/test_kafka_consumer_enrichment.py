import json
from datetime import UTC, datetime

from guardian_layer.consumer_producer.kafka_consumer.jobs.functions import (
    EnrichAndTransform,
    TransformTwin,
)
from guardian_layer.data_transformer.transformer.twin_v1 import (
    TwinToGuardian,
)
from guardian_layer.models.sensor import Metadata, Sensor


class MockContext:
    pass


def test_twin_to_guardian_with_device_metadata_enrichment():
    payload = {
        "reading_id": "r-z2-wl2-1",
        "device_id": "z2-wl2",
        "device_type": "water_level",
        "ts": "2026-10-17T12:00:00Z",
        "seq": 1,
        "value": 2.8,
        "unit": "meters",
        "location": "zone-2",
        "status": "ok",
        "device_metadata": {
            "operation": "upsert",
            "device_id": "z2-wl2",
            "zone_id": "zone-2",
            "device_type": "water_level",
            "unit": "meters",
            "lat": 36.8185,
            "lon": 10.305,
            "min_val": 0.0,
            "max_val": 12.0,
        },
    }

    transformer = TwinToGuardian()
    sensor = transformer.transform(payload)

    assert sensor == Sensor(
        sensor_id="z2-wl2",
        cluster_id="water_level",
        value=2.8,
        unit="m",  # normalized from meters
        timestamp=datetime(2026, 10, 17, 12, 0, 0, tzinfo=UTC),
        reading_id="r-z2-wl2-1",
        status="ok",
        metadata=Metadata(
            lat=36.8185,
            lon=10.305,
            packet_order=1,
            type="water_level",
            lower_range=0.0,
            upper_range=12.0,
            zone_id="zone-2",
        ),
    )


def test_twin_to_guardian_without_metadata_still_succeeds():
    payload = {
        "reading_id": "r-z2-wl2-1",
        "device_id": "z2-wl2",
        "device_type": "water_level",
        "ts": "2026-10-17T12:00:00Z",
        "seq": 1,
        "value": 2.8,
        "unit": "meters",
        "location": "zone-2",
        "status": "ok",
    }

    transformer = TwinToGuardian()
    sensor = transformer.transform(payload)

    assert sensor.sensor_id == "z2-wl2"
    assert sensor.cluster_id == "water_level"
    assert sensor.value == 2.8
    assert sensor.unit == "m"
    assert sensor.reading_id == "r-z2-wl2-1"
    assert sensor.status == "ok"
    assert sensor.metadata is not None
    assert sensor.metadata.zone_id == "zone-2"
    assert sensor.metadata.packet_order == 1
    assert sensor.metadata.type == "water_level"


def test_enrich_and_transform_process_function():
    payload = {
        "reading_id": "r-z2-wl2-1",
        "device_id": "z2-wl2",
        "device_type": "water_level",
        "ts": "2026-10-17T12:00:00Z",
        "seq": 1,
        "value": 2.8,
        "unit": "meters",
        "location": "zone-2",
        "status": "ok",
        "device_metadata": {
            "operation": "upsert",
            "device_id": "z2-wl2",
            "zone_id": "zone-2",
            "device_type": "water_level",
            "unit": "meters",
            "lat": 36.8185,
            "lon": 10.305,
            "min_val": 0.0,
            "max_val": 12.0,
        },
    }

    fn = EnrichAndTransform()
    fn.open(None)
    out = list(fn.process_element(json.dumps(payload), MockContext()))

    assert len(out) == 1
    sensor_json = out[0]
    sensor = Sensor.model_validate_json(sensor_json)
    assert sensor.sensor_id == "z2-wl2"
    assert sensor.cluster_id == "water_level"
    assert sensor.reading_id == "r-z2-wl2-1"
    assert sensor.status == "ok"
    assert sensor.metadata is not None
    assert sensor.metadata.zone_id == "zone-2"
    assert sensor.metadata.lower_range == 0.0
    assert sensor.metadata.upper_range == 12.0

    # Step 5: pass through TransformTwin
    step5 = TransformTwin()
    step5.open(None)
    out5 = list(step5.process_element(sensor_json, MockContext()))
    assert len(out5) == 1
    sensor5 = Sensor.model_validate_json(out5[0])
    assert sensor5 == sensor


def test_quarantined_reading_is_accepted_with_status():
    payload = {
        "reading_id": "r-z2-wl2-1",
        "device_id": "z2-wl2",
        "device_type": "water_level",
        "ts": "2026-10-17T12:00:00Z",
        "seq": 1,
        "value": 2.8,
        "unit": "meters",
        "location": "zone-2",
        "status": "quarantined",
    }

    fn = EnrichAndTransform()
    fn.open(None)
    out = list(fn.process_element(json.dumps(payload), MockContext()))
    assert len(out) == 1
    sensor = json.loads(out[0])
    assert sensor["status"] == "quarantined"
