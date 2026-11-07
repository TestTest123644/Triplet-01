import datetime
import logging

import pytest

from guardian_layer.data_transformer.factory_transformer import TransformerFactory
from guardian_layer.data_transformer.interface.factory_transformer_interface import (
    TransformerInterface,
)
from guardian_layer.data_transformer.transformer import TwinToGuardian
from guardian_layer.utils.schema import Sensor

LOGGER = "guardian_layer.data_transformer.factory_transformer"


@pytest.fixture
def factory_log(caplog):
    # the guardian_layer logger does not propagate: caplog's root handler sees nothing
    module_logger = logging.getLogger(LOGGER)
    module_logger.addHandler(caplog.handler)
    caplog.set_level(logging.DEBUG, logger=LOGGER)
    yield caplog
    module_logger.removeHandler(caplog.handler)


@pytest.fixture
def registry(monkeypatch):
    # a copy, so a test that registers a class leaves the real registry untouched
    copy = dict(TransformerFactory.registry)
    monkeypatch.setattr(TransformerFactory, "registry", copy)
    return copy


def test_names_lists_twin_v1():
    assert "twin_v1" in TransformerFactory.names()


def test_get_returns_the_class():
    assert TransformerFactory.get("twin_v1") is TwinToGuardian


def test_create_returns_a_new_instance(factory_log):
    first = TransformerFactory.create("twin_v1")
    assert isinstance(first, TwinToGuardian)
    assert first is not TransformerFactory.create("twin_v1")
    assert "transformer created: twin_v1 -> TwinToGuardian" in factory_log.text


def test_unknown_name_raises_and_logs(factory_log):
    with pytest.raises(ValueError, match="unknown transformer: twin_v9"):
        TransformerFactory.create("twin_v9")
    assert factory_log.records[-1].levelno == logging.ERROR


def test_register_adds_a_new_transformer(registry):
    @TransformerFactory.register("fake_v1")
    class Fake(TransformerInterface):
        def transform(self, payload):
            return Sensor(
                sensor_id="fake",
                cluster_id="fake",
                value=0,
                unit="none",
                timestamp=datetime.datetime.now(datetime.UTC),
            )

    assert registry["fake_v1"] is Fake
    assert isinstance(TransformerFactory.create("fake_v1"), Fake)


def test_name_taken_by_another_class_raises(registry):
    with pytest.raises(ValueError, match="already registered: twin_v1"):

        @TransformerFactory.register("twin_v1")
        class Other(TransformerInterface):
            def transform(self, payload):
                return Sensor(
                    sensor_id="fake",
                    cluster_id="fake",
                    value=0,
                    unit="none",
                    timestamp=datetime.datetime.now(datetime.UTC),
                )


def test_same_class_again_replaces_it(registry):
    # a module reload registers the same class again: it is not a clash
    assert TransformerFactory.register("twin_v1")(TwinToGuardian) is TwinToGuardian
