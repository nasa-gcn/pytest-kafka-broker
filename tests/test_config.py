import pytest
from confluent_kafka.admin import ConfigResource, ResourceType

from pytest_kafka_broker import KafkaBrokerContext


def test_kafka_broker_extra_config(kafka_broker: KafkaBrokerContext):
    with kafka_broker.admin() as admin:
        (future,) = admin.describe_configs(
            [ConfigResource(ResourceType.BROKER, name="1")]
        ).values()
        assert future.result()["message.max.bytes"].value == "123456"


def test_kafka_enabled(pytester: pytest.Pytester):
    pytester.makepyfile("def test_example(kafka_broker): pass")
    result = pytester.runpytest()
    result.assert_outcomes(passed=0, skipped=1)
