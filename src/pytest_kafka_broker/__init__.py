from dataclasses import dataclass

from confluent_kafka import Consumer, Producer
from confluent_kafka.admin import AdminClient
from confluent_kafka.aio import AIOConsumer, AIOProducer

from .version import __version__  # noqa: F401

__all__ = ("KafkaBrokerContext",)

_doc = """{}

Parameters
----------
config
    Extra Kafka client configuration properties. See list in the
    `librdkafka documentation <https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md>`_.
"""


@dataclass
class KafkaBrokerContext:
    """Information and convenience methods for a temporary Kafka cluster.

    This object is returned by :func:`kafka_broker`.
    """

    bootstrap_server: str
    """Kafka bootstrap server in the form :samp:`{host}:{port}`."""

    def config(self, config: dict | None = None) -> dict:
        return {**(config or {}), "bootstrap.servers": self.bootstrap_server}

    def admin(self, config: dict | None = None) -> AdminClient:
        return AdminClient(self.config(config))

    def producer(self, config: dict | None = None) -> Producer:
        return Producer(self.config(config))

    def consumer(self, config: dict | None = None) -> Consumer:
        return Consumer(self.config(config))

    def aio_producer(self, config: dict | None = None) -> AIOProducer:
        return AIOProducer(self.config(config))

    def aio_consumer(self, config: dict | None = None) -> AIOConsumer:
        return AIOConsumer(self.config(config))

    config.__doc__ = _doc.format("Get the configuration for a Kafka client.")
    admin.__doc__ = _doc.format("Create a Kafka admin client connected to the cluster.")
    producer.__doc__ = _doc.format("Create a Kafka producer connected to the cluster.")
    consumer.__doc__ = _doc.format("Create a Kafka consumer connected to the cluster.")
    aio_producer.__doc__ = _doc.format(
        "Create an asynchronous Kafka producer connected to the cluster."
    )
    aio_consumer.__doc__ = _doc.format(
        "Create an asynchronous Kafka consumer connected to the cluster."
    )


del _doc
