import asyncio
import subprocess
from collections.abc import AsyncGenerator
from dataclasses import dataclass
from pathlib import Path
from tarfile import TarFile
from tempfile import TemporaryDirectory
from uuid import uuid4

import pytest
import pytest_asyncio
from astropy.config import get_cache_dir_path  # type: ignore[import-untyped]
from astropy.utils.data import get_readable_fileobj  # type: ignore[import-untyped]
from confluent_kafka import Consumer, Producer
from confluent_kafka.admin import AdminClient
from confluent_kafka.aio import AIOConsumer, AIOProducer
from rich.status import Status

__all__ = (
    "kafka_broker",
    "KafkaBrokerContext",
)


SCALA_VERSION = "2.13"
KAFKA_VERSION = "4.1.1"


async def wait_port(port: int, timeout: float = 0.25) -> None:
    """Wait until a connection is detected listening on the given port."""
    while True:
        try:
            _, writer = await asyncio.open_connection("localhost", port)
        except OSError:
            await asyncio.sleep(timeout)
        else:
            writer.close()
            await writer.wait_closed()
            return


@pytest.fixture(scope="session")
def kafka_home() -> Path:
    """Download and install Kafka into a cached directory.

    Returns the path where Kafka is installed.
    """
    dirname = f"kafka_{SCALA_VERSION}-{KAFKA_VERSION}"
    cache_path = get_cache_dir_path() / __package__
    dest_path = cache_path / dirname
    if not dest_path.exists():
        dest_path.mkdir(parents=True, exist_ok=True)
        with (
            Status("Downloading Kafka"),
            get_readable_fileobj(
                f"https://dlcdn.apache.org/kafka/{KAFKA_VERSION}/{dirname}.tgz",
                encoding="binary",
                cache=True,
            ) as download,
            TarFile(fileobj=download) as tarfile,
            TemporaryDirectory(dir=cache_path) as temp_dir,
        ):
            tarfile.extractall(temp_dir)
            (Path(temp_dir) / dirname).rename(dest_path)
    return dest_path


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


@pytest_asyncio.fixture
async def kafka_broker(
    kafka_home, tmp_path, unused_tcp_port_factory
) -> AsyncGenerator[KafkaBrokerContext]:
    """Pytest fixture to run a local, temporary Kafka broker."""
    kafka_storage = kafka_home / "bin" / "kafka-storage.sh"
    kafka_server_start = kafka_home / "bin" / "kafka-server-start.sh"
    config_path = tmp_path / "server.properties"
    data_path = tmp_path / "run"
    data_path.mkdir()
    log_path = tmp_path / "log"
    log_path.mkdir()
    env = {"LOG_DIR": str(log_path)}
    plaintext_port = unused_tcp_port_factory()
    controller_port = unused_tcp_port_factory()
    config_path.write_text(
        f"""
        process.roles=broker,controller
        node.id=1
        controller.quorum.bootstrap.servers=127.0.0.1:{controller_port}
        listeners=PLAINTEXT://127.0.0.1:{plaintext_port},CONTROLLER://127.0.0.1:{controller_port}
        controller.listener.names=CONTROLLER
        listener.security.protocol.map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
        log.dirs={data_path}
        num.recovery.threads.per.data.dir=1
        offsets.topic.replication.factor=1
        share.coordinator.state.topic.replication.factor=1
        share.coordinator.state.topic.min.isr=1
        transaction.state.log.replication.factor=1
        transaction.state.log.min.isr=1
        """
    )
    with Status("Starting Kafka broker"):
        subprocess.run(
            [
                kafka_storage,
                "format",
                "--standalone",
                "-t",
                str(uuid4()),
                "-c",
                config_path,
            ],
            env=env,
            check=True,
            stdout=subprocess.DEVNULL,
        )
        process = await asyncio.create_subprocess_exec(
            kafka_server_start,
            config_path,
            env=env,
            stdin=None,
            stdout=subprocess.DEVNULL,
            stderr=None,
        )
    with Status(f"Waiting for connection on port {plaintext_port}"):
        exited = asyncio.create_task(process.wait())
        port = asyncio.create_task(wait_port(plaintext_port))
        done, _ = await asyncio.wait(
            (exited, port), return_when=asyncio.FIRST_COMPLETED
        )
        if exited in done:
            port.cancel()
            raise RuntimeError("Kafka broker terminated unexpectedly")
    try:
        yield KafkaBrokerContext(f"127.0.0.1:{plaintext_port}")
    finally:
        with Status("Stopping Kafka broker"):
            process.terminate()
            await exited
