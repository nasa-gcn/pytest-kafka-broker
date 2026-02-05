import asyncio
import subprocess
from collections.abc import AsyncGenerator
from errno import EADDRINUSE
from pathlib import Path
from socket import socket
from tarfile import TarFile
from tempfile import TemporaryDirectory
from uuid import uuid4

import pytest
import pytest_asyncio
from astropy.config import get_cache_dir_path  # type: ignore[import-untyped]
from astropy.utils.data import get_readable_fileobj  # type: ignore[import-untyped]
from rich.status import Status

from . import KafkaBrokerContext

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


def _unused_tcp_port(default: int = 0) -> int:
    with socket() as sock:
        try:
            sock.bind(("127.0.0.1", default))
        except OSError as e:
            if e.errno != EADDRINUSE:
                raise
            sock.bind(("127.0.0.1", 0))
        _, port = sock.getsockname()
    return port


@pytest.fixture
def find_unused_tcp_port():
    """Unused TCP port factory.

    This is similar to `unused_tcp_port_factory` from pytest_asyncio, but it
    supports a default port argument, and is not session-scoped.
    """
    used = set()

    def factory(default: int = 0) -> int:
        while (port := _unused_tcp_port(default)) in used:
            pass
        used.add(port)
        return port

    return factory


@pytest_asyncio.fixture
async def kafka_broker(
    kafka_home, tmp_path, find_unused_tcp_port, pytestconfig
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
    plaintext_port = find_unused_tcp_port(9092)
    controller_port = find_unused_tcp_port(9093)
    extra_config = "\n".join(pytestconfig.getini("kafka_broker_extra_config"))
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
        {extra_config}
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
        bootstrap_server = f"127.0.0.1:{plaintext_port}"
        print(f"Kafka broker running at {bootstrap_server}")
        yield KafkaBrokerContext(bootstrap_server)
    finally:
        with Status("Stopping Kafka broker"):
            try:
                process.terminate()
            except ProcessLookupError:
                pass  # Process has already terminated
            await exited


def pytest_addoption(parser: pytest.Parser):
    parser.addini(
        "kafka_broker_extra_config",
        type="linelist",
        help="Extra broker configuration settings. See https://kafka.apache.org/41/configuration/broker-configs/",
    )
