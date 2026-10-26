import asyncio

import pytest

# Note that these tests rely on the fact that `unused_tcp_port` from
# pytest-asyncio and `find_unused_tcp_port` from this package have separate
# lists of recently generated ports.


@pytest.mark.asyncio
async def test_find_unused_tcp_port_default_in_use(
    find_unused_tcp_port, unused_tcp_port
):
    server = await asyncio.start_server(lambda: None, "127.0.0.1", unused_tcp_port)
    try:
        port = find_unused_tcp_port(unused_tcp_port)
        assert port != unused_tcp_port
    finally:
        server.close()
        await server.wait_closed()


@pytest.mark.asyncio
async def test_find_unused_tcp_port_default_not_in_use(
    find_unused_tcp_port, unused_tcp_port
):
    port_1 = find_unused_tcp_port(unused_tcp_port)
    port_2 = find_unused_tcp_port(unused_tcp_port)
    assert port_1 == unused_tcp_port
    assert port_1 != port_2
