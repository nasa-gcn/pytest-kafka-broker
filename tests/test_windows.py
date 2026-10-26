from contextlib import ExitStack

import pytest

from pytest_kafka_broker.plugin import WINDOWS


@pytest.mark.skipif(not WINDOWS, reason="This test only works on Windows.")
def test_map_drive(tmp_path):
    from pytest_kafka_broker.windows import map_drive

    paths_seen = set()
    with (
        pytest.raises(RuntimeError, match="All drive letters are in use"),
        ExitStack() as stack,
    ):
        for i in range(26):
            long_path = tmp_path / str(i)
            long_path.mkdir()
            (long_path / "test.txt").write_text(str(i))

            path = stack.enter_context(map_drive(long_path))
            assert len(path.parts) == 1
            assert path not in paths_seen
            paths_seen.add(path)
            assert (path / "test.txt").read_text() == str(i)
    assert paths_seen
