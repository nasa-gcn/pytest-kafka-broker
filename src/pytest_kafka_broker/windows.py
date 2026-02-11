from contextlib import contextmanager
from pathlib import Path

from win32.lib.win32con import DDD_REMOVE_DEFINITION
from win32.win32file import DefineDosDeviceW, GetLogicalDrives


def get_unused_drive_letter():
    drives = GetLogicalDrives()
    index = f"{drives:026b}".find("0")
    if index == -1:
        raise RuntimeError("All drive letters are in use")
    return chr(ord("Z") - index)


@contextmanager
def map_drive(path: Path):
    letter = get_unused_drive_letter()
    drive = f"{letter}:"
    target = str(path)
    DefineDosDeviceW(0, drive, target)
    try:
        yield Path(f"{letter}:\\")
    finally:
        DefineDosDeviceW(DDD_REMOVE_DEFINITION, drive, target)
