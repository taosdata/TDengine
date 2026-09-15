import glob
import os
import time
from pathlib import Path
from typing import Iterable


_ANCHOR_BYTES = 64


class LogReader:
    """Read complete new log lines across file rotation and truncation."""

    def __init__(self, patterns: Iterable[str],
                 snapshot_timeout: float = 1.0) -> None:
        self.patterns = tuple(patterns)
        self.last_error = ""
        self._offsets: dict[tuple[str, int, int], int] = {}
        self._anchors: dict[tuple[str, int, int], bytes] = {}
        self._tails: dict[tuple[str, int, int], bytes] = {}
        self._skip_partial: set[tuple[str, int, int]] = set()
        deadline = time.monotonic() + snapshot_timeout
        while True:
            _, missing = self._read(snapshot=True)
            if not missing:
                break
            if time.monotonic() >= deadline:
                raise AssertionError(
                    f"could not snapshot log files: {self.last_error}"
                )
            time.sleep(0.05)

    def read(self) -> str:
        text, _ = self._read(snapshot=False)
        return text

    def _read(self, snapshot: bool) -> tuple[str, bool]:
        chunks = []
        missing = False
        for pattern in self.patterns:
            for log_file in glob.glob(pattern):
                if log_file.endswith(".gz"):
                    continue
                try:
                    with open(log_file, "rb") as log:
                        stat = os.fstat(log.fileno())
                        # A name may rotate between glob and open. Use the
                        # opened file's identity, scoped to its dnode folder.
                        file_id = (
                            str(Path(log_file).parent),
                            stat.st_dev, stat.st_ino,
                        )
                        if snapshot:
                            offset = log.seek(0, os.SEEK_END)
                            self._offsets[file_id] = offset
                            anchor_start = max(0, offset - _ANCHOR_BYTES)
                            log.seek(anchor_start)
                            self._anchors[file_id] = log.read(
                                offset - anchor_start
                            )
                            self._tails.pop(file_id, None)
                            self._skip_partial.discard(file_id)
                            if offset:
                                log.seek(offset - 1)
                                if log.read(1) != b"\n":
                                    self._skip_partial.add(file_id)
                            continue

                        offset = self._offsets.get(file_id, 0)
                        anchor = self._anchors.get(file_id, b"")
                        replaced = stat.st_size < offset
                        if not replaced and anchor:
                            log.seek(offset - len(anchor))
                            replaced = log.read(len(anchor)) != anchor
                        if replaced:
                            offset = 0
                            anchor = b""
                            self._tails.pop(file_id, None)
                            self._skip_partial.discard(file_id)
                        log.seek(offset)
                        data = log.read()
                        # stat predates the read and may miss concurrent
                        # appends. The actual cursor avoids replaying them.
                        self._offsets[file_id] = log.tell()
                        self._anchors[file_id] = (anchor + data)[
                            -_ANCHOR_BYTES:
                        ]
                except FileNotFoundError as error:
                    self.last_error = f"{log_file}: {error}"
                    missing = True
                    continue

                pending = self._tails.get(file_id, b"") + data
                if file_id in self._skip_partial:
                    newline = pending.find(b"\n")
                    if newline < 0:
                        continue
                    pending = pending[newline + 1:]
                    self._skip_partial.remove(file_id)
                # Keep split lines (including split UTF-8 characters) with
                # their file so different dnodes cannot form a false line.
                end = pending.rfind(b"\n") + 1
                chunks.append(pending[:end].decode("utf-8", errors="replace"))
                self._tails[file_id] = pending[end:]
        return "".join(chunks), missing
