import json
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional, Sequence

@dataclass
class StreamEvent:
    messageId: Optional[str]
    timestamp: Optional[int]
    streamName: Optional[str]
    eventType: Optional[str]
    triggerType: Optional[str]
    triggerId: Optional[str]
    tableName: Optional[str]
    windowStart: Optional[int]
    windowEnd: Optional[int]
    result: Any              # raw result object (dict or value)
    resultData: List[Dict]   # flattened result["data"] list if present
    raw: Dict[str, Any]      # original event dict

class NotifyLog:
    def __init__(self, path: str):
        self.path = path
        self._events: List[StreamEvent] = []
        self._loaded = False

    def _load(self):
        if self._loaded:
            return
        events: List[StreamEvent] = []
        with open(self.path, "r", encoding="utf-8") as f:
            for lineno, line in enumerate(f, 1):
                line = line.strip()
                if not line:
                    continue
                try:
                    obj = json.loads(line)
                except Exception as e:
                    raise ValueError(f"Line {lineno} not valid JSON: {e}\n{line}")
                streams = obj.get("streams") or []
                for s in streams:
                    streamName = s.get("streamName")
                    for ev in s.get("events") or []:
                        result = ev.get("result")
                        resultData = []
                        if isinstance(result, dict):
                            data = result.get("data")
                            if isinstance(data, list):
                                resultData = [d for d in data if isinstance(d, dict)]
                        events.append(StreamEvent(
                            messageId=obj.get("messageId"),
                            timestamp=obj.get("timestamp"),
                            streamName=streamName,
                            eventType=ev.get("eventType"),
                            triggerType=ev.get("triggerType"),
                            triggerId=ev.get("triggerId"),
                            tableName=ev.get("tableName"),
                            windowStart=ev.get("windowStart"),
                            windowEnd=ev.get("windowEnd"),
                            result=result,
                            resultData=resultData,
                            raw=ev
                        ))
        self._events = events
        self._loaded = True

    def events(self) -> Sequence[StreamEvent]:
        self._load()
        return self._events

    def find(self, **criteria) -> List[StreamEvent]:
        """
        criteria keys: streamName, eventType, triggerType, triggerId,
                       tableName, windowStart, windowEnd
        Values can be exact, list/tuple (membership), or callable(value)->bool.
        """
        self._load()
        out = []
        for ev in self._events:
            ok = True
            for k, v in criteria.items():
                val = getattr(ev, k, None)
                if callable(v):
                    if not v(val):
                        ok = False
                        break
                elif isinstance(v, (list, tuple, set)):
                    if val not in v:
                        ok = False
                        break
                else:
                    if val != v:
                        ok = False
                        break
            if ok:
                out.append(ev)
        return out

    def assert_one(self, msg: str = "", **criteria) -> StreamEvent:
        """
        Ensure exactly one event matches criteria.
        """
        matches = self.find(**criteria)
        if len(matches) != 1:
            raise AssertionError(f"{msg} expected exactly 1 match, got {len(matches)}; criteria={criteria}")
        return matches[0]

    def assert_exists(self, msg: str = "", **criteria) -> StreamEvent:
        """
        Ensure at least one event matches criteria; returns first.
        """
        matches = self.find(**criteria)
        if not matches:
            raise AssertionError(f"{msg} no event matches criteria={criteria}")
        return matches[0]
    
    def num_exists(self, msg: str = "", **criteria) -> int:
        """
        Ensure at least one event matches criteria; returns first.
        """
        matches = self.find(**criteria)
        if not matches:
            return 0
        return matches.__len__()

    def assert_result_data(self,
                           result_filter: Callable[[List[Dict]], bool],
                           msg: str = "",
                           **criteria) -> StreamEvent:
        """
        Assert an event matches criteria AND its resultData passes result_filter.
        """
        ev = self.assert_exists(msg, **criteria)
        if not result_filter(ev.resultData):
            raise AssertionError(f"{msg} resultData check failed; size={len(ev.resultData)} data={ev.resultData}")
        return ev

    def summary(self) -> Dict[str, int]:
        self._load()
        stats: Dict[str, int] = {}
        for ev in self._events:
            key = f"{ev.streamName}:{ev.eventType}"
            stats[key] = stats.get(key, 0) + 1
        return stats

# Convenience one-liner style function
def expect_event(log_path: str,
                 streamName: Optional[str] = None,
                 eventType: Optional[str] = None,
                 triggerType: Optional[str] = None,
                 tableName: Optional[str] = None,
                 windowStart: Optional[int] = None,
                 windowEnd: Optional[int] = None,
                 result_pred: Optional[Callable[[List[Dict]], bool]] = None,
                 retries: int = 30) -> StreamEvent:
    """
    One call assertion. Raises AssertionError if not found / predicate fails.
    Retry `retries` times (default 60), sleeping 1s between attempts.
    Example:
      expect_event("basic2_s0.log",
                   streamName="sdb2.s0",
                   eventType="WINDOW_CLOSE",
                   windowStart=1735660801000,
                   windowEnd=1735660804000,
                   result_pred=lambda d: len(d) == 4)
    """
    import time

    criteria: Dict[str, Any] = {}
    if streamName is not None: criteria["streamName"] = streamName
    if eventType is not None: criteria["eventType"] = eventType
    if triggerType is not None: criteria["triggerType"] = triggerType
    if tableName is not None: criteria["tableName"] = tableName
    if windowStart is not None: criteria["windowStart"] = windowStart
    if windowEnd is not None: criteria["windowEnd"] = windowEnd

    retries = max(0, int(retries))
    last_err: Optional[Exception] = None

    for attempt in range(retries + 1):
        try:
            # Recreate reader each attempt to capture newly appended lines
            nl = NotifyLog(log_path)
            ev = nl.assert_exists(**criteria)
            if result_pred is not None and not result_pred(ev.resultData):
                raise AssertionError(f"result predicate failed for event {criteria}; resultData={ev.resultData}")
            return ev
        except (AssertionError, ValueError, FileNotFoundError) as e:
            last_err = e
            if attempt < retries:
                print(f"Attempt {attempt+1} failed: {e}. Retrying...")
                time.sleep(1)
            else:
                raise last_err

def expect_rows(log_path: str, rows: int, retries: int = 60, **criteria) -> StreamEvent:
    """
    Assert that the first event matching criteria has exactly `rows` rows in result.data.
    Example:
      expect_rows("basic2_s0.log",
                  rows=4, streamName="sdb2.s0",
                  eventType="WINDOW_CLOSE", windowStart=1735660801000)
    """
    ev = expect_event(log_path, **criteria, retries=retries)
    actual = len(ev.resultData or [])
    if actual != rows:
        raise AssertionError(f"rows mismatch: expected {rows}, got {actual}; criteria={criteria}")
    return ev

def expect_event_count(log_path: str,
                 streamName: Optional[str] = None,
                 eventType: Optional[str] = None,
                 triggerType: Optional[str] = None,
                 tableName: Optional[str] = None,
                 windowStart: Optional[int] = None,
                 windowEnd: Optional[int] = None,
                 expectCount: int = 0,
                 retries: int = 30) -> int:
    """
    One call assertion. Raises AssertionError if not found / predicate fails.
    Retry `retries` times (default 60), sleeping 1s between attempts.
    Example:
      expect_event("basic2_s0.log",
                   streamName="sdb2.s0",
                   eventType="WINDOW_CLOSE",
                   windowStart=1735660801000,
                   windowEnd=1735660804000,
                   result_pred=lambda d: len(d) == 4)
    """
    import time

    criteria: Dict[str, Any] = {}
    if streamName is not None: criteria["streamName"] = streamName
    if eventType is not None: criteria["eventType"] = eventType
    if triggerType is not None: criteria["triggerType"] = triggerType
    if tableName is not None: criteria["tableName"] = tableName
    if windowStart is not None: criteria["windowStart"] = windowStart
    if windowEnd is not None: criteria["windowEnd"] = windowEnd

    retries = max(0, int(retries))
    last_err: Optional[Exception] = None

    for attempt in range(retries + 1):
        try:
            # Recreate reader each attempt to capture newly appended lines
            nl = NotifyLog(log_path)
            num = nl.num_exists(**criteria)
            if(num > expectCount):
                raise AssertionError(f"Too many events: expected {expectCount}, got {num}; criteria={criteria}.")
            if(num != expectCount):
                print(f"Attempt {attempt+1} failed: expected {expectCount}, got {num}; criteria={criteria}. Retrying...")
                time.sleep(1)
                continue
            return num
        except (AssertionError, ValueError, FileNotFoundError) as e:
            last_err = e
            if attempt < retries:
                print(f"Attempt {attempt+1} failed: {e}. Retrying...")
                time.sleep(1)
            else:
                raise last_err
    
    raise AssertionError(f"Attempt {attempt+1} failed: expected {expectCount}, got {num}; criteria={criteria}.")
