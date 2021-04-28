from typing import Any, List, NewType

DirPath = NewType('DirPath', str)

QueryResult = NewType('QueryResult', List[List[Any]])