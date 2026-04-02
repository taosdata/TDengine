from typing import List, Dict
from .errors import NotSupportedError, ConnectError
from .cursor import TaosRestCursor
from .restclient import RestClient
from typing import Optional


class Result:
    def __init__(self, resp: dict):
        if "status" in resp:
            self.status: str = resp["status"]
        else:
            self.code: str = resp["code"]
        self.column_meta: List[list] = resp["column_meta"]
        self.data: List[list] = resp["data"]
        self.rows: int = resp["rows"]

    @property
    def field_count(self):
        return len(self.column_meta)

    @property
    def fields(self) -> List[Dict]:
        """
        return a list of column meta dict which contains three keys:
            - name: column name
            - type: column type code
            - bytes: data length in bytes
        for more information about column meta,
         refer https://docs.tdengine.com/2.4/reference/rest-api/#http-return-format
        """
        return list(map(lambda meta: {"name": meta[0], "type": meta[1], "bytes": meta[2]}, self.column_meta))

    def __iter__(self):
        return self.data.__iter__()


class TaosRestConnection:
    """
    Implement [PEP 249 Connection API](https://peps.python.org/pep-0249/#connection-objects)
    """

    default_configs = {"url", "token", "user", "password", "database", "timeout", "convert_timestamp", "timezone"}

    def __init__(self, **kwargs):
        """
        Keyword Arguments
        ----------------------------
        - url: str, optional, default "http://localhost:6041"
             url to connect
        - token: str, optional, default None
             TDengine cloud Token, which is required only by TDengine cloud service
        - user : str, optional, default root
            username used to log in
        - password : str, optional, default taosdata
            password used to log in
        - database : str, optional, default None
             default database to use.
        - timeout : int, optional.
            the optional timeout parameter specifies a timeout in seconds for blocking operations
        - convert_timestamp: bool, optional, default true
             whether to convert timestamp in RFC3339 format to python datatime.
         - timezone: str | datetime.tzinfo, optional, default None.
             When convert_timestamp is true, which timezone to used.
             When the type of timezone is str, it should be recognized by
             [pytz package](https://pypi.org/project/pytz/).
             When the timezone is None, system timezone will be used and the returned datetime object will be
             offset-naive (no tzinfo), otherwise the returned datetime will be offset-aware(with tzinfo)
        """
        for key in kwargs:
            if key not in self.default_configs:
                raise ConnectError("Unrecognized configs: %s" % key)

        self._url = kwargs.get("url", "http://localhost:6041")
        self._token = kwargs.get("token")
        self._user = kwargs.get("user", "root")
        self._password = kwargs.get("password", "taosdata")
        self._database = kwargs.get("database")
        self._timeout = kwargs.get("timeout")
        self._convert_timestamp = kwargs.get("convert_timestamp", True)
        self._timezone = kwargs.get("timezone", None)
        self._client = RestClient(
            self._url,
            token=self._token,
            database=self._database,
            user=self._user,
            password=self._password,
            timeout=self._timeout,
            convert_timestamp=self._convert_timestamp,
            timezone=self._timezone,
        )

    def close(self):
        pass

    def commit(self):
        pass

    def rollback(self):
        raise NotSupportedError()

    def cursor(self):
        return TaosRestCursor(self._client)

    ############################################################################
    # Methods bellow are not PEP249 specified.
    # Add them for giving a similar programming experience as taos.Connection.
    ############################################################################

    @property
    def server_info(self):
        resp = self._client.sql("select server_version()")
        if len(resp["data"]) > 0:
            return resp["data"][0][0]
        return ""

    def execute(self, sql: str, req_id: Optional[int] = None) -> Optional[int]:
        """
        execute none query statement and return affected row count.
        If there is not a column named "affected_rows" in response, then None is returned.
        """
        resp = self._client.sql(sql, req_id=req_id)
        if resp["column_meta"][0][0] == "affected_rows":
            return resp["data"][0][0]
        return None

    def query(self, sql: str, req_id: Optional[int] = None) -> Result:
        """
        execute sql and wrap the http response as Result object.
        """
        resp = self._client.sql(sql, req_id=req_id)
        return Result(resp)
