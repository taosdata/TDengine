
from .connection import TDengineConnection
from .cursor import TDengineCursor

# For some reason, the following is needed for VS Code (through PyLance) to 
# recognize that "error" is a valid module of the "taos" package.
from .error import ProgrammingError

# Globals
threadsafety = 0
paramstyle = 'pyformat'

__all__ = ['connection', 'cursor']


def connect(*args, **kwargs):
    """ Function to return a TDengine connector object

    Current supporting keyword parameters:
    @dsn: Data source name as string
    @user: Username as string(optional)
    @password: Password as string(optional)
    @host: Hostname(optional)
    @database: Database name(optional)

    @rtype: TDengineConnector
    """
    return TDengineConnection(*args, **kwargs)
