
from .connection import TDengineConnection
from .cursor import TDengineCursor

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
