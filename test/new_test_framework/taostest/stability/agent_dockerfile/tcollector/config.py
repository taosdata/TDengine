#!/usr/bin/env python
import os
import sys

def onload(options, tags):
    """Function called by tcollector when it starts up.

    Args:
        options: The options as returned by the OptionParser.
        tags: A dictionnary that maps tag names to tag values.
    """
    pass

def get_defaults():
    """Configuration values to use as defaults in the code

        This is called by the OptionParser.
    """

    default_cdir = os.path.join(os.path.dirname(os.path.realpath(sys.argv[0])), 'collectors')

    defaults = {
        'verbose': False,
        'no_tcollector_stats': False,
        'evictinterval': 6000,
        'dedupinterval': 300,
        'deduponlyzero': False,
        'allowed_inactivity_time': 600,
        'dryrun': False,
        'maxtags': 8,
        'http_password': False,
        'reconnectinterval': 0,
        'http_username': False,
        'port': TaosadapterPort,
        'pidfile': '/var/run/tcollector.pid',
        'http': False,
        'http_api_path': "api/put",
        'tags': [],
        'remove_inactive_collectors': False,
        'host': 'TaosadapterIp',
        'logfile': '/var/log/tcollector.log',
        'cdir': default_cdir,
        'ssl': False,
        'stdin': False,
        'daemonize': False,
        'hosts': False,
        "monitoring_interface": None,
        "monitoring_port": 13280,
        "namespace_prefix": "",
    }

    return defaults
