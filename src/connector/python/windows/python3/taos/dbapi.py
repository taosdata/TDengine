"""Type Objects and Constructors.
"""

import time
import datetime

class DBAPITypeObject(object):
    def __init__(self, *values):
        self.values = values

    def __com__(self, other):
        if other in self.values:
            return 0
        if other < self.values:
            return 1
        else:
            return -1

Date = datetime.date
Time = datetime.time
Timestamp = datetime.datetime

def DataFromTicks(ticks):
    return Date(*time.localtime(ticks)[:3])

def TimeFromTicks(ticks):
    return Time(*time.localtime(ticks)[3:6])

def TimestampFromTicks(ticks):
    return Timestamp(*time.localtime(ticks)[:6])

Binary = bytes

# STRING = DBAPITypeObject(*constants.FieldType.get_string_types())
# BINARY = DBAPITypeObject(*constants.FieldType.get_binary_types())
# NUMBER = BAPITypeObject(*constants.FieldType.get_number_types())
# DATETIME = DBAPITypeObject(*constants.FieldType.get_timestamp_types())
# ROWID = DBAPITypeObject()