import time
from datetime import datetime

class Assert:
    @staticmethod
    def time_equals(datetime1: datetime, datetime2: datetime, deviation: int):
        seconds = (datetime1 - datetime2).seconds
        if seconds <= deviation:
            return True

        raise Exception(f"The time difference between '{datetime1}' and '{datetime2}' is bigger than expected deviation '{deviation}'")

    @staticmethod
    def str_equals(str1: str, str2: str):
        if str1 == str2:
            return True

        raise Exception(f"'{str1}' is different from '{str2}'")

    @staticmethod
    def str_contain(str: str, sub_str: str):
        if sub_str in str:
            return True

        raise Exception(f"'{sub_str}' is not included in '{str}'")