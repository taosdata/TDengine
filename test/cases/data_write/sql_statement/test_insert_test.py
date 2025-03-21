import pytest
from new_test_framework.utils.util.log import *
from new_test_framework.utils.util.sql import *
from new_test_framework.utils.util.cases import *


class TestInsertDouble:

    def run_tags(self, stable_name, dtype, bits):
        """_summary_

        Args:
            stable_name (_type_): _description_
            dtype (_type_): _description_
            bits (_type_): _description_
        """

    @pytest.mark.common
    @pytest.mark.ci
    def test_insert_double(self):  # sourcery skip: extract-duplicate-method, remove-redundant-fstring
        """测试插入各种double值  

        插入各种double值包括正负值、科学计数法、十六进制、二进制、字符串
        
        Since: v3.0.0.0

        Labels: common,ci

        Jira:

        History:
            - 2024-2-6 Feng Chao Created
            - 2025-2-26 Huo Hong Migrated to new test framework

        """