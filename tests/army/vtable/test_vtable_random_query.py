###################################################################
#           Copyright (c) 2016 by TAOS Technologies, Inc.
#                     All rights reserved.
#
#  This file is proprietary and confidential to TAOS Technologies.
#  No part of this file may be reproduced, stored, transmitted,
#  disclosed or used in any form or by any means other than as
#  expressly provided by the written permission from Jianhui Tao
#
###################################################################

# -*- coding: utf-8 -*-

from frame import etool
from frame.etool import *
from frame.log import *
from frame.cases import *
from frame.sql import *
from frame.caseBase import *
from frame.common import *

import re
import random
import taos


class TDTestCase(TBase):
    # -------------------------------
    # 1. 解析 sqlRule.txt 构造语法字典
    # -------------------------------
    def parse_grammar(self, file_path):
        """
        解析 Lemon 语法文件，返回一个字典，键为非终结符，值为产生式列表（每个产生式为 token 列表）。
        """
        grammar = {}
        # 正则表达式：匹配 "非终结符 ::= 产生式." 格式
        pattern = re.compile(r'^\s*(\S+)\s*::=\s*(.*?)\s*\.\s*$')
        with open(file_path, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith('//'):  # 忽略空行和注释
                    continue
                match = pattern.match(line)
                if match:
                    lhs = match.group(1)
                    rhs = match.group(2)
                    # 产生式为空表示空串，转换为空列表
                    tokens = [] if rhs == '' else rhs.split()
                    # 同一非终结符可能有多条产生式，合并到列表中
                    if lhs in grammar:
                        grammar[lhs].append(tokens)
                    else:
                        grammar[lhs] = [tokens]
                else:
                    tdLog.debug(f"无法解析行：{line}")
        return grammar

    # -------------------------------
    # 2. 生成 SQL 语句的辅助函数
    # -------------------------------

    # 为部分终结符预定义样例数据，可根据 TDengine 的要求扩展
    TERMINAL_SAMPLES = {
        "NK_ID": ["user1", "tableA", "db1"],
        "NK_STRING": ["'test'", "'sample'"],
        "NK_INTEGER": ["1", "100"],
        "NK_FLOAT": ["3.14", "2.718"],
        "NK_COMMA": [","],
        "NK_DOT": ["."],
        "NK_STAR": ["*"],
        "NK_LP": ["("],
        "NK_RP": [")"],
        "NK_SEMI": [";"],
        "NK_BITAND": ["&"],
        "NK_BITOR": ["|"],
        "NK_BITXOR": ["^"],
        "NK_PLUS": ["+"],
        "NK_MINUS": ["-"],
        "NK_MUL": ["*"],
        "NK_DIV": ["/"],
        # 针对其他终结符，如果未在字典中定义，则直接返回该字符串
    }

    def is_terminal(self, symbol, grammar):
        """
        如果 symbol 不在 grammar 字典中，或在 TERMINAL_SAMPLES 中定义，则认为是终结符
        """
        return symbol in self.TERMINAL_SAMPLES or symbol not in grammar

    def sample_for(self, symbol):
        """
        返回终结符的样例值，如果没有预定义，则直接返回 symbol 本身。
        """
        return random.choice(self.TERMINAL_SAMPLES.get(symbol, [symbol]))

    def generate(self, symbol, grammar, depth=0, max_depth=5):
        """
        递归生成 SQL 语句。对于终结符，返回预定义样例；对于非终结符，从 grammar 中遍历产生式。
        """
        # 防止无限递归，超过最大深度时返回空字符串
        if depth > max_depth:
            return ['']
        # 若 symbol 是终结符
        if self.is_terminal(symbol, grammar):
            return [self.sample_for(symbol)]
        results = []
        # 遍历该非终结符所有产生式
        for production in grammar[symbol]:
            # 从空串开始构建组合
            subresults = ['']
            for token in production:
                token_variants = self.generate(token, grammar, depth + 1, max_depth)
                # 组合当前 subresults 和 token_variants（笛卡尔积）
                subresults = [x + ' ' + y for x in subresults for y in token_variants]
            # 去除首尾空白后添加到结果中
            tdLog.debug(f"subresults: {subresults}\n")
            results.extend([s.strip() for s in subresults])
        return results

    # -------------------------------
    # 3. 执行 SQL 语句的测试部分
    # -------------------------------
    def execute_sql(self, sql):
        """
        连接 TDengine 执行 SQL 语句，并返回执行结果。
        注意：需根据实际环境配置 host、user、password、database。
        """
        tdSql.execute("create database test")
        tdSql.execute("use test_vtable_auth_create;")
        conn = taos.connect(host='localhost', port=6030, user='root', password='taosdata', database='test')
        cursor = conn.cursor()
        testSql = TDSql()
        testSql.init(cursor)
        testSql.execute(sql)

    def run_tests(self, sql_statements):
        """
        遍历 SQL 语句，执行并输出测试结果。
        """
        for sql in sql_statements:
            if not sql:
                continue
            success, result = self.execute_sql(sql)
            if success:
                tdLog.debug(f"[PASS] {sql}")
            else:
                tdLog.debug(f"[FAIL] {sql} -- {result}")

    # -------------------------------
    # 4. 主程序：解析语法、生成 SQL 并测试
    # -------------------------------
    def run(self):
        # 修改文件路径为实际 sqlRule.txt 文件路径
        tdLog.debug("开始解析")
        file_path = "/Users/simondominic/taosdata/sqlRule.txt"
        grammar = self.parse_grammar(file_path)
        tdLog.debug("语法解析完成。生成的非终结符如下：")
        for key, productions in grammar.items():
            tdLog.debug(f"{key}: {len(productions)} 产生式")

        tdLog.debug("开始生成语句")
        # 以 cmd 规则作为起始符生成 SQL 语句（此处只生成一部分，避免组合爆炸，可适当限制最大深度）
        sql_cases = self.generate("query_or_subquery", grammar, max_depth=6)
        # 去重并随机抽取部分测试用例
        sql_cases = list(set(sql_cases))
        sample_cases = random.sample(sql_cases, min(10, len(sql_cases)))

        tdLog.debug("\n生成的部分 SQL 测试用例：")
        for sql in sample_cases:
            tdLog.debug(sql)

        # 运行测试（请确保 TDengine 服务正常启动，并且 test 数据库存在）
        tdLog.debug("\n开始执行测试：")
        self.run_tests(sample_cases)

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())