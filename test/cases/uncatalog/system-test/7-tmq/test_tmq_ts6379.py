import time
from new_test_framework.utils import tdLog, tdSql
from taos.tmq import *
from taos import *
import inspect


class TestCase:
    updatecfgDict = {'debugFlag': 143, 'asynclog': 0}

    def setup_class(cls):
        tdLog.debug(f"start to excute {__file__}")

    def get_function_name(self):
        """获取当前调用函数的名字"""
        return inspect.currentframe().f_back.f_code.co_name

    def check_add_table(self):
        func_name = self.get_function_name()
        print(f"start to excute {func_name}")
        
        tdSql.execute(f'use db')
        tdSql.execute(f'CREATE STABLE {func_name} (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT, info nchar(4)) TAGS (location BINARY(8), groupId INT)')
        tdSql.execute(f"INSERT INTO {func_name}_1 USING {func_name} TAGS('d1', 2)  VALUES('2018-10-05 14:00:00.000',1.1,219,0.31000,'i1')")
        tdSql.execute(f"INSERT INTO {func_name}_2 USING {func_name} TAGS('d2', 20) VALUES('2018-10-05 15:00:00.000',1.2,219,0.31000,'i2')")

        tdSql.execute(f'create topic {func_name} as select * from {func_name} where groupId > 10')

        consumer_dict = {
            "group.id": "g1",
            "td.connect.user": "root",
            "td.connect.pass": "taosdata",
            "auto.offset.reset": "earliest",
        }
        consumer = Consumer(consumer_dict)

        try:
            consumer.subscribe([f"{func_name}"])
        except TmqError:
            tdLog.exit(f"subscribe error")

        index = 0;
        try:
            while True:
                res = consumer.poll(5)
                print(res)
                if not res:
                    print("res null")
                    break
                val = res.value()
                if val is None:
                    continue
                for block in val:
                    data = block.fetchall()
                    print(f"data len: {len(data)}")

                    if index == 0:
                        if len(data) != 1:
                            tdLog.exit(f"error data index 0 len: {len(data)}")
                        for element in data:
                            print(element)
                            if element[4] != 'i2' or element[-1] != 20:
                                tdLog.exit(f"error: {element[4]} {element[-1]}")

                        tdSql.execute(f"INSERT INTO {func_name}_3 USING {func_name} TAGS('d3', 2)  VALUES('2018-10-05 14:00:00.000',1.3,219,0.31000,'i3')")
                        tdSql.execute(f"INSERT INTO {func_name}_4 USING {func_name} TAGS('d4', 21) VALUES('2018-10-05 15:00:00.000',1.4,219,0.31000,'i4')")

                    if index == 1:
                            if len(data) != 1:
                                tdLog.exit(f"error data index 1 len: {len(data)}")
                            for element in data:
                                print(element)
                                if element[4] != 'i4' or element[-1] != 21:
                                    tdLog.exit(f"error: {element[4]} {element[-1]}")
                index += 1
        finally:
            consumer.close()

    def check_add_drop_tag(self):
        func_name = self.get_function_name()
        print(f"start to excute {func_name}")
        tdSql.execute(f'use db')
        tdSql.execute(f'CREATE STABLE {func_name} (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT, info nchar(4)) TAGS (location BINARY(8), groupId INT)')
        tdSql.execute(f"INSERT INTO {func_name}_1 USING {func_name} TAGS('d1', 2)  VALUES('2018-10-05 14:00:00.000',1.1,219,0.31000,'i1')")
        tdSql.execute(f"INSERT INTO {func_name}_2 USING {func_name} TAGS('d2', 20) VALUES('2018-10-05 15:00:00.000',1.2,219,0.31000,'i2')")

        tdSql.execute(f'create topic {func_name} as select * from {func_name} where groupId > 10')

        consumer_dict = {
            "group.id": "g1",
            "td.connect.user": "root",
            "td.connect.pass": "taosdata",
            "auto.offset.reset": "earliest",
        }
        consumer = Consumer(consumer_dict)

        try:
            consumer.subscribe([f"{func_name}"])
        except TmqError:
            tdLog.exit(f"subscribe error")

        index = 0;
        try:
            while True:
                res = consumer.poll(5)
                print(res)
                if not res:
                    print("res null")
                    break
                val = res.value()
                if val is None:
                    continue
                for block in val:
                    data = block.fetchall()
                    print(f"index {index},data len: {len(data)}")

                    if index == 0:
                        if len(data) != 1:
                            tdLog.exit(f"error data index 0 len: {len(data)}")
                        for element in data:
                            print(element)
                            if element[4] != 'i2' or element[-1] != 20:
                                tdLog.exit(f"error: {element[4]} {element[-1]}")

                        tdSql.execute(f"alter table {func_name} add tag t1 int")
                        tdSql.execute(f"alter table {func_name} drop tag groupId")
                        tdSql.execute(f"alter table {func_name} drop tag location")
                        tdSql.execute(f"INSERT INTO {func_name}_2 VALUES('2018-10-05 16:00:00.000',1.3,2129,0.31000,'i22')")

                    if index == 1:
                        if len(data) != 1:
                            tdLog.exit(f"error data index 1 len: {len(data)}")
                        for element in data:
                            print(element)
                            if element[4] != 'i22' or element[-1] != 20:
                                tdLog.exit(f"error: {element[4]} {element[-1]}")
                        tdSql.execute(f"reload topic {func_name} as select * from {func_name}")
                        tdSql.execute(f"INSERT INTO {func_name}_3 USING {func_name} TAGS(2)  VALUES('2018-10-05 14:00:00.000',1.3,219,0.31000,'i3')")
                        time.sleep(3)

                    if index == 2:
                        if len(data) != 1:
                            tdLog.exit(f"error data index 2 len: {len(data)}")
                        for element in data:
                            print(element)
                            if element[4] != 'i3' or element[-1] != 2:
                                tdLog.exit(f"error: {element[4]} {element[-1]}")
                                
                index += 1
        finally:
            consumer.close()
    
    def check_alter_tag_name_bytes(self):
        func_name = self.get_function_name()
        print(f"start to excute {func_name}")
        tdSql.execute(f'use db')
        tdSql.execute(f'CREATE STABLE {func_name} (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT, info nchar(4)) TAGS (location BINARY(8), groupId INT)')
        tdSql.execute(f"INSERT INTO {func_name}_1 USING {func_name} TAGS('d1', 2)  VALUES('2018-10-05 14:00:00.000',1.1,219,0.31000,'i1')")
        tdSql.execute(f"INSERT INTO {func_name}_2 USING {func_name} TAGS('dddd2', 20) VALUES('2018-10-05 15:00:00.000',1.2,219,0.31000,'i2')")

        tdSql.execute(f'create topic {func_name} as select * from {func_name} where length(location) > 4')

        consumer_dict = {
            "group.id": "g1",
            "td.connect.user": "root",
            "td.connect.pass": "taosdata",
            "auto.offset.reset": "earliest",
        }
        consumer = Consumer(consumer_dict)

        try:
            consumer.subscribe([f"{func_name}"])
        except TmqError:
            tdLog.exit(f"subscribe error")

        index = 0;
        try:
            while True:
                res = consumer.poll(5)
                print(res)
                if not res:
                    print("res null")
                    break
                val = res.value()
                if val is None:
                    continue
                for block in val:
                    data = block.fetchall()
                    print(f"data len: {len(data)}")

                    if index == 0:
                        if len(data) != 1:
                            tdLog.exit(f"error data index 0 len: {len(data)}")
                        for element in data:
                            print(element)
                            if element[4] != 'i2' or element[-1] != 20:
                                tdLog.exit(f"error: {element[4]} {element[-1]}")

                        tdSql.execute(f"alter table {func_name} rename tag groupId gId")
                        tdSql.execute(f"alter table {func_name} rename tag location loc")
                        tdSql.execute(f"alter table {func_name} modify tag loc BINARY(16)")
                        tdSql.execute(f"INSERT INTO {func_name}_3 USING {func_name} TAGS('d3', 2)  VALUES('2018-10-05 14:00:00.000',1.3,219,0.31000,'i3')")
                        tdSql.execute(f"INSERT INTO {func_name}_4 USING {func_name} TAGS('ddddddddddddd4', 21) VALUES('2018-10-05 15:00:00.000',1.4,219,0.31000,'i4')")

                index += 1
        finally:
            pass

        tdSql.execute(f"reload topic {func_name} as select * from {func_name} where length(loc) > 4")
        time.sleep(3)

        index = 0
        try:
            while True:
                res = consumer.poll(5)
                print(res)
                if not res:
                    print("res null")
                    break
                val = res.value()
                if val is None:
                    continue
                for block in val:
                    data = block.fetchall()
                    print(f"index {index},data len: {len(data)}")

                    if index == 0:
                        if len(data) != 1:
                            tdLog.exit(f"error data index 1 len: {len(data)}")
                        for element in data:
                            print(element)
                            if element[4] != 'i4' or element[-2] != 'ddddddddddddd4':
                                tdLog.exit(f"error: {element[4]} {element[-1]}")
                                
                index += 1
        finally:
            consumer.close()

    def check_alter_tag_value(self):
        func_name = self.get_function_name()
        print(f"start to excute {func_name}")
        tdSql.execute(f'use db')
        tdSql.execute(f'CREATE STABLE {func_name} (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT, info nchar(4)) TAGS (location BINARY(8), groupId INT)')
        tdSql.execute(f"INSERT INTO {func_name}_1 USING {func_name} TAGS('d1', 2)  VALUES('2018-10-05 14:00:00.000',1.1,219,0.31000,'i1')")
        tdSql.execute(f"INSERT INTO {func_name}_2 USING {func_name} TAGS('d2', 20) VALUES('2018-10-05 15:00:00.000',1.2,219,0.31000,'i2')")

        tdSql.execute(f'create topic {func_name} as select * from {func_name} where groupId > 10')

        consumer_dict = {
            "group.id": "g1",
            "td.connect.user": "root",
            "td.connect.pass": "taosdata",
            "auto.offset.reset": "earliest",
        }
        consumer = Consumer(consumer_dict)

        try:
            consumer.subscribe([f"{func_name}"])
        except TmqError:
            tdLog.exit(f"subscribe error")

        index = 0;
        try:
            while True:
                res = consumer.poll(5)
                print(res)
                if not res:
                    print("res null")
                    break
                val = res.value()
                if val is None:
                    continue
                for block in val:
                    data = block.fetchall()
                    print(f"data len: {len(data)}")

                    if index == 0:
                        if len(data) != 1:
                            tdLog.exit(f"error data index 0 len: {len(data)}")
                        for element in data:
                            print(element)
                            if element[4] != 'i2' or element[-1] != 20:
                                tdLog.exit(f"error: {element[1]} {element[-1]}")

                        tdSql.execute(f"alter table {func_name}_1 set tag groupId = 100")
                        tdSql.execute(f"alter table {func_name}_2 set tag groupId = 10")
                        
                        tdSql.execute(f"INSERT INTO {func_name}_1 VALUES('2018-10-05 14:00:00.000',1.3,219,0.31000,'i3')")
                        tdSql.execute(f"INSERT INTO {func_name}_2 VALUES('2018-10-05 14:00:00.000',1.4,219,0.31000,'i4')")

                if index == 2:
                        if len(data) != 1:
                            tdLog.exit(f"error data index 1 len: {len(data)}")
                        for element in data:
                            print(element)
                            if element[4] != 'i3' or element[-1] != 100:
                                tdLog.exit(f"error: {element[1]} {element[-1]}")
                index += 1
        finally:
            consumer.close()

    def check_add_drop_col(self):
        func_name = self.get_function_name()
        print(f"start to excute {func_name}")
        tdSql.execute(f'use db')
        tdSql.execute(f'CREATE STABLE {func_name} (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT, info nchar(4)) TAGS (location BINARY(8), groupId INT)')
        tdSql.execute(f"INSERT INTO {func_name}_1 USING {func_name} TAGS('d1', 2)  VALUES('2018-10-05 14:00:00.000',1.1,1,0.31000,'i1')")
        tdSql.execute(f"INSERT INTO {func_name}_2 USING {func_name} TAGS('d2', 20) VALUES('2018-10-05 15:00:00.000',1.2,219,0.31000,'i2')")

        tdSql.execute(f'create topic {func_name} as select * from {func_name} where voltage > 10')

        consumer_dict = {
            "group.id": "g1",
            "td.connect.user": "root",
            "td.connect.pass": "taosdata",
            "auto.offset.reset": "earliest",
        }
        consumer = Consumer(consumer_dict)

        try:
            consumer.subscribe([f"{func_name}"])
        except TmqError:
            tdLog.exit(f"subscribe error")

        index = 0
        try:
            while True:
                res = consumer.poll(5)
                print(res)
                if not res:
                    print("res null")
                    break
                val = res.value()
                if val is None:
                    continue
                for block in val:
                    data = block.fetchall()
                    print(f"index {index},data len: {len(data)}")

                    if index == 0:
                        if len(data) != 1:
                            tdLog.exit(f"error data index 0 len: {len(data)}")
                        for element in data:
                            print(element)
                            if element[4] != 'i2' or element[-1] != 20:
                                tdLog.exit(f"error: {element[4]} {element[-1]}")

                        tdSql.execute(f"alter table {func_name} add column c1 int")
                        tdSql.execute(f"alter table {func_name} drop column voltage")
                        tdSql.execute(f"alter table {func_name} drop tag location")
                        tdSql.execute(f"INSERT INTO {func_name}_2 VALUES('2018-10-05 16:00:00.000',1.3,0.31000,'i22',1)")

                index += 1
        finally:
            pass

        tdSql.execute(f"reload topic {func_name} as select * from {func_name}")
        tdSql.execute(f"INSERT INTO {func_name}_3 USING {func_name} TAGS(298)  VALUES('2018-10-05 14:00:00.000',1.3,0.31000,'i3',2)")
        time.sleep(3)

        index = 0
        try:
            while True:
                res = consumer.poll(5)
                print(res)
                if not res:
                    print("res null")
                    break
                val = res.value()
                if val is None:
                    continue
                for block in val:
                    data = block.fetchall()
                    print(f"index {index},data len: {len(data)}")

                    if index == 0:
                        if len(data) != 1:
                            tdLog.exit(f"error data index {index} len: {len(data)}")
                        for element in data:
                            print(element)
                            if element[4] != 1 or element[-1] != 20:
                                tdLog.exit(f"error: {element[4]} {element[-1]}")
                    
                    if index == 1:
                        if len(data) != 1:
                            tdLog.exit(f"error data index {index} len: {len(data)}")
                        for element in data:
                            print(element)
                            if element[4] != 2 or element[-1] != 298:
                                tdLog.exit(f"error: {element[4]} {element[-1]}")
                                
                    index += 1
        finally:
            consumer.close()

    def check_alter_col_name_bytes(self):
        func_name = self.get_function_name()
        print(f"start to excute {func_name}")
        tdSql.execute(f'use db')
        tdSql.execute(f'CREATE STABLE {func_name} (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT, info BINARY(4)) TAGS (location BINARY(8), groupId INT)')
        tdSql.execute(f"INSERT INTO {func_name}_1 USING {func_name} TAGS('d1', 2)  VALUES('2018-10-05 14:00:00.000',1.1,219,0.31000,'i13')")
        tdSql.execute(f"INSERT INTO {func_name}_2 USING {func_name} TAGS('dddd2', 20) VALUES('2018-10-05 15:00:00.000',1.2,219,0.31000,'i2')")

        tdSql.execute(f'create topic {func_name} as select * from {func_name} where length(info) > 2')

        consumer_dict = {
            "group.id": "g1",
            "td.connect.user": "root",
            "td.connect.pass": "taosdata",
            "auto.offset.reset": "earliest",
        }
        consumer = Consumer(consumer_dict)

        try:
            consumer.subscribe([f"{func_name}"])
        except TmqError:
            tdLog.exit(f"subscribe error")

        index = 0;
        try:
            while True:
                res = consumer.poll(5)
                print(res)
                if not res:
                    print("res null")
                    break
                val = res.value()
                if val is None:
                    continue
                for block in val:
                    data = block.fetchall()
                    print(f"data len: {len(data)}")

                    if index == 0:
                        if len(data) != 1:
                            tdLog.exit(f"error data index 0 len: {len(data)}")
                        for element in data:
                            print(element)
                            if element[4] != 'i13' or element[-1] != 2:
                                tdLog.exit(f"error: {element[4]} {element[-1]}")

                        tdSql.execute(f"alter table {func_name} modify column info BINARY(16)")
                        tdSql.execute(f"INSERT INTO {func_name}_2 VALUES('2018-10-05 16:00:00.000',1.3,219,0.31000,'i343434344')")

                    if index == 1:
                            if len(data) != 1:
                                tdLog.exit(f"error data index 1 len: {len(data)}")
                            for element in data:
                                print(element)
                                if element[4] != 'i343434344' or element[-1] != 20:
                                    tdLog.exit(f"error: {element[4]} {element[-1]}")
                index += 1
        finally:
            consumer.close()
    
    def check_add_drop_rename_col_normal_table(self):
        func_name = self.get_function_name()
        print(f"start to excute {func_name}")
        tdSql.execute(f'use db')
        tdSql.execute(f'CREATE TABLE {func_name} (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT, info nchar(4))')
        tdSql.execute(f"INSERT INTO {func_name} VALUES('2018-10-05 14:00:00.000',1.1,1,0.31000,'i1')")
        tdSql.execute(f"INSERT INTO {func_name} VALUES('2018-10-05 15:00:00.000',1.2,219,0.31000,'i2')")

        tdSql.execute(f'create topic {func_name} as select * from {func_name} where voltage > 10')

        consumer_dict = {
            "group.id": "g1",
            "td.connect.user": "root",
            "td.connect.pass": "taosdata",
            "auto.offset.reset": "earliest",
        }
        consumer = Consumer(consumer_dict)

        try:
            consumer.subscribe([f"{func_name}"])
        except TmqError:
            tdLog.exit(f"subscribe error")

        index = 0
        try:
            while True:
                res = consumer.poll(5)
                print(res)
                if not res:
                    print("res null")
                    break
                val = res.value()
                if val is None:
                    continue
                for block in val:
                    data = block.fetchall()
                    print(f"index {index},data len: {len(data)}")

                    if index == 0:
                        if len(data) != 1:
                            tdLog.exit(f"error data index 0 len: {len(data)}")
                        for element in data:
                            print(element)
                            if element[4] != 'i2':
                                tdLog.exit(f"error: {element[4]} {element[-1]}")

                        tdSql.execute(f"alter table {func_name} add column c1 int")
                        tdSql.execute(f"alter table {func_name} rename column voltage vol")
                        tdSql.execute(f"INSERT INTO {func_name} VALUES('2018-10-05 16:00:00.000',1.3,233,0.31000,'i22',1)")

                    if index == 1:
                        if len(data) != 1:
                            tdLog.exit(f"error data index 1 len: {len(data)}")
                        for element in data:
                            print(element)
                            if element[4] != 'i22':
                                tdLog.exit(f"error: {element[4]} {element[-1]}")
                index += 1
        finally:
            pass

        tdSql.execute(f"alter table {func_name} drop column vol")
        tdSql.execute(f"reload topic {func_name} as select * from {func_name}")
        tdSql.execute(f"INSERT INTO {func_name} VALUES('2018-10-05 23:00:00.000',1.3,0.31000,'i35',28)")
        time.sleep(3)

        index = 0
        try:
            while True:
                res = consumer.poll(5)
                print(res)
                if not res:
                    print("res null")
                    break
                val = res.value()
                if val is None:
                    continue
                for block in val:
                    data = block.fetchall()
                    print(f"index {index},data len: {len(data)}")

                    if index == 0:
                        if len(data) != 1:
                            tdLog.exit(f"error data index {index} len: {len(data)}")
                        for element in data:
                            print(element)
                            if element[3] != 'i35' or element[-1] != 28:
                                tdLog.exit(f"error: {element[4]} {element[-1]}")
                    
                    index += 1
        finally:
            consumer.close()

    def check_alter_col_name_bytes_normal_table(self):
        func_name = self.get_function_name()
        print(f"start to excute {func_name}")
        tdSql.execute(f'use db')
        tdSql.execute(f'CREATE TABLE {func_name} (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT, info BINARY(4))')
        tdSql.execute(f"INSERT INTO {func_name} VALUES('2018-10-05 14:00:00.000',1.1,219,0.31000,'i13')")
        tdSql.execute(f"INSERT INTO {func_name} VALUES('2018-10-05 15:00:00.000',1.2,219,0.31000,'i2')")

        tdSql.execute(f'create topic {func_name} as select * from {func_name} where length(info) > 2')

        consumer_dict = {
            "group.id": "g1",
            "td.connect.user": "root",
            "td.connect.pass": "taosdata",
            "auto.offset.reset": "earliest",
        }
        consumer = Consumer(consumer_dict)

        try:
            consumer.subscribe([f"{func_name}"])
        except TmqError:
            tdLog.exit(f"subscribe error")

        index = 0;
        try:
            while True:
                res = consumer.poll(5)
                print(res)
                if not res:
                    print("res null")
                    break
                val = res.value()
                if val is None:
                    continue
                for block in val:
                    data = block.fetchall()
                    print(f"data len: {len(data)}")

                    if index == 0:
                        if len(data) != 1:
                            tdLog.exit(f"error data index 0 len: {len(data)}")
                        for element in data:
                            print(element)
                            if element[4] != 'i13':
                                tdLog.exit(f"error: {element[4]} {element[-1]}")

                        tdSql.execute(f"alter table {func_name} modify column info BINARY(16)")
                        tdSql.execute(f"INSERT INTO {func_name} VALUES('2018-10-05 16:00:00.000',1.3,219,0.31000,'i343434344')")

                    if index == 1:
                            if len(data) != 1:
                                tdLog.exit(f"error data index 1 len: {len(data)}")
                            for element in data:
                                print(element)
                                if element[4] != 'i343434344':
                                    tdLog.exit(f"error: {element[4]} {element[-1]}")
                index += 1
        finally:
            consumer.close()

    def test_tmq_ts6379(self):
        """summary: xxx

        description: xxx

        Since: xxx

        Labels: xxx

        Jira: xxx

        Catalog:
        - xxx:xxx

        History:
        - xxx
        - xxx

        """
        tdSql.execute(f'create database if not exists db vgroups 1')
        tdSql.execute(f'alter dnode 1 "debugflag 143"')

        self.check_add_table()

        self.check_add_drop_tag()
        self.check_alter_tag_name_bytes()
        self.check_alter_tag_value()

        self.check_add_drop_col()
        self.check_alter_col_name_bytes()

        self.check_add_drop_rename_col_normal_table()
        self.check_alter_col_name_bytes_normal_table()

        tdLog.success(f"{__file__} successfully executed")

