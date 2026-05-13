# 查询（包括cache）测试用例

| 测试类型 | 测试场景 | 用例No. | 测试用例名称 | 数据准备 | 测试步骤 | 测试结果 | 备注 |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 功能测试 | 1、创建数据库 2、准备测试数据 3、数量结果验证 4、准备和2结构完全一样、数量一样的数据表，通过insert into进行数据插入 5、数量结果验证 | 1 【基础用例】 【用例1-11基于单副本】 |  | 创建数据库、超级表、普通表、数据插入、然后验证count数量是否一致，同时该数据集作为下面查询用例的数据集 | 1、Create db replica n[1\2\3] 2、准备3个超级表和3个普通班，每个超级表覆盖一种PK【uint64、int64、varchar】（如果在增加类型，同步新增超级表和普通表），对每个子表和普通表插入数据。 3、验证count数量。 4、对2中的6个表的数据通过insert into写入到新准备的6个表中。 5、数量结果验证。 | 1、创建成功，系统表中replica=n。 2、各表创建成功，show create table显示pk关键字，数据插入成功。 3、count数量=插入数量。 4、insert into成功。 5、count数量=插入数量。 | 通过 |
|  | 1、利用用例1的数据准备。 2、覆盖各个函数的查询。 3、explain解析。 | 2【基础用例】 |  | 复用用例1数据集 | 1、数据借用。 2、每个函数进行查询，每个函数6条查询语句。[sql1:select fun from stable_1; sql2:select fun from stable_2 order by ts,pk; sql3:select fun from stable_3 order by ts desc,pk desc; sql4:select fun from table_1; sql5:select fun from table_2 order by ts ,pk desc; sql6:select fun from table_3 order by ts desc,pk] 3、explain 上述语句。 | 1、数据借用。 2、各个函数冒烟测试通过。 3、explain执行不报错。 | 通过 |
|  | 1、在用例2的基础上对重点函数扩展查询 | 3【基础用例】 |  | 复用用例1数据集 | 1、选择本次影响的函数first、last、last_row、interp、diff、irate、unique、twa、derivative、count。 | 1、下面查询语句扩展，涉及函数相关的，只测试这10个函数。 | 目前用例覆盖的组合均已通过，后续会继续扩展场景在进行更多的组合 |
|  | 1、pk列投影相关查询+各语句explain解析 | 4【基础用例】 |  | 复用用例1数据集 | 1、支持投影查询 2、支持pk列的select过滤 3、支持伪列_c1的select过滤 4、对pk列的count查询 5、对pk列的distinct查询 6、对ts+pk列的distinct查询 7、对pk列的count（distinct ）查询 8、对pk列的select tags pk 的查询 9、对pk列的case when查询 10、对pk列的group by 11、对pk列的partition by 12、对ts、pk列的partition by 13、对pk的union、union all 14、对ts、pk列的union、union all 15、对pk列的嵌套查询(外层可以直接用) 16、对pk列在where中的过滤，包括数据类型时（>,>=,=,<=,<,in,between and) varchar类型时(match,nmatch,正则,like,%,_)等 17、对pk列order by asc/desc 18、对ts、pk列order by asc/desc 19、对ts、pk列进行join 20、对pk列进行limit、offset 21、对ts、pk列进行limit、offset 22、对pk列进行slimit、soffset 23、对ts、pk列进行slimit、soffset | 1、执行成功，结果正确，下同。 | 目前用例覆盖的组合均已通过，后续会继续扩展场景在进行更多的组合 |
|  | 1、pk列结合interp函数相关查询+各语句explain解析 | 5、【基础用例】 |  | 复用用例1数据集 | 1、选择功能用例4中的1-23中可以和interp一起搭配使用的部分。 2、对pk进行interp。 3、搭配range子句。 4、搭配every子句。 5、搭配fill子句。 | 同上 | 目前用例覆盖的组合均已通过，后续会继续扩展场景在进行更多的组合 |
|  | 1、pk列结合first、unique、derivative、diff、irate、twa函数相关查询+各语句explain解析 | 6、【基础用例】 |  | 复用用例1数据集 | 1、选择功能用例4中的1-23中可以和first、unique、derivative、diff、irate、twa一起搭配使用的部分。 2、对pk进行first、unique、derivative、diff、irate、twa。 3、搭配窗口子句。 | 同上 | 通过 |
|  | 1、pk列结合last、last_row函数相关查询+各语句explain解析 | 7、【基础用例】 |  | 复用用例1数据集 | 1、选择功能用例4中的1-23中可以和last、last_row一起搭配使用的部分。 2、对pk进行last、last_row。 3、搭配窗口子句。 4、搭配cachemodel= none、both、last_row、last_value切换。 | 同上 | 目前用例覆盖的组合均已通过，后续会继续扩展场景在进行更多的组合 |
|  | 1、在用例2的基础上对数据进行更新 2、覆盖各个函数的查询 | 8、【基础用例】 |  | 参考用例1数据集更新数据集 | 1、对数据进行相同时间戳+pk的更新 2、每个函数进行查询，每个函数6条查询语句同上。 | 1、数据更新成功。 2、各个函数冒烟测试通过。 | 通过 |
|  | 1、对用例3-7在更新的数据集上验证一遍 | 9、【基础用例】 |  | 复用用例8数据集 | 1、对用例3-7在更新的数据集上验证一遍 | 1、3-7组用例测试通过 | 通过 |
|  | 1、在用例2的基础上对数据进行数据删除 2、覆盖各个函数的查询 | 10、【基础用例】 |  | 对用例8数据集进行删除 | 1、对数据进行相同时间戳+pk的数据删除 2、每个函数进行查询，每个函数6条查询语句同上。 | 1、数据删除成功。 2、各个函数冒烟测试通过，查询之后返回行数=0。 | 通过 |
|  | 1、对用例3-7在更新的数据集上验证一遍 | 11、【基础用例】 |  | 复用用例10数据集 | 1、对用例3-7在更新的数据集上验证一遍 | 1、3-7组用例查询之后返回行数=0。 | 通过 |
| 双副本 | 基于上述用例1-11，修改replica=2，验证上述用例 |  |  |  |  |  | 通过 |
| 三副本 | 基于上述用例1-11，修改replica=3，验证上述用例 |  |  |  |  |  | 通过 |
| -R | 增加resful的测试脚本 |  |  |  |  |  | 通过 |
| Qnode=2、3、4 | 增加qnode=2、3、4的测试脚本 |  |  |  |  |  | 通过 |
