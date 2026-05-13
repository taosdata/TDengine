# 用例编写规范
## 1. 背景
在 TDengine 的测试中，长期存在以下两个问题：
### 问题一：测试用例缺乏整理
在 TDengine 的 CI 测试中，执行了 1,600+ 个测试用例。但随着产品迭代，这些用例覆盖了哪些功能，没有覆盖哪些功能，缺乏文档整理。构建和维护一个详尽的测试用例描述文档，将有助于我们识别缺失的用例，并进行针对性地补充。但使用传统的方法，用文档或测试用例管理系统来维护测试用例，存在以下两个弊端：

1. 维护成本高
2. 文档和代码分离，导致文档容易“生锈”
### 问题二：多个测试框架并存
由于历史原因，TDengine 的 CI 测试用例是使用不同的语言、框架 （包括 TSIM， System Test， ARMY 等） 编写的，分布在 TDengine/tests 目录中的不同的子目录下，用例的管理稍显混乱。为了执行“全量测试”，需要使用不同的框架，分别执行多个不同子目录下的用例才能完成。每个框架编写的用例，编写方式和执行方式都存在差异，也很难生成统一风格的测试报告。## 2. 变更历史

## 2. 变更历史
| 日期         | 版本         | 负责人          |主要修改内容|
|:-----------|:-----------|:-------------| :--- |
| 2025/2/12  | 0.1        | @冯超 @霍宏 @王旭  | 初始文档|
| 2025/2/17  | 0.2        | @霍宏 @王旭      | 修改用例编写规范 |  

## 3. 解决方案
为了解决以上问题，平台部经和研发负责人的讨论，达成一致意见，研发部需要进行以下调整：

1. 平台部负责对现有的测试框架进行整合和优化，以后所有用例的编写，均使用统一的语言、框架，主要改动如下：
    - 统一使用 Python 进行功能用例的编写
    - 引入 pytest 作为 test runner， pytest 是 Python 生态中应用非常广泛，可以方便地控制用例的执行粒度（执行一个用例、一组用例、按 marker 执行用例等），除此以外，它还提供了丰富的插件，例如：与测试报告相关的 allure-pytest 等
    - 将 system-test， TestNG 等现有框架的核心库进行整合，以简化测试用例的编写
2. 产品研发人员，编写用例时，需要以统一的格式为用例添加描述信息等 metadata， 平台部将使用工具，以自动化的方式，从测试用例代码自动生成测试用例文档，这也符合我们公司推行的“一切代码化”的要求。
3. 工具平台部将使用工具 mkdocs + mkdocstrings 提取以上信息，生成用例文档，并将其部署至 Github Pages 或内网
## 4. 目标和范围
本规范适用于以下范围：

- 仓库
    - TDengine: /tests/test_new （暂定名，为了和当前的 /tests 目录下存量用例进行区分）
    - taosX: /tests/e2e
- 测试类型：端到端的功能测试
- 编程语言：Python

其它仓库或其它类型的测试，基于不同框架/语言实现，暂时不对用例规范做强制要求，可仅作为参考。
## 5. 用例编写规范
### 5.1 用例目录
对用例目录的要求如下：

1. 新增用例统一存放至 tests/test_new 
2. 用例采用两级目录的形式组织，详见：附录1
### 5.2 用例文件
对用例文件的要求如下：

1. 用例 Python 文件命名应以 test_开头，例如：test_join.py
5.3 用例规范
对用例文件内容的要求如下：

1. 用例 Python 文件中，需要定义一个 Class，以 Test 开头命名，建议与文件名一致，例如：TestJoin
2. 一个用例 Python 文件可以包含多个用例，以 test class 下的 test method 的形式存在，test method 的命名需要以 test_ 开头，例如：test_join()
3. Class 中通常定义 init(),run(), stop()等方法，用于用例初始化数据、用例执行、用例清理环境等操作；这里，主要是为了与当前的 system-test 框架兼容，保证大家使用新规范编写的用例，可以用当前的框架执行，待新框架的整合完成后，将不存在这种要求，平台将进行统一的替换。
4. run()方法中，需要调用该类中定义的所有 test methods, 不涉及其他逻辑
5. 在每个 test method 中，以标准的 Python docstring 的形式，添加用例描述信息，包括多个字段，每个字段之间用空行间隔，字段及其要求，如下所示：
 | 字段名称|字段描述|是否必填|
|:--------| ---- | :---: |
|                                                  |用例的一句话描述，仅支持一行|是|
|                                                  |用例的详细描述，支持多行|是|
| Since                                            | 用例开始支持的 TDengine 版本，新 Feature 必填                | 是                                                 |
| Lables                                           | 用例标签，多个标签用英文逗号分隔，标签采用 snake case, 即全部小写，多个单词用 _ 连接|否|
| Jira                                             | Jira ID, 多个用英文逗号分隔                             | 否                                                 |
| History                                          |用例变更历史|否|

### 5.4 其它要求、建议

1. 建议相同环境/数据配置的用例放在同一文件中执行，提高运行效率
2. 用例中的调试信息，应使用框架提供的 log 打印，例如：util.log.tdLog，而不要使用 print() 函数
### 5.5 用例模板
```Python
# tests/test_new/xxx/xxx/test_xxx.py
import ...

class TestXxxx:
    def init(self, args=value ...):
        tdLog.debug("start to execute %s" % __file__)
        ...
        
        
    def user_defined_function(self, args=value ...):
        ...


    def test_template(self):
        """用例目标，必填，用一行简短总结
        <空行>
        用例详细描述，必填，允许多行
        <空行>
        Since: 用例开始支持的TDengine版本，新增用例必填
        <空行>
        Labels: 筛选标签，选填，多个标签用英文逗号分隔
        <空行>
        Jira: 相关jira任务id，选填
        <空行>
        History: 用例变更历史，选填，每行一次变更信息
            - 日期1 变更人1 变更原因1
            - 日期2 变更人2 变更原因2
        """
        ... # test case code


    def test_demo(self):
        """测试超级表插入各种数据类型

        使用多种数据类型创建超级表，向超级表插入数据，
        包括：常规数据，空数据，边界值等，插入均执行成功
        
        Since: v3.3.0.0
        
        Labels: stable, data_type
        
        Jira: TD-12345, TS-1234
        
        History:
            - 2024-2-6 Feng Chao Created
            - 2024-2-7 Huo Hong updated for feature TD-23456
        """
        ... # test case code
    
    
    def run(self):
        self.test_template()
        self.test_demo()
        ...# case function list
        

    def stop(self):
        ...
        tdLog.success("%s successfully executed" % __file__)
```
说明：

- 在 Docstring 中，每一部分之间需要用空行分隔，否则会出现格式错误

## 6. 附录
### 6.1 附录1：用例目录结构
新增用例在 TDengine/tests/test_new 目录下，采用二级目录的形式组织，以下仅作为建议，大家可以提出 comments:

```
test_new/
├── data_write/
│   ├── csv/
│   ├── sql_statement/
│   ├── stmt/
│   ├── stmt2/
│   └── schemaless/
├── metadata/
│   ├── child_table/
│   ├── data_type/
│   ├── db/
│   ├── dnode/
│   ├── mnode/
│   ├── naming_rule/
│   ├── qnode/
│   ├── reqular_table/
│   ├── snode/
│   ├── super_table/
│   ├── system_table/
│   ├── tag_index/
│   └── time_precision/
├── high_availability/
│   ├── 2_replica/
│   ├── 3_replica/
│   ├── active_active/
│   ├── cluster_expansion_reduction/
│   └── replica_change/
├── operation/
│   ├── balance/
│   ├── configuration/
│   ├── redistribute/
│   ├── restore/
│   ├── slow_query/
│   ├── split/
│   ├── transaction/
│   └── upgrade/
├── query/
│   ├── case_when/
│   ├── escape_character/
│   ├── function/
│   ├── having/
│   ├── hint/
│   ├── index/
│   ├── join/
│   ├── nested/
│   ├── operator/
│   ├── pseudo_column/
│   ├── sql_syntax/
│   ├── union/
│   ├── view/
│   └── window/
├── security/
│   ├── audit/
│   ├── authorization/
│   ├── permission/
│   └── encryption/
├── storage/
│   ├── compress/
│   ├── multilevel/
│   ├── sma/
│   ├── tsma/
│   └── ss/
├── stream/
├── tdgpt/
├── tmq/
└── udf/
```