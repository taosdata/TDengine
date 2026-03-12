---
title: "预测分析"
sidebar_label: "预测分析"
---

### 输入约定

`execute` 是预测分析算法的核心方法。框架调用该方法之前，在对象属性参数 `self.list` 中已经设置完毕用于预测的历史时间序列数据。

### 输出约定及父类属性说明

`execute` 方法执行完成后的返回一个如下字典对象，预测返回结果如下：

```python
return {
    "mse": mse, # 预测算法的拟合数据最小均方误差 (minimum squared error)
    "res": res  # 结果数组 [时间戳数组，预测结果数组，预测结果执行区间下界数组，预测结果执行区间上界数组]
}
```

预测算法的父类 `AbstractForecastService` 包含的对象属性如下：

| 属性名称        | 说明                                          | 默认值 |
|-------------|---------------------------------------------|-----|
| period      | 输入时间序列的周期性，多少个数据点表示一个完整的周期。如果没有周期性，设置为 0 即可 |  0  |
| start_ts    | 预测结果的开始时间                                   |  0  |
| time_step   | 预测结果的两个数据点之间时间间隔                            | 0   |
| rows        | 预测结果的数量                                     |  0  |
| return_conf | 预测结果中是否包含置信区间范围，如果不包含置信区间，那么上界和下界与自身相同      |  1  |
| conf        | 置信区间分位数                                     | 95  |

### 示例代码

下面我们开发一个示例预测算法，对于任何输入的时间序列数据，固定返回值 1 作为预测结果。

```python
from taosanalytics.service import AbstractForecastService

# 算法实现类名称 需要以下划线 "_" 开始，并以 Service 结束
class _MyForecastService(AbstractForecastService):
    """ 定义类，从 AbstractForecastService 继承并实现其定义的抽象方法 execute  """

    # 定义算法调用关键词，全小写 ASCII 码
    name = 'myfc'

    # 该算法的描述信息 (建议添加)
    desc = """return the forecast time series data"""

    def __init__(self):
        """类初始化方法"""
        super().__init__()

    def execute(self):
        """ 算法逻辑的核心实现"""
        res = []

        """这个预测算法固定返回 1 作为预测值，预测值的数量是用户通过 self.fc_rows 指定"""
        ts_list = [self.start_ts + i * self.time_step for i in range(self.rows)]
        res.append(ts_list)  # 设置预测结果时间戳列

        """生成全部为 1 的预测结果 """
        res_list = [1] * self.rows
        res.append(res_list)

        """检查用户输入，是否要求返回预测置信区间上下界"""
        if self.return_conf:
            """对于没有计算预测置信区间上下界的算法，直接返回预测值作为上下界即可"""
            bound_list = [1] * self.rows
            res.append(bound_list)  # 预测结果置信区间下界
            res.append(bound_list)  # 预测结果执行区间上界

        """返回结果"""
        return {"res": res, "mse": 0}

    def set_params(self, params):
        """该算法无需任何输入参数，直接调用父类函数，不处理算法参数设置逻辑"""
        return super().set_params(params)

```

将该文件保存在 `./lib/taosanalytics/algo/fc/` 目录下，然后重启 taosanode 服务。在 TDengine TSDB 命令行接口中执行 `SHOW ANODES FULL` 能够看到新加入的算法。应用就可以通过 SQL 语句调用该预测算法。

```SQL
--- 对 col 列进行异常检测，通过指定 algo 参数为 myfc 来调用新添加的预测类
SELECT  _flow, _fhigh, _frowts, FORECAST(col_name, "algo=myfc")
FROM foo;
```

如果是第一次启动 Anode, 请按照 [运维管理指南](../../../management) 里的步骤先将该 Anode 添加到 TDengine TSDB 系统中。

### 单元测试

在测试目录`taosanalytics/test`中的 forecast_test.py 中增加单元测试用例或添加新的测试文件。单元测试依赖 Python Unit test 包。

```python
def test_myfc(self):
    """ 测试 myfc 类 """
    s = loader.get_service("myfc")

    # 设置用于预测分析的数据
    s.set_input_list(self.get_input_list(), None)
    # 检查预测结果应该全部为 1
    r = s.set_params(
        {"fc_rows": 10, "start_ts": 171000000, "time_step": 86400 * 30, "start_p": 0}
    )
    r = s.execute()

    expected_list = [1] * 10
    self.assertEqlist(r["res"][0], expected_list)
```
