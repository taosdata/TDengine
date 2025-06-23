import re
import traceback
import yaml
from pathlib import Path

import pandas as pd
from prophet import Prophet

from taosanalytics.service import AbstractForecastService


# 算法实现类名称 需要以下划线 "_" 开始，并以 Service 结束
class _MyForecastService(AbstractForecastService):
    """ 定义类，从 AbstractForecastService 继承并实现其定义的抽象方法 execute  """
    name = 'prophet'

    # 该算法的描述信息 (建议添加)
    desc = """return the prophet time series data"""

    current_file = Path(__file__).resolve()
    # resource 目录路径：回退 3 层再拼接
    resource_dir = None
    config_file_prefix = "prophet_"
    config_file_extension = ".yaml"
    template_file_name = None

    # Prophet 支持的参数，避免传入不支持的参数
    allowed_params = [
        "growth",
        "changepoint_prior_scale",
        "changepoint_range",
        "yearly_seasonality",
        "weekly_seasonality",
        "daily_seasonality",
        "holidays",
        "seasonality_mode",
        "seasonality_prior_scale",
        "holidays_prior_scale",
        "interval_width",
        "mcmc_samples",
        "uncertainty_samples",
        "stan_backend"
    ]

    # 采样参数, 避免传入不支持的参数
    # 数据采样
    # '1S' 每 1 秒
    # '1T' 每 1 分钟
    # '1H' 每 1 小时
    # '1D' 每 1 天
    # '1W' 每 1 周
    # '1M' 每 1 月
    # '1Q' 每 1 季度
    # '1A' 每 1 年
    # 数据采样方法
    # .mean() 平均值（默认）
    # .sum() 求和
    # .min() 最小值
    # .max() 最大值
    # .count() 数量（样本个数）
    resample_allowed_params = [
        "resample",
        "resample_mode"
    ]

    # 饱和参数, 避免传入不支持的参数
    # 饱和最大值
    # 0 未指定, 如果saturating_cap不为None那么使用手动指定的值
    # 1: 自动选择最大值, saturating_cap = saturating_cap * saturating_cap_scale
    # 2: 自动选择最大值 * 1.2
    # 饱和最小值
    # 0 未指定, 如果saturating_floor不为None那么使用手动指定的值
    # 1: 自动选择最大值, saturating_floor = saturating_floor * saturating_floor_scale
    # 2: 自动选择最大值 * 1.2
    saturating_allowed_params = [
        "saturating_cap_max",
        "saturating_cap",
        "saturating_cap_scale",
        "saturating_floor_min",
        "saturating_floor"
        "saturating_floor_scale",
    ]

    def __init__(self):
        """类初始化方法"""
        super().__init__()
        current_file = Path(__file__).resolve()
        # resource 目录路径：回退 3 层再拼接
        self.resource_dir = current_file.parents[4] / "cfg"

    def execute(self):
        try:
            # 加载模型配置
            config = self.load_basic_config_from_yaml()

            # 加载模型数据
            data = self.__dict__

            # 数据预处理、数据读取
            df = pd.DataFrame({
                'ds': pd.to_datetime(data['ts_list'], unit='ms'),
                'y': data['list']
            })

            # 数据预处理、数据排序
            df = df.sort_values(by='ds').reset_index(drop=True)

            # 数据重采样
            resample_params = self.get_resample_params(config)
            df_resampled = self.do_resample(df, resample_params)

            # 开始训练、进行预测
            prophet_params = self.get_prophet_params(config)
            prophet = Prophet(**prophet_params)

            # 预测点开始时间
            start_time = df_resampled['ds'].max()
            if self.start_ts is not None:
                # 如果 start_ts 不为 None，则使用 start_ts
                start_time = pd.to_datetime(self.start_ts, unit='ms')

            # 预测点数量，预测点间隔, 单位为秒
            freq = None
            if self.time_step is not None:
                # 如果 time_step 不为 None，则使用 time_step
                freq = f'{self.time_step}S'

            validated_freq = freq
            if freq is None and resample_params is not None:
                validated_freq = self.get_validate_freq(resample_params)

            # 预测点数量，不需要包含历史数据
            future_pd = pd.date_range(start=start_time, periods=self.rows, freq=validated_freq)
            future = pd.DataFrame({'ds': future_pd})
            print(f"预测时间范围：{future_pd.min()} ~ {future_pd.max()}")
            # future = m.make_future_dataframe(start_time=start_time,
            #                                 periods=self.rows,
            #                                 freq=validated_freq,
            #                                 include_history=False)

            # 饱和参数设置
            self.set_saturating_params(prophet, df_resampled, future)

            # 参数设置 - 开启节假日参数设置

            """ 算法逻辑的核心实现"""
            # 训练并进行预测
            prophet.fit(df)
            forecast = prophet.predict(future)

            timestamp_ms = forecast['ds'].astype('int64') // 10 ** 6

            res = []
            # 设置预测结果时间戳列
            res.append(timestamp_ms.tolist())
            # 设置预测结果
            res.append(forecast['yhat'].tolist())

            """检查用户输入，是否要求返回预测置信区间上下界"""
            if self.return_conf:
                res.append(forecast['yhat_lower'].tolist())
                res.append(forecast['yhat_upper'].tolist())

            """返回结果"""
            return {"res": res, "mse": 0}

        except Exception as e:
            traceback.print_exc()  # 打印详细堆栈

    def set_saturating_params(self, m, df_resampled, future):
        growth = m.__getattribute__("growth")
        if growth != "logistic":
            return

        saturating_params = {k: self.params[k] for k in self.params if k in self.saturating_allowed_params}
        cap = self.get_cap(df_resampled, saturating_params)
        floor = self.get_floor(saturating_params)
        if cap is not None:
            df_resampled['cap'] = cap
            future['cap'] = cap

        if floor is not None:
            df_resampled['floor'] = floor
            future['floor'] = floor

        return

    # 设置算法参数
    def set_params(self, params):
        """该算法无需任何输入参数，直接调用父类函数，不处理算法参数设置逻辑"""
        # 获取默认参数设置
        super().set_params(params)

        # 获取扩展参数
        # 设置模型参数模板配置地址
        self.template_file_name = None
        if "p_cfg_tpl" in params:
            template_name = params['p_cfg_tpl']
            file_name = self.config_file_prefix + template_name + self.config_file_extension
            self.template_file_name = self.resource_dir / file_name

        # todo 调试目的，获取自定义参数
        filtered = {k: v for k, v in params.items() if k != "ts_list" and k != "list"}
        print(filtered)

        return

    def get_prophet_params(self, config):
        default_params = {'growth': 'linear'}
        return {**default_params, **{k: config[k] for k in self.allowed_params if k in config}}

    def get_resample_params(self, config):
        resample_params = {k: config[k] for k in config if k in self.resample_allowed_params}
        return resample_params

    # 获取饱和上限
    def get_cap(self, df_resampled, saturating_params):
        """获取饱和下限"""
        cap = None
        if self.str2bool(saturating_params.get("saturating_cap_max")):
            cap = df_resampled['y'].max()
        if saturating_params["saturating_cap"] is not None:
            cap = float(saturating_params["saturating_cap"])
        if cap is not None and saturating_params["saturating_cap_scale"] is not None:
            cap = cap * float(saturating_params["saturating_cap_scale"])

        return cap

    # 获取饱和下限
    def get_floor(self, df_resampled, saturating_params):
        """获取饱和下限"""
        floor = None
        if self.str2bool(saturating_params.get("saturating_floor_min")):
            floor = df_resampled['y'].min()
        if saturating_params["saturating_floor"] is not None:
            floor = float(saturating_params["saturating_floor"])
        if floor is not None and saturating_params["saturating_floor_scale"] is not None:
            floor = floor * float(saturating_params["saturating_floor_scale"])

        return floor

    def get_validate_freq(self, resample_params):
        if "resample" not in resample_params:
            return

        resample = resample_params["resample"]
        pattern = r'^\d*[STHDWMQA]$'  # 支持可选数字+单位，单位为秒、分钟、小时等
        if re.match(pattern, resample):
            return resample
        else:
            raise ValueError(f"Unsupported resample frequency: {resample}")

    def do_resample(self, df, resample_params):
        if resample_params is None or "resample" not in resample_params:
            return df

        default_resample_method = "mean"
        if "resample_method" in resample_params:
            default_resample_method = resample_params["resample_method"]
        return df.set_index('ds').resample(resample_params["resample"]).agg(default_resample_method).reset_index()

    def load_basic_config_from_yaml(self) -> dict:
        # 获取配置模板文件
        yaml_path = self.template_file_name
        print(yaml_path)
        with open(yaml_path, 'r', encoding='utf-8') as f:
            raw_params = yaml.safe_load(f)

        result = {}

        # 布尔型参数
        for key in ['daily_seasonality', 'weekly_seasonality', 'yearly_seasonality', 'saturating_cap_max', 'saturating_floor_min']:
            if key in raw_params:
                result[key] = self.str_to_bool(raw_params[key])

        # 浮点型参数
        for key in ['changepoint_prior_scale', 'seasonality_prior_scale', 'holidays_prior_scale',
                    'saturating_cap', 'saturating_cap_scale', 'saturating_floor', 'saturating_floor_scale']:
            if key in raw_params:
                result[key] = self.to_float(raw_params[key])

        # 字符串型参数
        for key in ['seasonality_mode', 'growth', 'stan_backend', 'resample', 'resample_mode']:
            if key in raw_params:
                result[key] = str(raw_params[key])

        holidays = []
        for item in raw_params.get('holidays', []):
            holiday_name = item.get('holiday')
            dates = item.get('dates', [])
            lower_window = item.get('lower_window', 0)
            upper_window = item.get('upper_window', 0)

            for date_str in dates:
                holidays.append({
                    'holiday': holiday_name,
                    'ds': pd.to_datetime(date_str),
                    'lower_window': lower_window,
                    'upper_window': upper_window
                })
        raw_params['holidays'] = holidays

        return result

    # 转换True 或者 False
    def str2bool(self, value):
        if value is None:
            return False
        return str(value).lower() in ("true", "false", "True", "False")

    def str_to_bool(self, val: str) -> bool:
        return str(val).lower() in ['true', 'True']

    def to_float(self, val: str) -> float | None:
        try:
            return float(val)
        except (ValueError, TypeError):
            return None
