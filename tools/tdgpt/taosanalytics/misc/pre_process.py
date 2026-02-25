# encoding:utf-8
# pylint: disable=c0103
import numpy as np
from matplotlib import pyplot as plt
from sklearn.preprocessing import MinMaxScaler
import pandas as pd
import json
import os


def load_config(config_path='config.json'):
    if not os.path.exists(config_path):
        # 如果配置文件不存在，创建一个默认的
        default_config = {
            "time_col": "ts",
            "value_col": "value",
            "use_mad": True,
            "mad_threshold": 3.0,
            "use_normalization": True,
            "input_path": "input_data.csv",
            "output_path": "processed_data.csv",
            "draw_data": False,
            "figure_path": "result.png"
        }

        with open(config_path, 'w') as f:
            json.dump(default_config, f, indent=4)

        return default_config

    with open(config_path, 'r') as f:
        return json.load(f)


def handle_outliers_mad(df, col, threshold=3.0):
    """使用 MAD 检测并移除异常值"""
    median_val = df[col].median()
    abs_deviation = (df[col] - median_val).abs()
    mad = abs_deviation.median()

    if mad == 0:
        return df, (df[col].min(), df[col].max())

    lower_bound = median_val - threshold * mad
    upper_bound = median_val + threshold * mad

    df.loc[(df[col] < lower_bound) | (df[col] > upper_bound), col] = np.nan
    return df, (lower_bound, upper_bound)


def plot_comparison(df_orig, df_proc, col, config, bounds=None, scaler=None):
    """可视化对比"""
    plt.figure(figsize=(10, 6))

    # 绘制原始数据
    plt.plot(df_orig.index, df_orig[col], label='Original Data', alpha=0.4, color='gray', linestyle='--')

    # 异常值 (在原始数据上标记)
    outliers_mad = df_orig[
        (df_orig[col] < bounds[0]) |
        (df_orig[col] > bounds[1])
        ]

    if not outliers_mad.empty and config.get('mad_threshold') != 0:
        plt.scatter(outliers_mad.index, outliers_mad[col], color='red', s=10,
                    label=f'MAD abnormal (Threshold={config.get("mad_threshold")})', zorder=5)

    # 如果启用了归一化，绘图时反转回原始量纲进行对比
    if config['use_normalization'] and scaler:
        display_val = scaler.inverse_transform(df_proc[[col]])
    else:
        display_val = df_proc[col]

    plt.plot(df_proc.index, display_val, label='Processed Data', color='teal', linewidth=2)

    if bounds and config['use_mad']:
        plt.axhline(y=bounds[0], color='salmon', linestyle=':', label='MAD Lower Bound')
        plt.axhline(y=bounds[1], color='salmon', linestyle=':', label='MAD Upper Bound')

    plt.title(f"Data Preprocessing Report (Norm: {config['use_normalization']}, MAD: {config['use_mad']})")
    plt.xlabel("Index")
    plt.ylabel(col)
    plt.legend()
    plt.tight_layout()

    plt.savefig(config.get('figure_path', 'result.png'))


def preprocess_v1():
    config = load_config()
    val_col = config['value_col']
    time_col = config['time_col']

    # 1. 读取数据
    if not os.path.exists(config['input_path']):
        print(f"Error: {config['input_path']} 不存在。请运行测试数据生成脚本。")
        return

    df = pd.read_csv(config['input_path'])
    df[time_col] = pd.to_datetime(df[time_col])
    df = df.sort_values(by=time_col).reset_index(drop=True)
    df_original = df.copy()

    # 2. MAD 异常值处理
    bounds = None
    if config.get('use_mad', True):
        df, bounds = handle_outliers_mad(df, val_col, config['mad_threshold'])
        print(f"MAD Filtering: Done. Bounds: {bounds}")

    # 3. 插值填充 (针对原始 NaN 和 MAD 产生的 NaN)
    df[val_col] = df[val_col].interpolate(method='linear', limit_direction='both')
    print("Interpolation: Done.")

    # 4. 归一化 (根据配置开关)
    scaler = None
    if config.get('use_normalization', True):
        scaler = MinMaxScaler()
        df[val_col] = scaler.fit_transform(df[[val_col]])
        print("Normalization: Applied.")
    else:
        print("Normalization: Skipped.")

    # 5. 保存结果
    df.to_csv(config['output_path'], index=False)
    print(f"Success: Result saved to {config['output_path']}")

    # 6. 可视化
    if config.get("draw_data", False):
        path = config.get('figure_path', 'result.png')
        print(f"Start to draw result data in {path}")
        plot_comparison(df_original, df, val_col, config, bounds, scaler)


def generate_mock_csv(filename="input_data.csv"):
    np.random.seed(42)
    rows = 100
    ts = pd.date_range(start='2024-01-01', periods=rows, freq='H')

    # 生成基础正弦波数据
    values = np.sin(np.linspace(0, 10, rows)) * 50 + 100

    # 注入一些噪声
    values += np.random.normal(0, 2, rows)

    # 注入 5 个缺失值 (NaN)
    nan_indices = np.random.choice(rows, 5, replace=False)
    values[nan_indices] = np.nan

    # 注入 3 个极端异常值
    outlier_indices = [20, 50, 80]
    values[20] = 500  # 极大值
    values[50] = -200  # 极小值
    values[80] = 450  # 极大值

    df = pd.DataFrame({'ts': ts, 'value': values})
    df.to_csv(filename, index=False)
    print(f"Mock data generated: {filename}")

if __name__ == "__main__":
    # generate_mock_csv()
    preprocess_v1()