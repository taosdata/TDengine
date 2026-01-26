# encoding:utf-8
# pylint: disable=c0103
import numpy as np
from matplotlib import pyplot as plt
from sklearn.preprocessing import MinMaxScaler
import pandas as pd
import json
import os

def draw_data_figure(df_original, df_processed, time_col, value_col, lower_bound_mad,
                     upper_bound_mad, mad, mad_threshold, scaler):
    plt.rcParams['font.sans-serif'] = ['AR PL UKai CN']  # 用来正常显示中文标签
    plt.rcParams['axes.unicode_minus'] = False  # 用来正常显示负号

    plt.figure(figsize=(10, 8))

    # 原始数据
    plt.plot(df_original[time_col], df_original[value_col], label='原始数据 (Original Data)', marker='o',
             markersize=4, linestyle='--', alpha=0.6, color='skyblue')

    # 异常值 (在原始数据上标记)
    outliers_mad = df_original[
        (df_original[value_col] < lower_bound_mad) |
        (df_original[value_col] > upper_bound_mad)
        ]
    if not outliers_mad.empty and mad != 0:
        plt.scatter(outliers_mad[time_col], outliers_mad[value_col], color='red', s=100,
                    label=f'MAD 异常值 (Threshold={mad_threshold})', zorder=5)

    # MAD 边界
    plt.axhline(y=lower_bound_mad, color='gray', linestyle=':', label='MAD 下限')
    plt.axhline(y=upper_bound_mad, color='gray', linestyle=':', label='MAD 上限')

    # 处理后的数据
    # 为了可视化，我们需要将处理后的数据 (已归一化) 反向缩放回来，
    # 这样才能和原始数据以及MAD边界在同一张图上对比。
    # 注意: 实际返回的是归一化后的数据，这里只是为了可视化做反向操作
    df_processed_rescaled = df_processed
    df_processed_rescaled[value_col] = scaler.inverse_transform(df_processed_rescaled[[value_col]])

    plt.plot(df_processed_rescaled[time_col], df_processed_rescaled[value_col],
             label='处理后数据 (异常值移除+插值)', color='green', linewidth=2)

    plt.title(f'{value_col} 数据预处理结果 (MAD 异常值检测)', fontsize=16)
    plt.xlabel('时间戳 (Timestamp)', fontsize=12)
    plt.ylabel('数值 (Value)', fontsize=12)
    plt.legend(fontsize=10)
    plt.grid(True, linestyle='--', alpha=0.7)
    plt.tight_layout()

    plt.savefig('result.png')

def preprocess_data(input_data, time_col='ts', value_col='value', mad_threshold=3.5):
    """
    数据预处理脚本：MAD 异常值处理 -> 插值填充 -> 归一化 -> 可视化

    参数:
    df (pd.DataFrame): 包含时间戳和数值的DataFrame
    time_col (str): 时间戳列的名称
    value_col (str): 数值列的名称
    mad_threshold (float): MAD异常值检测的阈值。
                           通常3.5或2.5是常见选择，数值越大越宽松。

    返回:
    pd.DataFrame: 处理后的DataFrame
    """

    # 0. 数据备份和排序，方便可视化对比
    df_original = input_data.copy()
    df = input_data.sort_values(by=time_col).reset_index(drop=True)

    # --- 1. MAD (Median Absolute Deviation) 异常值处理 ---
    # 计算中位数
    median_val = df[value_col].median()

    # 计算每个数据点与中位数的绝对偏差
    abs_deviation = (df[value_col] - median_val).abs()

    # 计算中位数绝对偏差 (MAD)
    # 使用 pd.Series.median() 来处理可能存在的NaN值
    mad = abs_deviation.median()

    # 为了使MAD更接近标准差，通常会乘以一个常数 (1.4826 适用于正态分布)
    # 但是对于MAD，我们直接用MAD本身作为尺度，mad_threshold就是多少个MAD
    if mad == 0:  # 避免除以零，如果所有数据都相同，则没有异常值
        print(f"Warning: MAD is 0 for column '{value_col}'. No outliers detected by MAD method.")
        # 如果mad为0，说明所有非NaN值都相同，此时认为没有异常值
        df_cleaned_mad = df.copy()
        lower_bound_mad = median_val
        upper_bound_mad = median_val
    else:
        # 定义MAD异常值边界
        lower_bound_mad = median_val - mad_threshold * mad
        upper_bound_mad = median_val + mad_threshold * mad

        # 标记异常值 (将其设为 NaN，以便后续插值处理)
        # 这里使用 df.loc 而不是直接 df[...] = np.nan 来避免SettingWithCopyWarning
        df_cleaned_mad = df.copy()
        df_cleaned_mad.loc[
            (df_cleaned_mad[value_col] < lower_bound_mad) |
            (df_cleaned_mad[value_col] > upper_bound_mad),
            value_col
        ] = np.nan

    print(f"\n--- MAD 异常值处理报告 for '{value_col}' ---")
    print(f"中位数 (Median): {median_val:.2f}")
    print(f"中位数绝对偏差 (MAD): {mad:.2f}")
    print(f"MAD 判定下界: {lower_bound_mad:.2f}")
    print(f"MAD 判定上界: {upper_bound_mad:.2f}")
    print(f"异常值数量 (MAD): {df_cleaned_mad[value_col].isna().sum() - df[value_col].isna().sum()}")

    # --- 2. 缺失值填充 (Interpolate) ---
    # 使用线性插值填充 NaN 值 (包括原始的和MAD标记的异常值)
    df_filled = df_cleaned_mad.copy()
    df_filled[value_col] = df_filled[value_col].interpolate(method='linear', limit_direction='both')

    # 如果首尾仍然有无法插值的 NaN (例如：如果整个列都是NaN)，则删除这些行
    df_filled = df_filled.dropna(subset=[value_col])

    # --- 3. 归一化 (Min-Max Scaling) ---
    scaler = MinMaxScaler()
    df_processed = df_filled.copy()
    # MinMaxScaler 期望输入是二维数组
    df_processed[value_col] = scaler.fit_transform(df_processed[[value_col]])

    print(f"\n处理后的数据预览 ('{value_col}' 列已归一化):")
    print(df_processed.head())
    print(
        f"归一化后的 '{value_col}' 列范围: [{df_processed[value_col].min():.2f}, {df_processed[value_col].max():.2f}]")

    draw_data_figure(df_original, df_processed.copy(), time_col, value_col,
                     lower_bound_mad, upper_bound_mad, mad, mad_threshold, scaler)

    return df_processed  # 返回的是归一化后的数据


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