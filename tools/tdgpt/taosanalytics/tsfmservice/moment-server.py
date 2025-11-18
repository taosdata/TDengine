import re
from datetime import timedelta
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np

from sklearn.preprocessing import StandardScaler

import torch
from flask import Flask, request, jsonify

from momentfm.utils.utils import control_randomness
from momentfm import MOMENTPipeline

from torch.utils.data import DataLoader
control_randomness(seed=13) # Set random seeds for PyTorch, Numpy etc.

app = Flask(__name__)
device = torch.device("cuda" if torch.cuda.is_available() else "cpu")

_model_list = [
    'AutonLab/MOMENT-1-small',  # small model with 37.9M parameters
    'AutonLab/MOMENT-1-base',   # small model with 113M parameters
    'AutonLab/MOMENT-1-large',  # small model with 346M parameters
]

model = MOMENTPipeline.from_pretrained(
    _model_list[0],
    model_kwargs={'task_name': 'reconstruction'} # For imputation, we will load MOMENT in `reconstruction` mode
    # local_files_only=True,  # Whether or not to only look at local files (i.e., do not try to download the model).
)

model.init()
print(model)

device = "cuda:1" if torch.cuda.is_available() else "cpu"
model = model.to(device).float()

@app.route('/imputation', methods=['POST'])
def moment():
    data = request.get_json()
    if not data or 'input' not in data:
        return jsonify({
           'status': 'error',
            'error': 'Invalid input, please provide "input" field in JSON'
        }), 400

    input_data = data['input']
    input_ts = data['ts']
    time_precision = data['precision']
    freq = data['freq']

    res = do_handle_input_data(input_data, input_ts, time_precision, freq)

    res["status"] = "success"
    return res


class InputDataset(torch.utils.data.Dataset):
    def __init__(
            self,
            input_data,
            input_ts,
            time_precision: str,
            freq,
            data_stride_len: int = 512,
    ):
        self.seq_len = data_stride_len
        self.data_stride_len = data_stride_len
        self.time_precision = time_precision
        self.input_ts = input_ts
        self.scaler = StandardScaler()

        self.n_channels = 1
        self.length_timeseries_original = len(input_data)

        self.freq_val = self.freq_unit = None
        self.freq = freq

        # self.freq_val, self.freq_unit = 1, 'ms'
        self.parse_freq()

        complete_df, self.mask = complete_timeseries(self.input_ts, input_data, self.time_precision, self.freq,
                                                     self.freq_val, self.freq_unit)

        self.timeseries_complete_length = complete_df.shape[0]
        self.timeseries_padding_length = ((complete_df.shape[0] // self.seq_len) + 1) * self.seq_len

        inc = self.timeseries_padding_length - complete_df.shape[0]
        data, self.input_mask, self.mask = padding_data_list(complete_df, np.min(input_data), inc, self.mask, self.freq, self.freq_val, self.freq_unit)

        self.data, self.ts = self._transform_data(data)

    def get_data(self):
        return self.data

    def get_mask(self):
        return self.mask

    def get_ts(self):
        return self.ts

    def get_length_info(self):
        return  self.timeseries_padding_length, self.timeseries_complete_length

    def _transform_data(self, input_data):
        self.scaler.fit(input_data.values)
        return self.scaler.transform(input_data.values), input_data.index.to_numpy()

    def __getitem__(self, index):
        seq_start = self.data_stride_len * index
        seq_end = seq_start + self.seq_len

        if seq_end > self.timeseries_padding_length:
            seq_end = self.timeseries_padding_length
            # seq_end = seq_end - self.seq_len

        timeseries = self.data[seq_start:seq_end, :].T
        mask = self.mask[seq_start:seq_end]

        return timeseries, mask

    def __len__(self):
        return self.timeseries_padding_length // self.data_stride_len

    def parse_freq(self):
        match = re.match(r'^(\d*)([DHTSLU])$', self.freq)
        if not match:
            raise ValueError(f"failed to parse time string: {self.freq}")

        self.freq_val = int(match.group(1)) if len(match.group(1)) > 0 else 1
        self.freq_unit  = match.group(2)


def padding_data_list(df, val, n_rows, mask, freq, freq_value, freq_unit):
    # get the last timestamp
    last_time = df.index[-1]

    unit_map = {
        'D': 'days',
        'H': 'hours',
        'T': 'minutes',
        'S': 'seconds',
        'L': 'milliseconds',
        'U': 'microseconds',
    }

    unit = freq_unit
    if unit in unit_map:
        delta = timedelta(**{unit_map[unit]: freq_value})
    else:
        raise ValueError(f"Unsupported frequency: {unit}")

    # generate the increase timestamps series
    if isinstance(last_time, (np.datetime64, pd.Timestamp)):
        new_timestamp = pd.date_range(
            start=last_time + delta,
            periods=n_rows,
            freq=freq
        )
    else:
        new_timestamp = [last_time + i + 1 for i in range(n_rows)]

    new_df = pd.DataFrame({'value': [val] * n_rows, }, index=new_timestamp)

    # append the new rows
    input_mask = np.ones(df.shape[0] + n_rows, dtype=int)
    input_mask[-n_rows:] = 0

    return pd.concat([df, new_df]), input_mask, np.append(mask, np.ones(n_rows, dtype=int))

def draw_imputation_stride_result(trues, preds, masks):
    fig, axs = plt.subplots(2, 1, figsize=(10, 5))
    axs[0].set_title(f"Channel=0")
    axs[0].plot(trues[0, 0, :].squeeze(), label='Ground Truth', c='darkblue')
    axs[0].plot(preds[0, 0, :].squeeze(), label='Predictions', c='red')
    axs[0].legend(fontsize=14)

    axs[1].imshow(np.tile(masks[np.newaxis, 0, 0], reps=(8, 1)), cmap='binary')
    plt.savefig("moment.png")


def complete_timeseries(timestamps, values, precision, freq, freq_val, freq_unit:str):
    """
    Complete the time series data by generating a DataFrame with a full time range and marking missing values.

    Args:
        timestamps (list): A list of timestamps.
        values (list): A list of values corresponding to the timestamps.
        precision (str): The precision of the timestamps, e.g., 's' for seconds.
        freq (str, optional): The frequency of the time series, default is 'T' (minutes).

    Returns:
        tuple: A tuple containing the completed DataFrame and an array of missing value masks.
    """

    # Create an initial DataFrame, convert timestamps to pandas datetime type and set as index
    df = pd.DataFrame({
        'timestamp': pd.to_datetime(timestamps, unit=precision, errors='coerce'),
        'value': values
    }).set_index('timestamp')

    # 生成完整时间范围
    norm_freq = '1' + freq_unit
    full_range = pd.date_range(
        start=df.index.min().floor(norm_freq),
        end=df.index.max().ceil(norm_freq),
        freq=freq
    )

    # rebuild value list and fill with min value
    complete_df = df.reindex(full_range)

    # fill missing value
    missing_mask = complete_df['value'].isna()
    missing_indices = np.where(missing_mask)[0]

    # fill with min value
    complete_df = complete_df.fillna(np.min(values))

    mask = np.ones(len(complete_df), dtype=int)
    mask[missing_indices] = 0

    return complete_df, mask

def do_handle_input_data(value_list, ts_list, precision, freq):
    stride_len = 512

    input_data = InputDataset(value_list, ts_list, precision, freq, data_stride_len=stride_len)

    padding_len, comp_len = input_data.get_length_info()

    dim = padding_len // stride_len
    input_masks = torch.from_numpy(input_data.get_mask()).reshape(dim, stride_len)

    loader = DataLoader(input_data, batch_size=64, shuffle=False)

    trues, preds, masks = [], [], []
    with torch.no_grad():
        for batch_x, mask in loader:
            trues.append(batch_x.numpy())

            batch_x = batch_x.to(device).float()
            n_channels = batch_x.shape[1]

            # Reshape to [batch_size * n_channels, 1, window_size]
            batch_x = batch_x.reshape((-1, 1, stride_len))
            output = model(x_enc=batch_x, input_mask=input_masks, mask=mask)  # [batch_size, n_channels, window_size]

            reconstruction = output.reconstruction.detach().cpu().numpy()
            mask = mask.detach().squeeze().cpu().numpy()

            # Reshape back to [batch_size, n_channels, window_size]
            reconstruction = reconstruction.reshape((-1, n_channels, stride_len))
            mask = mask.reshape((-1, n_channels, stride_len))

            preds.append(reconstruction)
            masks.append(mask)

    preds = np.concatenate(preds)
    trues = np.concatenate(trues)
    masks = np.concatenate(masks)

    print(f"Shapes: preds={preds.shape} | trues={trues.shape} | masks={masks.shape}")

    # draw_imputation_stride_result(trues, preds, masks)

    padding_len = input_data.timeseries_padding_length
    comp_len = input_data.timeseries_complete_length

    # discard the padding data
    ts_list = input_data.get_ts()[:comp_len]
    res_data_list = preds.reshape(padding_len, 1)[:comp_len]
    res_mask_list = masks.reshape(padding_len)[:comp_len]

    data = merge_imputation_res(input_data, res_data_list, res_mask_list)
    
    return {
        "ts": convert_ts(ts_list, precision),
        "target":data,
        "mask":(1-res_mask_list).tolist()
    }

def merge_imputation_res(input_data, res_data_list, res_mask_list):
    _, comp_len = input_data.get_length_info()
    
    data = input_data.get_data()[:comp_len]

    index = np.where(res_mask_list == 0)[0]
    data[index] = res_data_list[index]
    
    # restore the previous value
    data = input_data.scaler.inverse_transform(data)

    return data.reshape(comp_len).tolist()

def convert_ts(ts_list, precision):
    if precision == 'ms':
        ts_list = ts_list.astype('int64') // 10e5
    elif precision == 'us':
        ts_list = ts_list.astype('int64') // 10e2
    elif precision == 'ns':
        ts_list = ts_list
    else:
        raise ValueError(f"Unsupported precision: {precision}")

    return ts_list.tolist()

if __name__ == '__main__':
    app.run(
        host='0.0.0.0',
        port=6062,
        threaded=True,
        debug=False
    )