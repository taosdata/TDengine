import os.path
import sys

import torch
from flask import Flask, request, jsonify
from gluonts.dataset.pandas import PandasDataset
from gluonts.dataset.split import split
from huggingface_hub import snapshot_download
from tqdm import tqdm

from uni2ts.model.moirai_moe import MoiraiMoEForecast, MoiraiMoEModule
from einops import rearrange

import numpy as np
import pandas as pd

app = Flask(__name__)
device = torch.device("cuda" if torch.cuda.is_available() else "cpu")

# maximum allowed input data for forecasting
_max_input_length = 2880
pretrained_model = None

@app.route('/ds_predict', methods=['POST'])
def do_predict():
    try:
        data = request.get_json()
        if not data or 'input' not in data:
            return jsonify({
                'status': 'error',
                'error': 'Invalid input, please provide "input" field in JSON'
            }), 400

        input_data = data['input']
        input_data = input_data[-_max_input_length:]

        prediction_length = data.get('next_len', 10)
        interval = data.get('conf_interval', 0.95)   # confidence interval

        past_dynamic_real = data.get('past_dynamic_real', [])
        dynamic_real = data.get('dynamic_real', [])

        # truncate the input data list
        for i in range(len(past_dynamic_real)):
            past_dynamic_real[i] = past_dynamic_real[i][-_max_input_length:]

        for i in range(len(dynamic_real)):
            dynamic_real[i] = dynamic_real[i][-_max_input_length-prediction_length:]

        if len(past_dynamic_real) + len(dynamic_real) == 0:  # single-variate forecasting processing
            resp = handle_singlevariate_forecast(input_data, prediction_length, interval)
        elif len(dynamic_real) > 0: # co-variate forecasting processing
            resp = handle_future_covariate_forecast(input_data, prediction_length, interval, past_dynamic_real, dynamic_real)
        else:
            resp = handle_covariate_forecast(input_data, prediction_length, interval, past_dynamic_real)

        return jsonify(resp), 200

    except Exception as e:
        print(f"error:{e}")
        return jsonify({
            'error': f'Prediction failed: {str(e)}'
        }), 500

def handle_singlevariate_forecast(input_data, prediction_length, interval):
    """uni-variate forecasting processing"""
    # Time series values. Shape: (batch, time, variate)
    past_target = rearrange(
        torch.as_tensor(input_data, dtype=torch.float32), "t -> 1 t 1"
    )

    # 1s if the value is observed, 0s otherwise. Shape: (batch, time, variate)
    past_observed_target = torch.ones_like(past_target, dtype=torch.bool)
    
    # 1s if the value is padding, 0s otherwise. Shape: (batch, time)
    past_is_pad = torch.zeros_like(past_target, dtype=torch.bool).squeeze(-1)

    model = MoiraiMoEForecast(
        module=pretrained_model,
        prediction_length=prediction_length,
        context_length=len(input_data),
        patch_size=16,
        num_samples=20,
        target_dim=1,
        feat_dynamic_real_dim=0,
        past_feat_dynamic_real_dim=0,
    )

    forecast = model(
        past_target=past_target.to(device),
        past_observed_target=past_observed_target.to(device),
        past_is_pad=past_is_pad.to(device),
    )

    res = forecast[0].cpu()

    pred_y = np.round(np.median(res, axis=0), decimals=4)
    
    return {
        'status': 'success',
        'output': pred_y.tolist(),
        'lower': res.quantile(0.5 - interval/2, dim=0).tolist(),
        'upper': res.quantile(0.5 + interval/2, dim=0).tolist(),
        'conf_interval':interval
    }

def handle_covariate_forecast(input_data, prediction_length, interval, past_dynamic_real):
    df = pd.DataFrame({
        "target": np.array(input_data),
        "item_id": np.full(len(input_data), 'A'),
    })

    # set the past_dynamic_real data
    for i in range(len(past_dynamic_real)):
        df[f'past_dynamic_real_{i}'] = past_dynamic_real[i]

    # extract the past_dynamic_real_data
    past_dynamic_cols = [col for col in df.columns if col.startswith("past_dynamic_real_")]

    ds = PandasDataset.from_long_dataframe(
        df,
        item_id="item_id",
        past_feat_dynamic_real=past_dynamic_cols,
        target="target",  # target column name
    )

    model = MoiraiMoEForecast(
        module=pretrained_model,
        prediction_length=prediction_length,
        context_length=len(input_data),
        patch_size=16,
        num_samples=100,
        target_dim=1,
        feat_dynamic_real_dim=ds.num_feat_dynamic_real,
        past_feat_dynamic_real_dim=ds.num_past_feat_dynamic_real,
    )

    predictor = model.create_predictor(batch_size=16)
    forecasts = predictor.predict(ds)

    forecasts_list = list(forecasts)

    return {
        'status': 'success',
        'output': forecasts_list[0].median.tolist(),
        'lower': forecasts_list[0].quantile(0.5 - interval / 2).tolist(),
        'upper': forecasts_list[0].quantile(0.5 + interval / 2).tolist(),
        'conf_interval': interval
    }


def handle_future_covariate_forecast(input_data, prediction_length, interval, past_dynamic_real, dynamic_real):
    """co-variate forecasting processing"""
    d = {
        "item_id": np.full(len(input_data) + prediction_length, 'A').tolist(),
        "target": input_data + np.random.normal(size=prediction_length).tolist(),
        "timestamp": pd.date_range(
            start="2020-01-01", periods=len(input_data) + prediction_length, freq="H"
        ).tolist(),
    }

    # set the past_dynamic_real data
    for i in range(len(past_dynamic_real)):
        d[f'past_dynamic_real_{i}'] = past_dynamic_real[i] + np.random.normal(size=prediction_length).tolist()

    # set the dynamic_real data
    for i in range(len(dynamic_real)):
        d[f'dynamic_real_{i}'] = dynamic_real[i]

    df = pd.DataFrame(d)

    # extract the past_dynamic_real_data
    past_dynamic_cols = [col for col in df.columns if col.startswith("past_dynamic_real_")]
    dynamic_cols = [col for col in df.columns if col.startswith("dynamic_real_")]

    ds = PandasDataset.from_long_dataframe(
        df,
        item_id="item_id",
        past_feat_dynamic_real=past_dynamic_cols,
        feat_dynamic_real=dynamic_cols,
        target="target",  # target column name
        timestamp="timestamp"
    )

    model = MoiraiMoEForecast(
        module=pretrained_model,
        prediction_length=prediction_length,
        context_length=len(input_data),
        patch_size=16,
        num_samples=100,
        target_dim=1,
        feat_dynamic_real_dim=ds.num_feat_dynamic_real,
        past_feat_dynamic_real_dim=ds.num_past_feat_dynamic_real,
    )

    # Split into train/test set
    train, test_template = split(
        ds, offset=-prediction_length
    )

    # Construct rolling window evaluation
    test_data = test_template.generate_instances(
        prediction_length=prediction_length,  # number of time steps for each prediction
        windows=prediction_length // prediction_length,  # number of windows in rolling window evaluation
        distance=prediction_length,  # number of time steps between each window - distance=PDT for non-overlapping windows
    )

    predictor = model.create_predictor(batch_size=16)
    forecasts = predictor.predict(test_data.input)

    # input_it = iter(test_data.input)
    # label_it = iter(test_data.label)
    forecasts_list = list(forecasts)

    return {
        'status': 'success',
        'output': forecasts_list[0].median.tolist(),
        'lower': forecasts_list[0].quantile(0.5 - interval / 2).tolist(),
        'upper': forecasts_list[0].quantile(0.5 + interval / 2).tolist(),
        'conf_interval': interval
    }


def download_model(model_name, root_dir, enable_ep = False):
    # model_list = ['Salesforce/moirai-1.0-R-small']
    ep = 'https://hf-mirror.com' if enable_ep else None
    model_list = [model_name]

    # root_dir = '/var/lib/taos/taosanode/model/moirai/'
    if not os.path.exists(root_dir):
        os.mkdir(root_dir)

    dst_folder = root_dir + '/'
    if not os.path.exists(dst_folder):
        os.mkdir(dst_folder)

    for item in tqdm(model_list):
        snapshot_download(
            repo_id=item,
            local_dir=dst_folder,  # storage directory
            local_dir_use_symlinks=False,   # disable the link
            resume_download=True,
            endpoint=ep
        )


def usage():
    name = os.path.basename(__file__)
    s = [
    "Usage:",
    f"Python {name}                    #use implicit download of small model",
    f"Python {name} model_index        #specify the model that would load when starting",
    f"Python {name} model_path model_name enable_ep  #specify the model name, local directory, and the proxy"
    ]
    return '\n'.join(s)

def main():
    """
    main function
    """

    global pretrained_model

    model_list = [
        'Salesforce/moirai-moe-1.0-R-small',  # small model with 117M parameters
        'Salesforce/moirai-moe-1.0-R-base',  # base model with 205M parameters
    ]

    num_of_arg = len(sys.argv)

    if num_of_arg == 1:
        # use the implicit download capability
        pretrained_model = MoiraiMoEModule.from_pretrained(
            model_list[0]
        ).to(device)
    elif num_of_arg == 2:
        # python moirai-server.py model_index
        model_index = int(sys.argv[1])
        if model_index < 0 or model_index >= len(model_list):
            print(f"invalid model index parameter, valid index:\n 0. {model_list[0]}\n 1. {model_list[1]}")
            exit(-1)

        pretrained_model = MoiraiMoEModule.from_pretrained(
            model_list[model_index]
        ).to(device)
    elif num_of_arg == 4:
        # let's load the model file from the user specified directory
        model_folder = sys.argv[1].strip('\'"')
        model_name = sys.argv[2].strip('\'"')
        enable_ep = bool(sys.argv[3])

        if model_name not in model_list:
            print(f"invalid model_name, valid model name as follows: {model_list}")
            exit(-1)

        if not os.path.exists(model_folder):
            print(f"the specified folder: {model_folder} not exists, start to create it")

        # check if the model file exists or not
        model_file = model_folder + '/model.safetensors'
        model_conf_file = model_folder + '/config.json'

        if not os.path.exists(model_file) or not os.path.exists(model_conf_file):
            download_model(model_name, model_folder, enable_ep=enable_ep)
        else:
            print("model file exists, start directly")

        """load the model from local folder"""
        pretrained_model = MoiraiMoEModule.from_pretrained(
            model_folder
        ).to(device)
    else:
        print("invalid parameters")
        print(usage())
        exit(-1)

    app.run(
        host='0.0.0.0',
        port=6039,
        threaded=True,
        debug=False
    )


if __name__ == "__main__":
    main()
