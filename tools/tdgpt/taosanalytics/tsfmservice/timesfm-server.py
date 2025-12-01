import os
import sys

import torch
from flask import Flask, request, jsonify
import timesfm
import numpy as np
from huggingface_hub import snapshot_download
from tqdm import tqdm

app = Flask(__name__)
pretrained_model = None

def download_model(model_name, root_dir, enable_ep = False):
    # model_list = ['google/timesfm-2.0-500m-pytorch']
    ep = 'https://hf-mirror.com' if enable_ep else None
    model_list = [model_name]

    # root_dir = '/var/lib/taos/taosanode/model/timesfm/'
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
        prediction_length = data.get('next_len', 10)
        interval = data.get('conf_interval', 0.95)   # confidence interval

        forecast_input = [
            input_data
        ]
        frequency_input = [0]  # , 1, 2]

        point_forecast, experimental_quantile_forecast = pretrained_model.forecast(
            forecast_input,
            freq=frequency_input,
        )

        pred_y = point_forecast[0][:prediction_length].tolist()
        lower = np.percentile(experimental_quantile_forecast[0], (0.5 - interval / 2) * 100, axis=1)
        upper = np.percentile(experimental_quantile_forecast[0], (0.5 + interval / 2) * 100, axis=1)

        response = {
            'status': 'success',
            'output': pred_y,
            'lower': lower[:prediction_length].tolist(),
            'upper': upper[:prediction_length].tolist(),
            'conf_interval': interval
        }

        return jsonify(response), 200

    except Exception as e:
        print(f"error:{e}")
        return jsonify({
            'error': f'Prediction failed: {str(e)}'
        }), 500

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
    global pretrained_model

    model_list = [
        'google/timesfm-2.0-500m-pytorch',  # 499M parameters
    ]

    num_of_arg = len(sys.argv)
    if num_of_arg == 1:
        pretrained_model = timesfm.TimesFm(
            hparams=timesfm.TimesFmHparams(
                backend="cpu",
                per_core_batch_size=32,
                horizon_len=128,
                num_layers=50,
                use_positional_embedding=False,
                context_len=2048,
            ),
            checkpoint=timesfm.TimesFmCheckpoint(
                huggingface_repo_id=model_list[0]),
        )
    elif num_of_arg == 2:
        model_index = int(sys.argv[1])

        if model_index < 0 or model_index >= len(model_list):
            print(f"invalid model index parameter, valid index:\n 0. {model_list[0]}")
            exit(-1)

        pretrained_model = timesfm.TimesFm(
            hparams=timesfm.TimesFmHparams(
                backend="cpu",
                per_core_batch_size=32,
                horizon_len=128,
                num_layers=50,
                use_positional_embedding=False,
                context_len=2048,
            ),
            checkpoint=timesfm.TimesFmCheckpoint(
                huggingface_repo_id=model_list[model_index]),
        )
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
        pretrained_model = timesfm.TimesFm(
            hparams=timesfm.TimesFmHparams(
                backend="cpu",
                per_core_batch_size=32,
                horizon_len=128,
                num_layers=50,
                use_positional_embedding=False,
                context_len=2048,
            ),
            checkpoint=timesfm.TimesFmCheckpoint(
                huggingface_repo_id=model_folder),
        )
    else:
        print("invalid parameters")
        print(usage())
        exit(-1)

    app.run(
        host='0.0.0.0',
        port=6061,
        threaded=True,
        debug=False
    )


if __name__ == "__main__":
    main()
