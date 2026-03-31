import argparse
import os

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

def main():
    global pretrained_model

    model_list = [
        'google/timesfm-2.0-500m-pytorch',  # 499M parameters
    ]

    parser = argparse.ArgumentParser(
        description='TimesFM forecast model server',
        formatter_class=argparse.RawTextHelpFormatter,
    )

    source_group = parser.add_mutually_exclusive_group()
    source_group.add_argument(
        '-i', '--model-index',
        type=int,
        default=0,
        choices=range(len(model_list)),
        metavar=f'INDEX (0-{len(model_list) - 1})',
        help=(
            'Index of the pretrained model to load from HuggingFace Hub:\n'
            + '\n'.join(f'  {i}: {m}' for i, m in enumerate(model_list))
        ),
    )
    source_group.add_argument(
        '-f', '--model-folder',
        type=str,
        metavar='FOLDER',
        help='Local directory that contains (or will store) the model files.',
    )

    parser.add_argument(
        '-n', '--model-name',
        type=str,
        choices=model_list,
        metavar='MODEL_NAME',
        help=(
            'HuggingFace model name used when downloading to --model-folder.\n'
            f'Valid values: {model_list}'
        ),
    )
    parser.add_argument(
        '--enable-ep',
        action='store_true',
        default=False,
        help='Use the HF mirror endpoint (https://hf-mirror.com) when downloading.',
    )
    parser.add_argument(
        '--host',
        type=str,
        default='0.0.0.0',
        help='Host address the server listens on (default: 0.0.0.0).',
    )
    parser.add_argument(
        '--port',
        type=int,
        default=6061,
        help='Port the server listens on (default: 6061).',
    )

    args = parser.parse_args()

    def _build_model_from_checkpoint(checkpoint):
        return timesfm.TimesFm(
            hparams=timesfm.TimesFmHparams(
                backend="cpu",
                per_core_batch_size=32,
                horizon_len=128,
                num_layers=50,
                use_positional_embedding=False,
                context_len=2048,
            ),
            checkpoint=checkpoint,
        )

    if args.model_folder:
        if not args.model_name:
            parser.error('--model-name is required when --model-folder is specified.')

        model_folder = args.model_folder
        model_name = args.model_name

        if not os.path.exists(model_folder):
            print(f"the specified folder: {model_folder} not exists, start to create it")

        model_file = os.path.join(model_folder, 'model.safetensors')
        model_conf_file = os.path.join(model_folder, 'config.json')

        if not os.path.exists(model_file) or not os.path.exists(model_conf_file):
            download_model(model_name, model_folder, enable_ep=args.enable_ep)
        else:
            print("model file exists, start directly")

        checkpoint_path = os.path.join(model_folder, 'torch_model.ckpt')
        pretrained_model = _build_model_from_checkpoint(
            timesfm.TimesFmCheckpoint(path=checkpoint_path)
        )
    else:
        pretrained_model = _build_model_from_checkpoint(
            timesfm.TimesFmCheckpoint(
                huggingface_repo_id=model_list[args.model_index]
            )
        )

    app.run(
        host=args.host,
        port=args.port,
        threaded=True,
        debug=False
    )


if __name__ == "__main__":
    main()
