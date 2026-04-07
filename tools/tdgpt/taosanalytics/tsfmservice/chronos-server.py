import argparse
import os
import torch
from flask import Flask, request, jsonify
from chronos import BaseChronosPipeline
from huggingface_hub import snapshot_download
from tqdm import tqdm

app = Flask(__name__)
device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
pretrained_model = None


def download_model(model_name, root_dir, enable_ep = False):
    # model_list = ['Salesforce/moirai-1.0-R-small']
    ep = 'https://hf-mirror.com' if enable_ep else None
    model_list = [model_name]

    # root_dir = '/var/lib/taos/taosanode/model/chronos/'
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
                'status':'error',
                'error': 'Invalid input, please provide "input" field in JSON'
            }), 400

        input_data = data['input']
        prediction_length = data['next_len']

        seqs = torch.tensor(input_data).unsqueeze(0).float().to(device)

        quantiles, mean = pretrained_model.predict_quantiles(
            context=seqs, #torch.tensor(df["#Passengers"]),
            prediction_length=prediction_length,
            quantile_levels=[0.1, 0.5, 0.9],
        )

        #0 low, 1 median 2 high
        pred_y = quantiles[0, :, 1]
        pred_y = pred_y.cpu().numpy().tolist()
        print(f"pred_y:{pred_y}")

        response = {
            'status': 'success',
            'output': pred_y
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
        'amazon/chronos-bolt-tiny',   # 9M parameters, based on t5-efficient-tiny
        'amazon/chronos-bolt-mini',   # 21M parameters, based on t5-efficient-mini
        'amazon/chronos-bolt-small',  # 48M parameters, based on t5-efficient-small
        'amazon/chronos-bolt-base',   # 205M parameters, based on t5-efficient-base
    ]

    parser = argparse.ArgumentParser(
        description='Chronos forecast model server',
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
        default=6063
        help='Port the server listens on (default: 6038).',
    )

    args = parser.parse_args()

    if args.model_folder:
        # --- local folder mode ---
        if not args.model_name:
            parser.error('--model-name is required when --model-folder is specified.')

        model_folder = args.model_folder
        model_name   = args.model_name

        if not os.path.exists(model_folder):
            print(f"the specified folder: {model_folder} not exists, start to create it")

        model_file      = os.path.join(model_folder, 'model.safetensors')
        model_conf_file = os.path.join(model_folder, 'config.json')

        if not os.path.exists(model_file) or not os.path.exists(model_conf_file):
            download_model(model_name, model_folder, enable_ep=args.enable_ep)
        else:
            print("model file exists, start directly")

        pretrained_model = BaseChronosPipeline.from_pretrained(
            model_folder,
            device_map=device,
            torch_dtype=torch.bfloat16,
        )
    else:
        # --- HuggingFace Hub mode (by index) ---
        pretrained_model = BaseChronosPipeline.from_pretrained(
            model_list[args.model_index],
            device_map=device,
            torch_dtype=torch.bfloat16,
        )

    app.run(
        host=args.host,
        port=args.port,
        threaded=True,
        debug=False,
    )


if __name__ == "__main__":
    main()


