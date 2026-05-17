import argparse
import os

import torch
from flask import Flask, request, jsonify
from huggingface_hub import snapshot_download
from tqdm import tqdm
from transformers import AutoModelForCausalLM

app = Flask(__name__)
device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
pretrained_model = None

def download_model(model_name, root_dir, enable_ep = False):
    # model_list = ['Maple728/TimeMoE-50M']
    ep = 'https://hf-mirror.com' if enable_ep else None
    model_list = [model_name]

    # root_dir = '/var/lib/taos/taosanode/model/timemoe'
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

        if len(set(input_data)) == 1:
            # for identical array list, std is 0, return directly
            pred_y = [input_data[0] for _ in range(prediction_length)]
        else:
            seqs = torch.tensor(input_data).unsqueeze(0).float().to(device)

            mean, std = seqs.mean(dim=-1, keepdim=True), seqs.std(dim=-1, keepdim=True)
            normed_seqs = (seqs - mean) / std
            seqs = normed_seqs

            pred_y = pretrained_model.generate(seqs, max_new_tokens=prediction_length)

            normed_predictions = pred_y[:, -prediction_length:]

            # inverse normalize
            predictions = normed_predictions * std + mean
            print(predictions)
            pred_y = predictions[0].numpy().tolist()

        response = {
            'status': 'success',
            'output': pred_y
        }

        return jsonify(response), 200

    except Exception as e:
        return jsonify({
            'error': f'Prediction failed: {str(e)}'
        }), 500


def main():
    global pretrained_model

    model_list = [
        'Maple728/TimeMoE-50M',  # time-moe model with 50M  parameters
        'Maple728/TimeMoE-200M',  # time-moe model with 200M parameters
    ]

    parser = argparse.ArgumentParser(
        description='TimeMoE forecast model server',
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
        default=6062,
        help='Port the server listens on (default: 6062).',
    )

    args = parser.parse_args()

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

        pretrained_model = AutoModelForCausalLM.from_pretrained(
            model_folder,
            device_map=device,
            trust_remote_code=True,
        )
    else:
        pretrained_model = AutoModelForCausalLM.from_pretrained(
            model_list[args.model_index],
            device_map=device,
            trust_remote_code=True,
        )

    app.run(
        host=args.host,
        port=args.port,
        threaded=True,
        debug=False
    )


if __name__ == "__main__":
    main()

