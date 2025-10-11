import os
import sys
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
        'amazon/chronos-bolt-tiny',  # 9M parameters, based on t5-efficient-tiny
        'amazon/chronos-bolt-mini',  # 21M parameters, based on	t5-efficient-mini
        'amazon/chronos-bolt-small',  # 48M parameters, based on t5-efficient-small
        'amazon/chronos-bolt-base',  # 205M parameters, based on t5-efficient-base
    ]

    num_of_arg = len(sys.argv)

    if num_of_arg == 1:
        pretrained_model = BaseChronosPipeline.from_pretrained(
            model_list[0],
            device_map=device,
            torch_dtype=torch.bfloat16,
        )
    elif num_of_arg == 2:
        index = int(sys.argv[1])
        if index < 0 or index >= len(model_list):
            print(f"invalid model index parameter, valid index:\n 0. {model_list[0]}\n 1. {model_list[1]}\n "
                  f"2. {model_list[2]}\n 3. {model_list[3]}")
            exit(-1)

        pretrained_model = BaseChronosPipeline.from_pretrained(
            model_list[index],
            device_map=device,
            torch_dtype=torch.bfloat16,
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
        pretrained_model = BaseChronosPipeline.from_pretrained(
            model_folder,
            device_map=device,
            torch_dtype=torch.bfloat16,
        )
    else:
        print("invalid parameters")
        print(usage())
        exit(-1)

    app.run(
            host='0.0.0.0',
            port=6038,
            threaded=True,
            debug=False
        )


if __name__ == "__main__":
    main()


