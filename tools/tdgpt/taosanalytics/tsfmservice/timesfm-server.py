import argparse
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "misc"))

import numpy as np
from flask import Flask, jsonify, request
from hf_download import snapshot_download_with_fallback
from timesfm import ForecastConfig, TimesFM_2p5_200M_torch
from tqdm import tqdm

app = Flask(__name__)
pretrained_model = None

CONTEXT_LEN = 2048
HORIZON_LEN = 128
PER_CORE_BATCH_SIZE = 32


def download_model(model_name, root_dir, enable_ep=False):
    # model_list = ['google/timesfm-2.5-200m-pytorch']
    model_list = [model_name]

    # root_dir = '/var/lib/taos/taosanode/model/timesfm/'
    if not os.path.exists(root_dir):
        os.mkdir(root_dir)

    dst_folder = root_dir + "/"
    if not os.path.exists(dst_folder):
        os.mkdir(dst_folder)

    for item in tqdm(model_list):
        snapshot_download_with_fallback(
            repo_id=item,
            local_dir=dst_folder,  # storage directory
            enable_ep=enable_ep,
            local_dir_use_symlinks=False,  # disable the link
            resume_download=True,
        )


def _prepare_forecast_input(input_data):
    input_array = np.asarray(input_data, dtype=np.float32)
    if input_array.ndim != 1 or input_array.size == 0:
        raise ValueError('"input" must be a non-empty one-dimensional array')
    if not np.all(np.isfinite(input_array)):
        raise ValueError('"input" must contain only finite numeric values')

    mean = float(np.mean(input_array))
    std = float(np.std(input_array))
    if not np.isfinite(std) or std == 0.0:
        std = 1.0

    normalized = (input_array - mean) / std
    if normalized.size > CONTEXT_LEN:
        normalized = normalized[-CONTEXT_LEN:]

    return normalized.astype(np.float32), mean, std


def _restore_scale(values, mean, std):
    return np.asarray(values, dtype=np.float32) * std + mean


def _validate_forecast_parameters(data):
    next_len_val = data.get("next_len", 10)
    if isinstance(next_len_val, bool) or not isinstance(next_len_val, (int, float)):
        raise ValueError('"next_len" must be a number')

    horizon_length = int(next_len_val)
    if horizon_length <= 0 or horizon_length > HORIZON_LEN:
        raise ValueError(f'"next_len" must be between 1 and {HORIZON_LEN}')

    conf_val = data.get("conf_interval", 0.95)
    if isinstance(conf_val, bool) or not isinstance(conf_val, (int, float)):
        raise ValueError('"conf_interval" must be a number')

    interval = float(conf_val)
    if interval <= 0 or interval >= 1:
        raise ValueError('"conf_interval" must be between 0 and 1')

    return horizon_length, interval


def _load_model(model_path_or_repo):
    model = TimesFM_2p5_200M_torch.from_pretrained(
        model_path_or_repo,
        torch_compile=False,
    )
    model.compile(
        ForecastConfig(
            max_context=CONTEXT_LEN,
            max_horizon=HORIZON_LEN,
            per_core_batch_size=PER_CORE_BATCH_SIZE,
            normalize_inputs=False,
            fix_quantile_crossing=True,
        )
    )
    return model


@app.route("/ds_predict", methods=["POST"])
def do_predict():
    try:
        data = request.get_json()
        if not data or "input" not in data:
            return (
                jsonify(
                    {
                        "status": "error",
                        "error": 'Invalid input, please provide "input" field in JSON',
                    }
                ),
                400,
            )

        input_data = data["input"]

        try:
            horizon_length, interval = _validate_forecast_parameters(data)
        except ValueError as e:
            return jsonify({"status": "error", "error": str(e)}), 400

        try:
            forecast_input, mean, std = _prepare_forecast_input(input_data)
        except ValueError as e:
            return jsonify({"status": "error", "error": str(e)}), 400

        point_forecast, experimental_quantile_forecast = pretrained_model.forecast(
            horizon=horizon_length,
            inputs=[forecast_input],
        )

        pred_y = _restore_scale(point_forecast[0][:horizon_length], mean, std)
        lower = np.percentile(
            experimental_quantile_forecast[0][:horizon_length],
            (0.5 - interval / 2) * 100,
            axis=1,
        )
        upper = np.percentile(
            experimental_quantile_forecast[0][:horizon_length],
            (0.5 + interval / 2) * 100,
            axis=1,
        )
        lower = _restore_scale(lower, mean, std)
        upper = _restore_scale(upper, mean, std)

        response = {
            "status": "success",
            "output": pred_y.tolist(),
            "lower": lower.tolist(),
            "upper": upper.tolist(),
            "conf_interval": interval,
        }

        return jsonify(response), 200

    except Exception as e:
        print(f"error:{e}")
        return jsonify({"error": f"Prediction failed: {e!s}"}), 500


def main():
    global pretrained_model

    model_list = [
        "google/timesfm-2.5-200m-pytorch",  # 200M parameters
    ]

    parser = argparse.ArgumentParser(
        description="TimesFM forecast model server",
        formatter_class=argparse.RawTextHelpFormatter,
    )

    source_group = parser.add_mutually_exclusive_group()
    source_group.add_argument(
        "-i",
        "--model-index",
        type=int,
        default=0,
        choices=range(len(model_list)),
        metavar=f"INDEX (0-{len(model_list) - 1})",
        help=(
            "Index of the pretrained model to load from HuggingFace Hub:\n"
            + "\n".join(f"  {i}: {m}" for i, m in enumerate(model_list))
        ),
    )
    source_group.add_argument(
        "-f",
        "--model-folder",
        type=str,
        metavar="FOLDER",
        help="Local directory that contains (or will store) the model files.",
    )

    parser.add_argument(
        "-n",
        "--model-name",
        type=str,
        choices=model_list,
        metavar="MODEL_NAME",
        help=(
            "HuggingFace model name used when downloading to --model-folder.\n"
            f"Valid values: {model_list}"
        ),
    )
    parser.add_argument(
        "--enable-ep",
        action="store_true",
        default=False,
        help="Use the HF mirror endpoint (https://hf-mirror.com) when downloading.",
    )
    parser.add_argument(
        "--host",
        type=str,
        default="0.0.0.0",
        help="Host address the server listens on (default: 0.0.0.0).",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=6065,
        help="Port the server listens on (default: 6065).",
    )

    args = parser.parse_args()

    if args.model_folder:
        if not args.model_name:
            parser.error("--model-name is required when --model-folder is specified.")

        model_folder = args.model_folder
        model_name = args.model_name

        if not os.path.exists(model_folder):
            print(
                f"the specified folder: {model_folder} not exists, start to create it"
            )

        model_file = os.path.join(model_folder, "model.safetensors")
        model_conf_file = os.path.join(model_folder, "config.json")

        if not os.path.exists(model_file) or not os.path.exists(model_conf_file):
            download_model(model_name, model_folder, enable_ep=args.enable_ep)
        else:
            print("model file exists, start directly")

        pretrained_model = _load_model(model_folder)
    else:
        pretrained_model = _load_model(model_list[args.model_index])

    app.run(host=args.host, port=args.port, threaded=True, debug=False)


if __name__ == "__main__":
    main()
