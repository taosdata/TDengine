import torch
from flask import Flask, request, jsonify

from uni2ts.model.moirai_moe import MoiraiMoEForecast, MoiraiMoEModule
from einops import rearrange

import numpy as np

app = Flask(__name__)
device = torch.device("cuda" if torch.cuda.is_available() else "cpu")

_model_list = [
    'Salesforce/moirai-moe-1.0-R-small',  # small model with 117M parameters
    'Salesforce/moirai-moe-1.0-R-base',  # base model with 205M parameters
]

max_length = 2880

pretrained_model = MoiraiMoEModule.from_pretrained(
    _model_list[0]
).to(device)


@app.route('/ds_predict', methods=['POST'])
def uni2ts():
    try:
        data = request.get_json()
        if not data or 'input' not in data:
            return jsonify({
                'status': 'error',
                'error': 'Invalid input, please provide "input" field in JSON'
            }), 400

        input_data = data['input']
        prediction_length = data['next_len']
        input_data = input_data[:max_length]

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

        pred_y = np.round(np.median(forecast[0].cpu(), axis=0), decimals=4)
        pred_y = pred_y.tolist()

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
    app.run(
        host='0.0.0.0',
        port=5004,
        threaded=True,
        debug=False
    )


if __name__ == "__main__":
    main()
