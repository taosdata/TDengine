import torch
from flask import Flask, request, jsonify
import timesfm
import numpy as np

app = Flask(__name__)

_model_list = [
    'google/timesfm-2.0-500m-pytorch',  # 499M parameters
]

tfm = timesfm.TimesFm(
    hparams=timesfm.TimesFmHparams(
        backend="cpu",
        per_core_batch_size=32,
        horizon_len=128,
        num_layers=50,
        use_positional_embedding=False,
        context_len=2048,
    ),
    checkpoint=timesfm.TimesFmCheckpoint(
        huggingface_repo_id=_model_list[0]),
)


@app.route('/ds_predict', methods=['POST'])
def timesfm():
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

        point_forecast, experimental_quantile_forecast = tfm.forecast(
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
    app.run(
        host='0.0.0.0',
        port=6075,
        threaded=True,
        debug=False
    )


if __name__ == "__main__":
    main()
