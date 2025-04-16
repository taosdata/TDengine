import torch
from flask import Flask, request, jsonify
from gluonts.dataset.pandas import PandasDataset

from uni2ts.model.moirai_moe import MoiraiMoEForecast, MoiraiMoEModule
from einops import rearrange

import numpy as np
import pandas as pd

app = Flask(__name__)
device = torch.device("cuda" if torch.cuda.is_available() else "cpu")

_model_list = [
    'Salesforce/moirai-moe-1.0-R-small',  # small model with 117M parameters
    'Salesforce/moirai-moe-1.0-R-base',  # base model with 205M parameters
]

# maximum allowed input data for forecasting
_max_input_length = 2880

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
        input_data = input_data[:_max_input_length]

        prediction_length = data.get('next_len', 10)
        interval = data.get('conf_interval', 0.95)   # confidence interval

        past_dynamic_real = data.get('past_dynamic_real', [])

        if len(past_dynamic_real) == 0:  # uni-variate forecasting processing
            resp = handle_univariate_forecast(input_data, prediction_length, interval)
        else: # co-variate forecasting processing
            resp = handle_covariate_forecast(input_data, prediction_length, interval, past_dynamic_real)
            
        return jsonify(resp), 200

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


def handle_univariate_forecast(input_data, prediction_length, interval):
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
        'interval':interval
    }

def handle_covariate_forecast(input_data, prediction_length, interval, past_dynamic_real):
    """co-variate forecasting processing"""
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
        # feat_dynamic_real=dynamic_cols,  # not support yet
        target="target",  # target column name
        # time_col="timestamp"  # not set yet
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
        'interval': interval
    }
    
    
if __name__ == "__main__":
    main()
