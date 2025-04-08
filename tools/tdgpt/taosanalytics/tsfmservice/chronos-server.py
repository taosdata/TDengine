import torch
from flask import Flask, request, jsonify
from chronos import BaseChronosPipeline

app = Flask(__name__)
device = torch.device("cuda" if torch.cuda.is_available() else "cpu")

_model_list = [
    'amazon/chronos-bolt-tiny',  # 9M parameters, based on t5-efficient-tiny
    'amazon/chronos-bolt-mini',  # 21M parameters, based on	t5-efficient-mini
    'amazon/chronos-bolt-small',  # 48M parameters, based on t5-efficient-small
    'amazon/chronos-bolt-base',  # 205M parameters, based on t5-efficient-base
]

model = BaseChronosPipeline.from_pretrained(
    _model_list[0],
    device_map=device,
    torch_dtype=torch.bfloat16,
)

@app.route('/ds_predict', methods=['POST'])
def chronos():
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

        quantiles, mean = model.predict_quantiles(
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
    app.run(
            host='0.0.0.0',
            port=5002,
            threaded=True,
            debug=False
        )


if __name__ == "__main__":
    main()


