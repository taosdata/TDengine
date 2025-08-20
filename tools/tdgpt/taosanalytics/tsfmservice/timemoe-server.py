import torch
from flask import Flask, request, jsonify
from transformers import AutoModelForCausalLM

app = Flask(__name__)
device = torch.device("cuda" if torch.cuda.is_available() else "cpu")

_model_list = [
    'Maple728/TimeMoE-50M',  # time-moe model with 50M  parameters
    'Maple728/TimeMoE-200M',  # time-moe model with 200M parameters
]

model = AutoModelForCausalLM.from_pretrained(
    _model_list[0],
    device_map=device,
    trust_remote_code=True,
)

@app.route('/ds_predict', methods=['POST'])
def time_moe():
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

            pred_y = model.generate(seqs, max_new_tokens=prediction_length)

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
    app.run(
            host='0.0.0.0',
            port=6072,
            threaded=True,  
            debug=False     
        )


if __name__ == "__main__":
    main()

