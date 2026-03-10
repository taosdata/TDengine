from typing import Optional, Tuple, List, Union
import torch
from torch import nn
import torch.nn.functional as F
from typing import Any, Dict, List, Optional, Union, Callable
from flask import Flask, request, jsonify
import sys
import argparse
from logging.handlers import RotatingFileHandler
import os
import torch
from transformers import AutoModelForCausalLM

# not used from 3.3.7.7
log_file = "log.time-moe_server"
import logging

# 创建 logger 对象
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)  # 设置全局日志级别

# 创建文件处理器，将日志写入文件
file_handler = logging.FileHandler(log_file, mode='a')
file_handler.setLevel(logging.DEBUG)  # 文件日志级别

# 创建控制台处理器，将日志输出到控制台
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)  # 控制台日志级别

# 定义日志格式
formatter =   logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)
console_handler.setFormatter(formatter)

# 将处理器添加到 logger
logger.addHandler(file_handler)
logger.addHandler(console_handler)

handler = RotatingFileHandler(log_file, maxBytes=1024*1024, backupCount=5)
handler.setLevel(logging.DEBUG)
handler.setFormatter(formatter)
logger.addHandler(handler)

app = Flask(__name__)

device = 'cpu'

# native model files in docker
model_path = ["models/models--Maple728--TimeMoE-200M/snapshots/794591bfeb1225fdf742cec0f4c71f20c3f3b87e/"]

model = AutoModelForCausalLM.from_pretrained(
    model_path[0],
    local_files_only=True,
    device_map=device,  # use "cpu" for CPU inference, and "cuda" for GPU inference.
    trust_remote_code=True,
)

# use it when the flash-attn is available
# model = AutoModelForCausalLM.from_pretrained('Maple728/TimeMoE-50M', device_map="auto", attn_implementation='flash_attention_2', trust_remote_code=True)

"""
# normalize seqs
mean, std = seqs.mean(dim=-1, keepdim=True), seqs.std(dim=-1, keepdim=True)
normed_seqs = (seqs - mean) / std

# forecast
output = model.generate(normed_seqs, max_new_tokens=prediction_length)  # shape is [batch_size, 12 + 6]
normed_predictions = output[:, -prediction_length:]  # shape is [batch_size, 6]

# inverse normalize
predictions = normed_predictions * std + mean
print(predictions)
pred_y = predictions[0]
"""

@app.route('/ds_predict', methods=['POST'])
def ds_predict():
    logger.info(f"predict")

    """处理POST请求并返回模型预测结果"""
    try:
        # 获取POST请求中的JSON数据
        data = request.get_json()
        if not data or 'input' not in data:
            return jsonify({
                'status':'error',
                'error': 'Invalid input, please provide "input" field in JSON'
            }), 400

        logger.info(f"data:{data}")
        input_data = data['input']
        prediction_length = data['next_len']
        
        if len(set(input_data)) == 1:   # return value for identical list
            pred_y = [input_data[0] for _ in range(prediction_length)]
        else:
            max_length = 2880
            if len(input_data) > max_length:
                input_data = input_data[:max_length]

            seqs = torch.tensor(input_data).unsqueeze(0).float().to(device)
            mean, std = seqs.mean(dim=-1, keepdim=True), seqs.std(dim=-1, keepdim=True)
            normed_seqs = (seqs - mean) / std
            seqs = normed_seqs

            #seqs = torch.tensor(input_data).unsqueeze(0).float().to(device)
            pred_y = model.generate(seqs, max_new_tokens=prediction_length)
            normed_predictions = pred_y[:, -prediction_length:]  # shape is [batch_size, 6]

            # inverse normalize
            predictions = normed_predictions * std + mean
            print(predictions)
            pred_y = predictions[0].numpy().tolist()

        logger.info(f"now pred:({pred_y})")
        response = {
            'status': 'success',
            'output': pred_y
        }

        return jsonify(response), 200
    except Exception as e:
        logger.info(f"error when predict:{e}")
        return jsonify({
            'error': f'Prediction failed: {str(e)}'
        }), 500



# 主函数
def main():
    app.run(
        host='0.0.0.0',
        port=6037,
        threaded=True,  # 支持多线程处理并发请求
        debug=False     # 生产环境建议设为False
    )


if __name__ == "__main__":
    main()

