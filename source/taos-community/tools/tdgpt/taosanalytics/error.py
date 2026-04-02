# encoding:utf-8
# pylint: disable=c0103

def white_noise_error_msg():
    return "white noise data not processed"

def failed_load_model_except(model_name:str):
    raise ValueError(f"failed to load model: {model_name}")


