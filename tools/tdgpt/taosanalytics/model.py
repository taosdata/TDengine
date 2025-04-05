# encoding:utf-8
# pylint: disable=c0103

def get_avail_model():
    return [
        {
            "name": "ad_encoder_keras",
            "algo": "auto-encoder",
            "type": "anomaly-detection",
            "src-table": "*",
            "build-time": "2024-10-07 13:21:44"
        }
    ]


def train_model():
    pass


if __name__ == '__main__':
    a = get_avail_model()
    print(a)
