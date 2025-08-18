import sys
from taosanalytics.util import download_model


def do_download_tsfm(path: str, model_name: str, enable_ep:bool=False):
    download_model(model_name, path, enable_ep)

if __name__ == '__main__':
    """
    Usage:
    python3.10 model_downloader.py '/var/lib/taos/taosanode/model/moirai' 'Salesforce/moirai-moe-1.0-R-small' True
    """
    if len(sys.argv) < 4:
        print("invalid parameters, e.g.,:\n python model_downloader.py model_directory model_name ep_enable")
        exit(-1)

    path = sys.argv[1].strip('\'"')
    model_name = sys.argv[2].strip('\'"')
    ep_enable = bool(sys.argv[3])

    do_download_tsfm(path, model_name, ep_enable)