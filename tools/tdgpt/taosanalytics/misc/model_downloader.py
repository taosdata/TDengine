import os
import sys
from tqdm import tqdm

sys.path.append(os.path.dirname(os.path.abspath(__file__)) + "/../../")

try:
    from taosanalytics.misc.hf_download import parse_bool, snapshot_download_with_fallback
except ImportError:
    from hf_download import parse_bool, snapshot_download_with_fallback


def download_model(model_name, model_dir, enable_ep = False):
    # model_list = ['Salesforce/moirai-1.0-R-small']
    model_list = [model_name]

    if not os.path.exists(model_dir):
        print(f"create model directory: {model_dir}")
        os.mkdir(model_dir)

    for item in tqdm(model_list):
        snapshot_download_with_fallback(
            repo_id=item,
            local_dir=model_dir,  # storage directory
            enable_ep=enable_ep,
            local_dir_use_symlinks=False,   # disable the link
            resume_download=True,
        )


def do_download_tsfm(path: str, model_name: str, enable_ep:bool=False):
    download_model(model_name, path, enable_ep)

if __name__ == '__main__':
    """
    Usage:
    python3.10 model_downloader.py '/var/lib/taos/taosanode/model/moirai' 'Salesforce/moirai-moe-1.0-R-small' True
    """
    if len(sys.argv) < 4:
        print("invalid parameters, e.g.,:\n python model_downloader.py model_directory model_name ep_enable")
        sys.exit(1)

    path = sys.argv[1].strip('\'"')
    model_name = sys.argv[2].strip('\'"')
    ep_raw = sys.argv[3]
    try:
        ep_enable = parse_bool(ep_raw)
    except ValueError:
        print(f"invalid ep_enable parameter: {ep_raw}")
        sys.exit(1)

    do_download_tsfm(path, model_name, ep_enable)