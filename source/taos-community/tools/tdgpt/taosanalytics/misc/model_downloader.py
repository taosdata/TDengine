import os
import sys
from huggingface_hub import snapshot_download
from tqdm import tqdm


def download_model(model_name, model_dir, enable_ep = False):
    # model_list = ['Salesforce/moirai-1.0-R-small']
    ep = 'https://hf-mirror.com' if enable_ep else None
    model_list = [model_name]

    if not os.path.exists(model_dir):
        print(f"create model directory: {model_dir}")
        os.mkdir(model_dir)

    if ep:
        print(f"set the download ep:{ep}")

    for item in tqdm(model_list):
        snapshot_download(
            repo_id=item,
            local_dir=model_dir,  # storage directory
            local_dir_use_symlinks=False,   # disable the link
            resume_download=True,
            endpoint=ep
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
        exit(-1)

    path = sys.argv[1].strip('\'"')
    model_name = sys.argv[2].strip('\'"')
    ep_enable = bool(sys.argv[3])

    do_download_tsfm(path, model_name, ep_enable)