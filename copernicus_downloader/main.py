import sys

from .config import load_config
from .storage import get_storage
from .incremental import incremental_download
from .logs import get_logger

from datetime import datetime
from typing import List

logger = get_logger(__name__)


def download_datasets(
    config_path: str = None, dataset_name: str = None, years: List[int] = None
):
    cfg = load_config(config_path)
    storage = get_storage(cfg)

    years = years if years is not None else cfg.get("years", [datetime.now().year])

    datasets = [dataset_name] if dataset_name else list(cfg["datasets"].keys())

    for ds in datasets:

        if ds not in cfg["datasets"]:
            logger.info(f"Dataset '{ds}' not found in config")
            continue

        logger.info(f"Checking dataset {ds} ...")
        incremental_download(cfg["datasets"][ds], storage, years)


def main():
    # CLI arg: dataset name, or empty = all datasets
    param = sys.argv[1] if len(sys.argv) >= 2 else None
    download_datasets(param)


if __name__ == "__main__":
    main()
