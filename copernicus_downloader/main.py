import sys
from typing import Dict, Any

from .config import load_config
from .storage import FSStorage, S3Storage
from .incremental import incremental_download
from .logs import get_logger

logger = get_logger(__name__)


def get_storage(cfg: Dict[str, Any]):
    """Initialize storage backend from config."""
    storage_cfg = cfg.get("storage", {"type": "fs"})
    stype = storage_cfg.get("type", "fs")

    if stype == "fs":
        base_dir = storage_cfg.get("base_dir", "./data")
        return FSStorage(base_dir=base_dir)

    elif stype == "s3":
        bucket = storage_cfg["bucket"]
        endpoint = storage_cfg.get("endpoint_url")
        return S3Storage(bucket=bucket, endpoint_url=endpoint)

    else:
        raise ValueError(f"Unknown storage type: {stype}")


def main():
    cfg = load_config()
    storage = get_storage(cfg)

    # CLI arg: dataset name, or empty = all datasets
    param = sys.argv[1] if len(sys.argv) >= 2 else ""
    years = cfg.get("years", [2024])

    datasets = [param] if param else list(cfg["datasets"].keys())

    for ds in datasets:
        if ds not in cfg["datasets"]:
            logger.info(f"Dataset '{ds}' not found in config")
            continue
        logger.info(f"Checking dataset {ds} ...")
        incremental_download(cfg["datasets"][ds], storage, years)


if __name__ == "__main__":
    main()
