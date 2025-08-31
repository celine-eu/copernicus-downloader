import os
import yaml
from typing import Dict, Any
from dotenv import load_dotenv


def load_config() -> Dict[str, Any]:
    """
    Load CDS configuration from YAML.
    Priority:
    1. CDS_CONFIG env variable (must point to an existing file).
    2. 'cds_config.yaml' in the current working directory.
    Supports ${VAR} placeholders via os.path.expandvars.
    Loads .env file if present.
    """
    # Load .env into os.environ
    load_dotenv()

    config_path = os.getenv("CDS_CONFIG")

    if config_path and os.path.exists(config_path):
        source = config_path
    else:
        local_path = os.path.join(os.getcwd(), "cds_config.yaml")
        if os.path.exists(local_path):
            source = local_path
        else:
            raise FileNotFoundError(
                "No CDS configuration found. "
                "Set CDS_CONFIG env variable or place cds_config.yaml in current directory."
            )

    with open(source, "r") as f:
        raw = f.read()

    # expand ${VAR} placeholders using environment variables
    expanded = os.path.expandvars(raw)

    res = yaml.safe_load(expanded)
    return res
