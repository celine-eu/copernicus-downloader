import json
import os


def get_store_path(filename: str = "") -> str:
    """
    Get the store path from environment variable or default to '/store'.
    """

    path = os.getenv("STORE_PATH", "/store")
    if filename == "":
        return path

    return f"{path}/{filename}"


def save_json(filename: str, data: dict):
    """
    Save a dictionary to a JSON file.
    """
    with open(filename, "w") as f:
        json.dump(data, f, indent=4)
