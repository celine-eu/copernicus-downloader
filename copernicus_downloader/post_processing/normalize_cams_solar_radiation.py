import os
import csv
from typing import List

from copernicus_downloader.storage import Storage


def main(
    tmpfile: str,
    destfile: str,
    storage: Storage,
    params: dict[str, any],
    dataset_cfg: dict[str, any],
):
    """
    Normalize a CAMS Solar Radiation Service CSV file.

    This function reads a CSV file produced by the CAMS Radiation Service,
    which contains a long metadata header (lines beginning with `#`) followed by
    a semicolon-delimited table. It performs the following steps:

    1. Extracts the column headers from the last commented line before the data.
    2. Parses the data section using `csv.DictReader`.
    3. Splits the "Observation period" column into two new fields:
       - ``start``: ISO 8601 UTC string marking the beginning of the interval
       - ``end``: ISO 8601 UTC string marking the end of the interval
    4. Save rows back as ``<original_name>_normalized.csv``,
       using the same delimiter (`;`) and including the new fields.

    Parameters
    ----------
    file_path : str
        Path to the original CAMS CSV file.
    params : dict[str, Any]
        Additional parameters for customization (currently unused, placeholder for config-driven runs).
    dataset_cfg : dict[str, Any]
        Dataset configuration dictionary (currently unused, placeholder for config-driven runs).

    Returns
    -------
    None
        Writes a normalized CSV file alongside the original.
    """
    header_lines: List[str] = []
    headers: List[str] = []
    rows = []

    with open(tmpfile, "r", encoding="utf-8") as f:
        for line in f:
            if line.startswith("#"):
                header_lines.append(line.rstrip("\n"))
            else:
                # Last comment line before data = real header
                header_raw = header_lines[-1]
                headers = header_raw.lstrip("# ").split(";")
                first_data_line = line
                break

        reader = csv.DictReader(
            [first_data_line] + list(f),
            fieldnames=headers,
            delimiter=";",
        )

        for row in reader:
            obs = row.pop("Observation period", None)
            if obs and "/" in obs:
                start_str, end_str = obs.split("/", 1)
                # Normalize timestamps as UTC strings
                row["start"] = start_str.replace(".0", "").replace("Z", "") + "Z"
                row["end"] = end_str.replace(".0", "").replace("Z", "") + "Z"
            rows.append(row)

    # Build output path without duplicating extension
    base_tmpfile, _ = os.path.splitext(tmpfile)
    out_tmppath = base_tmpfile + "_normalized.csv"

    base_destfile, _ = os.path.splitext(destfile)
    out_destfile = base_destfile + "_normalized.csv"

    # Write rows back to CSV
    if rows:
        fieldnames = list(rows[0].keys())
        with open(out_tmppath, "w", encoding="utf-8", newline="") as f_out:
            writer = csv.DictWriter(f_out, fieldnames=fieldnames, delimiter=";")
            writer.writeheader()
            writer.writerows(rows)

        storage.save(out_tmppath, out_destfile)


if __name__ == "__main__":
    main("cams-solar-radiation-timeseries/2025/01.csv")
