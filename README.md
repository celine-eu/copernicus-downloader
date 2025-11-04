# Copernicus Downloader

A flexible **incremental downloader** for [Copernicus CDS / ADS datasets](https://cds.climate.copernicus.eu/), designed for reproducible and automated pipelines.  
Supports **yearly, monthly, daily** granularities with incremental catch-up, local or S3 storage, and `.env` + YAML configuration.

Repository: [celine-eu/copernicus-downloader](https://github.com/celine-eu/copernicus-downloader)  
License: [Apache-2.0](./LICENSE)

---

## Features

- Incremental **yearly / monthly / daily** downloads
- Automatic skipping of already-requested files
- **min_date** clamp to avoid overlap between monthly backfill and daily updates
- Local FS or S3 (Minio) storage abstraction
- Configurable with `.env` + YAML
- Logging with `LOG_LEVEL`
- [uv](https://github.com/astral-sh/uv) for fast Python environment management

---

## Setup

### 1. Clone the repo

```bash
git clone https://github.com/celine-eu/copernicus-downloader.git
cd copernicus-downloader
```

### 2. Install dependencies (via uv)

```bash
uv sync
```

This will create and manage a virtual environment automatically.

## Environment variables

Create a .env file in the repo root:

```yaml
# Copernicus CDS API credentials (find them in your CDS profile)
CDS_API_KEY="abcdefg-your-key"
CDSAPI_URL="https://cds.climate.copernicus.eu/api"

# Optional: Atmosphere ADS API credentials
ADS_API_KEY="hijklmn-your-key"
ADSAPI_URL="https://ads.atmosphere.copernicus.eu/api"

# Optional: storage backend
AWS_ACCESS_KEY_ID="minio"
AWS_SECRET_ACCESS_KEY="minio123"
AWS_DEFAULT_REGION="us-east-1"
# AWS_ENDPOINT_URL=http://localhost:19000

# Logging
LOG_LEVEL=INFO

# Optional: temporary dir for downloads
CDS_TMPDIR=/var/tmp/cds
# Optional: data storage path
DATA_DIR=./data
```

## Configuration

By default, the downloader loads `cds_config.yaml` (or path from `CDS_CONFIG`).
Supports `${VAR}` placeholders, expanded from .env.

Example config

```bash
cat > cds_config.yaml <<'YAML'
years: [2020, 2021, 2022, 2023, 2024]

storage:
  type: fs
  base_dir: ./data

# Example for Minio
# storage:
#   type: s3
#   bucket: my-bucket
#   endpoint_url: http://localhost:9000

datasets:
  era5:
    name: reanalysis-era5-single-levels
    url: ${CDSAPI_URL}
    key: ${CDS_API_KEY}
    granularity: daily
    min_date: 2025-08-01   # daily starts after monthly backfill
    request:
      product_type: ["reanalysis"]
      variable: ["2m_temperature", "total_precipitation"]
      time: ["00:00","06:00","12:00","18:00"]
      data_format: grib
      download_format: unarchived
      area: [45.96, 11.11, 45.84, 11.36]
```


## Running
Download all datasets

`uv run python -m copernicus_downloader.main`

Download only one dataset (e.g., era5)

`uv run python -m copernicus_downloader.main era5`

## Contributing

Issues and PRs are welcome! Please open an issue or submit a pull request.

## License

This project is licensed under the [Apache-2.0 License](./LICENSE).