import os

import requests
import cdsapi
from datetime import date, timedelta, datetime

from .storage import Storage
from .util import save_json

from .logs import get_logger

logger = get_logger(__name__)


def get_tmpdir() -> str:
    tmpdir = os.getenv("CDS_TMPDIR", "/tmp")
    os.makedirs(tmpdir, exist_ok=True)
    return tmpdir


def daterange(start: date, end: date):
    for n in range((end - start).days + 1):
        yield start + timedelta(days=n)


def parse_min_date(cfg_value) -> date | None:
    if not cfg_value:
        return None
    if isinstance(cfg_value, date):  # already a date
        return cfg_value
    if isinstance(cfg_value, datetime):  # full datetime, take the date part
        return cfg_value.date()
    if isinstance(cfg_value, str):  # parse from YYYY-MM-DD
        return datetime.strptime(cfg_value, "%Y-%m-%d").date()
    raise TypeError(f"Unsupported type for min_date: {type(cfg_value)}")


def ensure_months(request: dict) -> None:
    if "month" not in request or not request["month"]:
        request["month"] = [f"{m:02d}" for m in range(1, 13)]


def ensure_days(request: dict) -> None:
    if "day" not in request or not request["day"]:
        request["day"] = [f"{d:02d}" for d in range(1, 32)]


def already_requested(storage: Storage, key: str) -> bool:
    return storage.exists(f"{key}.json")


def safe_retrieve(client, dataset: str, request: dict, target: str):
    """
    Wrap cdsapi.Client.retrieve with stop-on-unavailable logic.
    If CDS responds with 400 'not available yet', stop loop.
    """
    try:
        client.retrieve(dataset, request, target)
        return True
    except requests.HTTPError as e:
        msg = str(e)
        # Stop gracefully on "not available yet" messages
        if "not available yet" in msg.lower():
            logger.warning(
                "CDS says %s: data not yet available for %s", dataset, request
            )
            raise
        raise


def incremental_download(dataset_cfg, storage: Storage, years: list[int]):
    """
    Incrementally download dataset files.
    - Supports yearly, monthly, daily granularity.
    - Skips already downloaded/requested files.
    - Stops cleanly when CDS says data not yet available.
    """
    dataset = dataset_cfg["name"]
    granularity = dataset_cfg.get("granularity", "yearly")
    request_template = dict(dataset_cfg["request"])

    client = cdsapi.Client(
        url=dataset_cfg.get("url", ""),
        key=dataset_cfg.get("key", None),
    )

    tmpdir = get_tmpdir()
    start_year = min(years)
    start_date = date(start_year, 1, 1)

    min_date = parse_min_date(dataset_cfg.get("min_date"))
    if min_date and min_date > start_date:
        logger.debug("Applying min_date clamp: %s", min_date)
        start_date = min_date

    # always ensure month/day present
    ensure_months(request_template)
    ensure_days(request_template)

    today = date.today()
    logger.info(
        "Starting incremental download for %s [%s], from %s to %s",
        dataset,
        granularity,
        start_date,
        today,
    )

    if granularity == "yearly":
        for year in range(start_year, today.year + 1):
            if year > today.year:
                continue
            key = f"{dataset}/{year}.grib"
            if storage.exists(key) or already_requested(storage, key):
                logger.debug("Skipping existing yearly file: %s", key)
                continue
            request = {**request_template, "year": [year]}
            tmpfile = os.path.join(tmpdir, f"{year}.grib")
            try:
                logger.info("Requesting yearly data for %s: %s", dataset, year)
                safe_retrieve(client, dataset, request, tmpfile)
                save_json(f"{tmpfile}.json", request)
                storage.save(f"{tmpfile}.json", f"{key}.json")
                storage.save(tmpfile, key)
            except requests.HTTPError:
                logger.warning("Stopping yearly loop at %s", year)
                break

    elif granularity == "monthly":
        for year in range(start_year, today.year + 1):
            for m in request_template["month"]:
                m_int = int(m)
                if year > today.year or (year == today.year and m_int > today.month):
                    continue
                key = f"{dataset}/{year}/{m}.grib"
                if storage.exists(key) or already_requested(storage, key):
                    logger.debug("Skipping existing monthly file: %s", key)
                    continue
                request = {**request_template, "year": [year], "month": [m]}
                tmpfile = os.path.join(tmpdir, f"{year}-{m}.grib")
                try:
                    logger.info(
                        "Requesting monthly data for %s: %s-%s", dataset, year, m
                    )
                    safe_retrieve(client, dataset, request, tmpfile)
                    save_json(f"{tmpfile}.json", request)
                    storage.save(f"{tmpfile}.json", f"{key}.json")
                    storage.save(tmpfile, key)
                except requests.HTTPError:
                    logger.warning(
                        "Stopping monthly loop at %s-%s",
                        year,
                        m,
                    )
                    break

    elif granularity == "daily":
        for d in daterange(start_date, today):
            if f"{d.month:02d}" not in request_template["month"]:
                continue
            if f"{d.day:02d}" not in request_template["day"]:
                continue
            key = f"{dataset}/{d.year}/{d.month:02d}/{d.day:02d}.grib"
            if storage.exists(key) or already_requested(storage, key):
                logger.debug("Skipping existing daily file: %s", key)
                continue
            request = {
                **request_template,
                "year": [d.year],
                "month": [f"{d.month:02d}"],
                "day": [f"{d.day:02d}"],
            }
            tmpfile = os.path.join(tmpdir, f"{d.isoformat()}.grib")
            try:
                logger.info("Requesting daily data for %s: %s", dataset, d.isoformat())
                safe_retrieve(client, dataset, request, tmpfile)
                save_json(f"{tmpfile}.json", request)
                storage.save(f"{tmpfile}.json", f"{key}.json")
                storage.save(tmpfile, key)
            except requests.HTTPError:
                logger.warning("Stopping daily loop at %s", d)
                break

    else:
        raise ValueError(f"Unsupported granularity {granularity}")
