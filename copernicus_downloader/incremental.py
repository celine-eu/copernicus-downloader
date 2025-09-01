import os
import requests
import cdsapi
from datetime import date, timedelta, datetime
import calendar

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
    if isinstance(cfg_value, date):
        return cfg_value
    if isinstance(cfg_value, datetime):
        return cfg_value.date()
    if isinstance(cfg_value, str):
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


def safe_retrieve(
    client, dataset: str, request: dict, target: str, fail_on_error: bool = True
):
    """
    Wrap cdsapi.Client.retrieve with enhanced error handling.
    Logs CDS error details (message, reason, traceback) when available.
    If fail_on_error=False, logs and returns False instead of raising.
    """
    try:
        client.retrieve(dataset, request, target)
        return True
    except requests.HTTPError as e:
        details = {}
        try:
            if e.response is not None:
                details = e.response.json()
        except Exception:
            pass

        # If CDS provided structured error details
        if "error" in details:
            err = details["error"]
            message = err.get("message", "Unknown error")
            reason = err.get("reason", "")
            logger.error("CDS request failed [%s]: %s", dataset, message)
            if reason:
                logger.error("Reason: %s", reason)

            tb = (
                err.get("context", {}).get("traceback", "")
                if isinstance(err.get("context", {}), dict)
                else ""
            )
            for line in tb.split("\n"):
                if line.strip():
                    logger.debug("Trace: %s", line)

            if "not available yet" in message.lower():
                logger.warning(
                    "CDS says %s: data not yet available for %s", dataset, request
                )
                return False if not fail_on_error else (_ for _ in ()).throw(e)

            if fail_on_error:
                raise RuntimeError(f"{dataset} request failed: {message}. {reason}")
            else:
                logger.warning(
                    "Continuing despite error on %s (fail_on_error=False)", dataset
                )
                return False
        else:
            logger.error(
                "HTTPError from CDS [%s]: %s",
                dataset,
                getattr(e.response, "text", str(e)),
            )
            if fail_on_error:
                raise
            else:
                return False


def build_request(request_template: dict, d: date, use_range: bool) -> dict:
    """Build request dict for a daily request."""
    if use_range:
        day_str = d.isoformat()
        return {
            **request_template,
            "date": [f"{day_str}/{day_str}"],
        }
    else:
        return {
            **request_template,
            "year": [d.year],
            "month": [f"{d.month:02d}"],
            "day": [f"{d.day:02d}"],
        }


def build_monthly_request(
    request_template: dict, year: int, month: int, use_range: bool
) -> dict:
    """Build request dict for a monthly request."""
    if use_range:
        last_day = calendar.monthrange(year, month)[1]
        start_str = f"{year}-{month:02d}-01"
        end_str = f"{year}-{month:02d}-{last_day:02d}"
        return {
            **request_template,
            "date": [f"{start_str}/{end_str}"],
        }
    else:
        return {
            **request_template,
            "year": [year],
            "month": [f"{month:02d}"],
            "day": [f"{d:02d}" for d in range(1, 32)],
        }


def incremental_download(dataset_cfg, storage: Storage, years: list[int]):
    """
    Incrementally download dataset files.
    - Supports yearly, monthly, daily granularity.
    - Respects dataset lag_days property.
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

    # Apply lag_days if configured
    lag_days = int(dataset_cfg.get("lag_days", 0))
    today = date.today() - timedelta(days=lag_days) if lag_days > 0 else date.today()

    use_range = dataset_cfg.get("date_format") == "range"
    fail_on_error = bool(dataset_cfg.get("fail_on_error", True))

    logger.info(
        "Starting incremental download for %s [%s], from %s to %s (lag_days=%s)",
        dataset,
        granularity,
        start_date,
        today,
        lag_days,
    )

    # always ensure month/day present
    ensure_months(request_template)
    ensure_days(request_template)

    if granularity == "yearly":
        for year in range(start_year, today.year + 1):
            if year > today.year:
                continue
            key = f"{dataset}/{year}.grib"
            if storage.exists(key) or already_requested(storage, key):
                logger.debug("Skipping existing yearly file: %s", key)
                continue

            if use_range:
                request = {
                    **request_template,
                    "date": [f"{year}-01-01/{year}-12-31"],
                }
            else:
                request = {**request_template, "year": [year]}

            tmpfile = os.path.join(tmpdir, f"{year}.grib")
            try:
                logger.info("Requesting yearly data for %s: %s", dataset, year)
                ok = safe_retrieve(
                    client, dataset, request, tmpfile, fail_on_error=fail_on_error
                )
                if not ok:
                    # stop if fail_on_error=True, else continue loop gracefully
                    if fail_on_error:
                        break
                    else:
                        continue
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

                request = build_monthly_request(
                    request_template, year, m_int, use_range
                )
                tmpfile = os.path.join(tmpdir, f"{year}-{m}.grib")
                try:
                    logger.info(
                        "Requesting monthly data for %s: %s-%s", dataset, year, m
                    )
                    ok = safe_retrieve(
                        client, dataset, request, tmpfile, fail_on_error=fail_on_error
                    )
                    if not ok:
                        # stop if fail_on_error=True, else continue loop gracefully
                        if fail_on_error:
                            break
                        else:
                            continue
                    save_json(f"{tmpfile}.json", request)
                    storage.save(f"{tmpfile}.json", f"{key}.json")
                    storage.save(tmpfile, key)
                except requests.HTTPError:
                    logger.warning("Stopping monthly loop at %s-%s", year, m)
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

            request = build_request(request_template, d, use_range)
            tmpfile = os.path.join(tmpdir, f"{d.isoformat()}.grib")

            try:
                logger.info("Requesting daily data for %s: %s", dataset, d.isoformat())
                ok = safe_retrieve(
                    client, dataset, request, tmpfile, fail_on_error=fail_on_error
                )
                if not ok:
                    # stop if fail_on_error=True, else continue loop gracefully
                    if fail_on_error:
                        break
                    else:
                        continue
                save_json(f"{tmpfile}.json", request)
                storage.save(f"{tmpfile}.json", f"{key}.json")
                storage.save(tmpfile, key)
            except requests.HTTPError:
                logger.warning("Stopping daily loop at %s", d)
                break

    else:
        raise ValueError(f"Unsupported granularity {granularity}")
