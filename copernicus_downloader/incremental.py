import os
import requests
import cdsapi
from datetime import date, timedelta, datetime
import calendar

import importlib
from types import ModuleType
from typing import Any

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
        start = (d - timedelta(days=1)).isoformat()
        end = d.isoformat()
        # Remove year/month/day if present in template
        base = {
            k: v
            for k, v in request_template.items()
            if k not in ("year", "month", "day")
        }
        return {
            **base,
            "date": [f"{start}/{end}"],
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
        base = {
            k: v
            for k, v in request_template.items()
            if k not in ("year", "month", "day")
        }
        return {
            **base,
            "date": [f"{start_str}/{end_str}"],
        }
    else:
        return {
            **request_template,
            "year": [year],
            "month": [f"{month:02d}"],
            "day": [f"{d:02d}" for d in range(1, 32)],
        }


def incremental_download(dataset_cfg, storage: Storage):
    """
    Incrementally download dataset files.
    Returns a dictionary summary with downloaded, skipped, failed.
    """
    dataset = dataset_cfg["name"]
    granularity = dataset_cfg.get("granularity", "yearly")
    request_template = dict(dataset_cfg["request"])

    client = cdsapi.Client(
        url=dataset_cfg.get("url", ""),
        key=dataset_cfg.get("key", None),
    )

    tmpdir = get_tmpdir()

    file_format = dataset_cfg.get("file_format", "grib")

    # ---- Date bounds ----
    min_date = parse_min_date(dataset_cfg.get("min_date"))
    max_date = parse_min_date(dataset_cfg.get("max_date"))
    lag_days = int(dataset_cfg.get("lag_days", 0))

    start_date = min_date or date.today().replace(month=1, day=1)
    end_date = date.today() - timedelta(days=lag_days) if lag_days > 0 else date.today()
    if max_date and max_date < end_date:
        end_date = max_date

    allowed_years = dataset_cfg.get("years")

    # ---- Flags ----
    use_range = dataset_cfg.get("date_format") == "range"
    fail_on_error = bool(dataset_cfg.get("fail_on_error", True))

    logger.info(
        "Starting incremental download for %s [%s], from %s to %s (lag_days=%s)",
        dataset,
        granularity,
        start_date,
        end_date,
        lag_days,
    )

    ensure_months(request_template)
    ensure_days(request_template)

    # ---- Summary tracking ----
    downloaded, skipped, failed = [], [], []

    # ---- Yearly loop ----
    if granularity == "yearly":
        years_iter = allowed_years or range(start_date.year, end_date.year + 1)
        for year in years_iter:
            if year < start_date.year or year > end_date.year:
                continue

            key = f"{dataset}/{year}.{file_format}"
            if storage.exists(key) or already_requested(storage, key):
                logger.debug("Skipping existing yearly file: %s", key)
                skipped.append(key)
                continue

            request = (
                {**request_template, "date": [f"{year}-01-01/{year}-12-31"]}
                if use_range
                else {**request_template, "year": [year]}
            )
            tmpfile = os.path.join(tmpdir, f"{year}.{file_format}")

            try:
                logger.info("Requesting yearly data for %s: %s", dataset, year)
                ok = safe_retrieve(client, dataset, request, tmpfile, fail_on_error)
                if not ok:
                    failed.append(key)
                    if fail_on_error:
                        break
                    else:
                        continue

                save_json(f"{tmpfile}.json", request)
                storage.save(f"{tmpfile}.json", f"{key}.json")
                storage.save(tmpfile, key)

                run_post_processing(dataset_cfg, tmpfile, key, storage)

                downloaded.append(key)
            except requests.HTTPError:
                failed.append(key)
                logger.warning("Stopping yearly loop at %s", year)
                break

    # ---- Monthly loop ----
    elif granularity == "monthly":
        years_iter = allowed_years or range(start_date.year, end_date.year + 1)
        for year in years_iter:
            if year < start_date.year or year > end_date.year:
                continue
            for m in request_template["month"]:
                m_int = int(m)
                if (year == start_date.year and m_int < start_date.month) or (
                    year == end_date.year and m_int > end_date.month
                ):
                    continue

                key = f"{dataset}/{year}/{m}.{file_format}"
                if storage.exists(key) or already_requested(storage, key):
                    logger.debug("Skipping existing monthly file: %s", key)
                    skipped.append(key)
                    continue

                request = build_monthly_request(
                    request_template, year, m_int, use_range
                )
                tmpfile = os.path.join(tmpdir, f"{year}-{m}.{file_format}")

                try:
                    logger.info(
                        "Requesting monthly data for %s: %s-%s", dataset, year, m
                    )
                    ok = safe_retrieve(client, dataset, request, tmpfile, fail_on_error)
                    if not ok:
                        failed.append(key)
                        if fail_on_error:
                            break
                        else:
                            continue

                    run_post_processing(dataset_cfg, tmpfile, key, storage)

                    save_json(f"{tmpfile}.json", request)
                    storage.save(f"{tmpfile}.json", f"{key}.json")
                    storage.save(tmpfile, key)

                    downloaded.append(key)
                except requests.HTTPError:
                    failed.append(key)
                    logger.warning("Stopping monthly loop at %s-%s", year, m)
                    break

    # ---- Daily loop ----
    elif granularity == "daily":
        for d in daterange(start_date, end_date):
            if allowed_years and d.year not in allowed_years:
                continue
            if f"{d.month:02d}" not in request_template["month"]:
                continue
            if f"{d.day:02d}" not in request_template["day"]:
                continue

            key = f"{dataset}/{d.year}/{d.month:02d}/{d.day:02d}.{file_format}"
            if storage.exists(key) or already_requested(storage, key):
                logger.debug("Skipping existing daily file: %s", key)
                skipped.append(key)
                continue

            request = build_request(request_template, d, use_range)
            tmpfile = os.path.join(tmpdir, f"{d.isoformat()}.{file_format}")

            try:
                logger.info("Requesting daily data for %s: %s", dataset, d.isoformat())
                ok = safe_retrieve(client, dataset, request, tmpfile, fail_on_error)
                if not ok:
                    failed.append(key)
                    if fail_on_error:
                        break
                    else:
                        continue

                run_post_processing(dataset_cfg, tmpfile, key, storage)

                save_json(f"{tmpfile}.json", request)
                storage.save(f"{tmpfile}.json", f"{key}.json")
                storage.save(tmpfile, key)
                downloaded.append(key)
            except requests.HTTPError:
                failed.append(key)
                logger.warning("Stopping daily loop at %s", d)
                break

    else:
        raise ValueError(f"Unsupported granularity {granularity}")

    # ---- Return summary ----
    summary = {
        "dataset": dataset,
        "granularity": granularity,
        "start_date": start_date,
        "end_date": end_date,
        "downloaded": downloaded,
        "skipped": skipped,
        "failed": failed,
    }
    logger.info(
        "Summary for %s: downloaded=%d, skipped=%d, failed=%d",
        dataset,
        len(downloaded),
        len(skipped),
        len(failed),
    )
    return summary


def run_post_processing(
    dataset_cfg: dict[str, Any] | None, tmpfile: str, destfile: str, storage: Storage
) -> Any:

    dataset_cfg = dataset_cfg or {}
    post_processing: dict[str, Any] | None = dataset_cfg.get("post_processing", None)
    fail_on_error: bool | None = dataset_cfg.get("fail_on_error", None)

    if not post_processing:
        return

    module_spec = post_processing.get("module")
    params = post_processing.get("params", {})

    if not module_spec or not params:
        return

    try:

        logger.info(f"Running post_processing {module_spec}")

        """
        Dynamically load a module and run a function with params.

        module_spec: "package.module:func" or just "package.module"
        params: dictionary of keyword arguments
        """
        parts = module_spec.split(":")
        module_name = parts[0]
        func_name = parts[1] if len(parts) > 1 else None

        # Import the module
        mod: ModuleType = importlib.import_module(module_name)

        # Resolve function to call
        if func_name:
            func = getattr(mod, func_name)
        else:
            # Try main or __main__
            func = getattr(mod, "main", None)
            if func is None:
                raise AttributeError(
                    f"No function provided and no main() in {module_name}"
                )

        func(tmpfile, destfile, storage, params, dataset_cfg)
        logger.info(f"Completed post_processing {module_spec}")
    except Exception as e:
        if fail_on_error:
            raise
        logger.exception(f"Failed to invoke postprocessing {module_spec}")
