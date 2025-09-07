import os
import azure.functions as func
import json
import logging
import datetime
import hashlib
from azure.data.tables import TableServiceClient, UpdateMode
from azure.core.exceptions import ResourceNotFoundError, ResourceExistsError, ResourceModifiedError
from azure.core import MatchConditions

COSMOS_CONN_STR = os.getenv("COSMOS_CONN_STR")
COSMOS_TABLE_COUNTER = os.getenv("COSMOS_TABLE_COUNTER")
COSMOS_TABLE_SEEN = os.getenv("COSMOS_TABLE_SEEN")

_svc = TableServiceClient.from_connection_string(COSMOS_CONN_STR)
# Ensure tables exist (no-op if they already do)
try:
    _svc.create_table_if_not_exists(COSMOS_TABLE_COUNTER)
    _svc.create_table_if_not_exists(COSMOS_TABLE_SEEN)
except Exception:
    # benign in most cases; continue
    pass

counter_table = _svc.get_table_client(COSMOS_TABLE_COUNTER)
seen_table = _svc.get_table_client(COSMOS_TABLE_SEEN)

def read_count() -> int:
    try:
        e = counter_table.get_entity(partition_key="counter", row_key="global")
        return int(e.get("count", 0))
    except ResourceNotFoundError:
        return 0

def create_counter_if_missing() -> None:
    try:
        counter_table.create_entity({
            "PartitionKey": "counter",
            "RowKey": "global",
            "count": 1  # set to 0 if you prefer
        })
    except ResourceExistsError:
        pass

def _get_etag(e) -> str | None:
    if hasattr(e, "metadata") and isinstance(e.metadata, dict) and "etag" in e.metadata:
        return e.metadata["etag"]
    return e.get("odata.etag") or e.get("etag")

def increment_count_with_retry(max_retries: int = 3) -> int:
    for _ in range(max_retries):
        try:
            e = counter_table.get_entity("counter", "global")
            current = int(e.get("count", 0))
            etag = _get_etag(e)
            e["count"] = current + 1
            counter_table.update_entity(
                entity=e,
                mode=UpdateMode.REPLACE,
                etag=etag,
                match_condition=MatchConditions.IfNotModified
            )
            return current + 1
        except ResourceNotFoundError:
            create_counter_if_missing()
            return read_count()
        except ResourceModifiedError:
            # race; retry
            continue
    return read_count()

def mark_seen(window_id: str, dedupe_key: str) -> bool:
    try:
        seen_table.create_entity({
            "PartitionKey": window_id,
            "RowKey": dedupe_key,
            "ts": datetime.datetime.utcnow().isoformat() + "Z"
        })
        return True
    except ResourceExistsError:
        return False

def current_window(window_hours: int) -> str:
    now = datetime.datetime.utcnow().replace(minute=0, second=0, microsecond=0)
    hours = (now.hour // window_hours) * window_hours
    slot = now.replace(hour=hours)
    return slot.strftime("%Y%m%d%H") + f"-{window_hours}h"

def dedupe_key_from(req: func.HttpRequest) -> str:
    ip = req.headers.get("x-forwarded-for", req.headers.get("x-client-ip", "")) or req.params.get("ip", "")
    ua = req.headers.get("user-agent", "")
    raw = f"{ip}|{ua}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()

app = func.FunctionApp()

@app.route(route="visitors", auth_level=func.AuthLevel.ANONYMOUS, methods=["GET", "POST"])
def visitors(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("visitors called: %s", req.method)

    headers = {
        "Content-Type": "application/json",
        # Helps avoid stale numbers at the edge
        "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0"
    }

    if req.method == "GET":
        body = {"count": read_count()}
        return func.HttpResponse(json.dumps(body), status_code=200, headers=headers)

    # POST
    window_hours = int(os.environ.get("DEDUPE_WINDOW_HOURS", "24"))
    win = current_window(window_hours)
    key = dedupe_key_from(req)

    if mark_seen(win, key):
        new_count = increment_count_with_retry()
        body = {"count": new_count, "deduped": False}
    else:
        body = {"count": read_count(), "deduped": True}

    return func.HttpResponse(json.dumps(body), status_code=200, headers=headers)
