"""Impact Radius Actions through dlt's declarative REST API source.

Contract: the Impact v14 ``Actions`` list for one advertiser account and
program (Impact requires the campaign), filtered by action date over a rolling
recent window. Pages are numbered from one until ``@numpages``. Rows merge on
the action ``Id``: a rerun updates only the actions it returns. Older actions
that change upstream are not revisited, a gap of more than one window between
runs is not backfilled, and nothing is deleted. Only the fields below are
kept; others (customer contact details and network data) are dropped before
normalization. All values stay source text.
Local target only: Snowflake activation needs separate authority.
"""

from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from urllib.parse import quote

import requests
from dlt.common.schema.typing import TTableSchemaColumns
from dlt.sources.helpers.rest_client.auth import HttpBasicAuth
from dlt.sources.helpers.rest_client.paginators import PageNumberPaginator
from dlt.sources.rest_api import rest_api_source
from dlt.sources.rest_api.typing import RESTAPIConfig

from analytics_ingestion.kit.dagster import Metadata
from analytics_ingestion.kit.runtime import read_settings, target, text
from analytics_ingestion.kit.writes import open_writes

SOURCE = "impact_radius"
DATASET = "raw_impact_radius"
# Impact limits one Actions date filter to 30 days; a short window keeps the
# manual run small while merge-on-Id absorbs overlap between runs.
WINDOW = timedelta(days=3)
FIELDS: dict[str, str] = {
    "Id": "Impact action identity; merge key.",
    "CampaignId": "Impact program (campaign) identity.",
    "CampaignName": "Program name at extraction.",
    "AdId": "Ad identity credited with the action.",
    "MediaPartnerId": "Media partner identity credited with the action.",
    "MediaPartnerName": "Media partner name at extraction.",
    "ActionTrackerId": "Event type (action tracker) identity.",
    "ActionTrackerName": "Event type name at extraction.",
    "EventDate": "Source event time text with its UTC offset.",
    "LockingDate": "Time after which the action can no longer be modified.",
    "Currency": "Currency of the amount and payout fields.",
    "ClientCost": "Total cost to the advertiser, as source text.",
    "Amount": "Sale amount, as source text.",
    "Payout": "Partner payout, as source text.",
    "DeltaAmount": "Change to the sale amount after modification, as source text.",
    "DeltaPayout": "Change to the payout after modification, as source text.",
    "Oid": "Advertiser order identity sent with the conversion.",
    "State": "Action state, such as PENDING, APPROVED or REVERSED.",
    "PromoCode": "Promotion code sent with the conversion.",
    "ReferringDomain": "Referring domain recorded by Impact.",
    "ReferringType": "Referral type recorded by Impact.",
    "CustomerStatus": "New or returning customer, as sent by the advertiser.",
    "CustomerCountry": "Customer country recorded by Impact.",
    "CustomerRegion": "Customer region recorded by Impact.",
}
COLUMNS: TTableSchemaColumns = {
    name: {"data_type": "text", "nullable": name != "Id", "description": text_}
    for name, text_ in FIELDS.items()
}


def keep(row: dict[str, object]) -> dict[str, object]:
    return {name: row.get(name) for name in FIELDS}


def load_actions(
    env: Mapping[str, str],
    *,
    session: requests.Session | None = None,
    now: datetime | None = None,
) -> Metadata:
    """Manual run over ``[now - WINDOW, now]``; tests supply ``session``."""
    destination = target(env, SOURCE, local_only=True)
    settings = read_settings(env, SOURCE)
    sid = text(settings, SOURCE, "account_sid")
    token = text(settings, SOURCE, "auth_token")
    campaign = text(settings, SOURCE, "campaign_id")
    if not campaign.isdigit():
        raise ValueError(f"{SOURCE}.json campaign_id must be a numeric string")
    # Impact rejects fractional seconds.
    end = (now or datetime.now(UTC)).astimezone(UTC).replace(microsecond=0)
    start = (end - WINDOW).isoformat()
    params: dict[str, object] = {
        "CampaignId": campaign,
        "ActionDateStart": start,
        "ActionDateEnd": end.isoformat(),
        "PageSize": 20000,
    }
    config: RESTAPIConfig = {
        "client": {
            "base_url": f"https://api.impact.com/Advertisers/{quote(sid, safe='')}/",
            "auth": HttpBasicAuth(sid, token),
            "headers": {"Accept": "application/json", "IR-Version": "14"},
            "paginator": PageNumberPaginator(
                base_page=1, page_param="Page", total_path="['@numpages']"
            ),
        },
        "resources": [
            {
                "name": "actions",
                "primary_key": "Id",
                "write_disposition": "merge",
                "columns": COLUMNS,
                "processing_steps": [{"map": keep}],
                "endpoint": {
                    "path": "Actions",
                    "data_selector": "Actions",
                    "params": params,
                },
            }
        ],
    }
    if session is not None:
        # Live runs keep dlt's default session, which retries and times out.
        config["client"]["session"] = session
    pipeline = destination.pipeline("impact_radius_local", DATASET)
    writes = open_writes(pipeline)
    source = rest_api_source(config)
    source.resources["actions"].apply_hints(
        additional_table_hints={
            "description": "Impact actions from recent windows, merged on Id."
        }
    )
    info = writes.run(source)
    trace = pipeline.last_trace
    normalized = trace.last_normalize_info if trace else None
    counts = normalized.row_counts if normalized else {}
    return {
        "row_count": counts.get("actions", 0),
        "window_start": start,
        "window_end": end.isoformat(),
        "load_ids": list(info.loads_ids),
    }
