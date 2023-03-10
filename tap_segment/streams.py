"""Stream type classes for tap-segment."""
import datetime

from pathlib import Path
from typing import Any, Dict, Optional, Union, List, Iterable

from singer_sdk import typing as th  # JSON Schema typing helpers

from tap_segment.client import SegmentStream


class SourceAPICallsDailyStream(SegmentStream):
    name = "source_api_calls_daily"
    path = "/usage/api-calls/sources/daily"
    primary_keys = ["sourceId", "timestamp"]
    replication_key = "timestamp"
    records_jsonpath = "$.dailyPerSourceAPICallsUsage[*]"
    schema = th.PropertiesList(
        th.Property("sourceId", th.StringType),
        th.Property("timestamp", th.DateTimeType),
        th.Property("apiCalls", th.IntegerType),
    ).to_dict()


class WorkspaceAPICallsDailyStream(SegmentStream):
    name = "workspace_api_calls_daily"
    path = "/usage/api-calls/daily"
    primary_keys = ["timestamp"]
    replication_key = "timestamp"
    records_jsonpath = "$.dailyWorkspaceAPICallsUsage[*]"
    schema = th.PropertiesList(
        th.Property("timestamp", th.DateTimeType),
        th.Property("apiCalls", th.IntegerType),
    ).to_dict()


class EventsVolumeDailyStream(SegmentStream):
    name = "events_volume_daily"
    path = "/events/volume"
    primary_keys = ["time"]
    replication_key = "time"
    records_jsonpath = "$.data.result[0].series[*]"
    schema = th.PropertiesList(
        th.Property("time", th.DateTimeType),
        th.Property("count", th.IntegerType),
    ).to_dict()

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization."""
        today = datetime.datetime.today()
        params: dict = {
            'pagination.count': 100,
            'startTime': max(datetime.datetime.strptime(self.config.get('start_date'), "%Y-%m-%dT%H:%M:%SZ"), today.replace(year=today.year - 2)).strftime("%Y-%m-%d"),
            'endTime': (today - datetime.timedelta(days=1)).strftime("%Y-%m-%d"),
            'granularity': 'DAY'
        }
        if next_page_token:
            params["pagination.cursor"] = next_page_token
        return params


class SourceMTUUUsageDailyStream(SegmentStream):
    name = "source_mtu_usage_daily"
    path = "/usage/mtu/sources/daily"
    primary_keys = ["timestamp"]
    replication_key = "timestamp"
    records_jsonpath = "$.data.dailyPerSourceMTUUsage[*]"
    schema = th.PropertiesList(
        th.Property("sourceId", th.StringType),
        th.Property("periodStart", th.NumberType),
        th.Property("periodEnd", th.NumberType),
        th.Property("anonymous", th.IntegerType),
        th.Property("anonymousIdentified", th.IntegerType),
        th.Property("identified", th.IntegerType),
        th.Property("neverIdentified", th.IntegerType),
        th.Property("timestamp", th.DateTimeType),
    ).to_dict()


class WorkspaceMTUUUsageDailyStream(SegmentStream):
    name = "workspace_mtu_usage_daily"
    path = "/usage/mtu/daily"
    primary_keys = ["timestamp"]
    replication_key = "timestamp"
    records_jsonpath = "$.data.dailyWorkspaceMTUUsage[*]"
    schema = th.PropertiesList(
        th.Property("periodStart", th.NumberType),
        th.Property("periodEnd", th.NumberType),
        th.Property("anonymous", th.IntegerType),
        th.Property("anonymousIdentified", th.IntegerType),
        th.Property("identified", th.IntegerType),
        th.Property("neverIdentified", th.IntegerType),
        th.Property("timestamp", th.DateTimeType),
    ).to_dict()
