"""Stream type classes for tap-segment."""
import datetime
import calendar
from urllib.parse import parse_qs, urlparse

import requests
from pathlib import Path
from typing import Any, Dict, Optional, Union, List, Iterable
from singer_sdk import typing as th  # JSON Schema typing helpers
from singer_sdk.helpers.jsonpath import extract_jsonpath

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


def add_months(sourcedate, months):
    month = sourcedate.month - 1 + months
    year = sourcedate.year + month // 12
    month = month % 12 + 1
    day = min(sourcedate.day, calendar.monthrange(year,month)[1])
    return datetime.date(year, month, day)


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

    def post_process(self, row: dict, context: Optional[dict] = None) -> Optional[dict]:
        row["anonymous"] = int(row["anonymous"])
        row["anonymousIdentified"] = int(row["anonymousIdentified"])
        row["identified"] = int(row["identified"])
        row["neverIdentified"] = int(row["neverIdentified"])
        return row

    def get_next_page_token(
        self, response: requests.Response, previous_token: Optional[Any]
    ) -> Optional[Any]:
        """Return a token for identifying next page or None if no more pages."""
        if self.next_page_token_jsonpath:
            all_matches = extract_jsonpath(
                self.next_page_token_jsonpath, response.json()
            )
            first_match = next(iter(all_matches), None)
            next_page_token = first_match
        else:
            next_page_token = response.headers.get("X-Next-Page", None)
        start_date = datetime.datetime.strptime(parse_qs(urlparse(response.request.url).query)['period'][0], '%Y-%m-%dT%H:%M:%SZ')
        this_month = datetime.datetime.today().replace(day=1)
        print(start_date)
        print(this_month)
        print(next_page_token)
        if not next_page_token and start_date.date() < this_month.date():
            next_page_token = add_months(start_date, 1)
        return next_page_token

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization."""
        if isinstance(next_page_token, datetime.datetime):
            start_date = next_page_token
        else:
            start_date = self.config.get('start_date')
        params: dict = {
            'pagination.count': 100,
            'period': start_date
        }
        if next_page_token and not isinstance(next_page_token, datetime.datetime):
            params["pagination.cursor"] = next_page_token
        return params


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

    def post_process(self, row: dict, context: Optional[dict] = None) -> Optional[dict]:
        row["anonymous"] = int(row["anonymous"])
        row["anonymousIdentified"] = int(row["anonymousIdentified"])
        row["identified"] = int(row["identified"])
        row["neverIdentified"] = int(row["neverIdentified"])
        return row

    def get_next_page_token(
        self, response: requests.Response, previous_token: Optional[Any]
    ) -> Optional[Any]:
        """Return a token for identifying next page or None if no more pages."""
        if self.next_page_token_jsonpath:
            all_matches = extract_jsonpath(
                self.next_page_token_jsonpath, response.json()
            )
            first_match = next(iter(all_matches), None)
            next_page_token = first_match
        else:
            next_page_token = response.headers.get("X-Next-Page", None)
        start_date = datetime.datetime.strptime(parse_qs(urlparse(response.request.url).query)['period'][0], '%Y-%m-%dT%H:%M:%SZ')
        this_month = datetime.datetime.today().replace(day=1)
        print(start_date)
        print(this_month)
        print(next_page_token)
        if not next_page_token and start_date.date() < this_month.date():
            next_page_token = add_months(start_date, 1)
        return next_page_token

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization."""
        if isinstance(next_page_token, datetime.datetime):
            start_date = next_page_token
        else:
            start_date = self.config.get('start_date')
        params: dict = {
            'pagination.count': 100,
            'period': start_date
        }
        if next_page_token and not isinstance(next_page_token, datetime.datetime):
            params["pagination.cursor"] = next_page_token
        return params
