"""Segment tap class."""

import datetime
from typing import List

from singer_sdk import Tap, Stream
from singer_sdk import typing as th  # JSON schema typing helpers

from tap_segment.streams import (
    SourceAPICallsDailyStream,
    WorkspaceAPICallsDailyStream,
    EventsVolumeDailyStream,
    SourceMTUUUsageDailyStream,
    WorkspaceMTUUUsageDailyStream,
)

STREAM_TYPES = [
    SourceAPICallsDailyStream,
    WorkspaceAPICallsDailyStream,
    EventsVolumeDailyStream,
    SourceMTUUUsageDailyStream,
    WorkspaceMTUUUsageDailyStream
]


class TapSegment(Tap):
    """Segment tap class."""
    name = "tap-segment"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "api_token",
            th.StringType,
            required=True,
            secret=True,
            description="The token to authenticate against the API service"
        ),
        th.Property(
            "start_date",
            th.DateTimeType,
            description="The earliest record date to sync",
            default=datetime.datetime(2023, 1, 1)
        ),
    ).to_dict()

    def discover_streams(self) -> List[Stream]:
        """Return a list of discovered streams."""
        return [stream_class(tap=self) for stream_class in STREAM_TYPES]


if __name__ == "__main__":
    TapSegment.cli()
