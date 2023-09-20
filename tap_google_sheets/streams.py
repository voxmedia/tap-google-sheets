"""Stream type classes for tap-google-sheets."""

from __future__ import annotations

from typing import Iterable

from tap_google_sheets.client import GoogleSheetsBaseStream


class GoogleSheetsStream(GoogleSheetsBaseStream):
    """Define custom stream."""

    def parse_response(self, response: list[dict]) -> Iterable[dict]:
        """Parse the response and yield each record."""
        for stream_map in self.stream_maps:
            if stream_map.stream_alias == self.name:
                stream_map.transformed_schema = self.schema

        self._write_schema_message()

        yield from response
