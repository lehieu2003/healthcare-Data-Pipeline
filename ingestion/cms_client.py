from __future__ import annotations

import time
from typing import Any

import requests

from ingestion.config import CMS_DATA_API_URL, CMS_STATS_API_URL


class CmsApiError(RuntimeError):
    """Raised when CMS Data API returns an invalid or failed response."""


class CmsDataApiClient:
    def __init__(self, timeout_seconds: int = 60, max_retries: int = 3, backoff_seconds: float = 1.0):
        self.timeout_seconds = timeout_seconds
        self.max_retries = max_retries
        self.backoff_seconds = backoff_seconds

    def fetch_page(self, dataset_id: str, size: int, offset: int) -> list[dict[str, Any]]:
        url = CMS_DATA_API_URL.format(dataset_id=dataset_id)
        payload = self._get_json(url, params={"size": size, "offset": offset})

        if not isinstance(payload, list):
            raise CmsApiError(f"Expected list response from CMS API, got {type(payload).__name__}")

        return payload

    def fetch_stats(self, dataset_id: str) -> dict[str, Any]:
        url = CMS_STATS_API_URL.format(dataset_id=dataset_id)
        payload = self._get_json(url, params=None)

        if not isinstance(payload, dict):
            raise CmsApiError(f"Expected object response from CMS stats API, got {type(payload).__name__}")

        return payload

    def _get_json(self, url: str, params: dict[str, Any] | None) -> Any:
        last_error: Exception | None = None

        for attempt in range(1, self.max_retries + 1):
            try:
                response = requests.get(url, params=params, timeout=self.timeout_seconds)
                response.raise_for_status()
                return response.json()
            except (requests.RequestException, ValueError) as exc:
                last_error = exc
                if attempt == self.max_retries:
                    break
                time.sleep(self.backoff_seconds * attempt)

        raise CmsApiError(f"CMS API request failed after {self.max_retries} attempts: {last_error}")

