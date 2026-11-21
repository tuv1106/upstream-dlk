"""Client for the Upstream vehicle messages HTTP API."""

import httpx


class VehicleMessagesClient:
    """Fetches recently broadcast vehicle messages."""

    def __init__(self, base_url: str, timeout_seconds: float) -> None:
        """
        Args:
            base_url: Base URL of the API server.
            timeout_seconds: HTTP request timeout.
        """
        self.base_url = base_url.rstrip("/")
        self.timeout_seconds = timeout_seconds

    def fetch(self, amount: int) -> bytes:
        """Fetches the latest messages.

        Args:
            amount: Number of messages to request.

        Returns:
            The raw JSON response body, untouched.
        """
        response = httpx.get(
            f"{self.base_url}/upstream/vehicle_messages",
            params={"amount": amount},
            timeout=self.timeout_seconds,
        )
        response.raise_for_status()
        return response.content
