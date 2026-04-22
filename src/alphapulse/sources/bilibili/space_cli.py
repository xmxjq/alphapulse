from __future__ import annotations

import asyncio
from typing import Any, Protocol


class SpaceDiscoveryClient(Protocol):
    def get_user_videos(self, *, uid: int, count: int) -> list[dict[str, Any]]: ...


class BilibiliCliSpaceDiscoveryClient:
    def get_user_videos(self, *, uid: int, count: int) -> list[dict[str, Any]]:
        try:
            from bili_cli import auth as bili_auth
            from bili_cli import client as bili_client
        except ImportError as exc:
            raise RuntimeError(
                "bilibili-cli is not installed. Run `uv sync` or rebuild the crawler image before using "
                "space_discovery_backend = \"cli\"."
            ) from exc

        credential = bili_auth.get_credential(mode="read")
        result = asyncio.run(
            bili_client.get_user_videos(
                uid=uid,
                count=count,
                credential=credential,
            )
        )
        if not isinstance(result, list):
            raise RuntimeError("bilibili-cli returned an unexpected user-videos payload")
        return [item for item in result if isinstance(item, dict)]
