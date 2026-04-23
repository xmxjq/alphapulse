from __future__ import annotations

import asyncio
from typing import Any, Protocol


class SpaceDiscoveryClient(Protocol):
    def get_user_videos(self, *, uid: int, count: int) -> list[dict[str, Any]]: ...

    def get_user_info(self, *, uid: int) -> dict[str, Any]: ...

    def search_videos(self, *, keyword: str, count: int) -> list[dict[str, Any]]: ...


class BilibiliCliSpaceDiscoveryClient:
    def get_user_videos(self, *, uid: int, count: int) -> list[dict[str, Any]]:
        bili_auth, bili_client = _import_bili_cli()
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

    def get_user_info(self, *, uid: int) -> dict[str, Any]:
        bili_auth, bili_client = _import_bili_cli()
        credential = bili_auth.get_credential(mode="read")
        result = asyncio.run(bili_client.get_user_info(uid=uid, credential=credential))
        if not isinstance(result, dict):
            raise RuntimeError("bilibili-cli returned an unexpected user-info payload")
        return result

    def search_videos(self, *, keyword: str, count: int) -> list[dict[str, Any]]:
        bili_auth, bili_client = _import_bili_cli()
        # Search is unauthenticated, but pass credential when available to avoid
        # rate limiting on heavy keyword queries.
        credential = bili_auth.get_credential(mode="read")
        results: list[dict[str, Any]] = []
        page = 1
        max_pages = 20
        while len(results) < count and page <= max_pages:
            page_results = asyncio.run(
                bili_client.search_video(keyword=keyword, page=page)
            )
            if not isinstance(page_results, list) or not page_results:
                break
            for item in page_results:
                if isinstance(item, dict):
                    results.append(item)
                if len(results) >= count:
                    break
            page += 1
        return results


def _import_bili_cli():
    try:
        from bili_cli import auth as bili_auth
        from bili_cli import client as bili_client
    except ImportError as exc:
        raise RuntimeError(
            "bilibili-cli is not installed. Run `uv sync` or rebuild the crawler image before using "
            "space_discovery_backend = \"cli\"."
        ) from exc
    return bili_auth, bili_client
