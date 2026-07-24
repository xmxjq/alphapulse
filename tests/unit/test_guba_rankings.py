from __future__ import annotations

from pathlib import Path

from alphapulse.runtime.config import GubaSettings
from alphapulse.sources.guba.api import GubaHttpResult
from alphapulse.sources.guba.rankings import (
    SECTION_HOT_CONCEPT,
    SECTION_HOT_STOCK,
    SECTION_HOT_THEME,
    fetch_hot_rankings,
)


FIXTURES = Path(__file__).parent.parent / "fixtures" / "guba"


def _read(name: str) -> str:
    return (FIXTURES / name).read_text(encoding="utf-8")


class FakeRankingClient:
    """Serve ranking fixtures by matching a substring of the requested URL."""

    def __init__(self, settings: GubaSettings) -> None:
        self._by_marker = {
            "stockrank": _read("rank_hot_stock.json"),
            "clist": _read("rank_hot_concept.json"),
            "getBulletin": _read("rank_hot_theme.html"),
        }
        self.get_calls: list[str] = []
        self.post_calls: list[tuple[str, dict]] = []

    def _match(self, url: str) -> GubaHttpResult:
        for marker, text in self._by_marker.items():
            if marker in url:
                return GubaHttpResult(url=url, status_code=200, text=text)
        return GubaHttpResult(url=url, status_code=404, text="")

    def get(self, url: str, *, expect_marker: str | None = None) -> GubaHttpResult:
        self.get_calls.append(url)
        return self._match(url)

    def post_json(self, url: str, payload: dict) -> GubaHttpResult:
        self.post_calls.append((url, payload))
        return self._match(url)


def test_fetch_hot_rankings_parses_all_sections() -> None:
    settings = GubaSettings(enabled=True)
    client = FakeRankingClient(settings)

    rankings = fetch_hot_rankings(client, settings)  # type: ignore[arg-type]

    # 个股: security ids map to bare board codes, ranked by rk.
    assert [(b.rank, b.board_code) for b in rankings.hot_stock] == [
        (1, "001309"),
        (2, "600584"),
        (3, "430047"),
    ]
    assert all(b.section == SECTION_HOT_STOCK for b in rankings.hot_stock)
    assert rankings.hot_stock[0].url.endswith("/list,001309.html")

    # 概念: f12 board code, f14 name.
    assert [(b.board_code, b.name) for b in rankings.hot_concept] == [
        ("BK1036", "半导体"),
        ("BK0966", "光刻机"),
    ]
    assert all(b.section == SECTION_HOT_CONCEPT for b in rankings.hot_concept)

    # 主题: whole theme boards parsed from the bulletin fragment (each a /list board).
    assert [(b.rank, b.board_code, b.name) for b in rankings.hot_theme] == [
        (1, "gssz", "股市实战吧"),
        (2, "cjpl", "财经评论吧"),
        (3, "zssh000001", "上证指数吧"),
    ]
    assert all(b.section == SECTION_HOT_THEME for b in rankings.hot_theme)
    assert rankings.hot_theme[0].url.endswith("/list,gssz.html")


def test_board_codes_dedupes_across_sections() -> None:
    settings = GubaSettings(enabled=True)
    client = FakeRankingClient(settings)
    rankings = fetch_hot_rankings(client, settings)  # type: ignore[arg-type]

    codes = rankings.board_codes()
    # Ordered stock → concept → theme boards, de-duplicated.
    assert codes == [
        "001309", "600584", "430047", "BK1036", "BK0966", "gssz", "cjpl", "zssh000001",
    ]
    assert len(codes) == len(set(codes))


def test_per_section_limit_is_respected() -> None:
    settings = GubaSettings(enabled=True, hot_boards_per_section=1)
    client = FakeRankingClient(settings)
    rankings = fetch_hot_rankings(client, settings)  # type: ignore[arg-type]

    assert len(rankings.hot_stock) == 1
    assert len(rankings.hot_concept) == 1
    assert len(rankings.hot_theme) == 1
    assert rankings.hot_theme[0].board_code == "gssz"


def test_failed_section_yields_empty_list() -> None:
    settings = GubaSettings(enabled=True)

    class BrokenClient(FakeRankingClient):
        def post_json(self, url: str, payload: dict) -> GubaHttpResult:
            return GubaHttpResult(url=url, status_code=0, text="", error_message="boom")

    client = BrokenClient(settings)
    rankings = fetch_hot_rankings(client, settings)  # type: ignore[arg-type]
    # Stock (人气榜) and theme (bulletin) both POST, so both fail here...
    assert rankings.hot_stock == []
    assert rankings.hot_theme == []
    # ...but the GET-based concept section still parses.
    assert rankings.hot_concept


def test_fetch_hot_rankings_can_limit_sections() -> None:
    settings = GubaSettings(enabled=True)
    client = FakeRankingClient(settings)

    rankings = fetch_hot_rankings(
        client,
        settings,
        sections={SECTION_HOT_CONCEPT},
    )

    assert rankings.hot_stock == []
    assert rankings.hot_concept
    assert rankings.hot_theme == []
    assert len(client.get_calls) == 1
    assert client.post_calls == []
