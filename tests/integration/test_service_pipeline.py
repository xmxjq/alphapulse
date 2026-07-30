import threading
from pathlib import Path

from alphapulse.pipeline.contracts import CrawlTask, FetchOutcome, SeedDefinition
from alphapulse.runtime.config import Settings, load_settings
from alphapulse.runtime.service import AlphaPulseService
from alphapulse.sources.bilibili.api import BilibiliApiResult
from alphapulse.sources.fetching import FetchResult, KuaidailiProxyProvider


class FakeStore:
    def __init__(self) -> None:
        self.posts = []
        self.comments = []
        self.authors = []
        self.errors = []

    def init_db(self) -> None:
        return None

    def healthcheck(self) -> bool:
        return True

    def upsert_posts(self, posts):
        self.posts.extend(posts)

    def upsert_comments(self, comments):
        self.comments.extend(comments)

    def upsert_authors(self, authors):
        self.authors.extend(authors)

    def insert_crawl_error(
        self,
        *,
        source: str,
        url: str,
        error_message: str,
        status_code: int | None = None,
        task_kind: str | None = None,
        error_kind: str | None = None,
    ) -> None:
        self.errors.append((source, url, error_message, status_code, task_kind, error_kind))

    def insert_crawl_run(self, *, run_id: str, started_at, finished_at, stats, status: str) -> None:
        return None


class StaticSeedDiscovery:
    def ensure_compiled_seed_sets(self, seed_set_name=None):
        del seed_set_name
        return [SeedDefinition(name="parallel-test")]


class BarrierAdapter:
    def __init__(self, source_name: str, barrier: threading.Barrier) -> None:
        self.source_name = source_name
        self.barrier = barrier

    def discover(self, seed):
        return [
            CrawlTask(
                source=self.source_name,
                kind="fetch_post",
                url=f"https://example.com/{self.source_name}",
                seed_name=seed.name,
            )
        ]

    def fetch_item(self, task):
        del task
        self.barrier.wait(timeout=2)
        return FetchOutcome(status_code=200)

    def refresh_comments(self, item_ref):
        del item_ref
        return []

    def comment_task_for_post(self, post, seed_name):
        raise AssertionError("not used")


def test_service_processes_source_queues_in_parallel(tmp_path: Path) -> None:
    settings = Settings()
    settings.crawl.state_path = tmp_path / "state.db"
    settings.crawl.concurrent_requests = 2
    barrier = threading.Barrier(2)
    sources = {
        "source-a": BarrierAdapter("source-a", barrier),
        "source-b": BarrierAdapter("source-b", barrier),
    }
    service = AlphaPulseService(
        settings,
        store=FakeStore(),
        sources=sources,  # type: ignore[arg-type]
        seed_discovery=StaticSeedDiscovery(),  # type: ignore[arg-type]
    )

    stats = service.run_cycle()

    assert stats.pages_fetched == 2


def test_service_shares_kuaidaili_pool_across_sources(tmp_path: Path) -> None:
    settings = load_settings(Path("settings.example.toml"))
    settings.crawl.state_path = tmp_path / "state.db"
    settings.crawl.proxy.enabled = True
    settings.crawl.proxy.provider = "kuaidaili"
    settings.crawl.proxy.sources = ["guba", "tgb", "jiuyan"]
    settings.crawl.kuaidaili.api_url_file = tmp_path / "kuaidaili-api-url.txt"
    settings.crawl.kuaidaili.metrics_path = tmp_path / "proxy-metrics.db"
    settings.crawl.kuaidaili.share_across_sources = True
    settings.sources.guba.enabled = True
    settings.sources.tgb.enabled = True
    settings.sources.jiuyan.enabled = True

    service = AlphaPulseService(settings, store=FakeStore())
    providers = [
        service.sources[source].client.proxy_provider
        for source in ("guba", "tgb", "jiuyan")
    ]

    assert all(isinstance(provider, KuaidailiProxyProvider) for provider in providers)
    assert providers[0].pool is providers[1].pool is providers[2].pool


class HybridGubaAdapter:
    source_name = "guba"

    def __init__(
        self,
        *,
        block_existing: bool = False,
        agent_capacity: int = 1,
    ) -> None:
        self.block_existing = block_existing
        self.agent_capacity = agent_capacity
        self.barrier = threading.Barrier(2 if agent_capacity else 1)
        self.calls: list[tuple[str, str]] = []
        self.client = type(
            "HybridClient",
            (),
            {"agent_pool": object(), "proxy_provider": object()},
        )()

    def discover(self, seed):
        return [
            CrawlTask(
                source="guba",
                kind="fetch_post",
                url=f"https://guba.eastmoney.com/news,600519,{post_id}.html",
                seed_name=seed.name,
                priority=150,
                metadata={"post_id": post_id, "pubdate_ts": int(post_id)},
            )
            for post_id in ("2", "1")
        ]

    def available_agent_capacity(self) -> int:
        return self.agent_capacity

    def fetch_item_with_transport(self, task, transport):
        self.calls.append((transport, str(task.url)))
        self.barrier.wait(timeout=2)
        if self.block_existing and transport in {"existing", "auto"}:
            return FetchOutcome(
                status_code=403,
                blocked=True,
                errors=["blocked"],
            )
        return FetchOutcome(status_code=200)

    def fetch_item(self, task):
        raise AssertionError("hybrid queue must select a transport")

    def is_circuit_open(self) -> bool:
        return False

    def refresh_comments(self, item_ref):
        raise AssertionError("not used")

    def comment_task_for_post(self, post, seed_name):
        raise AssertionError("not used")


def test_guba_hybrid_queue_uses_paid_and_agent_slots_together(tmp_path: Path) -> None:
    settings = Settings()
    settings.crawl.state_path = tmp_path / "state.db"
    settings.sources.guba.concurrent_paid_requests = 1
    settings.sources.guba.concurrent_agent_requests = 1
    adapter = HybridGubaAdapter()
    service = AlphaPulseService(
        settings,
        store=FakeStore(),
        sources={"guba": adapter},  # type: ignore[arg-type]
        seed_discovery=StaticSeedDiscovery(),  # type: ignore[arg-type]
    )

    stats = service.run_cycle()

    assert stats.pages_fetched == 2
    assert {route for route, _ in adapter.calls} == {"existing", "agent"}


def test_guba_hybrid_block_does_not_stop_other_pool(tmp_path: Path) -> None:
    settings = Settings()
    settings.crawl.state_path = tmp_path / "state.db"
    settings.sources.guba.concurrent_paid_requests = 1
    settings.sources.guba.concurrent_agent_requests = 1
    adapter = HybridGubaAdapter(block_existing=True)
    service = AlphaPulseService(
        settings,
        store=FakeStore(),
        sources={"guba": adapter},  # type: ignore[arg-type]
        seed_discovery=StaticSeedDiscovery(),  # type: ignore[arg-type]
    )

    stats = service.run_cycle()
    pending = service.state.load_pending_tasks("parallel-test")

    assert stats.pages_fetched == 2
    assert stats.blocked_responses == 1
    assert len(adapter.calls) == 2
    assert len(pending) == 1


def test_guba_without_online_agents_keeps_conservative_block_stop(tmp_path: Path) -> None:
    settings = Settings()
    settings.crawl.state_path = tmp_path / "state.db"
    settings.sources.guba.concurrent_paid_requests = 1
    settings.sources.guba.concurrent_agent_requests = 4
    adapter = HybridGubaAdapter(
        block_existing=True,
        agent_capacity=0,
    )
    service = AlphaPulseService(
        settings,
        store=FakeStore(),
        sources={"guba": adapter},  # type: ignore[arg-type]
        seed_discovery=StaticSeedDiscovery(),  # type: ignore[arg-type]
    )

    stats = service.run_cycle()

    assert stats.pages_fetched == 1
    assert stats.blocked_responses == 1
    assert [route for route, _ in adapter.calls] == ["auto"]


class RotatingTgbAdapter:
    source_name = "tgb"

    def __init__(self) -> None:
        self.calls: list[str] = []
        self.client = type(
            "RotatingClient",
            (),
            {"agent_pool": object(), "proxy_provider": object()},
        )()

    def discover(self, seed):
        return [
            CrawlTask(
                source="tgb",
                kind="fetch_post",
                url=f"https://www.tgb.cn/a/{post_id}",
                seed_name=seed.name,
                priority=150,
                metadata={"post_id": post_id},
            )
            for post_id in ("2", "1")
        ]

    def fetch_item(self, task):
        self.calls.append(str(task.url))
        if str(task.url).endswith("/2"):
            return FetchOutcome(status_code=200, blocked=True, errors=["blocked"])
        return FetchOutcome(status_code=200)

    def continue_after_blocked_task(self, task) -> bool:
        return task.kind == "fetch_post"

    def refresh_comments(self, item_ref):
        raise AssertionError("not used")

    def comment_task_for_post(self, post, seed_name):
        raise AssertionError("not used")


def test_tgb_rotating_transport_isolates_blocked_detail(tmp_path: Path) -> None:
    settings = Settings()
    settings.crawl.state_path = tmp_path / "state.db"
    adapter = RotatingTgbAdapter()
    service = AlphaPulseService(
        settings,
        store=FakeStore(),
        sources={"tgb": adapter},  # type: ignore[arg-type]
        seed_discovery=StaticSeedDiscovery(),  # type: ignore[arg-type]
    )

    stats = service.run_cycle()
    pending = service.state.load_pending_tasks("parallel-test")

    assert stats.pages_fetched == 2
    assert stats.blocked_responses == 1
    assert adapter.calls == [
        "https://www.tgb.cn/a/2",
        "https://www.tgb.cn/a/1",
    ]
    assert [task.metadata["post_id"] for task in pending] == ["2"]


class RecoverableSeedDiscovery:
    def ensure_compiled_seed_sets(self, seed_set_name=None):
        return [SeedDefinition(name=seed_set_name or "recovery-test")]


class InterruptingAdapter:
    source_name = "guba"

    def __init__(self, *, block_first_post: bool) -> None:
        self.block_first_post = block_first_post
        self.post_calls: list[str] = []

    def discover(self, seed):
        return [
            CrawlTask(
                source="guba",
                kind="discover",
                url="https://guba.eastmoney.com/list,600519.html",
                seed_name=seed.name,
                priority=160,
                metadata={"board_code": "600519", "page": 1},
            )
        ]

    def fetch_item(self, task):
        if task.kind == "discover":
            return FetchOutcome(
                status_code=200,
                discovered_tasks=[
                    CrawlTask(
                        source="guba",
                        kind="fetch_post",
                        url="https://guba.eastmoney.com/news,600519,2.html",
                        seed_name=task.seed_name,
                        priority=150,
                        metadata={"post_id": "2", "pubdate_ts": 200},
                    ),
                    CrawlTask(
                        source="guba",
                        kind="fetch_post",
                        url="https://guba.eastmoney.com/news,600519,1.html",
                        seed_name=task.seed_name,
                        priority=150,
                        metadata={"post_id": "1", "pubdate_ts": 100},
                    ),
                ],
            )
        self.post_calls.append(str(task.url))
        if self.block_first_post:
            self.block_first_post = False
            return FetchOutcome(status_code=200, blocked=True, errors=["blocked"])
        return FetchOutcome(status_code=200)

    def refresh_comments(self, item_ref):
        raise AssertionError("not used")

    def comment_task_for_post(self, post, seed_name):
        raise AssertionError("not used")


def test_service_recovers_dynamic_tasks_after_interrupted_source_queue(tmp_path: Path) -> None:
    settings = Settings()
    settings.crawl.state_path = tmp_path / "state.db"
    state = None
    first_adapter = InterruptingAdapter(block_first_post=True)
    first_service = AlphaPulseService(
        settings,
        state=state,
        store=FakeStore(),
        sources={"guba": first_adapter},  # type: ignore[arg-type]
        seed_discovery=RecoverableSeedDiscovery(),  # type: ignore[arg-type]
    )

    first = first_service.run_cycle(seed_set_name="recovery-test")
    pending = first_service.state.load_pending_tasks("recovery-test")

    assert first.blocked_responses == 1
    assert first_adapter.post_calls == [
        "https://guba.eastmoney.com/news,600519,2.html"
    ]
    assert {task.metadata["post_id"] for task in pending} == {"1", "2"}

    second_adapter = InterruptingAdapter(block_first_post=False)
    second_service = AlphaPulseService(
        settings,
        state=first_service.state,
        store=FakeStore(),
        sources={"guba": second_adapter},  # type: ignore[arg-type]
        seed_discovery=RecoverableSeedDiscovery(),  # type: ignore[arg-type]
    )

    second = second_service.run_cycle(seed_set_name="recovery-test")

    assert second.pages_fetched == 2
    assert second_adapter.post_calls == [
        "https://guba.eastmoney.com/news,600519,2.html",
        "https://guba.eastmoney.com/news,600519,1.html",
    ]
    assert second_service.state.load_pending_tasks("recovery-test") == []


class FakeClient:
    def __init__(self, fixtures: Path) -> None:
        self.fixtures = fixtures

    def fetch(self, url: str) -> FetchResult:
        if "comments.json" in url:
            return FetchResult(url=url, status_code=200, text=(self.fixtures / "comments.json").read_text(), headers={})
        if url.endswith("/987654321"):
            return FetchResult(url=url, status_code=200, text=(self.fixtures / "post.html").read_text(), headers={})
        return FetchResult(url=url, status_code=200, text=(self.fixtures / "discovery.html").read_text(), headers={})


class FailingPostClient:
    def fetch(self, url: str) -> FetchResult:
        return FetchResult(url=url, status_code=0, text="", headers={}, error_message="dial tcp failed")


class BlockedClient:
    def __init__(self) -> None:
        self.calls: list[str] = []

    def fetch(self, url: str) -> FetchResult:
        self.calls.append(url)
        return FetchResult(url=url, status_code=403, text="captcha", headers={}, proxy_url="http://1.2.3.4:8080")


class FailingCommentsClient:
    def __init__(self, fixtures: Path) -> None:
        self.fixtures = fixtures

    def fetch(self, url: str) -> FetchResult:
        if "comments.json" in url:
            return FetchResult(url=url, status_code=0, text="", headers={}, error_message="proxy connect failed")
        return FetchResult(url=url, status_code=200, text=(self.fixtures / "post.html").read_text(), headers={})


class FakeBilibiliApi:
    def get_video_info(self, *, bvid=None, aid=None) -> BilibiliApiResult:
        del bvid, aid
        return BilibiliApiResult(
            payload={
                "code": 0,
                "data": {
                    "aid": 123456,
                    "bvid": "BV1xx411c7mu",
                    "title": "Test video",
                    "desc": "Video description",
                    "pubdate": 1_776_205_307,
                    "owner": {"mid": 42, "name": "Uploader"},
                    "stat": {"like": 88, "reply": 2, "share": 5},
                },
            },
            status_code=200,
        )

    def get_comments(self, *, aid: int, next_cursor: int = 0, page: int = 1) -> BilibiliApiResult:
        del aid, page
        if next_cursor > 0:
            return BilibiliApiResult(payload={"code": 0, "data": {"replies": [], "cursor": {"is_end": True}}}, status_code=200)
        return BilibiliApiResult(
            payload={
                "code": 0,
                "data": {
                    "replies": [
                        {
                            "rpid": 1001,
                            "parent": 0,
                            "like": 3,
                            "rcount": 1,
                            "ctime": 1_776_205_308,
                            "member": {"mid": "42"},
                            "content": {"message": "root comment"},
                        }
                    ],
                    "cursor": {"is_end": True, "next": 0},
                },
            },
            status_code=200,
        )

    def get_replies(self, *, aid: int, root_rpid: int, page: int = 1) -> BilibiliApiResult:
        del aid, page
        return BilibiliApiResult(
            payload={
                "code": 0,
                "data": {
                    "replies": [
                        {
                            "rpid": 2001,
                            "parent": root_rpid,
                            "like": 1,
                            "ctime": 1_776_205_309,
                            "member": {"mid": "42"},
                            "content": {"message": "child reply"},
                        }
                    ],
                    "cursor": {"is_end": True},
                },
            },
            status_code=200,
        )


def _xueqiu_adapter(service: AlphaPulseService):
    return service.sources["xueqiu"]


def test_service_runs_one_cycle(tmp_path: Path) -> None:
    fixtures = Path("tests/fixtures/xueqiu")
    settings = load_settings(Path("settings.example.toml"))
    settings.crawl.state_path = tmp_path / "state.db"
    settings.sources.xueqiu.seed_catalog_path = tmp_path / "seed_catalog.toml"
    settings.sources.xueqiu.seed_refresh_minutes = 9999
    settings.sources.xueqiu.seed_catalog_path.write_text(
        """
[[logical_sets]]
name = "cn-core"
generators = ["manual-post"]

[[generators]]
name = "manual-post"
type = "manual"
post_urls = ["https://xueqiu.com/1234567890/987654321"]
""".strip()
    )
    service = AlphaPulseService(settings, store=FakeStore())
    _xueqiu_adapter(service).client = FakeClient(fixtures)

    stats = service.run_cycle(seed_set_name="cn-core")

    assert stats.posts_written >= 1
    assert stats.comments_written >= 2
    assert service.store.posts[0].source_entity_id == "987654321"
    assert service.state.list_compiled_seed_set_names() == ["cn-core"]


def test_service_reuses_fresh_compiled_seed_snapshot(tmp_path: Path) -> None:
    fixtures = Path("tests/fixtures/xueqiu")
    settings = load_settings(Path("settings.example.toml"))
    settings.crawl.state_path = tmp_path / "state.db"
    settings.crawl.post_recrawl_minutes = 0
    settings.crawl.comment_refresh_minutes = 0
    settings.sources.xueqiu.seed_catalog_path = tmp_path / "seed_catalog.toml"
    settings.sources.xueqiu.seed_refresh_minutes = 9999
    settings.sources.xueqiu.seed_catalog_path.write_text(
        """
[[logical_sets]]
name = "cn-core"
generators = ["manual-post"]

[[generators]]
name = "manual-post"
type = "manual"
post_urls = ["https://xueqiu.com/1234567890/987654321"]
""".strip()
    )

    service = AlphaPulseService(settings, store=FakeStore())
    _xueqiu_adapter(service).client = FakeClient(fixtures)

    first = service.run_cycle(seed_set_name="cn-core")
    settings.sources.xueqiu.seed_catalog_path.write_text(
        """
[[logical_sets]]
name = "cn-core"
generators = ["manual-post"]

[[generators]]
name = "manual-post"
type = "manual"
post_urls = ["https://xueqiu.com/1111111111/222222222"]
""".strip()
    )
    second = service.run_cycle(seed_set_name="cn-core")

    with service.state.connection() as conn:
        run_count = conn.execute("SELECT COUNT(*) AS count FROM generated_seed_runs").fetchone()["count"]

    assert first.posts_written >= 1
    assert second.posts_written >= 1
    assert run_count == 1


def test_service_handles_fetch_transport_failure_without_crashing(tmp_path: Path) -> None:
    settings = load_settings(Path("settings.example.toml"))
    settings.crawl.state_path = tmp_path / "state.db"
    settings.sources.xueqiu.seed_catalog_path = tmp_path / "seed_catalog.toml"
    settings.sources.xueqiu.seed_refresh_minutes = 9999
    settings.sources.xueqiu.seed_catalog_path.write_text(
        """
[[logical_sets]]
name = "cn-core"
generators = ["manual-post"]

[[generators]]
name = "manual-post"
type = "manual"
post_urls = ["https://xueqiu.com/1234567890/987654321"]
""".strip()
    )

    store = FakeStore()
    service = AlphaPulseService(settings, store=store)
    _xueqiu_adapter(service).client = FailingPostClient()

    stats = service.run_cycle(seed_set_name="cn-core")

    assert stats.errors == 1
    assert stats.posts_written == 0
    source, url, message, status_code, task_kind, error_kind = store.errors[0]
    assert message.startswith("Fetch failed for https://xueqiu.com/1234567890/987654321")
    assert error_kind == "fetch_failed"
    assert task_kind == "fetch_post"


def test_service_counts_blocked_responses(tmp_path: Path) -> None:
    settings = load_settings(Path("settings.example.toml"))
    settings.crawl.state_path = tmp_path / "state.db"
    settings.sources.xueqiu.seed_catalog_path = tmp_path / "seed_catalog.toml"
    settings.sources.xueqiu.seed_refresh_minutes = 9999
    settings.sources.xueqiu.seed_catalog_path.write_text(
        """
[[logical_sets]]
name = "cn-core"
generators = ["manual-post"]

[[generators]]
name = "manual-post"
type = "manual"
post_urls = [
    "https://xueqiu.com/1234567890/987654321",
    "https://xueqiu.com/1234567890/987654322",
]
""".strip()
    )

    store = FakeStore()
    service = AlphaPulseService(settings, store=store)
    client = BlockedClient()
    _xueqiu_adapter(service).client = client

    stats = service.run_cycle(seed_set_name="cn-core")

    assert stats.blocked_responses == 1
    assert stats.errors == 1
    assert stats.skipped_tasks >= 1
    assert client.calls == ["https://xueqiu.com/1234567890/987654321"]
    assert store.errors[0][2] == "Blocked response from https://xueqiu.com/1234567890/987654321"
    assert store.errors[0][5] == "blocked"
    with service.state.connection() as conn:
        row = conn.execute(
            "SELECT last_fetched_at FROM url_state WHERE url = ?",
            ("https://xueqiu.com/1234567890/987654321",),
        ).fetchone()
    assert row["last_fetched_at"] is None


def test_service_stops_comment_refresh_on_fetch_failure(tmp_path: Path) -> None:
    fixtures = Path("tests/fixtures/xueqiu")
    settings = load_settings(Path("settings.example.toml"))
    settings.crawl.state_path = tmp_path / "state.db"
    settings.crawl.comment_refresh_minutes = 0
    settings.sources.xueqiu.seed_catalog_path = tmp_path / "seed_catalog.toml"
    settings.sources.xueqiu.seed_refresh_minutes = 9999
    settings.sources.xueqiu.seed_catalog_path.write_text(
        """
[[logical_sets]]
name = "cn-core"
generators = ["manual-post"]

[[generators]]
name = "manual-post"
type = "manual"
post_urls = ["https://xueqiu.com/1234567890/987654321"]
""".strip()
    )

    store = FakeStore()
    service = AlphaPulseService(settings, store=store)
    _xueqiu_adapter(service).client = FailingCommentsClient(fixtures)

    stats = service.run_cycle(seed_set_name="cn-core")

    assert stats.posts_written == 1
    assert stats.comments_written == 0


def test_service_runs_bilibili_cycle(tmp_path: Path) -> None:
    settings = load_settings(Path("settings.example.toml"))
    settings.crawl.state_path = tmp_path / "state.db"
    settings.crawl.comment_refresh_minutes = 0
    settings.sources.xueqiu.enabled = False
    settings.sources.xueqiu.seed_catalog_path = tmp_path / "seed_catalog.toml"
    settings.sources.xueqiu.seed_refresh_minutes = 9999
    settings.sources.xueqiu.seed_catalog_path.write_text(
        """
[[logical_sets]]
name = "bili-core"
generators = ["manual-video"]

[[generators]]
name = "manual-video"
type = "manual"
bilibili_video_targets = ["BV1xx411c7mu"]
""".strip()
    )

    store = FakeStore()
    service = AlphaPulseService(settings, store=store)
    service.sources["bilibili"].api = FakeBilibiliApi()

    stats = service.run_cycle(seed_set_name="bili-core")

    assert stats.posts_written == 1
    assert stats.comments_written == 2
    assert store.posts[0].source == "bilibili"
    assert store.comments[1].parent_comment_entity_id == "1001"


class FakeGubaClient:
    def __init__(self, fixtures: Path) -> None:
        self.fixtures = fixtures
        self.reply_calls = 0

    def get(self, url: str, *, expect_marker: str | None = None):
        del expect_marker
        from alphapulse.sources.guba.api import GubaHttpResult

        if "/list," in url:
            text = (self.fixtures / "list_stock.html").read_text(encoding="utf-8")
        else:
            text = (self.fixtures / "post_detail.html").read_text(encoding="utf-8")
        return GubaHttpResult(url=url, status_code=200, text=text, duration_ms=10)

    def post_replies(self, *, post_id: str, board_code: str, page: int):
        from alphapulse.sources.guba.api import GubaHttpResult

        del post_id, board_code, page
        self.reply_calls += 1
        text = (self.fixtures / "replies.json").read_text(encoding="utf-8")
        return GubaHttpResult(
            url="https://guba.eastmoney.com/interface/GetData.aspx",
            status_code=200,
            text=text,
            duration_ms=10,
        )


class FakeGubaBrowserClient:
    def __init__(self, fixtures: Path) -> None:
        self.fixtures = fixtures
        self.get_calls: list[str] = []

    def get(self, url: str):
        from alphapulse.sources.guba.api import GubaHttpResult

        self.get_calls.append(url)
        return GubaHttpResult(
            url=url,
            status_code=200,
            text=(self.fixtures / "post_detail.html").read_text(encoding="utf-8"),
            duration_ms=10,
        )


def test_service_runs_guba_cycle(tmp_path: Path) -> None:
    settings = load_settings(Path("settings.example.toml"))
    settings.crawl.state_path = tmp_path / "state.db"
    settings.sources.xueqiu.enabled = False
    settings.sources.bilibili.enabled = False
    settings.sources.guba.enabled = True
    settings.sources.guba.max_list_pages = 1
    # This test exercises the classic path against a fixed-date fixture.
    settings.sources.guba.day_scoped = False
    settings.crawl.raw_store.enabled = True
    settings.crawl.raw_store.root_path = tmp_path / "raw"
    settings.sources.xueqiu.seed_catalog_path = tmp_path / "seed_catalog.toml"
    settings.sources.xueqiu.seed_refresh_minutes = 9999
    settings.sources.xueqiu.seed_catalog_path.write_text(
        """
[[logical_sets]]
name = "guba-core"
generators = ["manual-guba"]

[[generators]]
name = "manual-guba"
type = "manual"
guba_board_codes = ["600519"]
""".strip()
    )

    store = FakeStore()
    service = AlphaPulseService(settings, store=store)
    service.sources["guba"].client = FakeGubaClient(Path("tests/fixtures/guba"))

    stats = service.run_cycle(seed_set_name="guba-core")

    # The list fixture has 3 entries; every detail fetch serves the same
    # fixture post. Two entries carry comment counts, so two reply refreshes
    # run (3 comments each: 2 live top-level + 1 nested child).
    assert stats.posts_written == 3
    assert stats.comments_written == 6
    assert stats.blocked_responses == 0
    assert store.posts[0].source == "guba"
    assert {comment.post_entity_id for comment in store.comments} == {"1743987733", "1743507860"}
    assert any(comment.parent_comment_entity_id == "9926112093" for comment in store.comments)
    assert (tmp_path / "raw" / "fetch_log.db").exists()

    # Second cycle within the recrawl windows: everything is claim-gated.
    second = service.run_cycle(seed_set_name="guba-core")
    assert second.posts_written == 0
    assert second.comments_written == 0


def test_service_guba_cycle_skips_comments_when_fetch_comments_disabled(tmp_path: Path) -> None:
    settings = load_settings(Path("settings.example.toml"))
    settings.crawl.state_path = tmp_path / "state.db"
    settings.sources.xueqiu.enabled = False
    settings.sources.bilibili.enabled = False
    settings.sources.guba.enabled = True
    settings.sources.guba.max_list_pages = 1
    settings.sources.guba.day_scoped = False
    settings.sources.guba.fetch_comments = False
    settings.crawl.raw_store.enabled = True
    settings.crawl.raw_store.root_path = tmp_path / "raw"
    settings.sources.xueqiu.seed_catalog_path = tmp_path / "seed_catalog.toml"
    settings.sources.xueqiu.seed_refresh_minutes = 9999
    settings.sources.xueqiu.seed_catalog_path.write_text(
        """
[[logical_sets]]
name = "guba-core"
generators = ["manual-guba"]

[[generators]]
name = "manual-guba"
type = "manual"
guba_board_codes = ["600519"]
""".strip()
    )

    store = FakeStore()
    service = AlphaPulseService(settings, store=store)
    fake_client = FakeGubaClient(Path("tests/fixtures/guba"))
    service.sources["guba"].client = fake_client

    stats = service.run_cycle(seed_set_name="guba-core")

    # Posts still crawl normally; no reply requests fire at all — this is the
    # wall-clock saving, not just an absence of written comments.
    assert stats.posts_written == 3
    assert stats.comments_written == 0
    assert fake_client.reply_calls == 0


def test_service_caps_browser_posts_per_cycle(tmp_path: Path) -> None:
    settings = load_settings(Path("settings.example.toml"))
    settings.crawl.state_path = tmp_path / "state.db"
    settings.sources.xueqiu.enabled = False
    settings.sources.bilibili.enabled = False
    settings.sources.guba.enabled = True
    settings.sources.guba.max_list_pages = 1
    settings.sources.guba.day_scoped = False
    settings.sources.guba.browser.enabled = True
    settings.sources.guba.browser.max_posts_per_cycle = 2
    settings.sources.xueqiu.seed_catalog_path = tmp_path / "seed_catalog.toml"
    settings.sources.xueqiu.seed_refresh_minutes = 9999
    settings.sources.xueqiu.seed_catalog_path.write_text(
        """
[[logical_sets]]
name = "guba-core"
generators = ["manual-guba"]

[[generators]]
name = "manual-guba"
type = "manual"
guba_board_codes = ["600519"]
""".strip()
    )

    fixtures = Path("tests/fixtures/guba")
    store = FakeStore()
    service = AlphaPulseService(settings, store=store)
    service.sources["guba"].client = FakeGubaClient(fixtures)
    browser_client = FakeGubaBrowserClient(fixtures)
    service.sources["guba"].browser_client = browser_client

    stats = service.run_cycle(seed_set_name="guba-core")

    assert len(browser_client.get_calls) == 2
    assert stats.posts_written == 2
    assert stats.skipped_tasks >= 1
