from alphapulse.pipeline.contracts import CrawlTask
from alphapulse.runtime.service import TaskQueue


def _task(url: str, *, priority: int = 0, pubdate_ts: int | None = None, kind: str = "fetch_post") -> CrawlTask:
    metadata: dict = {}
    if pubdate_ts is not None:
        metadata["pubdate_ts"] = pubdate_ts
    return CrawlTask(
        source="bilibili",
        kind=kind,
        url=url,
        seed_name="s",
        priority=priority,
        metadata=metadata,
    )


def test_queue_orders_by_priority_desc() -> None:
    queue = TaskQueue()
    queue.push(_task("https://a", priority=100))
    queue.push(_task("https://b", priority=300))
    queue.push(_task("https://c", priority=180))

    order = [queue.pop().url for _ in range(3)]
    assert [str(u) for u in order] == ["https://b/", "https://c/", "https://a/"]


def test_queue_ties_broken_by_newer_pubdate_first() -> None:
    queue = TaskQueue()
    queue.push(_task("https://old", priority=180, pubdate_ts=1000))
    queue.push(_task("https://new", priority=180, pubdate_ts=5000))
    queue.push(_task("https://mid", priority=180, pubdate_ts=3000))

    order = [str(queue.pop().url) for _ in range(3)]
    assert order == ["https://new/", "https://mid/", "https://old/"]


def test_queue_fifo_within_same_priority_and_pubdate() -> None:
    queue = TaskQueue()
    queue.push(_task("https://first", priority=180))
    queue.push(_task("https://second", priority=180))
    queue.push(_task("https://third", priority=180))

    order = [str(queue.pop().url) for _ in range(3)]
    assert order == ["https://first/", "https://second/", "https://third/"]


def test_comments_beat_queued_fetch_posts() -> None:
    queue = TaskQueue()
    queue.push(_task("https://post-old", priority=180, pubdate_ts=1000))
    queue.push(_task("https://post-new", priority=180, pubdate_ts=5000))
    queue.push(_task("https://comments-for-first", priority=300, kind="refresh_comments"))

    assert str(queue.pop().url) == "https://comments-for-first/"
    assert str(queue.pop().url) == "https://post-new/"
    assert str(queue.pop().url) == "https://post-old/"
