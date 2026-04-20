from alphapulse.pipeline.contracts import CrawlTask


def test_task_dedupe_key_is_stable() -> None:
    task = CrawlTask(
        source="xueqiu",
        kind="fetch_post",
        url="https://xueqiu.com/1234567890/987654321",
        seed_name="cn-core",
    )
    assert task.dedupe_key == "xueqiu:fetch_post:https://xueqiu.com/1234567890/987654321"


def test_task_dedupe_key_uses_source_name() -> None:
    task = CrawlTask(
        source="bilibili",
        kind="refresh_comments",
        url="https://api.bilibili.com/x/v2/reply/main?oid=123&type=1",
        seed_name="bili-core",
    )
    assert task.dedupe_key == "bilibili:refresh_comments:https://api.bilibili.com/x/v2/reply/main?oid=123&type=1"
