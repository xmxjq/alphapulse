from alphapulse.pipeline.contracts import CrawlTask


def test_task_dedupe_key_is_stable() -> None:
    task = CrawlTask(
        source="xueqiu",
        kind="fetch_post",
        url="https://xueqiu.com/1234567890/987654321",
        seed_name="cn-core",
    )
    assert task.dedupe_key == "xueqiu:fetch_post:https://xueqiu.com/1234567890/987654321"

