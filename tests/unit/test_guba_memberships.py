import json

from alphapulse.pipeline.contracts import CrawlTask
from alphapulse.runtime.config import CrawlSettings, GubaSettings
from alphapulse.sources.guba.adapter import GubaAdapter
from alphapulse.sources.guba.api import GubaHttpResult


def test_list_memberships_survive_original_board_detail_url():
    adapter = GubaAdapter(GubaSettings(enabled=True, day_scoped=False), CrawlSettings())
    tasks = []
    for board in ["BK1152", "BK1128"]:
        url = f"https://guba.eastmoney.com/list,{board}.html"
        task = CrawlTask(source="guba", kind="discover", url=url, seed_name="test",
                         metadata={"board_code": board})
        text = "var article_list=" + json.dumps({"re": [{
            "post_id": 42, "stockbar_code": "600519", "post_title": "fixture",
            "post_publish_time": "2026-09-16 10:00:00",
        }]}) + ";"
        outcome = adapter._handle_list_page(task, GubaHttpResult(url=url, status_code=200, text=text))
        assert board in outcome.post_board_memberships["42"]
        tasks.extend(outcome.discovered_tasks)
    detail = tasks[0]
    assert "/news,600519,42.html" in str(detail.url)
    body = "var post_article=" + json.dumps({
        "post_id": 42, "post_content": "fixture body", "post_state": 0,
        "post_publish_time": "2026-09-16 10:00:00",
    }) + ";"
    outcome = adapter._handle_post_detail(detail, GubaHttpResult(url=str(detail.url), status_code=200, text=body))
    assert outcome.posts[0].raw_topic_ids == ["600519", "BK1128", "BK1152"]


def test_persisted_task_keeps_its_listing_board_after_adapter_restart():
    adapter = GubaAdapter(GubaSettings(enabled=True), CrawlSettings())
    task = CrawlTask(source="guba", kind="fetch_post",
        url="https://guba.eastmoney.com/news,600519,42.html", seed_name="test",
        metadata={"post_id": "42", "listing_board_code": "bk1152"})
    body = 'var post_article={"post_id":42,"post_content":"fixture","post_state":0};'
    outcome = adapter._handle_post_detail(task, GubaHttpResult(url=str(task.url), status_code=200, text=body))
    assert outcome.posts[0].raw_topic_ids == ["600519", "BK1152"]
