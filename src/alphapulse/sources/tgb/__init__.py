"""淘股吧 (tgb.cn) source adapter.

A day-scoped crawler for the 淘股吧 stock forum, mirroring the guba source. tgb.cn
serves plain HTML (no embedded-JSON payloads), so parsing is DOM-based (lxml + CSS
selectors). Each crawl day collects:

* 精华 (featured) posts  -> report "featured" section
* 社区总版 (general feed) -> report "general" section (catch-all)
* self-discovered 热门研股 (hot research stocks) per-stock boards -> report "general"

See ``docs/tgb-crawl.md`` for the day-grouping/pagination invariant and the report
semantics, and the ``tgb-crawl-structure`` project memory for the reverse-engineered
page structure.
"""
