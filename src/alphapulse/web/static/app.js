const POLL_INTERVAL_MS = 5000;

const state = {
  activeTab: "status",
  posts: [],
  postsSource: "",
  postsLimit: 50,
  postsOffset: 0,
  selectedPostKey: null,
  statusTimer: null,
  gubaLimit: 100,
  gubaBoards: [],
  gubaPosts: [],
  gubaBoardFilter: "",
  proxyHours: 24,
  agentsHours: 24,
  agents: null,
};

function el(tag, attrs = {}, children = []) {
  const node = document.createElement(tag);
  for (const [k, v] of Object.entries(attrs)) {
    if (k === "class") node.className = v;
    else if (k === "dataset") Object.assign(node.dataset, v);
    else if (k.startsWith("on") && typeof v === "function") node.addEventListener(k.slice(2), v);
    else if (v !== undefined && v !== null) node.setAttribute(k, v);
  }
  for (const child of [].concat(children)) {
    if (child === null || child === undefined || child === false) continue;
    node.appendChild(typeof child === "string" ? document.createTextNode(child) : child);
  }
  return node;
}

async function fetchJSON(path) {
  const res = await fetch(path);
  if (!res.ok) throw new Error(`${path} → ${res.status}`);
  return res.json();
}

function fmtDate(value) {
  if (!value) return "—";
  try { return new Date(value).toLocaleString(undefined, { hour12: false }); } catch { return value; }
}

function fmtDuration(startIso, endIso) {
  if (!startIso || !endIso) return "—";
  const ms = new Date(endIso).getTime() - new Date(startIso).getTime();
  if (Number.isNaN(ms)) return "—";
  if (ms < 1000) return `${ms} ms`;
  if (ms < 60_000) return `${(ms / 1000).toFixed(1)} s`;
  return `${(ms / 60_000).toFixed(1)} m`;
}

function statusClass(status) {
  return status === "succeeded" ? "status-ok"
    : status === "failed" ? "status-failed"
    : "status-running";
}

function setLastUpdated() {
  document.getElementById("last-updated").textContent = `updated ${new Date().toLocaleTimeString(undefined, { hour12: false })}`;
}

function renderRunStats(target, run) {
  target.innerHTML = "";
  if (!run) { target.appendChild(el("div", { class: "empty" }, "No runs recorded yet.")); return; }
  target.appendChild(el("div", { class: "stats" }, [
    el("div", { class: "stat" }, [el("div", { class: "label" }, "Status"), el("div", { class: `value ${statusClass(run.status)}` }, run.status || "—")]),
    el("div", { class: "stat" }, [el("div", { class: "label" }, "Started"), el("div", { class: "value" }, fmtDate(run.started_at))]),
    el("div", { class: "stat" }, [el("div", { class: "label" }, "Duration"), el("div", { class: "value" }, fmtDuration(run.started_at, run.finished_at))]),
    el("div", { class: "stat" }, [el("div", { class: "label" }, "Posts"), el("div", { class: "value" }, String(run.posts_written))]),
    el("div", { class: "stat" }, [el("div", { class: "label" }, "Comments"), el("div", { class: "value" }, String(run.comments_written))]),
    el("div", { class: "stat" }, [el("div", { class: "label" }, "Errors"), el("div", { class: "value" }, String(run.errors))]),
  ]));
}

function renderLatestRun(run) {
  renderRunStats(document.querySelector("#latest-run .body"), run);
}

function renderActivity(statusPayload) {
  const target = document.querySelector("#activity .body");
  target.innerHTML = "";
  target.appendChild(el("div", { class: "stats" }, [
    el("div", { class: "stat" }, [el("div", { class: "label" }, "URLs active"), el("div", { class: "value" }, String(statusPayload.in_flight_urls))]),
    el("div", { class: "stat" }, [el("div", { class: "label" }, "Seed sets"), el("div", { class: "value" }, String(statusPayload.seed_sets.length))]),
    el("div", { class: "stat" }, [el("div", { class: "label" }, "Recent errors"), el("div", { class: "value" }, String(statusPayload.recent_errors.length))]),
  ]));
}

function renderRunsTable(target, runs) {
  target.innerHTML = "";
  if (!runs.length) { target.appendChild(el("div", { class: "empty" }, "No runs.")); return; }
  const head = el("tr", {}, ["Started", "Status", "Duration", "Posts", "Comments", "Errors"].map(h => el("th", {}, h)));
  const rows = runs.map(r => el("tr", {}, [
    el("td", { class: "mono" }, fmtDate(r.started_at)),
    el("td", { class: statusClass(r.status) }, r.status || "—"),
    el("td", {}, fmtDuration(r.started_at, r.finished_at)),
    el("td", { class: "num" }, String(r.posts_written)),
    el("td", { class: "num" }, String(r.comments_written)),
    el("td", { class: "num" }, String(r.errors)),
  ]));
  target.appendChild(el("table", {}, [el("thead", {}, head), el("tbody", {}, rows)]));
}

function fmtErrorKind(e) {
  const parts = [];
  if (e.error_kind) parts.push(e.error_kind);
  if (e.status_code) parts.push(`HTTP ${e.status_code}`);
  return parts.length ? parts.join(" · ") : "—";
}

function renderErrorsTable(target, errors) {
  target.innerHTML = "";
  if (!errors.length) { target.appendChild(el("div", { class: "empty" }, "No errors.")); return; }
  const head = el("tr", {}, ["When", "Source", "Kind", "URL", "Message"].map(h => el("th", {}, h)));
  const rows = errors.map(e => el("tr", {}, [
    el("td", { class: "mono" }, fmtDate(e.created_at)),
    el("td", {}, e.source),
    el("td", {}, fmtErrorKind(e)),
    el("td", { class: "mono" }, e.url),
    el("td", { class: "err" }, e.error_message),
  ]));
  target.appendChild(el("table", {}, [el("thead", {}, head), el("tbody", {}, rows)]));
}

function renderSeedSets(target, sets) {
  target.innerHTML = "";
  if (!sets.length) { target.appendChild(el("div", { class: "empty" }, "No compiled seed sets.")); return; }
  const head = el("tr", {}, ["Name", "Refreshed", "Stocks", "Topics", "Users", "Bili videos", "Bili spaces", "Post URLs"].map(h => el("th", {}, h)));
  const rows = sets.map(s => el("tr", {}, [
    el("td", {}, s.name),
    el("td", { class: "mono" }, fmtDate(s.refreshed_at)),
    el("td", { class: "num" }, String(s.stock_count)),
    el("td", { class: "num" }, String(s.topic_count)),
    el("td", { class: "num" }, String(s.user_count)),
    el("td", { class: "num" }, String(s.bilibili_video_count)),
    el("td", { class: "num" }, String(s.bilibili_space_count)),
    el("td", { class: "num" }, String(s.post_url_count)),
  ]));
  target.appendChild(el("table", {}, [el("thead", {}, head), el("tbody", {}, rows)]));
}

async function refreshStatus() {
  try {
    const payload = await fetchJSON("/api/status");
    renderLatestRun(payload.latest_run);
    renderActivity(payload);
    renderRunsTable(document.getElementById("recent-runs"), payload.recent_runs);
    renderErrorsTable(document.getElementById("recent-errors"), payload.recent_errors);
    renderSeedSets(document.getElementById("seed-sets"), payload.seed_sets);
    setLastUpdated();
  } catch (err) {
    document.getElementById("last-updated").textContent = `error: ${err.message}`;
  }
}

function renderPostsList() {
  const target = document.getElementById("posts-list");
  target.innerHTML = "";
  if (!state.posts.length) { target.appendChild(el("div", { class: "empty" }, "No posts yet.")); return; }
  for (const post of state.posts) {
    const key = `${post.source}/${post.source_entity_id}`;
    const item = el("div", {
      class: `post-item ${key === state.selectedPostKey ? "active" : ""}`,
      onclick: () => openPost(post.source, post.source_entity_id),
    }, [
      el("div", { class: "title" }, post.title || post.content_preview || "(untitled)"),
      el("div", { class: "sub" }, [
        el("span", {}, `${post.source} · ${fmtDate(post.published_at || post.fetched_at)}`),
        el("span", {}, `♥ ${post.like_count ?? 0} · 💬 ${post.comment_count ?? 0}`),
      ]),
    ]);
    target.appendChild(item);
  }
  const paging = document.getElementById("posts-paging");
  paging.textContent = `showing ${state.postsOffset + 1}–${state.postsOffset + state.posts.length}`;
  document.getElementById("posts-prev").disabled = state.postsOffset === 0;
  document.getElementById("posts-next").disabled = state.posts.length < state.postsLimit;
}

async function refreshPosts() {
  const params = new URLSearchParams({ limit: String(state.postsLimit), offset: String(state.postsOffset) });
  if (state.postsSource) params.set("source", state.postsSource);
  try {
    const payload = await fetchJSON(`/api/posts?${params}`);
    state.posts = payload.posts;
    renderPostsList();
  } catch (err) {
    document.getElementById("posts-list").innerHTML = `<div class="empty err">error: ${err.message}</div>`;
  }
}

function renderCommentTree(comments) {
  const byParent = new Map();
  for (const c of comments) {
    const parent = c.parent_comment_entity_id || null;
    if (!byParent.has(parent)) byParent.set(parent, []);
    byParent.get(parent).push(c);
  }
  const out = el("div");
  const walk = (parent, depth) => {
    const children = byParent.get(parent) || [];
    for (const c of children) {
      out.appendChild(el("div", { class: `comment ${depth ? "reply" : ""}` }, [
        el("div", { class: "head" }, [
          el("span", {}, `#${c.source_entity_id}`),
          el("span", {}, c.author_entity_id ? `by ${c.author_entity_id}` : "by unknown"),
          el("span", {}, fmtDate(c.published_at || c.fetched_at)),
          el("span", {}, `♥ ${c.like_count ?? 0}`),
        ]),
        el("div", { class: "body" }, c.content_text || "(empty)"),
      ]));
      walk(c.source_entity_id, depth + 1);
    }
  };
  walk(null, 0);
  // Surface any orphans (parents we didn't see) at top level.
  for (const [parent, list] of byParent.entries()) {
    if (parent === null) continue;
    if (!comments.some(c => c.source_entity_id === parent)) {
      for (const c of list) {
        out.appendChild(el("div", { class: "comment" }, [
          el("div", { class: "head" }, [
            el("span", {}, `#${c.source_entity_id}`),
            el("span", {}, c.author_entity_id ? `by ${c.author_entity_id}` : "by unknown"),
            el("span", {}, fmtDate(c.published_at || c.fetched_at)),
          ]),
          el("div", { class: "body" }, c.content_text || "(empty)"),
        ]));
      }
    }
  }
  return out;
}

async function openPost(source, entityId) {
  state.selectedPostKey = `${source}/${entityId}`;
  renderPostsList();
  const target = document.getElementById("post-detail");
  target.innerHTML = "<p class=\"empty\">Loading…</p>";
  try {
    const payload = await fetchJSON(`/api/posts/${encodeURIComponent(source)}/${encodeURIComponent(entityId)}`);
    const post = payload.post;
    target.innerHTML = "";
    target.appendChild(el("h3", {}, post.title || "(untitled)"));
    target.appendChild(el("div", { class: "sub" }, [
      `${post.source} · ${post.source_entity_id} · `,
      el("a", { href: post.canonical_url, target: "_blank", rel: "noopener" }, post.canonical_url),
      ` · ${fmtDate(post.published_at)} · author ${post.author_entity_id || "?"}`,
      ` · ♥ ${post.like_count ?? 0} · 💬 ${post.comment_count ?? 0}`,
    ]));
    target.appendChild(el("div", { class: "content" }, post.content_text || "(empty)"));
    target.appendChild(el("div", { class: "comments" }, [
      el("h4", {}, `Comments (${payload.comments.length})`),
      payload.comments.length ? renderCommentTree(payload.comments) : el("div", { class: "empty" }, "No comments."),
    ]));
  } catch (err) {
    target.innerHTML = `<div class="empty err">error: ${err.message}</div>`;
  }
}

function normalizeErrorMessage(message) {
  return (message || "")
    .replace(/https?:\/\/\S+/g, "<url>")
    .replace(/\b[0-9a-f]{8,}\b/gi, "<id>")
    .replace(/\d+/g, "<n>")
    .replace(/\s+/g, " ")
    .trim()
    .slice(0, 200) || "(empty message)";
}

function renderGubaSummary(errors, posts) {
  const target = document.querySelector("#guba-summary .body");
  target.innerHTML = "";
  const patterns = new Set(errors.map(e => normalizeErrorMessage(e.error_message)));
  const lastError = errors.length ? errors[0].created_at : null;
  const lastPost = posts.length ? posts[0].fetched_at : null;
  target.appendChild(el("div", { class: "stats" }, [
    el("div", { class: "stat" }, [el("div", { class: "label" }, "Errors fetched"), el("div", { class: "value" }, String(errors.length))]),
    el("div", { class: "stat" }, [el("div", { class: "label" }, "Error patterns"), el("div", { class: "value" }, String(patterns.size))]),
    el("div", { class: "stat" }, [el("div", { class: "label" }, "Last error"), el("div", { class: "value" }, fmtDate(lastError))]),
    el("div", { class: "stat" }, [el("div", { class: "label" }, "Last post fetched"), el("div", { class: "value" }, fmtDate(lastPost))]),
  ]));
}

function renderGubaErrorPatterns(errors) {
  const target = document.getElementById("guba-error-patterns");
  target.innerHTML = "";
  if (!errors.length) { target.appendChild(el("div", { class: "empty" }, "No guba errors recorded.")); return; }
  const groups = new Map();
  for (const e of errors) {
    const key = normalizeErrorMessage(e.error_message);
    if (!groups.has(key)) groups.set(key, { count: 0, lastSeen: e.created_at, sample: e });
    const g = groups.get(key);
    g.count += 1;
    if (e.created_at > g.lastSeen) { g.lastSeen = e.created_at; g.sample = e; }
  }
  const sorted = [...groups.entries()].sort((a, b) => b[1].count - a[1].count);
  const head = el("tr", {}, ["Count", "Pattern", "Last seen", "Sample URL"].map(h => el("th", {}, h)));
  const rows = sorted.map(([pattern, g]) => el("tr", {}, [
    el("td", { class: "num" }, String(g.count)),
    el("td", { class: "err" }, pattern),
    el("td", { class: "mono" }, fmtDate(g.lastSeen)),
    el("td", { class: "mono" }, g.sample.url),
  ]));
  target.appendChild(el("table", {}, [el("thead", {}, head), el("tbody", {}, rows)]));
}

function postBoardCode(post) {
  const match = /\/news,([^,]+),/.exec(post.canonical_url || "");
  return match ? match[1] : "";
}

function setGubaBoardFilter(code) {
  state.gubaBoardFilter = state.gubaBoardFilter === code ? "" : code;
  renderGubaBoards();
  renderGubaPosts();
}

function fmtEta(iso, nowMs) {
  if (!iso) return "now";
  const ms = new Date(iso).getTime() - nowMs;
  if (ms <= 0) return "now";
  if (ms < 60_000) return `in ${Math.round(ms / 1000)}s`;
  if (ms < 3_600_000) return `in ${Math.round(ms / 60_000)}m`;
  return `in ${(ms / 3_600_000).toFixed(1)}h`;
}

function renderGubaNextCrawl(plan) {
  const target = document.getElementById("guba-next-crawl");
  target.innerHTML = "";
  const nowMs = new Date(plan.generated_at).getTime();
  const dueBoards = plan.boards.filter(b => b.due_now).length;
  const posts = plan.task_forecasts.find(f => f.kind === "fetch_post") || { due_now: 0, tracked: 0, next_eligible_at: null };
  const comments = plan.task_forecasts.find(f => f.kind === "refresh_comments") || { due_now: 0, tracked: 0, next_eligible_at: null };

  target.appendChild(el("div", { class: "stats" }, [
    el("div", { class: "stat" }, [
      el("div", { class: "label" }, "Next cycle"),
      el("div", { class: "value" }, plan.next_cycle_at ? fmtEta(plan.next_cycle_at, nowMs) : "unknown"),
    ]),
    el("div", { class: "stat" }, [
      el("div", { class: "label" }, "Boards due"),
      el("div", { class: "value" }, `${dueBoards} / ${plan.boards.length}`),
    ]),
    el("div", { class: "stat" }, [
      el("div", { class: "label" }, "Posts due"),
      el("div", { class: "value" }, `${posts.due_now} / ${posts.tracked}`),
    ]),
    el("div", { class: "stat" }, [
      el("div", { class: "label" }, "Comment refreshes due"),
      el("div", { class: "value" }, `${comments.due_now} / ${comments.tracked}`),
    ]),
  ]));

  if (plan.boards.length) {
    const head = el("tr", {}, ["Board", "Seed", "Last crawled", "Next list crawl"].map(h => el("th", {}, h)));
    const rows = plan.boards.map(b => el("tr", {}, [
      el("td", { class: "mono" }, b.board_code),
      el("td", {}, b.seed_name || "—"),
      el("td", { class: "mono" }, b.last_fetched_at ? fmtDate(b.last_fetched_at) : "never"),
      el("td", { class: b.due_now ? "status-ok" : "" },
        b.due_now ? "next cycle" : `${fmtEta(b.eligible_at, nowMs)} (${fmtDate(b.eligible_at)})`),
    ]));
    target.appendChild(el("table", {}, [el("thead", {}, head), el("tbody", {}, rows)]));
  } else {
    target.appendChild(el("div", { class: "empty" }, "No guba boards seeded or tracked yet."));
  }

  target.appendChild(el("div", { class: "meta" }, [
    `recrawl intervals: lists ${plan.list_recrawl_minutes}m · posts ${plan.post_recrawl_minutes}m · `
    + `comments ${plan.comment_refresh_minutes}m · cycle every ${plan.poll_interval_seconds}s`
    + (posts.next_eligible_at ? ` · next post due ${fmtEta(posts.next_eligible_at, nowMs)}` : "")
    + (comments.next_eligible_at ? ` · next comment refresh due ${fmtEta(comments.next_eligible_at, nowMs)}` : ""),
  ]));
}

function renderGubaBoards() {
  const target = document.getElementById("guba-boards");
  target.innerHTML = "";
  if (!state.gubaBoards.length) {
    target.appendChild(el("div", { class: "empty" }, "No guba posts stored yet."));
    return;
  }
  const head = el("tr", {}, ["Board", "Seed sets", "Posts", "Comments", "Latest post", "Last fetched"].map(h => el("th", {}, h)));
  const rows = state.gubaBoards.map(b => el("tr", {
    class: `board-row ${b.board_code === state.gubaBoardFilter ? "active" : ""}`,
    onclick: () => setGubaBoardFilter(b.board_code),
  }, [
    el("td", {}, [
      el("a", {
        href: `https://guba.eastmoney.com/list,${encodeURIComponent(b.board_code)}.html`,
        target: "_blank", rel: "noopener",
        onclick: (e) => e.stopPropagation(),
      }, b.board_code),
    ]),
    el("td", {}, b.seed_sets.join(", ") || "—"),
    el("td", { class: "num" }, String(b.post_count)),
    el("td", { class: "num" }, String(b.comment_count)),
    el("td", { class: "mono" }, fmtDate(b.latest_published_at)),
    el("td", { class: "mono" }, fmtDate(b.latest_fetched_at)),
  ]));
  target.appendChild(el("table", {}, [el("thead", {}, head), el("tbody", {}, rows)]));
}

function renderGubaPosts() {
  const target = document.getElementById("guba-posts");
  target.innerHTML = "";
  const filtered = state.gubaBoardFilter
    ? state.gubaPosts.filter(p => postBoardCode(p) === state.gubaBoardFilter)
    : state.gubaPosts;
  if (state.gubaBoardFilter) {
    target.appendChild(el("div", { class: "filter-chip" }, [
      el("span", {}, `board ${state.gubaBoardFilter} (${filtered.length} of last ${state.gubaPosts.length} fetched)`),
      el("button", { onclick: () => setGubaBoardFilter(state.gubaBoardFilter) }, "clear"),
    ]));
  }
  if (!filtered.length) {
    target.appendChild(el("div", { class: "empty" }, state.gubaBoardFilter
      ? "No posts for this board in the latest fetch window."
      : "No guba posts stored yet."));
    return;
  }
  const head = el("tr", {}, ["Fetched", "Board", "Title", "Likes", "Comments"].map(h => el("th", {}, h)));
  const rows = filtered.slice(0, 15).map(p => el("tr", {}, [
    el("td", { class: "mono" }, fmtDate(p.fetched_at)),
    el("td", { class: "mono" }, postBoardCode(p) || "—"),
    el("td", {}, el("a", { href: p.canonical_url, target: "_blank", rel: "noopener" }, p.title || p.content_preview || "(untitled)")),
    el("td", { class: "num" }, String(p.like_count ?? 0)),
    el("td", { class: "num" }, String(p.comment_count ?? 0)),
  ]));
  target.appendChild(el("table", {}, [el("thead", {}, head), el("tbody", {}, rows)]));
}

async function refreshGuba() {
  const meta = document.getElementById("guba-meta");
  try {
    const [errorsPayload, statusPayload, postsPayload, boardsPayload, nextCrawlPayload] = await Promise.all([
      fetchJSON(`/api/errors?source=guba&limit=${state.gubaLimit}`),
      fetchJSON("/api/status"),
      fetchJSON("/api/posts?source=guba&limit=100"),
      fetchJSON("/api/guba/boards?limit=50"),
      fetchJSON("/api/guba/next-crawl"),
    ]);
    state.gubaPosts = postsPayload.posts;
    state.gubaBoards = boardsPayload.boards;
    renderGubaSummary(errorsPayload.errors, postsPayload.posts);
    renderRunStats(document.querySelector("#guba-latest-run .body"), statusPayload.latest_run);
    renderGubaNextCrawl(nextCrawlPayload);
    renderGubaBoards();
    renderGubaErrorPatterns(errorsPayload.errors);
    renderErrorsTable(document.getElementById("guba-errors"), errorsPayload.errors);
    renderGubaPosts();
    meta.textContent = `updated ${new Date().toLocaleTimeString(undefined, { hour12: false })}`;
  } catch (err) {
    meta.textContent = `error: ${err.message}`;
  }
}

function fmtPercent(value) {
  return value === null || value === undefined ? "n/a" : `${(value * 100).toFixed(1)}%`;
}

function fmtRelative(value) {
  if (!value) return "-";
  const seconds = Math.floor((Date.now() - new Date(value).getTime()) / 1000);
  if (!Number.isFinite(seconds) || seconds < 0) return fmtDate(value);
  if (seconds < 60) return `${seconds}s ago`;
  if (seconds < 3600) return `${Math.floor(seconds / 60)}m ago`;
  if (seconds < 86400) return `${Math.floor(seconds / 3600)}h ago`;
  return `${Math.floor(seconds / 86400)}d ago`;
}

function fmtRemaining(value) {
  if (!value) return "-";
  const seconds = Math.floor((new Date(value).getTime() - Date.now()) / 1000);
  if (!Number.isFinite(seconds)) return fmtDate(value);
  if (seconds <= 0) return "expired";
  if (seconds < 60) return `${seconds}s`;
  if (seconds < 3600) return `${Math.floor(seconds / 60)}m ${seconds % 60}s`;
  return `${Math.floor(seconds / 3600)}h ${Math.floor((seconds % 3600) / 60)}m`;
}

const SITE_LABELS = {
  guba: "Guba",
  tgb: "TGB",
  jiuyan: "Jiuyan",
  hupu: "Hupu",
  bilibili: "Bilibili",
  xueqiu: "Xueqiu",
  unknown: "Unknown",
};

function siteForSource(source) {
  return source === "guba" || source.startsWith("guba_") ? "guba" : source;
}

function siteLabel(source) {
  const site = siteForSource(source);
  return SITE_LABELS[site] || site;
}

function isOldExperimentSource(source) {
  return source.startsWith("guba_ab_");
}

function aggregateBySite(sources, fields) {
  const groups = new Map();
  for (const source of sources.filter(item => !isOldExperimentSource(item.source))) {
    const site = siteForSource(source.source);
    if (!groups.has(site)) {
      groups.set(site, { site, last_activity_at: null });
      for (const field of fields) groups.get(site)[field] = 0;
    }
    const group = groups.get(site);
    for (const field of fields) group[field] += Number(source[field] || 0);
    if (!group.last_activity_at || new Date(source.last_activity_at) > new Date(group.last_activity_at)) {
      group.last_activity_at = source.last_activity_at;
    }
  }
  return [...groups.values()].sort((a, b) => {
    const aTotal = fields.reduce((sum, field) => sum + a[field], 0);
    const bTotal = fields.reduce((sum, field) => sum + b[field], 0);
    return bTotal - aTotal;
  });
}

function renderProxySummary(payload) {
  const target = document.getElementById("proxy-summary");
  target.innerHTML = "";
  const sources = aggregateBySite(payload.sources, ["successes", "failures"]);
  const successes = sources.reduce((sum, source) => sum + source.successes, 0);
  const failures = sources.reduce((sum, source) => sum + source.failures, 0);
  const successRate = successes + failures ? successes / (successes + failures) : null;
  const health = payload.active_nodes === 0
    ? "idle"
    : payload.benched_nodes > 0 || (successRate !== null && successRate < 0.9)
      ? "degraded"
      : "healthy";
  target.appendChild(el("div", { class: "stats" }, [
    el("div", { class: "stat stat-primary" }, [el("div", { class: "label" }, "Status"), el("div", { class: `value pool-health ${health}` }, health)]),
    el("div", { class: "stat" }, [el("div", { class: "label" }, "Usable IPs"), el("div", { class: "value status-ok" }, String(payload.active_nodes))]),
    el("div", { class: "stat" }, [el("div", { class: "label" }, "Benched"), el("div", { class: "value status-running" }, String(payload.benched_nodes))]),
    el("div", { class: "stat" }, [el("div", { class: "label" }, "Success rate"), el("div", { class: "value" }, fmtPercent(successRate))]),
    el("div", { class: "stat" }, [el("div", { class: "label" }, "Last success"), el("div", { class: "value value-time" }, fmtRelative(payload.last_success_at))]),
    el("div", { class: "stat" }, [el("div", { class: "label" }, "Last issue"), el("div", { class: "value value-time" }, fmtRelative(payload.last_failure_at))]),
  ]));
}

function renderProxyEconomics(payload) {
  const target = document.getElementById("proxy-economics");
  target.innerHTML = "";
  const sources = aggregateBySite(payload.sources, ["successes", "failures", "extracted", "pool_empty_events"]);
  const requests = sources.reduce((sum, source) => sum + source.successes + source.failures, 0);
  const extracted = sources.reduce((sum, source) => sum + source.extracted, 0);
  const poolEmpty = sources.reduce((sum, source) => sum + source.pool_empty_events, 0);
  target.appendChild(el("div", { class: "stats" }, [
    el("div", { class: "stat stat-primary" }, [el("div", { class: "label" }, "Requests"), el("div", { class: "value" }, String(requests))]),
    el("div", { class: "stat" }, [el("div", { class: "label" }, "IPs extracted"), el("div", { class: "value" }, String(extracted))]),
    el("div", { class: "stat" }, [el("div", { class: "label" }, "Requests / IP"), el("div", { class: "value" }, extracted ? (requests / extracted).toFixed(1) : "n/a")]),
    el("div", { class: "stat" }, [el("div", { class: "label" }, "Pool empty"), el("div", { class: "value" }, String(poolEmpty))]),
    el("div", { class: "stat" }, [el("div", { class: "label" }, "API errors"), el("div", { class: "value" }, String(payload.api_errors))]),
    el("div", { class: "stat" }, [el("div", { class: "label" }, "Last extraction"), el("div", { class: "value value-time" }, fmtRelative(payload.last_batch_at))]),
  ]));
}

function renderProxySources(payload) {
  const target = document.getElementById("proxy-sources");
  target.innerHTML = "";
  const sources = aggregateBySite(payload.sources, ["successes", "failures", "leases", "extracted", "pool_empty_events"]);
  if (!sources.length) {
    target.appendChild(el("div", { class: "empty" }, "No source-specific proxy activity in this window."));
    return;
  }
  const head = el("tr", {}, ["Website", "Requests", "Success", "Failed", "Rate", "IPs", "Pool empty", "Last activity"].map(h => el("th", {}, h)));
  const rows = sources.map(source => el("tr", {}, [
    el("td", { class: "source-name" }, siteLabel(source.site)),
    el("td", { class: "num" }, String(source.successes + source.failures)),
    el("td", { class: "num status-ok" }, String(source.successes)),
    el("td", { class: "num status-failed" }, String(source.failures)),
    el("td", { class: "num" }, fmtPercent(source.successes + source.failures ? source.successes / (source.successes + source.failures) : null)),
    el("td", { class: "num" }, String(source.extracted)),
    el("td", { class: "num" }, String(source.pool_empty_events)),
    el("td", { class: "mono" }, fmtRelative(source.last_activity_at)),
  ]));
  target.appendChild(el("table", {}, [el("thead", {}, head), el("tbody", {}, rows)]));
}

function renderProxyTrend(payload) {
  const target = document.getElementById("proxy-trend");
  target.innerHTML = "";
  if (!payload.trend.length) {
    target.appendChild(el("div", { class: "empty" }, "No proxy activity in this window."));
    return;
  }
  const maxValue = Math.max(1, ...payload.trend.map(p => p.successes + p.failures));
  const chart = el("div", { class: "proxy-chart" });
  for (const point of payload.trend) {
    const successWidth = `${(point.successes / maxValue) * 100}%`;
    const failureWidth = `${(point.failures / maxValue) * 100}%`;
    chart.appendChild(el("div", { class: "proxy-chart-row" }, [
      el("div", { class: "proxy-chart-time" }, fmtDate(point.hour)),
      el("div", { class: "proxy-bars" }, [
        el("div", { class: "proxy-bar success", style: `width:${successWidth}`, title: `successes ${point.successes}` }),
        el("div", { class: "proxy-bar failure", style: `width:${failureWidth}`, title: `failures ${point.failures}` }),
      ]),
      el("div", { class: "proxy-chart-values" }, `${point.successes + point.failures} requests | ${point.extracted} IPs | ${point.failures} failed`),
    ]));
  }
  target.appendChild(chart);
}

function renderProxyNodes(payload) {
  const target = document.getElementById("proxy-nodes");
  target.innerHTML = "";
  if (!payload.nodes.length) {
    target.appendChild(el("div", { class: "empty" }, "No proxy nodes recorded yet."));
    return;
  }
  const head = el("tr", {}, ["Node", "Status", "Time left", "Requests", "Rate", "Last used", "Last issue"].map(h => el("th", {}, h)));
  const rows = payload.nodes.map(node => el("tr", {}, [
    el("td", { class: "mono" }, node.proxy_id),
    el("td", { class: `proxy-status ${node.status}` }, node.status),
    el("td", { class: "mono" }, fmtRemaining(node.expires_at)),
    el("td", { class: "num" }, String(node.acquire_count)),
    el("td", { class: "num" }, fmtPercent(node.success_rate)),
    el("td", { class: "mono" }, fmtRelative(node.last_acquired_at)),
    el("td", { class: "cell-detail", title: node.last_failure_reason || "" }, node.last_failure_reason || "-"),
  ]));
  target.appendChild(el("table", {}, [el("thead", {}, head), el("tbody", {}, rows)]));
}

function renderProxyEvents(payload) {
  const target = document.getElementById("proxy-events");
  target.innerHTML = "";
  const issues = payload.events.filter(event =>
    !isOldExperimentSource(event.source) &&
    ["proxy_benched", "request_failure", "api_error", "pool_empty"].includes(event.event_type)
  ).slice(0, 20);
  if (!issues.length) {
    target.appendChild(el("div", { class: "empty status-ok" }, "No recent proxy issues."));
    return;
  }
  const eventLabels = { proxy_benched: "benched", request_failure: "request failed", api_error: "API error", pool_empty: "pool empty" };
  const head = el("tr", {}, ["When", "Website", "Issue", "Node", "Count", "Detail"].map(h => el("th", {}, h)));
  const rows = issues.map(event => el("tr", {}, [
    el("td", { class: "mono" }, fmtRelative(event.occurred_at)),
    el("td", { class: "source-name" }, siteLabel(event.source)),
    el("td", { class: "status-failed" }, eventLabels[event.event_type] || event.event_type),
    el("td", { class: "mono" }, event.proxy_id || "-"),
    el("td", { class: "num" }, String(event.count)),
    el("td", { class: "cell-detail" }, Object.entries(event.detail || {}).map(([key, value]) => `${key}=${value}`).join(", ") || "-"),
  ]));
  target.appendChild(el("table", {}, [el("thead", {}, head), el("tbody", {}, rows)]));
}

async function refreshProxyPool() {
  const meta = document.getElementById("proxy-meta");
  try {
    const payload = await fetchJSON(`/api/proxy-pool?hours=${state.proxyHours}`);
    renderProxySummary(payload);
    renderProxyEconomics(payload);
    renderProxySources(payload);
    renderProxyTrend(payload);
    renderProxyNodes(payload);
    renderProxyEvents(payload);
    meta.textContent = `${state.proxyHours}h window | updated ${new Date().toLocaleTimeString(undefined, { hour12: false })}`;
  } catch (err) {
    meta.textContent = `error: ${err.message}`;
  }
}

function renderAgentSummary(payload) {
  const target = document.getElementById("agents-summary");
  target.innerHTML = "";
  const health = !payload.enabled
    ? "disabled"
    : payload.online_nodes === 0
      ? "offline"
      : payload.benched_nodes > 0 ? "degraded" : "healthy";
  const lastSeen = payload.nodes.reduce(
    (latest, node) => !latest || new Date(node.last_seen_at) > new Date(latest) ? node.last_seen_at : latest,
    null,
  );
  target.appendChild(el("div", { class: "stats" }, [
    el("div", { class: "stat stat-primary" }, [el("div", { class: "label" }, "Status"), el("div", { class: `value pool-health ${health}` }, health)]),
    el("div", { class: "stat" }, [el("div", { class: "label" }, "Online nodes"), el("div", { class: "value status-ok" }, `${payload.online_nodes} / ${payload.nodes.length}`)]),
    el("div", { class: "stat" }, [el("div", { class: "label" }, "Benched"), el("div", { class: "value status-running" }, String(payload.benched_nodes))]),
    el("div", { class: "stat" }, [el("div", { class: "label" }, "Free agent slots"), el("div", { class: "value" }, `${payload.online_capacity} / ${payload.agent_slot_limit}`)]),
    el("div", { class: "stat" }, [el("div", { class: "label" }, "Paid fallback"), el("div", { class: "value" }, String(payload.paid_slots))]),
    el("div", { class: "stat" }, [el("div", { class: "label" }, "Total available"), el("div", { class: "value" }, String(payload.combined_capacity))]),
    el("div", { class: "stat" }, [el("div", { class: "label" }, "Last heartbeat"), el("div", { class: "value value-time" }, fmtRelative(lastSeen))]),
  ]));
}

function renderAgentJobSummary(payload) {
  const target = document.getElementById("agents-jobs-summary");
  target.innerHTML = "";
  target.appendChild(el("div", { class: "stats" }, [
    el("div", { class: "stat stat-primary" }, [el("div", { class: "label" }, "Active"), el("div", { class: "value" }, String(payload.queued_jobs + payload.leased_jobs))]),
    el("div", { class: "stat" }, [el("div", { class: "label" }, "Queued"), el("div", { class: "value" }, String(payload.queued_jobs))]),
    el("div", { class: "stat" }, [el("div", { class: "label" }, "In flight"), el("div", { class: "value" }, String(payload.leased_jobs))]),
    el("div", { class: "stat" }, [el("div", { class: "label" }, "Completed"), el("div", { class: "value status-ok" }, String(payload.completed_jobs))]),
    el("div", { class: "stat" }, [el("div", { class: "label" }, "Failed"), el("div", { class: "value status-failed" }, String(payload.failed_jobs))]),
    el("div", { class: "stat" }, [el("div", { class: "label" }, "Unclaimed"), el("div", { class: "value status-running" }, String(payload.cancelled_jobs))]),
  ]));
}

function renderAgentSources(payload) {
  const target = document.getElementById("agents-sources");
  target.innerHTML = "";
  const sources = aggregateBySite(payload.sources, ["queued_jobs", "leased_jobs", "completed_jobs", "failed_jobs", "cancelled_jobs", "successes", "failures", "blocked"]);
  if (!sources.length) {
    target.appendChild(el("div", { class: "empty" }, "No source-specific agent activity recorded yet."));
    return;
  }
  const head = el("tr", {}, ["Website", "Jobs", "Success", "Blocked", "Failed", "Unclaimed", "Rate", "Last activity"].map(h => el("th", {}, h)));
  const rows = sources.map(source => el("tr", {}, [
    el("td", { class: "source-name" }, siteLabel(source.site)),
    el("td", { class: "num" }, String(source.queued_jobs + source.leased_jobs + source.completed_jobs + source.failed_jobs + source.cancelled_jobs)),
    el("td", { class: "num status-ok" }, String(source.successes)),
    el("td", { class: "num status-running" }, String(source.blocked)),
    el("td", { class: "num status-failed" }, String(Math.max(0, source.failures - source.blocked) + source.failed_jobs)),
    el("td", { class: "num" }, String(source.cancelled_jobs)),
    el("td", { class: "num" }, fmtPercent(source.successes + source.failures ? source.successes / (source.successes + source.failures) : null)),
    el("td", { class: "mono" }, fmtRelative(source.last_activity_at)),
  ]));
  target.appendChild(el("table", {}, [el("thead", {}, head), el("tbody", {}, rows)]));
}

function renderAgentNodes(payload) {
  const target = document.getElementById("agents-nodes");
  target.innerHTML = "";
  if (!payload.nodes.length) {
    target.appendChild(el("div", { class: "empty" }, "No self-hosted agents have connected yet."));
    return;
  }
  const leasesByAgent = new Map();
  for (const job of payload.jobs.filter(job => job.status === "leased" && job.leased_by)) {
    leasesByAgent.set(job.leased_by, (leasesByAgent.get(job.leased_by) || 0) + 1);
  }
  const head = el("tr", {}, ["Agent", "Status", "Public IP", "Build", "In use", "Last seen", "Last success", "Last issue"].map(h => el("th", {}, h)));
  const rows = payload.nodes.map(node => {
    const usedSlots = leasesByAgent.get(node.agent_id) || 0;
    return el("tr", {}, [
      el("td", { class: "source-name" }, node.agent_id),
      el("td", { class: `agent-status ${node.status}` }, node.status),
      el("td", { class: "mono" }, node.last_ip_address || "-"),
      el("td", { class: "mono" }, `${node.version} | ${node.os}/${node.arch}`),
      el("td", { class: "num" }, `${usedSlots} / ${node.max_concurrency}`),
      el("td", { class: "mono" }, fmtRelative(node.last_seen_at)),
      el("td", { class: "mono" }, fmtRelative(node.last_success_at)),
      el("td", { class: "cell-detail", title: node.last_failure_reason || "" }, node.last_failure_at ? `${fmtRelative(node.last_failure_at)} | ${node.last_failure_reason || "failure"}` : "-"),
    ]);
  });
  target.appendChild(el("table", {}, [el("thead", {}, head), el("tbody", {}, rows)]));
}

function renderAgentJobs(payload) {
  const target = document.getElementById("agents-jobs");
  target.innerHTML = "";
  const jobs = payload.jobs.slice(0, 20);
  if (!jobs.length) {
    target.appendChild(el("div", { class: "empty" }, "No agent work in this window."));
    return;
  }
  const head = el("tr", {}, ["Created", "Website", "Status", "Agent", "HTTP", "Duration", "Detail"].map(h => el("th", {}, h)));
  const rows = jobs.map(job => el("tr", {}, [
    el("td", { class: "mono" }, fmtRelative(job.created_at)),
    el("td", { class: "source-name" }, siteLabel(job.source)),
    el("td", { class: `agent-status ${job.status}` }, job.status),
    el("td", { class: "mono" }, job.leased_by || "-"),
    el("td", { class: "num" }, job.response_status === null ? "-" : String(job.response_status)),
    el("td", { class: "num" }, job.duration_ms === null ? "-" : `${job.duration_ms} ms`),
    el("td", { class: job.error_message ? "cell-detail err" : "cell-detail", title: job.error_message || "" }, job.error_message || job.outcome || "-"),
  ]));
  target.appendChild(el("table", {}, [el("thead", {}, head), el("tbody", {}, rows)]));
}

async function refreshAgentPool() {
  const meta = document.getElementById("agents-meta");
  try {
    const payload = await fetchJSON(`/api/agent-pool?hours=${state.agentsHours}`);
    state.agents = payload;
    renderAgentSummary(payload);
    renderAgentJobSummary(payload);
    renderAgentSources(payload);
    renderAgentNodes(payload);
    renderAgentJobs(payload);
    meta.textContent = `${state.agentsHours}h window | updated ${new Date().toLocaleTimeString(undefined, { hour12: false })}`;
  } catch (err) {
    meta.textContent = `error: ${err.message}`;
  }
}

function switchTab(tab) {
  state.activeTab = tab;
  for (const b of document.querySelectorAll(".tab")) b.classList.toggle("active", b.dataset.tab === tab);
  for (const v of document.querySelectorAll(".view")) v.classList.toggle("active", v.id === `${tab}-view`);
  if (tab === "posts") refreshPosts();
  if (tab === "guba") refreshGuba();
  if (tab === "proxy") refreshProxyPool();
  if (tab === "agents") refreshAgentPool();
}

function wireEvents() {
  for (const btn of document.querySelectorAll(".tab")) {
    btn.addEventListener("click", () => switchTab(btn.dataset.tab));
  }
  document.getElementById("posts-source").addEventListener("change", (e) => {
    state.postsSource = e.target.value;
    state.postsOffset = 0;
    refreshPosts();
  });
  document.getElementById("posts-limit").addEventListener("change", (e) => {
    state.postsLimit = Number(e.target.value);
    state.postsOffset = 0;
    refreshPosts();
  });
  document.getElementById("posts-refresh").addEventListener("click", refreshPosts);
  document.getElementById("posts-prev").addEventListener("click", () => {
    state.postsOffset = Math.max(0, state.postsOffset - state.postsLimit);
    refreshPosts();
  });
  document.getElementById("posts-next").addEventListener("click", () => {
    state.postsOffset += state.postsLimit;
    refreshPosts();
  });
  document.getElementById("guba-limit").addEventListener("change", (e) => {
    state.gubaLimit = Number(e.target.value);
    refreshGuba();
  });
  document.getElementById("guba-refresh").addEventListener("click", refreshGuba);
  document.getElementById("proxy-hours").addEventListener("change", (e) => {
    state.proxyHours = Number(e.target.value);
    refreshProxyPool();
  });
  document.getElementById("proxy-refresh").addEventListener("click", refreshProxyPool);
  document.getElementById("agents-hours").addEventListener("change", (e) => {
    state.agentsHours = Number(e.target.value);
    refreshAgentPool();
  });
  document.getElementById("agents-refresh").addEventListener("click", refreshAgentPool);
}

function start() {
  wireEvents();
  refreshStatus();
  state.statusTimer = setInterval(() => {
    refreshStatus();
    if (state.activeTab === "guba") refreshGuba();
    if (state.activeTab === "proxy") refreshProxyPool();
    if (state.activeTab === "agents") refreshAgentPool();
  }, POLL_INTERVAL_MS);
}

document.addEventListener("DOMContentLoaded", start);
