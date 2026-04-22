const POLL_INTERVAL_MS = 5000;

const state = {
  activeTab: "status",
  posts: [],
  postsSource: "",
  postsLimit: 50,
  postsOffset: 0,
  selectedPostKey: null,
  statusTimer: null,
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
  try { return new Date(value).toLocaleString(); } catch { return value; }
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
  document.getElementById("last-updated").textContent = `updated ${new Date().toLocaleTimeString()}`;
}

function renderLatestRun(run) {
  const target = document.querySelector("#latest-run .body");
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

function renderErrorsTable(target, errors) {
  target.innerHTML = "";
  if (!errors.length) { target.appendChild(el("div", { class: "empty" }, "No errors.")); return; }
  const head = el("tr", {}, ["When", "Source", "URL", "Message"].map(h => el("th", {}, h)));
  const rows = errors.map(e => el("tr", {}, [
    el("td", { class: "mono" }, fmtDate(e.created_at)),
    el("td", {}, e.source),
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

function switchTab(tab) {
  state.activeTab = tab;
  for (const b of document.querySelectorAll(".tab")) b.classList.toggle("active", b.dataset.tab === tab);
  for (const v of document.querySelectorAll(".view")) v.classList.toggle("active", v.id === `${tab}-view`);
  if (tab === "posts") refreshPosts();
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
}

function start() {
  wireEvents();
  refreshStatus();
  state.statusTimer = setInterval(refreshStatus, POLL_INTERVAL_MS);
}

document.addEventListener("DOMContentLoaded", start);
