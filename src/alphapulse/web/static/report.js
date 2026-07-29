// Per-day forum "newspaper" report. Fetches /api/{source}/report/{date} and renders
// ranking sections → boards → the day's posts, with lazily-loaded comment threads.
// `source` is guba, tgb, or jiuyan, read from the /report/{source}/{date} path.

const REPORT_TZ = "Asia/Shanghai";

const SOURCE_META = {
  guba: {
    label: "股吧",
    edition: "东方财富 · 股吧",
    title: "股 吧 日 报",
    subtitle: "Guba Daily · 热门个股吧 · 热门概念吧 · 热门主题吧",
    colophon: "Generated from crawled guba posts. Rankings are a snapshot of the day's hot boards.",
    comments: true,
  },
  tgb: {
    label: "淘股吧",
    edition: "淘股吧 · TaoGuBa",
    title: "淘 股 吧 日 报",
    subtitle: "TaoGuBa Daily · 精华 · 热门个股 · 综合",
    colophon: "Generated from crawled tgb.cn posts. Featured (精华) and general boards, snapshotted daily.",
    comments: true,
  },
  jiuyan: {
    label: "韭研公社",
    edition: "韭研公社 · Jiuyan Gongshe",
    title: "韭研公社日报",
    subtitle: "Jiuyan Daily · 固定指数 · 公社热门搜索",
    colophon: "Generated from Jiuyan Gongshe search targets. Fixed indices and hot searches are snapshotted daily.",
    comments: false,
  },
};
const DEFAULT_SOURCE = "guba";

function beijingToday() {
  // en-CA locale yields YYYY-MM-DD.
  return new Date().toLocaleDateString("en-CA", { timeZone: REPORT_TZ });
}

function pathParts() {
  // /report/{source}/{date}, /report/{source} (today), or the legacy /report/{date}.
  const withDate = location.pathname.match(/\/report\/([a-z]+)\/(\d{4}-\d{2}-\d{2})/);
  if (withDate && SOURCE_META[withDate[1]]) return { source: withDate[1], day: withDate[2] };
  const sourceOnly = location.pathname.match(/\/report\/([a-z]+)\/?$/);
  if (sourceOnly && SOURCE_META[sourceOnly[1]]) return { source: sourceOnly[1], day: beijingToday() };
  const dateOnly = location.pathname.match(/\/report\/(\d{4}-\d{2}-\d{2})/);
  return { source: DEFAULT_SOURCE, day: dateOnly ? dateOnly[1] : beijingToday() };
}

function shiftDay(day, delta) {
  const d = new Date(`${day}T00:00:00Z`);
  d.setUTCDate(d.getUTCDate() + delta);
  return d.toISOString().slice(0, 10);
}

function weekday(day) {
  try {
    return new Date(`${day}T00:00:00`).toLocaleDateString(undefined, { weekday: "long" });
  } catch { return ""; }
}

let state = { ...pathParts(), commentCache: new Map() };

function navigate(day, source = state.source) {
  state.day = day;
  state.source = source;
  history.pushState({ day, source }, "", `/report/${source}/${day}`);
  applySourceChrome();
  loadReport();
}

function applySourceChrome() {
  const meta = SOURCE_META[state.source] || SOURCE_META[DEFAULT_SOURCE];
  document.getElementById("edition").textContent = meta.edition;
  document.getElementById("masthead-title").textContent = meta.title;
  document.getElementById("subtitle").textContent = meta.subtitle;
  document.getElementById("colophon").textContent = meta.colophon;
  document.title = `${meta.label}日报`;

  const nav = document.getElementById("source-switch");
  nav.innerHTML = "";
  for (const key of Object.keys(SOURCE_META)) {
    const link = el("a", {
      class: `source-tab ${key === state.source ? "active" : ""}`,
      href: `/report/${key}/${state.day}`,
      onclick: (e) => { e.preventDefault(); navigate(state.day, key); },
    }, SOURCE_META[key].label);
    nav.appendChild(link);
  }
}

function postItem(post) {
  const meta = el("div", { class: "post-meta" }, [
    el("span", { class: "ptime" }, fmtTime(post.published_at)),
    post.author_entity_id ? el("span", { class: "pauthor" }, `@${post.author_entity_id}`) : null,
    el("span", { class: "plike" }, `♥ ${fmtNum(post.like_count)}`),
    el("span", { class: "pcmt" }, `💬 ${fmtNum(post.comment_count)}`),
  ]);
  const title = el("a", {
    class: "post-title",
    href: post.canonical_url,
    target: "_blank",
    rel: "noopener",
  }, post.title || post.content_preview || "(无标题)");

  const children = [title, meta];
  if (post.content_preview && post.title) {
    children.push(el("div", { class: "post-preview" }, post.content_preview));
  }

  const sourceMeta = SOURCE_META[state.source] || SOURCE_META[DEFAULT_SOURCE];
  if (sourceMeta.comments && (post.comment_count || 0) > 0) {
    const commentsBox = el("div", { class: "post-comments", hidden: "hidden" });
    const toggle = el("button", {
      class: "comments-toggle",
      type: "button",
      onclick: () => toggleComments(post, commentsBox, toggle),
    }, `展开评论 (${fmtNum(post.comment_count)})`);
    children.push(toggle, commentsBox);
  }
  return el("article", { class: "post" }, children);
}

async function toggleComments(post, box, toggle) {
  if (box.dataset.loaded === "1") {
    const showing = box.hasAttribute("hidden") ? false : true;
    if (showing) { box.setAttribute("hidden", "hidden"); toggle.textContent = `展开评论 (${fmtNum(post.comment_count)})`; }
    else { box.removeAttribute("hidden"); toggle.textContent = "收起评论"; }
    return;
  }
  toggle.disabled = true;
  toggle.textContent = "加载中…";
  try {
    const payload = await fetchJSON(`/api/posts/${state.source}/${encodeURIComponent(post.source_entity_id)}`);
    box.innerHTML = "";
    box.appendChild(
      payload.comments.length
        ? renderCommentTree(payload.comments)
        : el("div", { class: "no-comments" }, "无评论。")
    );
    box.dataset.loaded = "1";
    box.removeAttribute("hidden");
    toggle.textContent = "收起评论";
  } catch (err) {
    box.innerHTML = "";
    box.appendChild(el("div", { class: "no-comments err" }, `加载失败：${err.message}`));
    box.dataset.loaded = "1";
    box.removeAttribute("hidden");
    toggle.textContent = "收起评论";
  } finally {
    toggle.disabled = false;
  }
}

function boardCard(board, { sub = false } = {}) {
  const head = el("div", { class: "board-head" }, [
    board.rank != null ? el("span", { class: "rank" }, `#${board.rank}`) : null,
    el("a", { class: "board-name", href: board.url, target: "_blank", rel: "noopener" },
      board.name || board.code),
    el("span", { class: "board-code" }, board.code),
    el("span", { class: "board-stat" }, `${fmtNum(board.post_count)} 帖 · ${fmtNum(board.comment_count)} 评`),
  ]);
  const body = board.posts && board.posts.length
    ? el("div", { class: "post-list" }, board.posts.map(postItem))
    : el("div", { class: "empty-board" }, "今日暂无新帖。");
  return el("section", { class: `board ${sub ? "board-sub" : ""}` }, [head, body]);
}

function themeCard(theme) {
  const head = el("div", { class: "board-head theme-head" }, [
    theme.rank != null ? el("span", { class: "rank" }, `#${theme.rank}`) : null,
    el("a", { class: "board-name", href: theme.url, target: "_blank", rel: "noopener" },
      `#${theme.name}#`),
    el("span", { class: "board-stat" }, `${fmtNum(theme.post_count)} 帖 · ${fmtNum(theme.comment_count)} 评`),
  ]);
  const members = theme.members && theme.members.length
    ? theme.members.map((m) => boardCard(m, { sub: true }))
    : [el("div", { class: "empty-board" }, "成员板块今日暂无新帖。")];
  return el("section", { class: "theme" }, [head, ...members]);
}

function renderReport(data) {
  const main = document.getElementById("report");
  main.innerHTML = "";

  document.getElementById("dateline").textContent =
    `${data.day}　${weekday(data.day)}　(${data.timezone})`;

  const summary = document.getElementById("summary");
  summary.textContent = `${fmtNum(data.total_posts)} 帖 · ${fmtNum(data.total_comments)} 评`
    + (data.has_snapshot ? "" : " · 无榜单快照（按板块聚合）");

  if (!data.sections.length || data.total_posts === 0) {
    main.appendChild(el("p", { class: "loading" }, "该日暂无已抓取的帖子。"));
    return;
  }

  for (const section of data.sections) {
    const secEl = el("div", { class: "section" }, [
      el("h2", { class: "section-title" }, section.title),
    ]);
    const grid = el("div", { class: "section-grid" });
    for (const entry of section.entries) {
      grid.appendChild(entry.kind === "theme" ? themeCard(entry) : boardCard(entry));
    }
    secEl.appendChild(grid);
    main.appendChild(secEl);
  }
}

async function loadReport() {
  const picker = document.getElementById("day-picker");
  picker.value = state.day;
  const main = document.getElementById("report");
  main.innerHTML = '<p class="loading">Loading…</p>';
  try {
    const data = await fetchJSON(`/api/${state.source}/report/${state.day}`);
    renderReport(data);
  } catch (err) {
    main.innerHTML = "";
    main.appendChild(el("p", { class: "loading err" }, `加载失败：${err.message}`));
  }
}

function init() {
  document.getElementById("prev-day").addEventListener("click", () => navigate(shiftDay(state.day, -1)));
  document.getElementById("next-day").addEventListener("click", () => navigate(shiftDay(state.day, 1)));
  document.getElementById("day-picker").addEventListener("change", (e) => {
    if (e.target.value) navigate(e.target.value);
  });
  window.addEventListener("popstate", () => {
    Object.assign(state, pathParts());
    applySourceChrome();
    loadReport();
  });
  applySourceChrome();
  loadReport();
}

init();
