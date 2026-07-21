// Per-day guba "newspaper" report. Fetches /api/guba/report/{date} and renders
// ranking sections → boards → the day's posts, with lazily-loaded comment threads.

const REPORT_TZ = "Asia/Shanghai";

function beijingToday() {
  // en-CA locale yields YYYY-MM-DD.
  return new Date().toLocaleDateString("en-CA", { timeZone: REPORT_TZ });
}

function dateFromPath() {
  const m = location.pathname.match(/\/report\/(\d{4}-\d{2}-\d{2})/);
  return m ? m[1] : beijingToday();
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

let state = { day: dateFromPath(), commentCache: new Map() };

function navigate(day) {
  state.day = day;
  history.pushState({ day }, "", `/report/${day}`);
  loadReport();
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

  if ((post.comment_count || 0) > 0) {
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
    const payload = await fetchJSON(`/api/posts/guba/${encodeURIComponent(post.source_entity_id)}`);
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
    const data = await fetchJSON(`/api/guba/report/${state.day}`);
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
  window.addEventListener("popstate", () => { state.day = dateFromPath(); loadReport(); });
  loadReport();
}

init();
