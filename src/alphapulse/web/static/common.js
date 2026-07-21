// Small shared DOM/format helpers used by the report page.

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
  if (!res.ok) {
    let detail = `${res.status}`;
    try { detail = (await res.json()).detail || detail; } catch { /* ignore */ }
    throw new Error(detail);
  }
  return res.json();
}

function fmtDateTime(value) {
  if (!value) return "—";
  try { return new Date(value).toLocaleString(undefined, { hour12: false }); } catch { return value; }
}

function fmtTime(value) {
  if (!value) return "";
  try { return new Date(value).toLocaleTimeString(undefined, { hour12: false, hour: "2-digit", minute: "2-digit" }); } catch { return value; }
}

function fmtNum(value) {
  const n = Number(value || 0);
  if (n >= 10000) return `${(n / 10000).toFixed(1)}万`;
  return String(n);
}

// Build a nested comment tree keyed by parent_comment_entity_id (guba threads).
function renderCommentTree(comments) {
  const byParent = new Map();
  for (const c of comments) {
    const parent = c.parent_comment_entity_id || null;
    if (!byParent.has(parent)) byParent.set(parent, []);
    byParent.get(parent).push(c);
  }
  const out = el("div", { class: "comment-tree" });
  const commentNode = (c, depth) => el("div", { class: `comment ${depth ? "reply" : ""}` }, [
    el("div", { class: "chead" }, [
      el("span", { class: "cauthor" }, c.author_entity_id ? `@${c.author_entity_id}` : "@unknown"),
      el("span", { class: "ctime" }, fmtDateTime(c.published_at || c.fetched_at)),
      el("span", { class: "clike" }, `♥ ${fmtNum(c.like_count)}`),
    ]),
    el("div", { class: "cbody" }, c.content_text || "(empty)"),
  ]);
  const walk = (parent, depth) => {
    for (const c of byParent.get(parent) || []) {
      out.appendChild(commentNode(c, depth));
      walk(c.source_entity_id, depth + 1);
    }
  };
  walk(null, 0);
  // Surface orphans (replies whose parent we never fetched) at top level.
  const known = new Set(comments.map(c => c.source_entity_id));
  for (const [parent, list] of byParent.entries()) {
    if (parent === null || known.has(parent)) continue;
    for (const c of list) out.appendChild(commentNode(c, 0));
  }
  return out;
}
