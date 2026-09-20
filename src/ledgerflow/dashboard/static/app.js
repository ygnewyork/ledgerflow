/* LedgerFlow dashboard.
 *
 * Vanilla JS, inline SVG, no build step. It calls the same public v1 API any
 * customer would -- there is no privileged dashboard endpoint, which keeps the
 * API honest: if something is awkward to render here, it is awkward for every
 * integrator too.
 */

const KEY_STORAGE = "ledgerflow.api_key";
const THEME_STORAGE = "ledgerflow.theme";

const state = {
  key: null,
  accounts: [],
  accountId: null,
  account: null,
  entries: [],
  series: [],
  transactions: [],
  categoryFilter: null,   // set by clicking a bar in Spend by category
};

// Deposits and transfers carry no merchant descriptor, so the feed names them
// by what they are. "card_purchase" is a posting rule, not something a person
// recognizes on their own statement.
const KIND_LABELS = {
  deposit: "Deposit",
  transfer: "Transfer",
  card_purchase: "Purchase",
  refund: "Refund",
  fee: "Fee",
  "card_purchase.reversal": "Reversal",
};

const $ = (id) => document.getElementById(id);
const tooltip = $("tooltip");

/* ---------- formatting ---------- */

const money = (minor, currency = "usd") =>
  new Intl.NumberFormat(undefined, { style: "currency", currency: currency.toUpperCase() })
    .format((minor || 0) / 100);

const compactMoney = (minor) => {
  const v = (minor || 0) / 100;
  if (Math.abs(v) >= 1000) return "$" + (v / 1000).toFixed(1) + "k";
  return "$" + v.toFixed(0);
};

const when = (seconds) =>
  new Date(seconds * 1000).toLocaleString(undefined, {
    month: "short", day: "numeric", hour: "2-digit", minute: "2-digit",
  });

const dayLabel = (seconds) =>
  new Date(seconds * 1000).toLocaleDateString(undefined, { month: "short", day: "numeric" });

/* ---------- api ---------- */

async function api(path) {
  const res = await fetch(path, { headers: { Authorization: `Bearer ${state.key}` } });
  if (res.status === 401) {
    localStorage.removeItem(KEY_STORAGE);
    askForKey();
    throw new Error("unauthorized");
  }
  const body = await res.json();
  if (!res.ok) throw new Error(body?.error?.message || `HTTP ${res.status}`);
  return body;
}

/* ---------- svg helpers ---------- */

const NS = "http://www.w3.org/2000/svg";
function el(name, attrs = {}, text) {
  const node = document.createElementNS(NS, name);
  for (const [k, v] of Object.entries(attrs)) node.setAttribute(k, v);
  if (text !== undefined) node.textContent = text;
  return node;
}

function showTip(evt, html) {
  tooltip.innerHTML = html;
  tooltip.classList.add("on");
  const pad = 14;
  const rect = tooltip.getBoundingClientRect();
  let x = evt.clientX + pad;
  let y = evt.clientY + pad;
  if (x + rect.width > window.innerWidth - 8) x = evt.clientX - rect.width - pad;
  if (y + rect.height > window.innerHeight - 8) y = evt.clientY - rect.height - pad;
  tooltip.style.left = `${Math.max(8, x)}px`;
  tooltip.style.top = `${Math.max(8, y)}px`;
}
const hideTip = () => tooltip.classList.remove("on");

/* ---------- health tiles ---------- */
/* Stat tiles, not charts: each of these is a single number, and a chart of one
 * number is the most common way a dashboard misses its own point. */

function statusFor(value, warnAt, criticalAt) {
  if (value >= criticalAt) return "critical";
  if (value >= warnAt) return "warning";
  return "good";
}

function renderHealth(h) {
  const lag = h.consumer_lag || {};
  const worstLag = Math.max(0, ...Object.values(lag));
  const worstGroup = Object.entries(lag).sort((a, b) => b[1] - a[1])[0];
  const wh = h.webhooks_24h || {};
  const attempted = (wh.succeeded || 0) + (wh.exhausted || 0);
  const driftOk = (h.ledger_drift || []).length === 0 && h.reconciliation_mismatches === 0;

  const tiles = [
    {
      label: "Outbox pending",
      value: h.outbox_pending,
      note: h.outbox_pending
        ? `oldest ${h.outbox_oldest_seconds.toFixed(1)}s`
        : "relay caught up",
      status: statusFor(h.outbox_pending, 50, 500),
      word: h.outbox_pending ? "lagging" : "drained",
    },
    {
      label: "Consumer lag",
      value: worstLag,
      note: worstGroup ? `worst: ${worstGroup[0]}` : "no consumers",
      status: statusFor(worstLag, 100, 1000),
      word: worstLag ? "behind" : "caught up",
    },
    {
      label: "Dead letters",
      value: h.dlq_depth,
      note: h.dlq_depth ? "needs redrive" : "none unresolved",
      status: statusFor(h.dlq_depth, 1, 10),
      word: h.dlq_depth ? "attention" : "clear",
    },
    {
      label: "Webhooks 24h",
      value: attempted ? `${Math.round((wh.succeeded / attempted) * 100)}%` : "—",
      note: attempted ? `${wh.succeeded}/${attempted} delivered` : "no deliveries",
      status: !attempted ? "good" : wh.exhausted ? "serious" : "good",
      word: !attempted ? "idle" : wh.exhausted ? "exhausted" : "healthy",
    },
    {
      label: "Ledger drift",
      value: driftOk ? "0" : "≠ 0",
      note: driftOk ? "debits = credits" : `${h.reconciliation_mismatches} mismatches`,
      status: driftOk ? "good" : "critical",
      word: driftOk ? "balanced" : "STOP",
    },
  ];

  $("health-tiles").innerHTML = tiles.map((t) => `
    <div class="tile">
      <div class="label">${t.label}</div>
      <div class="value">${t.value}</div>
      <div class="note"><span class="status ${t.status}">${t.word}</span> &middot; ${t.note}</div>
    </div>`).join("");
}

/* ---------- balance chart ---------- */
/* One series, so no legend: the panel title names it. Crosshair + tooltip,
 * because an SVG chart on a page is interactive by default. */

function buildSeries(entries, normalBalance) {
  // The API returns entries in ledger order (entries.id), which is the correct
  // cursor for paging an append-only log -- but it is NOT business-time order
  // once anything is backdated, and a balance-over-time chart is a function of
  // business time. Sort before accumulating, or the x-axis collapses the first
  // time a late-arriving transaction lands.
  const ordered = [...entries].sort((a, b) => a.effective_at - b.effective_at || a.id - b.id);
  let running = 0;
  return ordered.map((e) => {
    running += e.direction === normalBalance ? e.amount : -e.amount;
    return { t: e.effective_at, balance: running, entry: e };
  });
}

function renderBalance() {
  const svg = $("balance-chart");
  svg.replaceChildren();
  const series = state.series;
  if (series.length < 2) {
    svg.setAttribute("height", 60);
    svg.appendChild(el("text", { x: 12, y: 32, class: "tick" }, "not enough history to plot"));
    return;
  }

  const W = svg.clientWidth || 720;
  const H = 260;
  const m = { top: 12, right: 16, bottom: 26, left: 58 };
  svg.setAttribute("viewBox", `0 0 ${W} ${H}`);
  svg.setAttribute("height", H);

  const t0 = series[0].t;
  const t1 = series[series.length - 1].t;
  const values = series.map((p) => p.balance);
  const lo = Math.min(0, ...values);
  const hi = Math.max(...values);
  const pad = (hi - lo) * 0.08 || 1;

  const x = (t) => m.left + ((t - t0) / Math.max(1, t1 - t0)) * (W - m.left - m.right);
  const y = (v) =>
    H - m.bottom - ((v - lo + pad) / (hi - lo + pad * 2)) * (H - m.top - m.bottom);

  // recessive hairline grid; solid, never dashed
  const ticks = 4;
  for (let i = 0; i <= ticks; i++) {
    const v = lo + ((hi - lo) / ticks) * i;
    svg.appendChild(el("line", { x1: m.left, x2: W - m.right, y1: y(v), y2: y(v), class: "grid-line" }));
    svg.appendChild(el("text", { x: m.left - 8, y: y(v) + 4, class: "tick", "text-anchor": "end" },
      compactMoney(v)));
  }
  svg.appendChild(el("line", {
    x1: m.left, x2: W - m.right, y1: H - m.bottom, y2: H - m.bottom, class: "axis-line",
  }));
  const mid = t0 + (t1 - t0) / 2;
  [[t0, m.left, "start"], [mid, x(mid), "middle"], [t1, W - m.right, "end"]]
    .forEach(([t, px, anchor]) => svg.appendChild(el("text", {
      x: px, y: H - 8, class: "tick", "text-anchor": anchor,
    }, dayLabel(t))));

  // step line: a balance holds flat between postings and jumps at one. a
  // straight interpolation would draw money arriving that never did.
  let d = `M ${x(series[0].t)} ${y(series[0].balance)}`;
  for (let i = 1; i < series.length; i++) {
    d += ` L ${x(series[i].t)} ${y(series[i - 1].balance)} L ${x(series[i].t)} ${y(series[i].balance)}`;
  }
  svg.appendChild(el("path", {
    d: `${d} L ${x(t1)} ${H - m.bottom} L ${x(t0)} ${H - m.bottom} Z`, class: "series-area",
  }));
  svg.appendChild(el("path", { d, class: "series-line" }));

  // endpoint direct label -- selective, not a number on every point
  const last = series[series.length - 1];
  svg.appendChild(el("circle", { cx: x(last.t), cy: y(last.balance), r: 4, class: "marker" }));

  // A layer the time-travel marker draws into. Kept separate from the series
  // so scrubbing never re-renders the chart itself -- redrawing the whole path
  // on every input event is what makes a slider feel laggy.
  const travelLayer = el("g", { id: "travel-layer" });
  svg.appendChild(travelLayer);

  // stash the scales so onTravel() can place the marker directly
  state.chart = { x, y, t0, t1, W, H, m, layer: travelLayer };

  const crossX = el("line", { class: "crosshair", y1: m.top, y2: H - m.bottom, opacity: 0 });
  const dot = el("circle", { r: 4.5, class: "marker", opacity: 0 });
  svg.append(crossX, dot);

  const hit = el("rect", {
    x: m.left, y: m.top, width: W - m.left - m.right, height: H - m.top - m.bottom,
    fill: "transparent",
  });
  hit.style.cursor = "crosshair";
  hit.addEventListener("pointermove", (evt) => {
    const box = svg.getBoundingClientRect();
    const px = ((evt.clientX - box.left) / box.width) * W;
    const t = t0 + ((px - m.left) / (W - m.left - m.right)) * (t1 - t0);
    let best = series[0];
    for (const p of series) if (Math.abs(p.t - t) < Math.abs(best.t - t)) best = p;
    crossX.setAttribute("x1", x(best.t));
    crossX.setAttribute("x2", x(best.t));
    crossX.setAttribute("opacity", 1);
    dot.setAttribute("cx", x(best.t));
    dot.setAttribute("cy", y(best.balance));
    dot.setAttribute("opacity", 1);
    const e = best.entry;
    showTip(evt, `<div class="t-title">${money(best.balance, e.currency)}</div>
      <div class="t-row">${when(best.t)}</div>
      <div class="t-row">${e.direction} ${money(e.amount, e.currency)}</div>`);
  });
  hit.addEventListener("pointerleave", () => {
    crossX.setAttribute("opacity", 0);
    dot.setAttribute("opacity", 0);
    hideTip();
  });
  svg.appendChild(hit);

  if (travel.pinned) drawTravelMarker();
}

/* Marks where the slider is sitting, and shades everything after it.
 *
 * Without this the slider changes a number and nothing else, so there is no
 * way to see WHERE in the history you are. The shaded region is the point: it
 * is the part of the ledger that had not happened yet at the instant you are
 * looking at. */
function drawTravelMarker() {
  const chart = state.chart;
  if (!chart || state.series.length < 2) return;
  chart.layer.replaceChildren();
  if (!travel.pinned) return;

  const pct = Number($("travel").value) / 100;
  const at = chart.t0 + (chart.t1 - chart.t0) * pct;
  const px = chart.x(at);

  // the balance at that instant, from the series we already hold -- the
  // authoritative figure still comes from the API for the readout
  let point = state.series[0];
  for (const p of state.series) {
    if (p.t <= at) point = p; else break;
  }

  chart.layer.appendChild(el("rect", {
    x: px, y: chart.m.top,
    width: Math.max(0, chart.W - chart.m.right - px),
    height: chart.H - chart.m.top - chart.m.bottom,
    class: "travel-future",
  }));
  chart.layer.appendChild(el("line", {
    x1: px, x2: px, y1: chart.m.top, y2: chart.H - chart.m.bottom,
    class: "travel-line",
  }));
  chart.layer.appendChild(el("circle", {
    cx: px, cy: chart.y(point.balance), r: 5, class: "travel-dot",
  }));
}

function renderBalanceTable() {
  const rows = state.series.slice(-60).reverse();
  $("balance-table").innerHTML = `<table><thead><tr>
      <th>Effective</th><th>Direction</th><th class="num">Amount</th><th class="num">Balance</th>
    </tr></thead><tbody>${rows.map((p) => `<tr>
      <td class="mono">${when(p.t)}</td>
      <td class="${p.entry.direction}">${p.entry.direction}</td>
      <td class="num">${money(p.entry.amount, p.entry.currency)}</td>
      <td class="num">${money(p.balance, p.entry.currency)}</td>
    </tr>`).join("")}</tbody></table>`;
}

/* ---------- time travel ---------- */
/* The slider does not read the chart. It asks the API for the balance as of
 * that instant, which recomputes it from entries -- so what you see is the
 * ledger's own answer, not a client-side approximation of it.
 *
 * The state below exists because this panel and the 5-second auto-refresh want
 * opposite things. A refresh rebuilds the series and would naturally snap the
 * handle back to "now"; a reader who dragged it to August wants it left alone.
 * So a refresh only re-pins the handle while the view is still live.
 */

const travel = {
  pinned: false,    // the reader has scrubbed away from "now"
  dragging: false,  // a drag is in progress; never touch the handle mid-gesture
  seq: 0,           // guards against a slow response overwriting a newer one
};

let travelTimer = null;

function setLive() {
  travel.pinned = false;
  $("travel").value = 100;
  $("travel-live").hidden = true;
  if (state.chart) state.chart.layer.replaceChildren();
  onTravel();
}

function onTravel() {
  const series = state.series;
  const slider = $("travel");
  if (series.length < 2) {
    $("travel-readout").textContent = "not enough history";
    return;
  }

  const pct = Number(slider.value) / 100;
  travel.pinned = pct < 1;
  $("travel-live").hidden = !travel.pinned;

  const t0 = series[0].t;
  const t1 = series[series.length - 1].t;
  const at = Math.round(t0 + (t1 - t0) * pct);
  const label = travel.pinned ? when(at) : `${when(at)} (now)`;

  // Keep the previous number on screen while the new one is in flight. Blanking
  // it to an ellipsis on every input event makes a drag look like it is failing.
  // move the marker synchronously; waiting on the network to show the handle's
  // own position is what would make dragging feel unresponsive
  drawTravelMarker();

  const readout = $("travel-readout");
  readout.dataset.at = label;
  readout.classList.add("loading");

  const seq = ++travel.seq;
  clearTimeout(travelTimer);
  travelTimer = setTimeout(async () => {
    try {
      const b = await api(
        `/v1/accounts/${state.accountId}/balance?as_of=${new Date(at * 1000).toISOString()}`
      );
      if (seq !== travel.seq) return;   // a newer scrub already superseded this
      readout.textContent = `${label} · ${money(b.balance, b.currency)}`;
    } catch (err) {
      if (seq !== travel.seq) return;
      readout.textContent = `${label} · ${err.message}`;
    } finally {
      if (seq === travel.seq) readout.classList.remove("loading");
    }
  }, 120);
}

/* ---------- category chart ---------- */
/* Horizontal bars, one series, ONE color. Coloring each bar by its own size
 * would double-encode length as hue and burn the only free channel. */

function renderCategories(rows) {
  const svg = $("category-chart");
  svg.replaceChildren();
  if (!rows.length) {
    svg.setAttribute("height", 60);
    svg.appendChild(el("text", { x: 12, y: 32, class: "tick" }, "no categorized spend yet"));
    return;
  }

  const W = svg.clientWidth || 520;
  const rowH = 30;
  const H = rows.length * rowH + 12;
  const labelW = 118;
  const valueW = 74;
  svg.setAttribute("viewBox", `0 0 ${W} ${H}`);
  svg.setAttribute("height", H);

  const max = Math.max(...rows.map((r) => r.spend));
  const trackW = Math.max(40, W - labelW - valueW - 12);
  const selected = state.categoryFilter;

  rows.forEach((r, i) => {
    const yTop = i * rowH + 6;
    const w = Math.max(2, (r.spend / max) * trackW);
    const isSelected = selected === r.category;
    // Emphasis, not recoloring: the selected bar keeps the series color and
    // the others recede. Assigning a different hue per category would burn
    // the one free channel on information the bar length already carries.
    const muted = selected && !isSelected;

    const group = el("g", {
      class: "cat-row" + (isSelected ? " is-selected" : "") + (muted ? " is-muted" : ""),
      role: "button",
      tabindex: "0",
      "aria-pressed": String(isSelected),
      "aria-label": `Filter the feed to ${r.category}`,
    });

    // full-width hit target: clicking the label or the empty track counts too
    group.appendChild(el("rect", {
      x: 0, y: yTop, width: W, height: rowH - 4, class: "cat-hit",
    }));
    group.appendChild(el("text", {
      x: 0, y: yTop + 14, class: "bar-name", "dominant-baseline": "middle",
    }, r.category));
    group.appendChild(el("rect", {
      x: labelW, y: yTop + 3, width: w, height: 16, rx: 4, class: "bar",
    }));
    group.appendChild(el("text", {
      x: labelW + w + 8, y: yTop + 14, class: "bar-label", "dominant-baseline": "middle",
    }, money(r.spend)));

    const toggle = () => selectCategory(isSelected ? null : r.category);
    group.addEventListener("click", toggle);
    group.addEventListener("keydown", (evt) => {
      if (evt.key === "Enter" || evt.key === " ") { evt.preventDefault(); toggle(); }
    });
    group.addEventListener("pointermove", (evt) => showTip(evt,
      `<div class="t-title">${r.category}</div>
       <div class="t-row">${money(r.spend)} &middot; ${r.txn_count} transactions</div>
       ${r.unresolved ? `<div class="t-row">${r.unresolved} unresolved descriptors</div>` : ""}
       <div class="t-row">${isSelected ? "click to clear filter" : "click to filter the feed"}</div>`));
    group.addEventListener("pointerleave", hideTip);
    svg.appendChild(group);
  });
}

function selectCategory(category) {
  state.categoryFilter = category;
  renderCategories(state.categories || []);
  refreshFeed();
}

async function refreshFeed() {
  const chip = $("feed-filter");
  const category = state.categoryFilter;
  chip.hidden = !category;
  if (category) chip.querySelector(".chip-label").textContent = category;

  const query = category ? `&category=${encodeURIComponent(category)}` : "";
  try {
    const list = await api(`/v1/transactions?limit=30${query}`);
    state.transactions = list.data;
    renderFeed(list.data);
  } catch (err) {
    if (err.message !== "unauthorized") console.error(err);
  }
}

/* ---------- feed, signals, explorer ---------- */

function renderFeed(transactions) {
  if (!transactions.length) {
    $("feed").innerHTML = state.categoryFilter
      ? `<p class="empty">Nothing in ${state.categoryFilter}.</p>`
      : `<p class="empty">No transactions yet.</p>`;
    return;
  }
  $("feed").innerHTML = `<table><thead><tr>
      <th>When</th><th>Merchant</th><th>Category</th><th class="num">Amount</th>
    </tr></thead><tbody>${transactions.map((t) => {
      const name = t.merchant || KIND_LABELS[t.kind] || t.kind;
      // an unresolved descriptor is worth seeing: it is what the normalizer
      // could not place, and the reason the Uncategorized bar exists
      // show the raw descriptor when the normalizer could not place it -- that
      // string is exactly what the unresolved count in the breakdown refers to
      const sub = !t.merchant && t.descriptor
        ? `<div class="mono sub">${t.descriptor.slice(0, 38)}</div>` : "";
      return `<tr class="clickable" data-txn="${t.id}">
        <td class="mono">${when(t.effective_at)}</td>
        <td>${name}${sub}${t.status === "reversed"
            ? ' <span class="status serious">reversed</span>' : ""}</td>
        <td>${t.category
            ? `<span class="chip">${t.category}</span>`
            : `<span class="chip chip--none">&mdash;</span>`}</td>
        <td class="num">${money(t.amount || 0, t.currency || "usd")}</td>
      </tr>`;
    }).join("")}</tbody></table>`;

  $("feed").querySelectorAll("tr[data-txn]").forEach((row) =>
    row.addEventListener("click", () => showTransaction(row.dataset.txn)));
}

function renderSignals(signals) {
  if (!signals.length) {
    $("signals").innerHTML = `<p class="empty">No signals.</p>`;
    return;
  }
  const severity = (s) => (s >= 0.7 ? "critical" : s >= 0.5 ? "serious" : "warning");
  $("signals").innerHTML = signals.map((s) => `
    <div style="padding:9px 0;border-bottom:1px solid var(--grid)">
      <span class="status ${severity(s.score)}">${s.rule}</span>
      <span class="mono" style="float:right">${s.score.toFixed(2)}</span>
      <div class="mono">${when(s.evaluated_at)}</div>
      <div class="mono">1h spend ${money(s.features?.spend_1h || 0)}
        &middot; ${s.features?.txn_count_1h ?? 0} txns/h
        &middot; z ${(s.features?.amount_zscore ?? 0).toFixed(2)}</div>
    </div>`).join("");
}

async function showTransaction(id) {
  const box = $("explorer");
  box.innerHTML = `<p class="empty">Loading…</p>`;
  try {
    const t = await api(`/v1/transactions/${id}`);
    const debits = t.entries.filter((e) => e.direction === "debit")
      .reduce((a, e) => a + e.amount, 0);
    const credits = t.entries.filter((e) => e.direction === "credit")
      .reduce((a, e) => a + e.amount, 0);
    const byId = Object.fromEntries(state.accounts.map((a) => [a.id, a.name]));

    box.innerHTML = `
      <div class="mono" style="margin-bottom:10px">
        ${t.id} &middot; ${t.kind} &middot; effective ${when(t.effective_at)}
        &middot; recorded ${when(t.recorded_at)}
        ${t.reverses ? `&middot; reverses ${t.reverses}` : ""}
      </div>
      <table><thead><tr>
        <th>Account</th><th class="num">Debit</th><th class="num">Credit</th>
      </tr></thead><tbody>
        ${t.entries.map((e) => `<tr>
          <td>${byId[e.account] || e.account}</td>
          <td class="num debit">${e.direction === "debit" ? money(e.amount, e.currency) : ""}</td>
          <td class="num credit">${e.direction === "credit" ? money(e.amount, e.currency) : ""}</td>
        </tr>`).join("")}
        <tr>
          <td><strong>Total</strong></td>
          <td class="num"><strong>${money(debits)}</strong></td>
          <td class="num"><strong>${money(credits)}</strong></td>
        </tr>
      </tbody></table>
      <p style="margin:10px 0 0">
        <span class="status ${debits === credits ? "good" : "critical"}">
          ${debits === credits ? "balanced" : "UNBALANCED"}</span>
        <span class="mono"> &middot; Σ debits − Σ credits = ${debits - credits}</span>
      </p>`;
  } catch (err) {
    box.innerHTML = `<p class="empty">${err.message}</p>`;
  }
}

/* ---------- load ---------- */

async function loadAccounts() {
  const list = await api("/v1/accounts?limit=100");
  // Asset accounts first -- a balance chart of Expenses:Groceries is
  // technically correct and not what anyone opens this page to see -- then by
  // balance, so the default is an account with history rather than whichever
  // one sorts first alphabetically. "Assets:Cash" beating "Assets:Checking" to
  // the front and rendering an empty chart is not a good first impression.
  state.accounts = list.data.sort((a, b) =>
    (a.type === "asset" ? 0 : 1) - (b.type === "asset" ? 0 : 1)
    || Math.abs(b.balance || 0) - Math.abs(a.balance || 0)
    || a.name.localeCompare(b.name));

  const picker = $("account-picker");
  picker.innerHTML = state.accounts
    .map((a) => `<option value="${a.id}">${a.name}</option>`).join("");
  if (!state.accountId || !state.accounts.some((a) => a.id === state.accountId)) {
    state.accountId = state.accounts[0]?.id || null;
  }
  picker.value = state.accountId || "";
}

async function loadAll() {
  if (!state.key) return;
  try {
    if (!state.accounts.length) await loadAccounts();
    state.account = state.accounts.find((a) => a.id === state.accountId);

    const [health, entries, transactions, signals, categories] = await Promise.all([
      api("/v1/health"),
      api(`/v1/ledger/entries?account=${state.accountId}&limit=500`),
      api("/v1/transactions?limit=30" + (state.categoryFilter
          ? `&category=${encodeURIComponent(state.categoryFilter)}` : "")),
      api("/v1/fraud_signals?limit=25"),
      api("/v1/analytics/spend_by_category?days=180"),
    ]);

    renderHealth(health);
    state.entries = entries.data;
    state.series = buildSeries(entries.data, state.account?.normal_balance || "debit");
    renderBalance();
    renderBalanceTable();
    // only re-pin to "now" when the reader has not scrubbed and is not
    // mid-drag; otherwise a background refresh yanks the handle out of
    // their hand every five seconds
    if (!travel.pinned && !travel.dragging) {
      $("travel").value = 100;
      onTravel();
    }
    state.transactions = transactions.data;
    state.categories = categories.data;
    renderFeed(transactions.data);
    renderSignals(signals.data);
    renderCategories(categories.data);
    $("feed-filter").hidden = !state.categoryFilter;

    $("mode-line").textContent =
      `${state.account?.name || ""} · ${state.entries.length} entries`;
  } catch (err) {
    if (err.message !== "unauthorized") console.error(err);
  }
}

/* ---------- key + theme ---------- */

function askForKey() {
  const dialog = $("key-dialog");
  if (!dialog.open) dialog.showModal();
}

$("key-save").addEventListener("click", () => {
  const value = $("key-input").value.trim();
  if (!value) return;
  state.key = value;
  localStorage.setItem(KEY_STORAGE, value);
  $("key-dialog").close();
  state.accounts = [];
  loadAll();
});

$("theme-toggle").addEventListener("click", () => {
  const next = document.documentElement.getAttribute("data-theme") === "dark" ? "light" : "dark";
  document.documentElement.setAttribute("data-theme", next);
  localStorage.setItem(THEME_STORAGE, next);
  renderBalance();
  loadAll();
});

$("refresh").addEventListener("click", loadAll);
$("account-picker").addEventListener("change", (e) => {
  state.accountId = e.target.value;
  travel.pinned = false;          // a different account starts at "now"
  $("travel-live").hidden = true;
  loadAll();
});
$("travel").addEventListener("input", onTravel);
// pointer and keyboard both count as "in progress": a refresh landing between
// two arrow-key presses is just as disruptive as one landing mid-drag
["pointerdown", "keydown"].forEach((evt) =>
  $("travel").addEventListener(evt, () => { travel.dragging = true; }));
["pointerup", "pointercancel", "blur", "keyup"].forEach((evt) =>
  $("travel").addEventListener(evt, () => { travel.dragging = false; }));
$("travel-live").addEventListener("click", setLive);
$("feed-filter").addEventListener("click", () => selectCategory(null));
$("balance-table-toggle").addEventListener("click", (e) => {
  const table = $("balance-table");
  const shown = !table.hidden;
  table.hidden = shown;
  e.target.textContent = shown ? "Show as table" : "Hide table";
  e.target.setAttribute("aria-expanded", String(!shown));
});

let resizeTimer = null;
window.addEventListener("resize", () => {
  clearTimeout(resizeTimer);
  resizeTimer = setTimeout(renderBalance, 150);
});

(function init() {
  const theme = localStorage.getItem(THEME_STORAGE);
  if (theme) document.documentElement.setAttribute("data-theme", theme);
  state.key = localStorage.getItem(KEY_STORAGE);
  if (!state.key) askForKey();
  else loadAll();
  setInterval(loadAll, 5000);
})();
