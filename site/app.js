const WORKER_URL = "https://5ch-tracker.arakawa47.workers.dev";
let chartWidget = null;
let radarChart = null;
let ongiHistoryChart = null; // NEW GLOBAL
let currentTicker = null;
let currentTopics = [];
let currentItems = [];
let currentPolymarket = null;
let latestData = null;
let insightHistory = [];
let insightHistoryIndex = 0;
let insightHistoryLoaded = false;
let insightHistoryLoading = false;
let insightHistoryBound = false;
let investBriefMode = "swing";
let investBriefBound = false;
let lastHistoryUpdatedAt = null;
const WATCHLIST_TARGET = 8;
const NO_DATA_LABEL = "NO DATA";

function escapeHtml(value) {
  const s = value == null ? "" : String(value);
  return s.replace(/[&<>"']/g, (ch) => {
    switch (ch) {
      case "&": return "&amp;";
      case "<": return "&lt;";
      case ">": return "&gt;";
      case "\"": return "&quot;";
      case "'": return "&#39;";
      default: return ch;
    }
  });
}

function safeText(value) {
  return escapeHtml(value);
}

function safeUrl(raw) {
  try {
    const u = new URL(String(raw), window.location.href);
    if (u.protocol === "http:" || u.protocol === "https:") {
      return u.toString();
    }
  } catch { }
  return "about:blank";
}

function getOverviewText(payload) {
  if (!payload) return "";
  const overview = payload.overview;
  if (typeof overview === "string") return overview;
  if (overview && typeof overview === "object") {
    if (typeof overview.summary === "string") return overview.summary;
    if (typeof overview.text === "string") return overview.text;
  }
  if (typeof payload.summary === "string") return payload.summary;
  return "";
}

function getOngiCommentText(payload) {
  if (!payload) return "";
  if (typeof payload.ongi_comment === "string") return payload.ongi_comment;
  const overview = payload.overview;
  if (overview && typeof overview === "object") {
    if (typeof overview.ongi_comment === "string") return overview.ongi_comment;
    if (typeof overview.comment === "string") return overview.comment;
  }
  return "";
}

function localizeMarketRegime(value) {
  if (!value) return "";
  let text = String(value);
  const replacements = [
    { en: /Deep\s*Fear/gi, ja: "深い恐怖" },
    { en: /Liquidation\s*Mode/gi, ja: "投げ売りモード" },
    { en: /Macro\s*Transition/gi, ja: "マクロ転換" },
    { en: /Volatility\s*Expansion/gi, ja: "ボラ拡大" },
    { en: /Risk[\s-]*On/gi, ja: "リスクオン" },
    { en: /Risk[\s-]*Off/gi, ja: "リスクオフ" },
    { en: /Mixed/gi, ja: "混在" },
    { en: /Neutral/gi, ja: "中立" },
  ];
  replacements.forEach(({ en, ja }) => {
    text = text.replace(en, ja);
  });
  return text;
}

function getSnapshotTime(snapshot) {
  if (!snapshot) return null;
  const payload = snapshot.payload || {};
  if (payload.updatedAt) {
    const d = new Date(payload.updatedAt);
    if (!Number.isNaN(d.getTime())) return d;
  }
  if (snapshot.timestamp) {
    const d = new Date(snapshot.timestamp * 1000);
    if (!Number.isNaN(d.getTime())) return d;
  }
  return null;
}

function getSnapshotUpdatedAt(snapshot) {
  if (!snapshot || !snapshot.payload) return null;
  const value = snapshot.payload.updatedAt;
  if (value == null) return null;
  return String(value);
}

function formatInsightLabel(snapshot, index) {
  const ts = getSnapshotTime(snapshot);
  const timeLabel = ts
    ? `${ts.getMonth() + 1}/${ts.getDate()} ${String(ts.getHours()).padStart(2, "0")}:${String(ts.getMinutes()).padStart(2, "0")}`
    : "--";
  if (index === 0) return `\u6700\u65b0 ${timeLabel}`;
  return `${index}\u56de\u524d ${timeLabel}`;
}

function normalizeTicker(value) {
  if (value == null) return "";
  return String(value).trim().toUpperCase();
}

function getTickerHistorySeries(ticker) {
  const key = normalizeTicker(ticker);
  if (!key || !Array.isArray(insightHistory) || insightHistory.length === 0) return [];
  const series = [];
  for (let i = insightHistory.length - 1; i >= 0; i -= 1) {
    const snapshot = insightHistory[i];
    const items = (snapshot && snapshot.payload && snapshot.payload.items) || [];
    const match = items.find(item => normalizeTicker(item.ticker) === key);
    series.push(match ? (Number(match.count) || 0) : 0);
  }
  return series;
}

function getSeriesTrend(series) {
  if (!series || series.length < 2) return { label: "--", className: "flat" };
  const latest = series[series.length - 1];
  const prev = series[series.length - 2];
  const delta = latest - prev;
  if (delta > 0) return { label: `▲ +${delta}`, className: "up" };
  if (delta < 0) return { label: `▼ ${delta}`, className: "down" };
  return { label: "→ 0", className: "flat" };
}

function formatSeries(series) {
  if (!series || series.length === 0) return "--";
  return series.map(v => Number(v) || 0).join(" → ");
}

function classifyDeadline(value) {
  if (!value) return { label: "", className: "deadline-unknown" };
  const raw = String(value).trim();
  if (!raw) return { label: "", className: "deadline-unknown" };
  if (/未定|不明|様子見|未設定/i.test(raw)) return { label: raw, className: "deadline-unknown" };

  const dateMatch = raw.match(/(\d{1,2})\s*[\/月]\s*(\d{1,2})/);
  if (dateMatch) {
    const now = new Date();
    const month = Number(dateMatch[1]);
    const day = Number(dateMatch[2]);
    const year = now.getFullYear();
    const target = new Date(year, month - 1, day, 23, 59, 59);
    if (!Number.isNaN(target.getTime())) {
      const diffMs = target.getTime() - now.getTime();
      const diffHours = diffMs / (1000 * 60 * 60);
      if (diffHours <= 24) return { label: raw, className: "deadline-urgent" };
      if (diffHours <= 168) return { label: raw, className: "deadline-soon" };
      return { label: raw, className: "deadline-later" };
    }
  }

  if (/次回更新|決算|イベント|今週/i.test(raw)) return { label: raw, className: "deadline-soon" };
  return { label: raw, className: "deadline-unknown" };
}

function getDeadlineSortKey(value) {
  if (!value) return Number.POSITIVE_INFINITY;
  const raw = String(value).trim();
  if (!raw) return Number.POSITIVE_INFINITY;

  if (/\u672a\u5b9a|\u4e0d\u660e|\u69d8\u5b50\u898b|\u672a\u8a2d\u5b9a/i.test(raw)) {
    return Number.POSITIVE_INFINITY;
  }

  const now = new Date();
  const nowMs = now.getTime();

  if (/24h|24\u6642\u9593/i.test(raw)) return nowMs + 6 * 60 * 60 * 1000;
  if (/\u672c\u65e5|\u4eca\u65e5|\u4eca\u591c|\u4eca\u671d/i.test(raw)) return nowMs + 6 * 60 * 60 * 1000;
  if (/\u660e\u5f8c\u65e5/.test(raw)) return nowMs + 2 * 24 * 60 * 60 * 1000;
  if (/\u660e\u65e5/.test(raw)) return nowMs + 24 * 60 * 60 * 1000;

  const dateMatch = raw.match(/(\d{1,2})\s*(?:\/|\u6708)\s*(\d{1,2})/);
  if (dateMatch) {
    const month = Number(dateMatch[1]);
    const day = Number(dateMatch[2]);
    let year = now.getFullYear();
    let target = new Date(year, month - 1, day, 23, 59, 59);
    if (target.getTime() < nowMs - 7 * 24 * 60 * 60 * 1000) {
      year += 1;
      target = new Date(year, month - 1, day, 23, 59, 59);
    }
    return target.getTime();
  }

  const monthMatch = raw.match(/(\d{1,2})\s*\u6708/);
  if (monthMatch) {
    const month = Number(monthMatch[1]);
    let year = now.getFullYear();
    if (month < now.getMonth() + 1) year += 1;
    let day = 15;
    const monthEnd = /\u6708\u672b|\u6700\u7d42/.test(raw);
    if (/\u4e0a\u65ec/.test(raw)) day = 5;
    else if (/\u4e2d\u65ec/.test(raw)) day = 15;
    else if (/\u4e0b\u65ec/.test(raw)) day = 25;
    if (monthEnd) {
      day = new Date(year, month, 0).getDate();
    }
    return new Date(year, month - 1, day, 23, 59, 59).getTime();
  }

  if (/\u4eca\u9031/.test(raw)) return nowMs + 4 * 24 * 60 * 60 * 1000;
  if (/\u6765\u9031/.test(raw)) return nowMs + 10 * 24 * 60 * 60 * 1000;

  if (/\u4eca\u6708/.test(raw)) {
    const last = new Date(now.getFullYear(), now.getMonth() + 1, 0, 23, 59, 59, 999);
    return last.getTime();
  }

  if (/\u6765\u6708/.test(raw)) {
    const year = now.getFullYear();
    const month = now.getMonth() + 1;
    const last = new Date(year, month + 1, 0, 23, 59, 59, 999);
    return last.getTime();
  }

  if (/\u6c7a\u7b97|\u767a\u8868|\u30ed\u30fc\u30f3\u30c1|\u30a4\u30d9\u30f3\u30c8|\u958b\u59cb|\u7d42\u4e86/.test(raw)) {
    return nowMs + 14 * 24 * 60 * 60 * 1000;
  }

  return Number.POSITIVE_INFINITY;
}

function buildChangeTop(history, limit = 3) {
  if (!Array.isArray(history) || history.length < 2) return [];
  const latestItems = (history[0]?.payload?.items) || [];
  const prevItems = (history[1]?.payload?.items) || [];
  const latestMap = new Map(latestItems.map(item => [normalizeTicker(item.ticker), Number(item.count) || 0]));
  const prevMap = new Map(prevItems.map(item => [normalizeTicker(item.ticker), Number(item.count) || 0]));
  const allTickers = new Set([...latestMap.keys(), ...prevMap.keys()].filter(Boolean));
  const changes = [];

  allTickers.forEach(ticker => {
    const latest = latestMap.get(ticker) || 0;
    const prev = prevMap.get(ticker) || 0;
    const delta = latest - prev;
    if (latest === 0 && prev === 0) return;
    const isNew = prev === 0 && latest > 0;
    changes.push({ ticker, delta, latest, prev, isNew });
  });

  changes.sort((a, b) => {
    const aScore = Math.abs(a.delta) + (a.isNew ? 1000 : 0);
    const bScore = Math.abs(b.delta) + (b.isNew ? 1000 : 0);
    if (bScore !== aScore) return bScore - aScore;
    return b.latest - a.latest;
  });

  return changes.slice(0, limit);
}

function buildFallbackBrief(data) {
  const items = (data && Array.isArray(data.items) ? data.items.slice(0, WATCHLIST_TARGET) : []);
  return {
    headline: getOverviewText(data) || NO_DATA_LABEL,
    market_regime: null,
    focus_themes: [],
    watchlist: items.map(item => ({
      ticker: item.ticker,
      reason: "話題上位のため監視",
      catalyst: "",
      risk: "",
      invalidation: "",
      valid_until: "次回更新まで"
    })),
    cautions: [NO_DATA_LABEL]
  };
}

function buildBriefWatchlist(displayBrief, data) {
  const output = [];
  const seen = new Set();

  function pushItem(item, fallbackReason) {
    if (!item) return;
    const ticker = normalizeTicker(item.ticker);
    if (!ticker || seen.has(ticker)) return;
    seen.add(ticker);
    output.push({
      ticker,
      reason: item.reason || fallbackReason || "監視対象",
      catalyst: item.catalyst || "",
      risk: item.risk || "",
      invalidation: item.invalidation || "",
      valid_until: item.valid_until || item.deadline || ""
    });
  }

  const briefItems = displayBrief && Array.isArray(displayBrief.watchlist) ? displayBrief.watchlist : [];
  briefItems.forEach(item => pushItem(item, "監視対象"));

  if (output.length < WATCHLIST_TARGET && data && Array.isArray(data.items)) {
    data.items.forEach(item => {
      if (output.length >= WATCHLIST_TARGET) return;
      pushItem({ ticker: item.ticker, reason: "話題上位のため監視" }, "話題上位のため監視");
    });
  }

  return output.slice(0, WATCHLIST_TARGET);
}

function getActiveBrief(data) {
  if (!data) return null;
  return investBriefMode === "long" ? data.brief_long : data.brief_swing;
}

function renderInvestBrief(data) {
  const headlineEl = document.getElementById("brief-headline");
  const regimeEl = document.getElementById("brief-regime");
  const updatedEl = document.getElementById("brief-updated");
  const modelEl = document.getElementById("brief-model-name");
  const modeEl = document.getElementById("brief-model-mode");
  const themesEl = document.getElementById("brief-themes");
  const cautionsEl = document.getElementById("brief-cautions");
  const watchlistEl = document.getElementById("brief-watchlist");
  const calendarEl = document.getElementById("brief-calendar");
  const changesEl = document.getElementById("brief-changes");

  if (!headlineEl || !regimeEl || !updatedEl || !themesEl || !cautionsEl || !watchlistEl || !calendarEl || !changesEl) return;

  const brief = getActiveBrief(data);
  const hasBrief = brief && (brief.headline || (brief.watchlist && brief.watchlist.length));
  const displayBrief = hasBrief ? brief : buildFallbackBrief(data || {});
  const watchlist = buildBriefWatchlist(displayBrief, data || {});
  const sortedWatchlist = watchlist
    .map((item, index) => ({
      item,
      index,
      key: getDeadlineSortKey(item && (item.valid_until || item.deadline))
    }))
    .sort((a, b) => {
      if (a.key === b.key) return a.index - b.index;
      return a.key - b.key;
    })
    .map(entry => entry.item);

  headlineEl.textContent = displayBrief.headline || NO_DATA_LABEL;

  if (modelEl) {
    const modelName = data && data.ai_model ? String(data.ai_model) : "";
    if (modelName) {
      modelEl.textContent = `POWERED BY ${modelName.toUpperCase()}`;
    } else {
      modelEl.textContent = "POWERED BY --";
    }
  }

  if (modeEl) {
    const raw = data && data.ai_model ? String(data.ai_model).toLowerCase() : "";
    let modeLabel = "";
    if (raw.includes("gemini-2.5-flash")) {
      modeLabel = "低性能モード";
    } else if (raw.includes("gemini-3")) {
      modeLabel = "高性能モード";
    }
    modeEl.textContent = modeLabel || "モード: --";
  }

  if (displayBrief.market_regime) {
    regimeEl.textContent = localizeMarketRegime(displayBrief.market_regime);
    regimeEl.style.display = "inline-flex";
  } else {
    regimeEl.textContent = "--";
    regimeEl.style.display = "inline-flex";
  }

  if (data && data.updatedAt) {
    const date = new Date(data.updatedAt);
    if (!Number.isNaN(date.getTime())) {
      updatedEl.textContent = `更新: ${date.getMonth() + 1}/${date.getDate()} ${String(date.getHours()).padStart(2, "0")}:${String(date.getMinutes()).padStart(2, "0")}`;
    } else {
      updatedEl.textContent = "--";
    }
  } else {
    updatedEl.textContent = "--";
  }

  const themes = Array.isArray(displayBrief.focus_themes) ? displayBrief.focus_themes : [];
  themesEl.textContent = "";
  if (themes.length > 0) {
    themes.forEach(theme => {
      const tag = document.createElement("span");
      tag.className = "brief-tag";
      tag.textContent = theme == null ? "" : String(theme);
      themesEl.appendChild(tag);
    });
  } else {
    const tag = document.createElement("span");
    tag.className = "brief-tag";
    tag.textContent = NO_DATA_LABEL;
    themesEl.appendChild(tag);
  }

  const cautions = Array.isArray(displayBrief.cautions) ? displayBrief.cautions : [];
  cautionsEl.textContent = "";
  if (cautions.length > 0) {
    cautions.forEach(caution => {
      const li = document.createElement("li");
      li.textContent = caution == null ? "" : String(caution);
      cautionsEl.appendChild(li);
    });
  } else {
    const li = document.createElement("li");
    li.textContent = NO_DATA_LABEL;
    cautionsEl.appendChild(li);
  }

  const calendar = Array.isArray(displayBrief.catalyst_calendar) ? displayBrief.catalyst_calendar : [];
  calendarEl.textContent = "";
  if (calendar.length > 0) {
    calendar.forEach(entry => {
      const li = document.createElement("li");
      if (typeof entry === "string") {
        li.textContent = entry;
      } else if (entry && typeof entry === "object") {
        const date = entry.date ? String(entry.date) : "";
        const event = entry.event ? String(entry.event) : "";
        const note = entry.note ? String(entry.note) : "";
        const impactRaw = entry.impact ? String(entry.impact).toLowerCase() : "";
        const impactClass = impactRaw === "high" ? "high" : impactRaw === "mid" ? "mid" : impactRaw === "low" ? "low" : "";
        if (impactClass) {
          const badge = document.createElement("span");
          badge.className = `brief-impact ${impactClass}`;
          badge.textContent = impactClass === "high" ? "高" : impactClass === "mid" ? "中" : "低";
          li.appendChild(badge);
        }
        const text = [date, event].filter(Boolean).join(" ") + (note ? ` / ${note}` : "");
        li.appendChild(document.createTextNode(text));
      } else {
        li.textContent = "";
      }
      calendarEl.appendChild(li);
    });
  } else {
    const li = document.createElement("li");
    li.textContent = NO_DATA_LABEL;
    calendarEl.appendChild(li);
  }

  const changeItems = buildChangeTop(insightHistory, 3);
  changesEl.textContent = "";
  if (changeItems.length > 0) {
    changeItems.forEach(item => {
      const chip = document.createElement("div");
      const label = item.isNew ? "NEW" : item.delta > 0 ? `+${item.delta}` : `${item.delta}`;
      const chipClass = item.isNew ? "new" : item.delta > 0 ? "up" : "down";
      chip.className = `brief-change-chip ${chipClass}`;
      chip.textContent = `${item.ticker} ${label}`;
      changesEl.appendChild(chip);
    });
  } else {
    const chip = document.createElement("div");
    chip.className = "brief-change-chip";
    chip.textContent = "変化データ準備中";
    changesEl.appendChild(chip);
  }

  watchlistEl.textContent = "";
  if (sortedWatchlist.length === 0) {
    const empty = document.createElement("div");
    empty.className = "brief-card-reason";
    empty.textContent = NO_DATA_LABEL;
    watchlistEl.appendChild(empty);
    return;
  }

  const redditTickers = new Set((data && Array.isArray(data.reddit_rankings) ? data.reddit_rankings : [])
    .map(r => normalizeTicker(r.ticker)));

  sortedWatchlist.forEach(item => {
    const ticker = item && item.ticker ? String(item.ticker) : "--";
    const reason = item && item.reason ? String(item.reason) : "監視対象";
    const catalyst = item && item.catalyst ? String(item.catalyst) : "";
    const risk = item && item.risk ? String(item.risk) : "";
    const invalidation = item && item.invalidation ? String(item.invalidation) : "";

    const series = getTickerHistorySeries(ticker);
    const trend = getSeriesTrend(series);
    const historyText = formatSeries(series);

    const metaFallback = !catalyst && !risk && !invalidation;
    const fallbackCatalyst = metaFallback ? (
      trend.className === "up"
        ? "\u8a71\u984c\u5897\u52a0\uff08\u63a8\u79fb\uff09"
        : trend.className === "down"
          ? "\u53cd\u8ee2\u5f85\u3061\uff08\u63a8\u79fb\uff09"
          : "\u8a71\u984c\u7d99\u7d9a\uff08\u63a8\u79fb\uff09"
    ) : "";
    const fallbackRisk = metaFallback ? (
      trend.className === "down"
        ? "\u8a71\u984c\u6e1b\u901f\uff08\u63a8\u79fb\uff09"
        : "\u53cd\u52d5\u30ea\u30b9\u30af\uff08\u63a8\u79fb\uff09"
    ) : "";
    const fallbackInvalidation = metaFallback ? "\u8a71\u984c\u6c88\u9759\uff08\u63a8\u79fb\uff09" : "";
    const catalystText = catalyst || fallbackCatalyst;
    const riskText = risk || fallbackRisk;
    const invalidationText = invalidation || fallbackInvalidation;

    const card = document.createElement("div");
    card.className = "brief-watch-card";

    const header = document.createElement("div");
    header.className = "brief-card-header";

    const tickerEl = document.createElement("span");
    tickerEl.className = "brief-ticker";
    tickerEl.textContent = ticker;

    const rightGroup = document.createElement("div");
    rightGroup.className = "brief-card-right";

    const trendEl = document.createElement("span");
    trendEl.className = `brief-trend ${trend.className}`;
    trendEl.textContent = trend.label;

    const confidence = item && item.confidence ? String(item.confidence).toLowerCase() : "";
    if (confidence === "high") {
      const evidenceBadge = document.createElement("span");
      evidenceBadge.className = "brief-badge evidence";
      evidenceBadge.textContent = "\u6839\u62e0";
      rightGroup.appendChild(evidenceBadge);
    }

    const deadlineInfo = classifyDeadline(item && item.valid_until ? String(item.valid_until) : "");
    if (deadlineInfo.label) {
      const deadlineEl = document.createElement("span");
      deadlineEl.className = `brief-deadline ${deadlineInfo.className}`;
      deadlineEl.textContent = deadlineInfo.label;
      rightGroup.appendChild(deadlineEl);
    } else {
      const deadlineEl = document.createElement("span");
      deadlineEl.className = "brief-deadline deadline-unknown";
      deadlineEl.textContent = "未定";
      rightGroup.appendChild(deadlineEl);
    }

    rightGroup.appendChild(trendEl);

    if (redditTickers.has(normalizeTicker(ticker))) {
      const badge = document.createElement("span");
      badge.className = "brief-badge";
      badge.textContent = "CONSENSUS";
      rightGroup.appendChild(badge);
    }

    header.appendChild(tickerEl);
    header.appendChild(rightGroup);

    const reasonEl = document.createElement("div");
    reasonEl.className = "brief-card-reason";
    reasonEl.textContent = reason;

    const meta = document.createElement("div");
    meta.className = "brief-card-meta";

    function addMeta(label, value) {
      if (!value) return;
      const line = document.createElement("div");
      const strong = document.createElement("strong");
      strong.textContent = label;
      line.appendChild(strong);
      line.appendChild(document.createTextNode(value));
      meta.appendChild(line);
    }

    addMeta("\u89e6\u5a92", catalystText);
    addMeta("\u30ea\u30b9\u30af", riskText);
    addMeta("\u7121\u52b9\u5316", invalidationText);
    addMeta("\u671f\u9650", item && item.valid_until ? String(item.valid_until) : "");

    const historyWrap = document.createElement("div");
    historyWrap.className = "brief-card-history-wrap";

    const historyToggle = document.createElement("button");
    historyToggle.type = "button";
    historyToggle.className = "brief-history-toggle";
    historyToggle.textContent = "推移";
    historyToggle.setAttribute("aria-expanded", "false");

    const history = document.createElement("div");
    history.className = "brief-card-history";
    history.textContent = `推移(5ch件数・古→新): ${historyText || NO_DATA_LABEL}`;
    history.hidden = true;

    historyToggle.addEventListener("click", () => {
      const open = history.hidden;
      history.hidden = !open;
      historyToggle.setAttribute("aria-expanded", open ? "true" : "false");
      historyToggle.textContent = open ? "推移を隠す" : "推移";
    });

    card.appendChild(header);
    card.appendChild(reasonEl);
    if (meta.childNodes.length > 0) {
      card.appendChild(meta);
    }
    historyWrap.appendChild(historyToggle);
    historyWrap.appendChild(history);
    card.appendChild(historyWrap);

    watchlistEl.appendChild(card);
  });
}

function bindInvestBriefControls() {
  if (investBriefBound) return;
  document.querySelectorAll(".brief-toggle-btn").forEach(btn => {
    btn.addEventListener("click", () => {
      const mode = btn.dataset.mode;
      if (!mode || mode === investBriefMode) return;
      investBriefMode = mode;
      document.querySelectorAll(".brief-toggle-btn").forEach(toggle => {
        const active = toggle.dataset.mode === investBriefMode;
        toggle.classList.toggle("active", active);
        toggle.setAttribute("aria-selected", active ? "true" : "false");
      });
      if (latestData) {
        renderInvestBrief(latestData);
      }
    });
  });
  investBriefBound = true;
}

function applyInsightSnapshot(snapshot, index) {
  if (!snapshot || !snapshot.payload) return;
  const payload = snapshot.payload;

  const overviewEl = document.getElementById("market-overview");
  const overviewText = document.getElementById("overview-text");
  const labelEl = document.getElementById("overview-history-label");

  if (overviewEl && overviewText) {
    const overviewTextValue = getOverviewText(payload);
    if (overviewTextValue) {
      overviewEl.style.display = "block";
      overviewText.textContent = overviewTextValue;
    } else {
      overviewEl.style.display = "block";
      overviewText.textContent = NO_DATA_LABEL;
    }
  }

  if (labelEl) {
    labelEl.textContent = formatInsightLabel(snapshot, index);
  }
}

async function loadInsightHistory(windowKey = "24h") {
  if (insightHistoryLoading) return;
  insightHistoryLoading = true;
  const historyLimit = 10;
  try {
    const res = await fetch(`${WORKER_URL}/api/ranking-history?window=${encodeURIComponent(windowKey)}&limit=${historyLimit}`, { cache: "no-store" });
    if (res.ok) {
      const json = await res.json();
      if (Array.isArray(json.history)) {
        insightHistory = json.history;
      } else if (json && json.items) {
        insightHistory = [{ payload: json, timestamp: Math.floor(Date.now() / 1000) }];
      } else {
        insightHistory = [];
      }
    } else {
      insightHistory = [];
    }
  } catch (e) {
    insightHistory = [];
  } finally {
    insightHistoryLoading = false;
    insightHistoryLoaded = true;
  }

  let latestSnapshot = null;
  if (latestData) {
    latestSnapshot = { payload: latestData, timestamp: Math.floor(Date.now() / 1000) };
    const latestUpdatedAt = getSnapshotUpdatedAt(latestSnapshot);
    const hasLatest = latestUpdatedAt
      ? insightHistory.some((snap) => getSnapshotUpdatedAt(snap) === latestUpdatedAt)
      : false;
    const latestTime = getSnapshotTime(latestSnapshot);
    const firstTime = getSnapshotTime(insightHistory[0]);
    if (!hasLatest && latestTime && (!firstTime || latestTime > firstTime)) {
      insightHistory = [latestSnapshot, ...insightHistory];
    }
  }

  if (insightHistory.length === 0 && latestSnapshot) {
    insightHistory = [latestSnapshot];
  }

  if (insightHistory.length > historyLimit) {
    insightHistory = insightHistory.slice(0, historyLimit);
  }

  const safeIndex = Math.min(insightHistoryIndex, insightHistory.length - 1);
  insightHistoryIndex = Math.max(0, safeIndex);

  if (insightHistory[insightHistoryIndex]) {
    applyInsightSnapshot(insightHistory[insightHistoryIndex], insightHistoryIndex);
  }
  updateInsightControls();
}

function bindInsightControls() {
  if (insightHistoryBound) return;
  const prevBtn = document.getElementById("overview-history-prev");
  const nextBtn = document.getElementById("overview-history-next");
  if (prevBtn) {
    prevBtn.addEventListener("click", () => {
      if (insightHistoryIndex + 1 >= insightHistory.length) return;
      insightHistoryIndex += 1;
      applyInsightSnapshot(insightHistory[insightHistoryIndex], insightHistoryIndex);
      updateInsightControls();
    });
  }
  if (nextBtn) {
    nextBtn.addEventListener("click", () => {
      if (insightHistoryIndex <= 0) return;
      insightHistoryIndex -= 1;
      applyInsightSnapshot(insightHistory[insightHistoryIndex], insightHistoryIndex);
      updateInsightControls();
    });
  }
  insightHistoryBound = true;
}

function updateInsightControls() {
  const prevBtn = document.getElementById("overview-history-prev");
  const nextBtn = document.getElementById("overview-history-next");
  const labelEl = document.getElementById("overview-history-label");
  if (prevBtn) {
    prevBtn.disabled = insightHistoryIndex + 1 >= insightHistory.length;
  }
  if (nextBtn) {
    nextBtn.disabled = insightHistoryIndex <= 0;
  }
  if (labelEl && insightHistory.length === 0) {
    labelEl.textContent = "--";
  }
}

// Tab Switching
document.addEventListener("DOMContentLoaded", () => {
  // --- Slide Menu Logic ---
  const menuToggle = document.getElementById("menu-toggle");
  const menuClose = document.getElementById("menu-close");
  const menuOverlay = document.getElementById("menu-overlay");
  const slideMenu = document.getElementById("slide-menu");

  function toggleMenu(show) {
    if (show) {
      slideMenu.classList.add("active");
      menuOverlay.classList.add("active");
    } else {
      slideMenu.classList.remove("active");
      menuOverlay.classList.remove("active");
    }
  }

  if (menuToggle) menuToggle.addEventListener("click", () => toggleMenu(true));
  if (menuClose) menuClose.addEventListener("click", () => toggleMenu(false));
  if (menuOverlay) menuOverlay.addEventListener("click", () => toggleMenu(false));

  // --- Brief Guide Modal ---
  const guideOpen = document.getElementById("brief-guide-open");
  const guideOverlay = document.getElementById("brief-guide-overlay");
  const guideClose = document.getElementById("brief-guide-close");

  const openGuide = () => {
    if (guideOverlay) guideOverlay.style.display = "flex";
  };

  const closeGuide = () => {
    if (guideOverlay) guideOverlay.style.display = "none";
  };

  if (guideOpen && guideOverlay && guideClose) {
    guideOpen.addEventListener("click", openGuide);
    guideClose.addEventListener("click", closeGuide);
    guideOverlay.addEventListener("click", (e) => {
      if (e.target === guideOverlay) closeGuide();
    });
    document.addEventListener("keydown", (e) => {
      if (e.key === "Escape" && guideOverlay.style.display === "flex") {
        closeGuide();
      }
    });
  }

  // --- View Switching Logic ---
  const allViews = ["view-dashboard", "view-topics", "view-invest-brief", "view-ongi-greed", "view-about"];

  function switchView(targetId) {
    // Hide all
    allViews.forEach(id => {
      const el = document.getElementById(id);
      if (el) el.style.display = "none";
    });

    // Show target
    const targetEl = document.getElementById(`view-${targetId}`);
    if (targetEl) {
      if (targetId === 'dashboard') targetEl.style.display = "grid"; // Dashboard uses grid
      else targetEl.style.display = "block";
    }

    // Update Header Tabs Focus
    document.querySelectorAll(".nav-tab").forEach(t => {
      if (t.dataset.tab === targetId) t.classList.add("active");
      else t.classList.remove("active");
    });

    // Specific Logics
    if (targetId === 'topics') {
      setTimeout(() => {
        bindInsightControls();
        if (!insightHistoryLoaded) {
          loadInsightHistory("24h");
        } else if (insightHistory[insightHistoryIndex]) {
          applyInsightSnapshot(insightHistory[insightHistoryIndex], insightHistoryIndex);
        }
        renderWordCloud();
        renderPolymarket(currentPolymarket);
      }, 50);
    } else if (targetId === 'ongi-greed') {
      fetchOngiHistory();
    } else if (targetId === 'invest-brief') {
      bindInvestBriefControls();
      const renderNow = () => {
        if (latestData) renderInvestBrief(latestData);
      };
      if (!insightHistoryLoaded) {
        loadInsightHistory("24h").then(renderNow);
      } else {
        renderNow();
      }
    }

    // Close menu if open
    toggleMenu(false);

    // Scroll to top
    window.scrollTo({ top: 0, behavior: 'smooth' });
  }

  // Bind to Header Tabs
  document.querySelectorAll(".nav-tab").forEach(btn => {
    btn.addEventListener("click", () => switchView(btn.dataset.tab));
  });

  // Bind to Menu Items
  document.querySelectorAll(".slide-menu-item").forEach(item => {
    item.addEventListener("click", (e) => {
      e.preventDefault();
      switchView(item.dataset.target);
    });
  });

  // Set initial active tab (dashboard by default)
  const initialTabBtn = document.querySelector('.nav-tab[data-tab="dashboard"]');
  if (initialTabBtn) {
    switchView('dashboard');
  } else {
    const fallbackTab = document.querySelector('.nav-tab[data-tab="invest-brief"]');
    if (fallbackTab) {
      switchView('invest-brief');
    }
  }

  loadChart("SPX"); // Default
  main();
  // Refresh every 30s for Monitor Mode
  setInterval(main, 30000);
});

// ... (rest of main function remains until end of file)

async function fetchOngiHistory() {
  try {
    const res = await fetch(`${WORKER_URL}/api/ongi-history`);
    if (!res.ok) return;
    const history = await res.json();
    renderOngiHistoryChart(history);
  } catch (err) {
    console.error("History fetch failed", err);
  }
}

function renderOngiHistoryChart(history) {
  const canvas = document.getElementById("ongi-history-chart");
  if (!canvas) return;
  const ctx = canvas.getContext("2d");

  if (ongiHistoryChart) {
    ongiHistoryChart.destroy();
  }

  // Format Data
  // Format Data
  const labels = history.map(h => {
    const d = new Date(h.timestamp * 1000);
    const month = d.getMonth() + 1;
    const date = d.getDate();
    const hours = d.getHours();
    const mins = String(d.getMinutes()).padStart(2, '0');
    return `${month}/${date} ${hours}:${mins}`;
  });
  const dataPoints = history.map(h => h.score);

  // Gradient
  const gradient = ctx.createLinearGradient(0, 0, 0, 300);
  gradient.addColorStop(0, 'rgba(0, 212, 255, 0.4)'); // Cyan Top
  gradient.addColorStop(1, 'rgba(0, 0, 0, 0)');   // Fade Bottom

  ongiHistoryChart = new Chart(ctx, {
    type: 'line',
    data: {
      labels: labels,
      datasets: [{
        label: 'Sentiment Score',
        data: dataPoints,
        borderColor: '#00d4ff', // Cyan
        backgroundColor: gradient,
        borderWidth: 2,
        tension: 0.4, // Smooth Spline
        pointBackgroundColor: '#111',
        pointBorderColor: '#00ff88', // Green points
        pointBorderWidth: 2,
        pointRadius: 3,
        pointHoverRadius: 6,
        fill: true
      }]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      scales: {
        y: {
          min: 0,
          max: 100,
          grid: { color: 'rgba(255,255,255,0.05)' },
          ticks: {
            color: '#888',
            font: { family: '"JetBrains Mono", sans-serif' }
          }
        },
        x: {
          grid: { display: false },
          ticks: {
            color: '#888',
            maxTicksLimit: 8,
            font: { family: '"JetBrains Mono", sans-serif' },
            callback: function (val, index, ticks) {
              const label = this.getLabelForValue(val);
              const datePart = label.split(" ")[0];

              // Always show first and last
              if (index === 0 || index === ticks.length - 1) {
                return datePart;
              }

              // Show only if different from previous visible tick
              // Note: Chart.js 'ticks' array contains the actual ticks being rendered
              // But 'val' is the index in the data array.
              // Simple approach: Check difference from PREVIOUS data point
              const prevLabel = this.getLabelForValue(ticks[index - 1].value);
              const prevDate = prevLabel.split(" ")[0];

              if (datePart === prevDate) {
                return ""; // Hide duplicate
              }
              return datePart;
            }
          }
        }
      },
      plugins: {
        legend: { display: false },
        tooltip: {
          mode: 'index',
          intersect: false,
          backgroundColor: 'rgba(0,0,0,0.8)',
          titleColor: '#00d4ff',
          bodyColor: '#fff',
          borderColor: '#333',
          borderWidth: 1,
          callbacks: {
            label: function (context) {
              return `Score: ${context.parsed.y}`;
            }
          }
        }
      },
      interaction: {
        mode: 'nearest',
        axis: 'x',
        intersect: false
      }
    }
  });
}

async function main() {
  const loadingEl = document.getElementById("loading");
  const tableEl = document.getElementById("ranking-table");
  const tbody = document.getElementById("ranking-body");
  const updatedEl = document.getElementById("last-updated");

  try {
    const res = await fetch(`${WORKER_URL}/api/ranking?window=24h`, { cache: "no-store" });
    if (!res.ok) throw new Error("API Connection Failed");

    const data = await res.json();
    console.log("Fetcher API Response:", data); // DEBUG
    if (data.yield_curve) console.log("Yield Curve Data Received:", data.yield_curve);
    else console.warn("No Yield Curve data in response.");

    latestData = data;
    const items = data.items || [];
    currentItems = items;

    if (data.updatedAt && data.updatedAt !== lastHistoryUpdatedAt) {
      lastHistoryUpdatedAt = data.updatedAt;
      loadInsightHistory("24h");
    }

    bindInsightControls();
    if (insightHistoryIndex === 0) {
      applyInsightSnapshot({ payload: data, timestamp: Math.floor(Date.now() / 1000) }, 0);
    }
    updateInsightControls();
    const investView = document.getElementById("view-invest-brief");
    if (investView && investView.style.display !== "none") {
      renderInvestBrief(data);
    }

    // Store topics
    if (data.topics) {
      currentTopics = data.topics.map(t => [t.word, t.count]);
      if (document.getElementById("view-topics").style.display !== "none") {
        renderWordCloud();
      }
    }

    // Polymarket
    if (data.polymarket) {
      currentPolymarket = data.polymarket;
      if (document.getElementById("view-topics").style.display !== "none") {
        renderPolymarket(currentPolymarket);
      }
    }

    // Imakita Sangyo (TL;DR) from latest data
    const imakitaContainer = document.getElementById("imakita-container");
    const imakitaContent = document.getElementById("imakita-content");
    if (data.breaking_news && Array.isArray(data.breaking_news) && data.breaking_news.length > 0) {
      if (imakitaContainer) imakitaContainer.style.display = "block";
      if (imakitaContent) {
        const top3 = data.breaking_news.slice(0, 3);
        imakitaContent.textContent = "";
        top3.forEach((news) => {
          const div = document.createElement("div");
          div.className = "imakita-line";
          div.textContent = news == null ? "" : String(news);
          imakitaContent.appendChild(div);
        });
      }
    } else {
      if (imakitaContainer) imakitaContainer.style.display = "none";
    }

    // NEW: Model Name
    if (data.ai_model) {
      const modelEl = document.getElementById("ai-model-name");
      if (modelEl) modelEl.textContent = `POWERED BY ${data.ai_model.toUpperCase()}`;
    }

    // ... top of file
    let chartWidget = null;
    let radarChart = null; // NEW GLOBAL
    // ...

    // Fear & Ongi Update
    if (data.fear_greed !== undefined) {
      updateFearOngi(data.fear_greed);

      // CNN Fear & Greed (NEW)
      const cnnEl = document.getElementById("cnn-fear-greed-label");
      if (cnnEl && data.cnn_fear_greed) {
        cnnEl.textContent = `🇺🇸 CNN Fear & Greed: ${data.cnn_fear_greed.score} (${data.cnn_fear_greed.rating})`;
      }
    }

    // NEW: Radar Chart Update
    if (data.radar) {
      updateRadarChart(data.radar);
    } else {
      // Init with empty/random data for visualization if missing
      updateRadarChart({ hype: 5, panic: 5, faith: 5, gamble: 5, iq: 5 });
    }

    // NEW: DOUGHCON
    if (data.doughcon) {
      updateDoughcon(data.doughcon);
    }

    // NEW: SAHM RULE
    if (data.sahm_rule) {
      updateSahmRule(data.sahm_rule);
    }

    // NEW: YIELD CURVE
    if (data.yield_curve) {
      updateYieldCurve(data.yield_curve);
    }

    // NEW: CRYPTO FEAR & GREED
    if (data.crypto_fear_greed) {
      updateCryptoFG(data.crypto_fear_greed);
    }

    // NEW: CRYPTO FEAR & GREED
    if (data.crypto_fear_greed) {
      updateCryptoFG(data.crypto_fear_greed);
    }

    // AI Overview
    // ...

    function updateRadarChart(radarData) {
      const canvas = document.getElementById('sentiment-radar');
      if (!canvas) return;
      const ctx = canvas.getContext('2d');

      // Default values if missing
      const dataValues = [
        radarData.hype || 0,
        radarData.panic || 0,
        radarData.faith || 0,
        radarData.gamble || 0,
        radarData.iq || 0
      ];

      if (radarChart) {
        radarChart.data.datasets[0].data = dataValues;
        radarChart.update();
        return;
      }

      radarChart = new Chart(ctx, {
        type: 'radar',
        data: {
          labels: ['熱狂 (HYPE)', '阿鼻叫喚 (PANIC)', '信仰心 (FAITH)', '射幸心 (GAMBLE)', '知性 (IQ)'],
          datasets: [{
            label: '5ch Sentiment',
            data: dataValues,
            backgroundColor: 'rgba(0, 255, 136, 0.2)', // Neon Greenish fill
            borderColor: '#00ff88',
            pointBackgroundColor: '#fff',
            pointBorderColor: '#00ff88',
            pointHoverBackgroundColor: '#fff',
            pointHoverBorderColor: '#00ff88',
            borderWidth: 2,
            pointRadius: 4,
            pointHoverRadius: 6
          }]
        },
        options: {
          responsive: true,
          maintainAspectRatio: false,
          scales: {
            r: {
              angleLines: {
                color: 'rgba(255, 255, 255, 0.2)'
              },
              grid: {
                color: 'rgba(255, 255, 255, 0.1)'
              },
              pointLabels: {
                color: '#eee',
                font: {
                  size: 11,
                  family: '"JetBrains Mono", sans-serif',
                  weight: 'bold'
                }
              },
              ticks: {
                display: false,
                backdropColor: 'transparent'
              },
              suggestedMin: 0,
              suggestedMax: 10
            }
          },
          plugins: {
            legend: { display: false }
          }
        }
      });
    }

  const ongiCommentEl = document.getElementById("fear-ongi-comment");
  if (ongiCommentEl) {
    const ongiText = getOngiCommentText(data);
    const overviewText = getOverviewText(data);
    ongiCommentEl.textContent = ongiText || overviewText || NO_DATA_LABEL;
  }

    // Comparative Insight Logic
    const compToggle = document.getElementById("comparative-toggle");
    const compContent = document.getElementById("comparative-content");
    const compText = document.getElementById("comparative-text");

    // Populate data if exists (check metadata directly or data root depending on API structure)
    // Based on storage.ts: payload.comparative_insight is at root of response object due to flattening in getRanking
    const insightText = data.comparative_insight;

    if (compToggle && compText) {
      if (insightText) {
        compText.innerHTML = safeText(insightText).replace(/\n/g, "<br>"); // Simple formatting
        // Add click listener if not already added (simple check: clone or just ensure idempotent)
        // Better: Remove old listener or just assign onclick
        compToggle.onclick = () => {
          const isHidden = compContent.style.display === "none";
          compContent.style.display = isHidden ? "block" : "none";
          compToggle.classList.toggle("open", isHidden);
        };
        // Ensure container is visible (if we hid it by default in CSS, but here we just populate)
      } else {
        compText.textContent = "Analyzing global trends...";
        // Optional: Hide entire container if no data?
        // document.querySelector(".comparative-insight-container").style.display = "none";
      }
    }

    // Update timestamp
    if (data.updatedAt) {
      const date = new Date(data.updatedAt);
      updatedEl.textContent = `LAST: ${date.toLocaleTimeString('ja-JP', { hour: '2-digit', minute: '2-digit' })}`;
    }

    // Store global data
    let globalData = data;
    let currentSource = "5ch";

    // Source Tabs Logic
    document.querySelectorAll(".source-tab").forEach(tab => {
      tab.addEventListener("click", () => {
        document.querySelectorAll(".source-tab").forEach(t => t.classList.remove("active"));
        tab.classList.add("active");
        currentSource = tab.dataset.source;
        updateTable();
      });
    });

    function updateTable() {
      if (!globalData) return;

      let items = [];
      if (currentSource === "reddit") {
        items = globalData.reddit_rankings || [];
      } else {
        items = globalData.items || [];
      }

      renderTable(items, currentSource);
    }

    function renderTable(items, source) {
      const tbody = document.getElementById("ranking-body");
      const loadingEl = document.getElementById("loading");
      const tableEl = document.getElementById("ranking-table");

      if (!items || items.length === 0) {
        loadingEl.textContent = "No Data Available";
        loadingEl.style.display = "block";
        tableEl.style.display = "none";
        return;
      }

      loadingEl.style.display = "none";
      tableEl.style.display = "table";
      tbody.innerHTML = "";

      const maxCount = items.length > 0 ? (Number(items[0].count) || 1) : 1;

      // Set initial chart if 5ch (main workflow), or just first item
      if (items.length > 0 && source === "5ch") {
        // Keep existing chart logic? Or update chart on tab switch?
        // Maybe better to only update chart on CLICK to avoid jumping around freely
      }

      items.forEach((item, index) => {
        // Rank logic: ApeWisdom provides 'rank', 5ch uses index+1.
        // For consistency, use index+1 unless 'rank' property is explicit and valuable?
        // Actually index+1 is safer for visual list order.
        const rank = index + 1;

        const row = document.createElement("tr");
        row.className = "ranking-row";
        if (index === 0) row.classList.add("selected");
        if (rank <= 3) row.classList.add(`rank-${rank}`);

        const itemCount = Number(item.count) || 0;
        const barPercent = Math.max((itemCount / maxCount) * 100, 1);
        const safeTicker = safeText(item.ticker);
        const safeCount = safeText(itemCount);
        const logoTicker = encodeURIComponent(item.ticker == null ? "" : String(item.ticker));

        // Sentiment / Mood
        let moodHtml = "";

        if (source === "5ch") {
          let moodClass = "neutral";
          let moodIcon = "😐";
          let moodText = "NEUTRAL";
          const score = item.sentiment || 0;
          if (score >= 0.1) {
            moodClass = "bullish"; moodIcon = "🚀"; moodText = "BULL";
          } else if (score <= -0.1) {
            moodClass = "bearish"; moodIcon = "🐻"; moodText = "BEAR";
          }
          moodHtml = `<span class="mood-badge ${moodClass}">${moodIcon} ${moodText}</span>`;
        } else if (source === "reddit") {
          // Reddit specific badge (e.g. Upvotes)
          const ups = new Intl.NumberFormat('en-US', { notation: "compact" }).format(item.upvotes || 0);
          const safeUps = safeText(ups);
          moodHtml = `<span class="mood-badge" style="border-color:#ff4500; color:#ff4500;">🔥 ${safeUps} Ups</span>`;
        }

        // Trend Logic
        let trendHtml = "";
        let delta = item.rank_delta; // 5ch

        if (source === "reddit" && item.rank_24h_ago) {
          // Calculate delta for Reddit: (Prev - Current)
          // e.g. Prev 5, Curr 1 -> Delta +4
          delta = item.rank_24h_ago - item.rank;
        }

        if (item.is_new) {
          trendHtml = `<span class="trend-new">NEW</span>`;
        } else if (delta !== undefined && delta !== 0 && delta !== null) {
          const isUp = delta > 0;
          const arrow = isUp ? "▲" : "▼";
          const colorClass = isUp ? "trend-up" : "trend-down";
          trendHtml = `<span class="${colorClass}">${arrow}</span>`;
        } else {
          trendHtml = `<span class="trend-stay">─</span>`;
        }

        row.innerHTML = `
                <td>
                <div style="display:flex; flex-direction:column; align-items:center; line-height:1.1; gap:3px;">
                    <span>${rank}</span>
                    ${trendHtml}
                </div>
                </td>
                <td>
                <div class="ticker-cell">
                    <img class="ticker-icon" src="https://assets.parqet.com/logos/symbol/${logoTicker}?format=png" loading="lazy" onerror="this.style.display='none'">
                    <div class="ticker-info">
                        <span class="ticker-name">${safeTicker}</span>
                        ${moodHtml}
                    </div>
                </div>
                </td>
                <td>
                <div class="count-cell">
                    <div class="count-val">${safeCount}</div>
                    <div class="bar-container">
                    <div class="bar-fill" style="width: ${barPercent}%"></div>
                    </div>
                </div>
                </td>
            `;

        row.addEventListener("click", () => {
          document.querySelectorAll(".ranking-row").forEach(r => r.classList.remove("selected"));
          row.classList.add("selected");
          loadChart(item.ticker);
          document.querySelector(".chart-panel").classList.add("active");
        });

        tbody.appendChild(row);
      });
    }

    // Initial render
    updateTable();
  } catch (err) {
    console.error(err);
    loadingEl.textContent = "System Offline";
    loadingEl.style.color = "#ff6b6b";
  }
}

function renderWordCloud() {
  const canvas = document.getElementById("topic-canvas");
  const container = document.getElementById("wordcloud-container");
  const loading = document.getElementById("topic-loading");

  if (!currentTopics || currentTopics.length === 0) {
    loading.textContent = "No trending keywords found.";
    return;
  }

  // Resize canvas to fit container
  canvas.width = container.offsetWidth;
  canvas.height = container.offsetHeight;

  // Calculate scaling factor
  const maxCount = Math.max(...currentTopics.map(t => t[1]));

  // Mobile Optimization
  const isMobile = window.innerWidth <= 768;
  const baseSize = isMobile ? 16 : (canvas.width > 600 ? 40 : 24); // PC: 60->40, Tablet: 30->24
  const scale = baseSize / maxCount;

  const displayList = isMobile ? currentTopics.slice(0, 40) : currentTopics; // Limit items on mobile
  const gridSize = isMobile ? 18 : 10; // Larger grid = Faster but less precise
  const rotationRatio = isMobile ? 0 : 0.5; // No rotation on mobile to save calc time

  WordCloud(canvas, {
    list: displayList,
    gridSize: gridSize,
    weightFactor: function (size) {
      return Math.max((size * scale) * 2.5, 12); // Minimum 12px
    },
    fontFamily: '"Inter", "JetBrains Mono", sans-serif',
    color: function (word, weight) {
      const colors = ['#00f0ff', '#ffd700', '#ffffff', '#ff003c', '#adff00'];
      return colors[Math.floor(Math.random() * colors.length)];
    },
    rotateRatio: rotationRatio,
    rotationSteps: 2,
    backgroundColor: 'transparent',
    shrinkToFit: true,
    drawOutOfBound: false,
    wait: isMobile ? 10 : 0 // Small delay to unblock UI on mobile
  });

  loading.style.display = 'none';
}

function updateFearOngi(score) {
  const scoreEl = document.getElementById("fear-ongi-score");
  const labelEl = document.getElementById("fear-ongi-label");
  const needleEl = document.getElementById("gauge-needle");
  const commentEl = document.getElementById("fear-ongi-comment");

  if (score === undefined || score === null) {
    scoreEl.textContent = "--";
    labelEl.textContent = "No Data";
    return;
  }

  scoreEl.textContent = score;

  // Rotate Needle: 0-100 => -90deg to +90deg
  const rotation = (Math.max(0, Math.min(100, score)) / 100 * 180) - 90;
  if (needleEl) {
    needleEl.style.transform = `rotate(${rotation}deg)`;
  }

  let label = "NEUTRAL";
  let color = "#ffff00";

  if (score <= 25) {
    label = "EXTREME ONGI (Despair)";
    color = "#ff4444";
  } else if (score <= 45) {
    label = "ONGI (Fear)";
    color = "#ff8844";
  } else if (score >= 75) {
    label = "EXTREME GREED";
    color = "#00ff88";
  } else if (score >= 55) {
    label = "GREED";
    color = "#ccff00";
  }

  labelEl.textContent = label;
  labelEl.style.color = color;

  // AI Comment logic moved to main() to use actual AI summary
  // if (commentEl) { ... }
}

function loadChart(ticker) {
  if (currentTicker === ticker) return;
  currentTicker = ticker;

  // Cleanup if needed (TradingView widget replaces content, so mostly safe)
  // Re-render widget
  new TradingView.widget({
    "autosize": true,
    "symbol": ticker,
    "interval": "D",
    "timezone": "Asia/Tokyo",
    "theme": "dark",
    "style": "1",
    "locale": "ja",
    "toolbar_bg": "#f1f3f6",
    "enable_publishing": false,
    "allow_symbol_change": true,
    "container_id": "tradingview_widget",
    "hide_side_toolbar": false
  });
}

// Add global click handler to close chart on mobile when clicking outside
document.addEventListener('click', (e) => {
  const chartPanel = document.querySelector(".chart-panel");
  const isRow = e.target.closest(".ranking-row");
  const isChart = e.target.closest(".chart-panel");

  // If clicking outside chart AND outside a row, close chart
  if (!isRow && !isChart && chartPanel.classList.contains("active")) {
    chartPanel.classList.remove("active");
    // Deselect rows
    document.querySelectorAll(".ranking-row").forEach(r => r.classList.remove("selected"));
  }
});

/* Polymarket Rendering */
function renderPolymarket(data) {
  const container = document.getElementById("polymarket-container");
  const list = document.getElementById("polymarket-list");

  if (!data || data.length === 0) {
    if (container) container.style.display = "none";
    return;
  }

  if (container) container.style.display = "block";
  if (list) {
    list.innerHTML = "";

    data.forEach(item => {
      // Outcomes string "Yes: 80% | No: 20%" -> split
      let outcomesHtml = "";
      if (item.outcomes) {
        outcomesHtml = item.outcomes.split(" | ").map(o => {
          const parts = o.split(": ");
          const label = parts[0];
          const val = parts[1] || "";
          return `<div class="poly-outcome"><span>${safeText(label)}</span><strong>${safeText(val)}</strong></div>`;
        }).join("");
      }

      // Volume format
      let vol = "0";
      try {
        vol = new Intl.NumberFormat('en-US', { style: 'currency', currency: 'USD', maximumFractionDigits: 0, notation: "compact" }).format(item.volume || 0);
      } catch (e) { }

      const card = document.createElement("a");
      card.className = "poly-card poly-card-link";
      card.href = safeUrl(item.url);
      card.target = "_blank";
      card.rel = "noopener noreferrer";
      card.style.textDecoration = "none";

      const safeTitle = safeText(item.title_ja || item.title || "");
      card.innerHTML = `
        <div class="poly-title">${safeTitle}</div>
        <div class="poly-outcomes-list">
          ${outcomesHtml}
        </div>
        <div class="poly-volume">Vol: <span>${vol}</span></div>
      `;
      list.appendChild(card);
    });
  }
}

// Mobile Tooltip Logic
document.addEventListener('click', (e) => {
  const container = e.target.closest('.tooltip-container') || e.target.closest('.header-help-container');
  if (container && window.innerWidth <= 768) {
    e.preventDefault();
    e.stopPropagation();

    // Try both selectors
    const textEl = container.querySelector('.tooltip-text') || container.querySelector('.header-help-text');

    if (textEl) {
      const text = textEl.innerHTML;
      const overlay = document.getElementById('mobile-tooltip-overlay');
      const modalText = document.getElementById('mobile-tooltip-text');
      if (overlay && modalText) {
        modalText.innerHTML = text;
        overlay.style.display = 'flex';
      }
    }
  }
});

// Close modal when clicking outside content
const tooltipOverlay = document.getElementById('mobile-tooltip-overlay');
if (tooltipOverlay) {
  tooltipOverlay.addEventListener('click', (e) => {
    if (e.target === tooltipOverlay) {
      tooltipOverlay.style.display = 'none';
    }
  });
}

function updateDoughcon(data) {
  const levelEl = document.getElementById("doughcon-level");
  const descEl = document.getElementById("doughcon-desc");

  if (!data || !levelEl) return;

  levelEl.textContent = data.description;
  if (descEl) descEl.textContent = `DEFCON ${data.level}`;

  // Color Logic (1=Red, 2=Orange, 3=Yellow, 4=Blue, 5=Green)
  let color = "#fff";
  const level = parseInt(data.level);

  if (level === 1) color = "#ff0000";       // Critical (Red)
  else if (level <= 2) color = "#ff6600";   // High (Orange)
  else if (level <= 3) color = "#ffff00";   // High (Yellow)
  else if (level <= 4) color = "#00aaff";   // Elevated (Blue)
  else color = "#00ff00";                   // Low (Green)

  levelEl.style.color = color;
  levelEl.style.textShadow = `0 0 8px ${color}`;
}

function updateSahmRule(data) {
  const levelEl = document.getElementById("sahm-level");
  const descEl = document.getElementById("sahm-desc");

  if (!data || !levelEl) return;

  levelEl.textContent = data.state.toUpperCase();
  if (descEl) descEl.textContent = `Value: ${data.value.toFixed(2)}`;

  // Color Logic (0.50+ = Danger/Red, 0.30+ = Warning/Yellow, <0.30 = Safe/Green)
  let color = "#00ff00";
  if (data.value >= 0.50) color = "#ff0000";
  else if (data.value >= 0.30) color = "#ffff00";

  levelEl.style.color = color;
  levelEl.style.textShadow = `0 0 10px ${color}55`;
}

function updateYieldCurve(data) {
  const levelEl = document.getElementById("yield-level");
  const descEl = document.getElementById("yield-desc");

  if (!data || !levelEl) {
    console.warn("updateYieldCurve: Missing data or element", { data, levelEl });
    return;
  }

  levelEl.textContent = data.state.toUpperCase();
  if (descEl) descEl.textContent = `Value: ${data.value.toFixed(2)}%`;

  // Color Logic (Inverted (<0) = Danger/Red, Flattening (<0.2) = Warning/Yellow, Normal = Safe/Green)
  let color = "#00ff00"; // Green
  if (data.value < 0) color = "#ff0000";       // Inverted
  else if (data.value < 0.2) color = "#ffff00"; // Flattening

  levelEl.style.color = color;
  levelEl.style.textShadow = `0 0 10px ${color}55`;
}

function updateCryptoFG(data) {
  const levelEl = document.getElementById("crypto-fg-level");
  const descEl = document.getElementById("crypto-fg-desc");

  if (!data || !levelEl) return;

  levelEl.textContent = data.classification.toUpperCase();
  if (descEl) descEl.textContent = `Value: ${data.value}`;

  // Color Logic (Typically 0-24 Extreme Fear, 25-49 Fear, 50-74 Greed, 75-100 Extreme Greed)
  let color = "#ffffff";
  const val = parseInt(data.value);

  if (val <= 25) color = "#ff0000";       // Extreme Fear (Red)
  else if (val <= 46) color = "#ff6600";  // Fear (Orange)
  else if (val <= 54) color = "#ffff00";  // Neutral (Yellow)
  else if (val <= 75) color = "#ccff00";  // Greed (Light Green)
  else color = "#00ff00";                 // Extreme Greed (Green)

  levelEl.style.color = color;
  levelEl.style.textShadow = `0 0 10px ${color}55`;
}

// History Chart Toggle
function toggleHistoryChart() {
  const content = document.getElementById('history-content');
  const icon = document.getElementById('history-toggle-icon');

  if (!content || !icon) return;

  const isCollapsed = content.classList.contains('collapsed');

  if (isCollapsed) {
    // Expand
    content.classList.remove('collapsed');
    icon.style.transform = 'rotate(0deg)';
  } else {
    // Collapse
    content.classList.add('collapsed');
    icon.style.transform = 'rotate(-90deg)';
  }
}



document.addEventListener("DOMContentLoaded", () => {
  // --- Back to Top Logic ---
  const backToTopBtn = document.getElementById("back-to-top");

  if (backToTopBtn) {
    window.addEventListener("scroll", () => {
      if (window.scrollY > 300) {
        backToTopBtn.classList.add("show");
      } else {
        backToTopBtn.classList.remove("show");
      }
    });

    backToTopBtn.addEventListener("click", () => {
      window.scrollTo({
        top: 0,
        behavior: "smooth"
      });
    });
  }

  // --- Swipe Navigation Logic ---
  const tabs = ['dashboard', 'invest-brief', 'topics', 'ongi-greed'];

  // Use a minimum threshold to avoid accidental swipes while scrolling vertically
  const SWIPE_THRESHOLD = 50;
  const ANGLE_THRESHOLD = 30; // Degrees to allow for vertical tolerance

  let touchStartX = 0;
  let touchStartY = 0;
  let touchEndX = 0;
  let touchEndY = 0;

  document.body.addEventListener('touchstart', e => {
    touchStartX = e.changedTouches[0].screenX;
    touchStartY = e.changedTouches[0].screenY;
  }, { passive: true });

  document.body.addEventListener('touchend', e => {
    touchEndX = e.changedTouches[0].screenX;
    touchEndY = e.changedTouches[0].screenY;
    handleSwipe();
  }, { passive: true });

  function handleSwipe() {
    const diffX = touchEndX - touchStartX;
    const diffY = touchEndY - touchStartY;

    // Check if horizontal distance greater than threshold
    if (Math.abs(diffX) < SWIPE_THRESHOLD) return;

    // Check angle (if mostly vertical, ignore)
    const angle = Math.abs((Math.atan2(diffY, diffX) * 180) / Math.PI);
    // Left swipe (0-30 or -30 to 0) or Right swipe (150-180 or -180 to -150)
    // Angles: 0 is Right, 180 is Left, 90 is Down, -90 is Up

    // We only care if swipe is mostly horizontal
    // Left Swipe: diffX < 0. Angle approx 180.
    // Right Swipe: diffX > 0. Angle approx 0.

    // Simplified: Check if horizontal movement is significantly larger than vertical
    if (Math.abs(diffX) <= Math.abs(diffY)) return;

    // Find current active tab
    const activeTab = document.querySelector('.nav-tab.active');
    if (!activeTab) return;

    const currentTabName = activeTab.dataset.tab;
    const currentIndex = tabs.indexOf(currentTabName);

    if (currentIndex === -1) return; // Not on a main tab

    let nextIndex = -1;

    if (diffX > 0) {
      // Swiped Right -> Go to Previous Tab (e.g. Topics -> Dashboard)
      if (currentIndex > 0) nextIndex = currentIndex - 1;
    } else {
      // Swiped Left -> Go to Next Tab (e.g. Dashboard -> Topics)
      if (currentIndex < tabs.length - 1) nextIndex = currentIndex + 1;
    }

    if (nextIndex !== -1) {
      const nextTabName = tabs[nextIndex];
      const nextTabBtn = document.querySelector(`.nav-tab[data-tab="${nextTabName}"]`);
      if (nextTabBtn) {
        nextTabBtn.click(); // Reuse existing click handler
      }
    }
  }
});
