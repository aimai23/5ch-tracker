const WORKER_URL = "https://5ch-tracker.arakawa47.workers.dev";
let chartWidget = null;
let radarChart = null;
let ongiHistoryChart = null; // NEW GLOBAL
let currentTicker = null;
let currentTopics = [];
let currentItems = [];
let currentPolymarket = null;

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

  // --- View Switching Logic ---
  const allViews = ["view-dashboard", "view-topics", "view-ongi-greed", "view-about", "view-trade-indepth"];

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
        renderWordCloud();
        if (currentPolymarket) renderPolymarket(currentPolymarket);
      }, 50);
    } else if (targetId === 'ongi-greed') {
      fetchOngiHistory();
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
    initialTabBtn.classList.add('active');
    document.getElementById("view-dashboard").style.display = "grid";
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

    const items = data.items || [];
    currentItems = items;

    // Store topics
    if (data.topics) {
      currentTopics = data.topics.map(t => [t.word, t.count]);
      // Lazy Render logic: only if visible
      if (document.getElementById("view-topics").style.display !== "none") {
        renderWordCloud();
      }
    }

    // Trade Recommendations
    if (data.trade_recommendations) {
      renderTradeRecommendations(data.trade_recommendations);
    }

    // NEW: Model Name
    if (data.ai_model) {
      const modelEl = document.getElementById("ai-model-name");
      if (modelEl) modelEl.textContent = `POWERED BY ${data.ai_model.toUpperCase()}`;
    }

    // NEW: Polymarket
    if (data.polymarket) {
      currentPolymarket = data.polymarket;
      if (document.getElementById("view-topics").style.display !== "none") {
        renderPolymarket(currentPolymarket);
      }
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

    // Imakita Sangyo (TL;DR)
    const imakitaContainer = document.getElementById("imakita-container");
    const imakitaContent = document.getElementById("imakita-content");

    if (data.breaking_news && Array.isArray(data.breaking_news) && data.breaking_news.length > 0) {
      if (imakitaContainer) imakitaContainer.style.display = "block";
      if (imakitaContent) {
        // Take top 3 items
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

    // AI Overview
    const overviewEl = document.getElementById("market-overview");
    const overviewText = document.getElementById("overview-text");
    const ongiCommentEl = document.getElementById("fear-ongi-comment");

    if (overviewEl) {
      if (data.overview) {
        overviewEl.style.display = "block";
        overviewText.textContent = data.overview;
        if (ongiCommentEl) {
          // Prefer ongi_comment, fallback to overview
          ongiCommentEl.textContent = data.ongi_comment || data.overview;
        }
      } else {
        overviewEl.style.display = "none";
        if (ongiCommentEl) ongiCommentEl.textContent = "Waiting for AI analysis...";
      }
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

function renderTradeRecommendations(tradeData) {
  if (!tradeData) return;

  // Update Rendering for List of Picks
  function renderList(containerId, items, type) {
    const list = document.getElementById(containerId);
    if (!list) return;

    list.textContent = "";
    if (items && items.length > 0) {
      items.forEach(item => {
        const div = document.createElement("div");
        div.className = `trade-card-item ${type}-item`;

        const tickerSpan = document.createElement("span");
        tickerSpan.className = "item-ticker";
        tickerSpan.textContent = item.ticker == null ? "" : String(item.ticker);

        const reasonSpan = document.createElement("span");
        reasonSpan.className = "item-reason";
        reasonSpan.textContent = item.reason == null ? "" : String(item.reason);

        div.appendChild(tickerSpan);
        div.appendChild(reasonSpan);
        list.appendChild(div);
      });
    } else {
      const placeholder = document.createElement("div");
      placeholder.className = "trade-placeholder";
      placeholder.textContent = "None identified.";
      list.appendChild(placeholder);
    }
  }

  // Bullish
  const bullishItems = Array.isArray(tradeData.bullish) ? tradeData.bullish : (tradeData.bullish ? [tradeData.bullish] : []);
  renderList("bullish-list", bullishItems, "bullish");

  // Bearish
  const bearishItems = Array.isArray(tradeData.bearish) ? tradeData.bearish : (tradeData.bearish ? [tradeData.bearish] : []);
  renderList("bearish-list", bearishItems, "bearish");
}

// Ensure global scope access if needed, or just let it exist in the module
window.renderTradeRecommendations = renderTradeRecommendations;

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
  const tabs = ['dashboard', 'topics', 'ongi-greed', 'trade-indepth'];

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
