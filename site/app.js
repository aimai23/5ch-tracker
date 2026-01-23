const WORKER_URL = "https://5ch-tracker.arakawa47.workers.dev";
let chartWidget = null;
let radarChart = null;
let ongiHistoryChart = null; // NEW GLOBAL
let currentTicker = null;
let currentTopics = [];
let currentItems = [];
let currentPolymarket = null;

// Tab Switching
document.addEventListener("DOMContentLoaded", () => {
  const tabBtns = document.querySelectorAll(".tab-btn");

  tabBtns.forEach(btn => {
    btn.addEventListener("click", () => {
      // Remove active class
      tabBtns.forEach(b => b.classList.remove("active"));
      btn.classList.add("active");

      const tabId = btn.getAttribute("data-tab");

      // Hide all views
      document.getElementById("view-dashboard").style.display = "none";
      document.getElementById("view-topics").style.display = "none";
      document.getElementById("view-ongi-greed").style.display = "none";

      // Show selected view
      if (tabId === 'dashboard') {
        document.getElementById("view-dashboard").style.display = "grid";
      } else if (tabId === 'topics') {
        document.getElementById("view-topics").style.display = "block";
        setTimeout(() => {
          renderWordCloud();
          if (currentPolymarket) renderPolymarket(currentPolymarket);
        }, 50);
      } else if (tabId === 'ongi_greed') {
        document.getElementById("view-ongi-greed").style.display = "block";
        fetchOngiHistory(); // NEW CALL
      }
    });
  });

  // Set initial active tab (dashboard by default)
  const initialTabBtn = document.querySelector('.tab-btn[data-tab="dashboard"]');
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
            callback: function (val, index) {
              // Show only Date part (split by space)
              const label = this.getLabelForValue(val);
              return label.split(" ")[0];
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

    // Breaking News Ticker
    const tickerContainer = document.getElementById("breaking-news-container");
    const tickerText = document.getElementById("news-marquee");

    if (data.breaking_news && Array.isArray(data.breaking_news) && data.breaking_news.length > 0) {
      if (tickerContainer) tickerContainer.style.display = "flex";
      if (tickerText) {
        // Join with spacing - Duplicate for length safety
        tickerText.textContent = data.breaking_news.join("        ") + "        " + data.breaking_news.join("        ");
      }
    } else {
      if (tickerContainer) tickerContainer.style.display = "none";
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

    // Update timestamp
    if (data.updatedAt) {
      const date = new Date(data.updatedAt);
      updatedEl.textContent = `LAST: ${date.toLocaleTimeString('ja-JP', { hour: '2-digit', minute: '2-digit' })}`;
    }

    loadingEl.style.display = "none";
    tableEl.style.display = "table";

    const maxCount = items.length > 0 ? items[0].count : 1;

    // Set initial ticker (first item or default)
    if (items.length > 0) {
      loadChart(items[0].ticker);
    } else {
      loadChart("SPX");
    }

    tbody.innerHTML = ""; // Clear existing

    items.forEach((item, index) => {
      const rank = index + 1;
      const row = document.createElement("tr");
      row.className = "ranking-row";
      if (index === 0) row.classList.add("selected"); // Select first by default
      if (rank <= 3) row.classList.add(`rank-${rank}`);

      // Calculate bar width percentage
      const barPercent = Math.max((item.count / maxCount) * 100, 1);

      // Sentiment Logic
      let moodClass = "neutral";
      let moodIcon = "😐";
      let moodText = "NEUTRAL";
      const score = item.sentiment || 0;

      if (score >= 0.1) {
        moodClass = "bullish";
        moodIcon = "🚀";
        moodText = "BULL";
      } else if (score <= -0.1) {
        moodClass = "bearish";
        moodIcon = "🐻";
        moodText = "BEAR";
      }

      row.innerHTML = `
        <td>${rank}</td>
        <td>
          <div class="ticker-cell">
            <img class="ticker-icon" src="https://assets.parqet.com/logos/symbol/${item.ticker}?format=png" loading="lazy" onerror="this.style.display='none'">
            <div class="ticker-info">
                <span class="ticker-name">${item.ticker}</span>
                <span class="mood-badge ${moodClass}">${moodIcon} ${moodText}</span>
            </div>
          </div>
        </td>
        <td>
          <div class="count-cell">
            <div class="count-val">${item.count}</div>
            <div class="bar-container">
              <div class="bar-fill" style="width: ${barPercent}%"></div>
            </div>
          </div>
        </td>
      `;

      // Click event to update chart
      row.addEventListener("click", () => {
        // Remove select from all
        document.querySelectorAll(".ranking-row").forEach(r => r.classList.remove("selected"));
        // Add to current
        row.classList.add("selected");
        // Update chart
        loadChart(item.ticker);

        // Mobile: Show chart panel
        document.querySelector(".chart-panel").classList.add("active");
      });

      tbody.appendChild(row);
    });

    if (items.length === 0) {
      loadingEl.textContent = "No Data Available";
      loadingEl.style.display = "block";
      tableEl.style.display = "none";
    }

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
  const baseSize = canvas.width > 600 ? 60 : 30; // Base font size
  const scale = baseSize / maxCount;

  WordCloud(canvas, {
    list: currentTopics,
    gridSize: 10,
    weightFactor: function (size) {
      return Math.max((size * scale) * 1.5, 10); // Minimum 10px
    },
    fontFamily: '"Inter", "JetBrains Mono", sans-serif',
    color: function (word, weight) {
      const colors = ['#00f0ff', '#ffd700', '#ffffff', '#ff003c', '#adff00'];
      return colors[Math.floor(Math.random() * colors.length)];
    },
    rotateRatio: 0.5,
    rotationSteps: 2,
    backgroundColor: 'transparent',
    shrinkToFit: true,
    drawOutOfBound: false
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
          return `<div class="poly-outcome"><span>${label}</span><strong>${val}</strong></div>`;
        }).join("");
      }

      // Volume format
      let vol = "0";
      try {
        vol = new Intl.NumberFormat('en-US', { style: 'currency', currency: 'USD', maximumFractionDigits: 0, notation: "compact" }).format(item.volume || 0);
      } catch (e) { }

      const card = document.createElement("a");
      card.className = "poly-card";
      card.href = item.url;
      card.target = "_blank";
      card.style.textDecoration = "none";

      card.innerHTML = `
        <div class="poly-title">${item.title_ja || item.title}</div>
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
  const container = e.target.closest('.tooltip-container');
  if (container && window.innerWidth <= 768) {
    e.preventDefault();
    e.stopPropagation();

    const textEl = container.querySelector('.tooltip-text');
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
