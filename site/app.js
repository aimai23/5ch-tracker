const WORKER_URL = "https://5ch-tracker.arakawa47.workers.dev";
let chartWidget = null;
let currentTicker = null;
let currentTopics = []; // NEW: Store topics globally
let currentItems = []; // Global for heatmap
let watchlistData = {};

// Tab Switching
document.addEventListener("DOMContentLoaded", () => {
  const tabBtns = document.querySelectorAll(".tab-btn");

  tabBtns.forEach(btn => {
    btn.addEventListener("click", () => {
      tabBtns.forEach(b => b.classList.remove("active"));
      btn.classList.add("active");

      const tabId = btn.getAttribute("data-tab");

      // Hide all
      document.getElementById("view-dashboard").style.display = "none";
      document.getElementById("view-topics").style.display = "none";
      document.getElementById("view-heatmap").style.display = "none";

      // Show selected
      if (tabId === 'dashboard') {
        document.getElementById("view-dashboard").style.display = "grid";
      } else if (tabId === 'topics') {
        document.getElementById("view-topics").style.display = "block";
        // Trigger render after display is handled to ensure canvas sizing works
        setTimeout(renderWordCloud, 100);
      } else if (tabId === 'heatmap') {
        document.getElementById("view-heatmap").style.display = "block";
        renderHeatmap();
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
  fetchWatchlist();
  main();
  // Refresh every 30s for Monitor Mode
  setInterval(main, 30000);
});


async function fetchWatchlist() {
  try {
    const res = await fetch("config/watchlist.json");
    if (res.ok) {
      watchlistData = await res.json();
    }
  } catch (e) {
    console.error("Failed to load watchlist config", e);
  }
}

let currentPrices = {}; // Global for prices

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
    currentItems = items; // Store global
    currentPrices = data.prices || {}; // Store prices global

    // Store topics
    if (data.topics) {
      currentTopics = data.topics.map(t => [t.word, t.count]);
    }

    // AI Overview
    const overviewEl = document.getElementById("market-overview");
    const overviewText = document.getElementById("overview-text");
    if (overviewEl) {
      if (data.overview) {
        overviewEl.style.display = "block";
        overviewText.textContent = data.overview;
      } else {
        overviewEl.style.display = "none";
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

      // Price Info for Table (Optional, tiny)
      // const pInfo = currentPrices[item.ticker];

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

function renderHeatmap() {
  const container = document.getElementById("heatmap-container");
  container.innerHTML = "";

  // Grid for Categories using flex/grid
  container.style.display = "grid";
  container.style.gridTemplateColumns = "repeat(auto-fill, minmax(250px, 1fr))";
  container.style.gap = "1rem";

  Object.entries(watchlistData).forEach(([category, tickers]) => {
    const catEl = document.createElement("div");
    catEl.className = "heatmap-category";
    catEl.style.background = "rgba(255, 255, 255, 0.05)";
    catEl.style.padding = "1rem";
    catEl.style.borderRadius = "8px";
    catEl.style.border = "1px solid #333";

    catEl.innerHTML = `<h3 style="color:var(--accent-cyan); font-size:0.9rem; margin-bottom:0.5rem; text-transform:uppercase;">${category}</h3>`;

    const grid = document.createElement("div");
    grid.className = "heatmap-grid";
    grid.style.display = "grid";
    grid.style.gridTemplateColumns = "repeat(2, 1fr)";
    grid.style.gap = "8px";

    tickers.forEach(ticker => {
      const item = currentItems.find(i => i.ticker === ticker);
      const priceData = currentPrices[ticker]; // LOOKUP PRICE

      let bgStyle = "rgba(43, 43, 60, 0.5)";
      let borderStyle = "1px solid #444";

      // Display Values
      let mainVal = "--%";
      let subVal = "$--";

      // Use Price Data if available
      if (priceData) {
        const chg = priceData.change_percent;
        mainVal = `${chg > 0 ? '+' : ''}${chg}%`;
        subVal = `$${priceData.price}`;

        // Color Logic (S&P 500 Style)
        if (chg > 0) {
          const opacity = 0.3 + Math.min(Math.abs(chg) / 3, 0.7);
          bgStyle = `rgba(0, 160, 80, ${opacity})`;
          borderStyle = "1px solid #4f4";
        } else if (chg < 0) {
          const opacity = 0.3 + Math.min(Math.abs(chg) / 3, 0.7);
          bgStyle = `rgba(180, 40, 40, ${opacity})`;
          borderStyle = "1px solid #f44";
        } else {
          bgStyle = "#444";
        }
      } else {
        // No price data
        if (item) {
          subVal = `${item.count} res`;
          mainVal = "No Price";
        } else {
          bgStyle = "#222";
          borderStyle = "1px dashed #444";
        }
      }

      const card = document.createElement("div");
      card.className = "heatmap-card";
      card.style.background = bgStyle;
      card.style.border = borderStyle;
      card.style.padding = "10px";
      card.style.borderRadius = "4px";
      card.style.cursor = "pointer";
      card.style.transition = "transform 0.1s";
      card.style.display = "flex";
      card.style.flexDirection = "column";
      card.style.alignItems = "center";
      card.style.justifyContent = "center";
      card.style.minHeight = "80px";

      if (!priceData && !item) {
        card.style.opacity = "0.6";
      }

      card.innerHTML = `
            <div style="font-weight:bold; color:white; font-size:1.1rem; letter-spacing:1px;">${ticker}</div>
            <div style="font-size:1rem; font-weight:bold; margin:4px 0; text-shadow:0 1px 2px rgba(0,0,0,0.5);">${mainVal}</div>
            <div style="font-size:0.75rem; color:#ddd;">${subVal}</div>
        `;

      card.addEventListener("mouseenter", () => card.style.transform = "scale(1.05)");
      card.addEventListener("mouseleave", () => card.style.transform = "scale(1)");

      card.addEventListener("click", () => {
        document.querySelector('[data-tab="dashboard"]').click();
        loadChart(ticker);
      });

      grid.appendChild(card);
    });

    catEl.appendChild(grid);
    container.appendChild(catEl);
  });
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

main();
