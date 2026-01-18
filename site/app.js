const WORKER_URL = "https://5ch-tracker.arakawa47.workers.dev";
let chartWidget = null;
let currentTicker = null;
let currentTopics = []; // NEW: Store topics globally

// Tab Switching
document.querySelectorAll(".tab-btn").forEach(btn => {
  btn.addEventListener("click", () => {
    // Remove active class
    document.querySelectorAll(".tab-btn").forEach(b => b.classList.remove("active"));
    btn.classList.add("active");

    const tab = btn.dataset.tab;
    if (tab === "dashboard") {
      document.getElementById("view-dashboard").style.display = "grid";
      document.getElementById("view-topics").style.display = "none";
    } else {
      document.getElementById("view-dashboard").style.display = "none";
      document.getElementById("view-topics").style.display = "block";
      // Render WordCloud when tab becomes visible
      setTimeout(renderWordCloud, 50);
    }
  });
});

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
        // Simple typing effect simulation via textContent updates could be done here, 
        // but strict replacement is safer for now.
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

    // Initial Render if needed (but Dashboard is default)
    // renderWordCloud(); 

    const maxCount = items.length > 0 ? items[0].count : 1;

    // Set initial ticker (first item or default)
    if (items.length > 0) {
      loadChart(items[0].ticker);
    } else {
      loadChart("SPX");
    }

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
  // We want the largest word to be ~80px (desktop) or ~50px (mobile)
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
      // Cyberpunk Palette
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
