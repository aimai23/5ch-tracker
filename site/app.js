const WORKER_URL = "https://5ch-tracker.arakawa47.workers.dev";
let chartWidget = null;
let currentTicker = null;

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

    items.forEach((item, index) => {
      const rank = index + 1;
      const row = document.createElement("tr");
      row.className = "ranking-row";
      if (index === 0) row.classList.add("selected"); // Select first by default
      if (rank <= 3) row.classList.add(`rank-${rank}`);

      // Calculate bar width percentage
      const barPercent = Math.max((item.count / maxCount) * 100, 1);

      row.innerHTML = `
        <td>${rank}</td>
        <td>
          <div class="ticker-cell">
            <span class="ticker-name">${item.ticker}</span>
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

main();
