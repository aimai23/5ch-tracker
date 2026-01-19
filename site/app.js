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
      // Hide all
      document.getElementById("view-dashboard").style.display = "none";
      document.getElementById("view-topics").style.display = "none";
      document.getElementById("view-ongi-greed").style.display = "none";

      // Show selected
      // ...
      // Tab Logic Change (Need to target the block)
      // ...
      // Show selected
      if (tabId === 'dashboard') {
        document.getElementById("view-dashboard").style.display = "grid";
      } else if (tabId === 'topics') {
        document.getElementById("view-topics").style.display = "block";
        setTimeout(renderWordCloud, 100);
      } else if (tabId === 'ongi_greed') {
        document.getElementById("view-ongi-greed").style.display = "block";
      }
    });
  });

  // ...

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

    // AI Comment
    if (commentEl) {
      const comments = [
        "恩義(ONGI)の嵐です…市場は阿鼻叫喚！ (Extreme Ongi/Panic)",
        "弱気ムード。恩義マンが出没中？ (Ongi/Fear)",
        "どっちつかずの展開です。 (Neutral)",
        "強欲ムード！イケイケですね。 (Greed)",
        "強欲の極み！靴磨きの少年も株の話をしてるかも？ (Extreme Greed)"
      ];
      const idx = Math.min(Math.floor(score / 20), 4);
      commentEl.textContent = `AI Analysis: ${comments[idx]}`;
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
