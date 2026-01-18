const WORKER_URL = "https://5ch-tracker.arakawa47.workers.dev"; // Updated to real endpoint

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

    // Find max count for bar scaling
    const maxCount = items.length > 0 ? items[0].count : 1;

    items.forEach((item, index) => {
      const rank = index + 1;
      const row = document.createElement("tr");
      row.className = "ranking-row";
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

main();
