const WORKER_URL = "https://5ch-tracker.arakawa47.workers.dev";

async function main() {
  const res = await fetch(`${WORKER_URL}/api/ranking?window=24h`, { cache: "no-store" });
  const data = await res.json();
  document.getElementById("out").textContent = JSON.stringify(data, null, 2);
}

main().catch(err => {
  document.getElementById("out").textContent = String(err);
});
