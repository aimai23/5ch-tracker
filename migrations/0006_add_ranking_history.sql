-- Create ranking_history table
CREATE TABLE IF NOT EXISTS ranking_history (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  window TEXT NOT NULL,
  timestamp INTEGER NOT NULL,
  payload TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_ranking_history_window_time
  ON ranking_history(window, timestamp);
