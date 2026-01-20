-- Create ongi_history table
CREATE TABLE IF NOT EXISTS ongi_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp INTEGER NOT NULL,
    score INTEGER NOT NULL,
    label TEXT,
    metrics TEXT
);

-- Index for fast retrieval by time
CREATE INDEX IF NOT EXISTS idx_ongi_history_timestamp ON ongi_history(timestamp);
