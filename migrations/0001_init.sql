-- Migration number: 0001 	 2026-01-18T00:00:00.000Z
DROP TABLE IF EXISTS rankings;
CREATE TABLE rankings (
  window TEXT NOT NULL,
  ticker TEXT NOT NULL,
  count INTEGER NOT NULL,
  last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (window, ticker)
);

DROP TABLE IF EXISTS meta;
CREATE TABLE meta (
  key TEXT PRIMARY KEY,
  value TEXT
);
