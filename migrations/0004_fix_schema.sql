-- 1. Remove unnecessary columns from rankings (if they exist)
-- SQLite requires recreating table for DROP COLUMN in some versions, but D1 supports it.
-- If this fails because columns don't exist (user never ran 0003), you can ignore the DROP statements.
ALTER TABLE rankings DROP COLUMN price;
ALTER TABLE rankings DROP COLUMN change_percent;

-- 2. Create the dedicated prices table
CREATE TABLE prices (
  ticker TEXT PRIMARY KEY,
  price REAL,
  change_percent REAL,
  updated_at TEXT
);
