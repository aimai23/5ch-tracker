-- Add price columns to rankings
ALTER TABLE rankings ADD COLUMN price REAL;
ALTER TABLE rankings ADD COLUMN change_percent REAL;
