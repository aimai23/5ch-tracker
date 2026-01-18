-- Migration number: 0002 	 2026-01-18T19:45:00.000Z
ALTER TABLE rankings ADD COLUMN sentiment REAL DEFAULT 0.0;
