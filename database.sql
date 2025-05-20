-- Radar table: stores radar metadata
CREATE TABLE IF NOT EXISTS radar (
    id SERIAL PRIMARY KEY,
    name VARCHAR(64) UNIQUE NOT NULL
);

-- Radar files table: stores file metadata, references radar
CREATE TABLE IF NOT EXISTS radar_files (
    id SERIAL PRIMARY KEY,
    radar_id INTEGER NOT NULL REFERENCES radar(id) ON DELETE CASCADE,
    s3_key TEXT NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL,
    local_path TEXT NOT NULL,
    bbox geometry(Point, 4326) NOT NULL,
    sweep_number INTEGER NOT NULL,
    UNIQUE (radar_id, s3_key, sweep_number)
);

-- Optional: Indexes for performance
CREATE INDEX IF NOT EXISTS idx_radar_files_radar_id ON radar_files(radar_id);
CREATE INDEX IF NOT EXISTS idx_radar_files_processed_at ON radar_files(processed_at);