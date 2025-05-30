-- Radar table: stores radar metadata
CREATE EXTENSION IF NOT EXISTS postgis;

CREATE TABLE IF NOT EXISTS radar (
    id SERIAL PRIMARY KEY,
    name VARCHAR(64) UNIQUE NOT null,
    radar_location geometry(Point, 4326)
);

-- Radar files table: stores file metadata, references radar
CREATE TABLE IF NOT EXISTS radar_file (
    id SERIAL PRIMARY KEY,
    radar_id INTEGER NOT NULL REFERENCES radar(id) ON DELETE CASCADE,
    s3_key TEXT NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL,
    file_time TIMESTAMPTZ NOT null,
    local_path TEXT NOT NULL,
    sweep_fixed_angle TEXT NOT NULL, -- Fixed angle of the sweep
    UNIQUE (radar_id, s3_key)
);

CREATE TABLE IF NOT EXISTS radar_statistics (
    id SERIAL PRIMARY KEY,
    file_id INTEGER NOT NULL REFERENCES radar_file(id) ON DELETE CASCADE,
    -- Estadísticas numéricas generales por archivo
    mean_reflectivity REAL,       -- Reflectividad promedio
    max_reflectivity REAL,        -- Reflectividad máxima
    min_reflectivity REAL,        -- Reflectividad mínima
    rain_area_percent REAL,       -- % del área con lluvia (> umbral)
    -- Agregado temporal
    duration_minutes INTEGER,     -- Duración del evento en minutos, si aplica
    -- Bounding box del evento (si se extrae espacialmente)
    bbox geometry(Polygon, 4326), -- Área aproximada cubierta por el radar en ese archivo

    created_at TIMESTAMPTZ DEFAULT NOW(),

    UNIQUE(file_id)
);

-- Índices útiles
CREATE INDEX IF NOT EXISTS idx_radar_statistics_file_id ON radar_statistics(file_id);

-- Optional: Indexes for performance
CREATE INDEX IF NOT EXISTS idx_file_radar_id ON radar_file(radar_id);
CREATE INDEX IF NOT EXISTS idx_file_file_time ON radar_file(file_time);
