-- Crear schemas
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS analytics;

-- Dar permisos
GRANT ALL PRIVILEGES ON SCHEMA raw TO root;
GRANT ALL PRIVILEGES ON SCHEMA analytics TO root;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA raw TO root;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA analytics TO root;

-- Tabla de lookup de zonas (se cargará desde el notebook)
CREATE TABLE IF NOT EXISTS raw.taxi_zone_lookup (
    LocationID INTEGER PRIMARY KEY,
    Borough VARCHAR(50),
    Zone VARCHAR(100),
    service_zone VARCHAR(50)
);

-- Comentarios
COMMENT ON SCHEMA raw IS 'Raw data from NYC TLC Parquet files (2015-2025)';
COMMENT ON SCHEMA analytics IS 'Analytics layer with One Big Table (OBT)';