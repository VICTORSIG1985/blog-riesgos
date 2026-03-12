-- ============================================================
-- RiesgoECU - Esquema de base de datos multiamenaza
-- PostgreSQL + PostGIS en Supabase
-- ============================================================

-- Extensiones
CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS postgis_topology;

-- ============================================================
-- 1. PARROQUIAS (límites administrativos)
-- ============================================================
DROP TABLE IF EXISTS indice_riesgo CASCADE;
DROP TABLE IF EXISTS poblacion_inec CASCADE;
DROP TABLE IF EXISTS amenaza_hidro CASCADE;
DROP TABLE IF EXISTS amenaza_volcanica CASCADE;
DROP TABLE IF EXISTS amenaza_sismica CASCADE;
DROP TABLE IF EXISTS eventos_historicos CASCADE;
DROP TABLE IF EXISTS parroquias CASCADE;

CREATE TABLE parroquias (
    id SERIAL,
    codigo_parroquia VARCHAR(10) PRIMARY KEY,
    nombre VARCHAR(100) NOT NULL,
    canton VARCHAR(100),
    provincia VARCHAR(50),
    geom GEOMETRY(MultiPolygon, 4326)
);

CREATE INDEX idx_parroquias_geom ON parroquias USING GIST (geom);
CREATE INDEX idx_parroquias_provincia ON parroquias (provincia);
CREATE INDEX idx_parroquias_canton ON parroquias (canton);

-- ============================================================
-- 2. EVENTOS HISTÓRICOS (DesInventar / SNGR)
-- ============================================================
CREATE TABLE eventos_historicos (
    id SERIAL PRIMARY KEY,
    codigo_parroquia VARCHAR(10) REFERENCES parroquias(codigo_parroquia),
    tipo_evento VARCHAR(50) NOT NULL,
    fecha DATE,
    muertos INT DEFAULT 0,
    heridos INT DEFAULT 0,
    viviendas_afectadas INT DEFAULT 0,
    perdidas_usd NUMERIC DEFAULT 0,
    fuente VARCHAR(50),
    descripcion TEXT
);

CREATE INDEX idx_eventos_parroquia ON eventos_historicos (codigo_parroquia);
CREATE INDEX idx_eventos_tipo ON eventos_historicos (tipo_evento);
CREATE INDEX idx_eventos_fecha ON eventos_historicos (fecha);

-- ============================================================
-- 3. AMENAZA SÍSMICA
-- ============================================================
CREATE TABLE amenaza_sismica (
    codigo_parroquia VARCHAR(10) PRIMARY KEY REFERENCES parroquias(codigo_parroquia),
    nivel VARCHAR(20) NOT NULL,
    pga_475 NUMERIC,
    fuente VARCHAR(100),
    fecha_actualizacion DATE
);

-- ============================================================
-- 4. AMENAZA VOLCÁNICA
-- ============================================================
CREATE TABLE amenaza_volcanica (
    codigo_parroquia VARCHAR(10) PRIMARY KEY REFERENCES parroquias(codigo_parroquia),
    nivel VARCHAR(20) NOT NULL,
    volcan_referencia VARCHAR(100),
    distancia_km NUMERIC,
    fuente VARCHAR(100)
);

-- ============================================================
-- 5. AMENAZA HIDROMETEOROLÓLGICA
-- ============================================================
CREATE TABLE amenaza_hidro (
    codigo_parroquia VARCHAR(10) PRIMARY KEY REFERENCES parroquias(codigo_parroquia),
    nivel_inundacion VARCHAR(20),
    nivel_deslizamiento VARCHAR(20),
    precipitacion_anual_mm NUMERIC,
    fuente VARCHAR(100)
);

-- ============================================================
-- 6. POBLACIÓN INEC
-- ============================================================
CREATE TABLE poblacion_inec (
    codigo_parroquia VARCHAR(10) PRIMARY KEY REFERENCES parroquias(codigo_parroquia),
    poblacion_total INT,
    viviendas INT,
    nbi_porcentaje NUMERIC,
    densidad_hab_km2 NUMERIC,
    anio_censo INT
);

-- ============================================================
-- 7. ÍNDICE DE RIESGO (INFORM-LAC simplificado)
-- ============================================================
CREATE TABLE indice_riesgo (
    codigo_parroquia VARCHAR(10) PRIMARY KEY REFERENCES parroquias(codigo_parroquia),
    indice_amenaza NUMERIC,
    indice_exposicion NUMERIC,
    indice_capacidad NUMERIC,
    riesgo_total NUMERIC,
    clasificacion VARCHAR(20),
    fecha_calculo DATE
);

-- Verificación
SELECT 'Schema creado exitosamente' AS status;
