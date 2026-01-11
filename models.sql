------------------------------------------------------------
-- 1) CREAR BASE DE DATOS (ejecuta en DB "postgres")
------------------------------------------------------------
CREATE DATABASE estudiantes_programacion
    WITH
    OWNER = postgres
    ENCODING = 'UTF8'
    TEMPLATE = template0;

------------------------------------------------------------
-- 2) DESDE AQUÍ:
-- psql -h localhost -U postgres -d estudiantes_programacion -f models.sql
------------------------------------------------------------

CREATE SCHEMA IF NOT EXISTS infoda AUTHORIZATION postgres;
SET search_path TO infoda, public;

------------------------------------------------------------
-- 3) TABLAS MAESTRAS
------------------------------------------------------------

-- 3.1 Carrera
CREATE TABLE IF NOT EXISTS carrera (
    id_carrera      SERIAL PRIMARY KEY,
    codigo_carrera  INT UNIQUE,
    nombre_carrera  TEXT NOT NULL UNIQUE
);

-- 3.2 Sección (incluye nombre_asignatura)
CREATE TABLE IF NOT EXISTS seccion (
    id_seccion        SERIAL PRIMARY KEY,
    codigo_seccion    VARCHAR(50) NOT NULL UNIQUE,
    nombre_asignatura TEXT
);

-- 3.3 Archivo origen
CREATE TABLE IF NOT EXISTS archivo_origen (
    id_archivo     SERIAL PRIMARY KEY,
    nombre_archivo TEXT NOT NULL UNIQUE,
    fecha_carga    TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW()
);

------------------------------------------------------------
-- 4) TABLA PRINCIPAL ESTUDIANTES (incluye id_archivo)
------------------------------------------------------------

CREATE TABLE IF NOT EXISTS estudiantes (
    id_estudiante       SERIAL PRIMARY KEY,
    matricula           VARCHAR(20) NOT NULL,
    apellidos_nombres   TEXT        NOT NULL,

    id_carrera          INT NOT NULL,
    id_seccion          INT NOT NULL,
    id_archivo          INT, -- ahora existe

    creado_en           TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW(),

    CONSTRAINT fk_estudiante_carrera
        FOREIGN KEY (id_carrera)
        REFERENCES carrera (id_carrera)
        ON UPDATE CASCADE ON DELETE RESTRICT,

    CONSTRAINT fk_estudiante_seccion
        FOREIGN KEY (id_seccion)
        REFERENCES seccion (id_seccion)
        ON UPDATE CASCADE ON DELETE RESTRICT,

    CONSTRAINT fk_estudiante_archivo
        FOREIGN KEY (id_archivo)
        REFERENCES archivo_origen (id_archivo)
        ON UPDATE CASCADE ON DELETE SET NULL
);

-- Evitar duplicados por sección
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'estudiantes_unicos_por_seccion'
    ) THEN
        ALTER TABLE estudiantes
        ADD CONSTRAINT estudiantes_unicos_por_seccion UNIQUE (matricula, id_seccion);
    END IF;
END $$;

------------------------------------------------------------
-- 5) ÍNDICES
------------------------------------------------------------

CREATE INDEX IF NOT EXISTS idx_estudiantes_matricula ON estudiantes (matricula);
CREATE INDEX IF NOT EXISTS idx_estudiantes_carrera   ON estudiantes (id_carrera);
CREATE INDEX IF NOT EXISTS idx_estudiantes_seccion   ON estudiantes (id_seccion);

------------------------------------------------------------
-- 6) VISTAS
------------------------------------------------------------

CREATE OR REPLACE VIEW vw_estudiantes_programacion AS
SELECT
    e.id_estudiante,
    e.matricula,
    e.apellidos_nombres,
    c.nombre_carrera,
    s.codigo_seccion,
    s.nombre_asignatura,
    a.nombre_archivo,
    e.creado_en
FROM estudiantes e
JOIN carrera c ON e.id_carrera = c.id_carrera
JOIN seccion s ON e.id_seccion = s.id_seccion
LEFT JOIN archivo_origen a ON e.id_archivo = a.id_archivo;

CREATE OR REPLACE VIEW vw_estudiantes_programacion_divididos AS
SELECT
    e.id_estudiante,
    e.matricula,
    e.apellidos_nombres,
    split_part(e.apellidos_nombres, ' ', 1) AS primer_apellido,
    split_part(e.apellidos_nombres, ' ', 2) AS segundo_apellido,
    substring(
        e.apellidos_nombres
        FROM position(' ' IN e.apellidos_nombres || ' ')
           + position(
               ' ' IN substring(e.apellidos_nombres || ' '
               FROM position(' ' IN e.apellidos_nombres || ' ') + 1)
             )
    ) AS nombres_aproximados,
    c.nombre_carrera,
    s.codigo_seccion,
    s.nombre_asignatura,
    a.nombre_archivo,
    e.creado_en
FROM estudiantes e
JOIN carrera c ON e.id_carrera = c.id_carrera
JOIN seccion s ON e.id_seccion = s.id_seccion
LEFT JOIN archivo_origen a ON e.id_archivo = a.id_archivo;

------------------------------------------------------------
-- 7) STAGING (para CSV maestro)
------------------------------------------------------------

CREATE TABLE IF NOT EXISTS staging_estudiantes (
    matricula           VARCHAR(20),
    apellidos_nombres   TEXT,
    codigo_carrera      INT,
    nombre_carrera      TEXT,
    codigo_seccion      VARCHAR(50),
    nombre_asignatura   TEXT,
    archivo_origen      TEXT
);

------------------------------------------------------------
-- 8) POBLAR TABLAS DESDE STAGING (alineado con nombres reales)
------------------------------------------------------------

-- 8.1 Carreras
INSERT INTO carrera (codigo_carrera, nombre_carrera)
SELECT DISTINCT
    codigo_carrera,
    btrim(nombre_carrera)
FROM staging_estudiantes
WHERE nombre_carrera IS NOT NULL AND btrim(nombre_carrera) <> ''
ON CONFLICT (nombre_carrera) DO NOTHING;

-- 8.2 Secciones
INSERT INTO seccion (codigo_seccion, nombre_asignatura)
SELECT DISTINCT
    btrim(codigo_seccion),
    NULLIF(btrim(nombre_asignatura), '')
FROM staging_estudiantes
WHERE codigo_seccion IS NOT NULL AND btrim(codigo_seccion) <> ''
ON CONFLICT (codigo_seccion) DO NOTHING;

-- 8.3 Archivos origen
INSERT INTO archivo_origen (nombre_archivo)
SELECT DISTINCT btrim(archivo_origen)
FROM staging_estudiantes
WHERE archivo_origen IS NOT NULL AND btrim(archivo_origen) <> ''
ON CONFLICT (nombre_archivo) DO NOTHING;

-- 8.4 Estudiantes
INSERT INTO estudiantes (matricula, apellidos_nombres, id_carrera, id_seccion, id_archivo)
SELECT
    btrim(s.matricula),
    btrim(s.apellidos_nombres),
    c.id_carrera,
    sec.id_seccion,
    ao.id_archivo
FROM staging_estudiantes s
JOIN carrera c
  ON c.codigo_carrera = s.codigo_carrera
JOIN seccion sec
  ON sec.codigo_seccion = btrim(s.codigo_seccion)
LEFT JOIN archivo_origen ao
  ON ao.nombre_archivo = btrim(s.archivo_origen)
WHERE s.matricula IS NOT NULL AND btrim(s.matricula) <> ''
  AND s.apellidos_nombres IS NOT NULL AND btrim(s.apellidos_nombres) <> ''
ON CONFLICT (matricula, id_seccion) DO NOTHING;
