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
-- 5) TABLA PRINCIPAL AYUDANTES  (incluye id_archivo)
------------------------------------------------------------

CREATE TABLE IF NOT EXISTS ayudante (
    matricula_ayudante       SERIAL PRIMARY KEY,
    nombre_ayudante          TEXT        NOT NULL,
    correo_ayudante          TEXT        NOT NULL,
    creado_en                TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW()
);

------------------------------------------------------------
-- 6) TABLA PRINCIPAL LABORATORIOS (incluye id_archivo)
------------------------------------------------------------
CREATE TABLE IF NOT EXISTS laboratorio (
    id_laboratorio       SERIAL PRIMARY KEY,
    id_seccion          INT NOT NULL,
    nombre_laboratorio   TEXT        NOT NULL,
    creado_en            TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW()
    CONSTRAINT fk_laboratorio_seccion
        FOREIGN KEY (id_seccion)
        REFERENCES seccion (id_seccion)
        ON UPDATE CASCADE ON DELETE RESTRICT
);

-- 9.3 Relación laboratorio <-> ayudantes (muchos a muchos)
CREATE TABLE IF NOT EXISTS laboratorio_ayudante (
    id_laboratorio  INT NOT NULL,
    id_ayudante     INT NOT NULL,
    rol             TEXT, -- opcional: 'titular', 'apoyo', etc.
    asignado_en     TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW(),

    PRIMARY KEY (id_laboratorio, id_ayudante),

    CONSTRAINT fk_lab_ayu_laboratorio
        FOREIGN KEY (id_laboratorio)
        REFERENCES laboratorio (id_laboratorio)
        ON UPDATE CASCADE ON DELETE CASCADE,

    CONSTRAINT fk_lab_ayu_ayudante
        FOREIGN KEY (id_ayudante)
        REFERENCES ayudante (id_ayudante)
        ON UPDATE CASCADE ON DELETE RESTRICT
);

-- 9.4 Sesiones de laboratorio (cada sesión con fecha y horario)
CREATE TABLE IF NOT EXISTS laboratorio_sesion (
    id_sesion       SERIAL PRIMARY KEY,
    id_laboratorio  INT NOT NULL,
    fecha           DATE NOT NULL,
    hora_inicio     TIME NOT NULL,
    hora_fin        TIME NOT NULL,
    sala            TEXT,
    tema            TEXT,
    creado_en       TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW(),

    CONSTRAINT fk_sesion_laboratorio
        FOREIGN KEY (id_laboratorio)
        REFERENCES laboratorio (id_laboratorio)
        ON UPDATE CASCADE ON DELETE CASCADE
);

-- 9.5 Enrolamiento de estudiantes al laboratorio (muchos a muchos)
CREATE TABLE IF NOT EXISTS laboratorio_enrolamiento (
    id_laboratorio  INT NOT NULL,
    id_estudiante   INT NOT NULL,
    enrolado_en     TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW(),

    PRIMARY KEY (id_laboratorio, id_estudiante),

    CONSTRAINT fk_enrol_lab
        FOREIGN KEY (id_laboratorio)
        REFERENCES laboratorio (id_laboratorio)
        ON UPDATE CASCADE ON DELETE CASCADE,

    CONSTRAINT fk_enrol_estudiante
        FOREIGN KEY (id_estudiante)
        REFERENCES estudiantes (id_estudiante)
        ON UPDATE CASCADE ON DELETE CASCADE
);

-- 9.6 Registro por sesión y estudiante: asistencia + pybonus + décimas
CREATE TABLE IF NOT EXISTS laboratorio_asistencia (
    id_sesion      INT NOT NULL,
    id_estudiante  INT NOT NULL,

    -- Estado de asistencia:
    -- presente: asistió
    -- tarde: asistió, pero tarde (cuenta como asistencia si tú quieres)
    -- ausente: no asistió
    -- justificado: no asistió, pero con justificación
    estado         VARCHAR(15) NOT NULL DEFAULT 'presente',

    pybonus        INT NOT NULL DEFAULT 0,
    decimas        NUMERIC(5,2) NOT NULL DEFAULT 0,

    observacion    TEXT,
    registrado_en  TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW(),

    PRIMARY KEY (id_sesion, id_estudiante),

    CONSTRAINT fk_asist_sesion
        FOREIGN KEY (id_sesion)
        REFERENCES laboratorio_sesion (id_sesion)
        ON UPDATE CASCADE ON DELETE CASCADE,

    CONSTRAINT fk_asist_estudiante
        FOREIGN KEY (id_estudiante)
        REFERENCES estudiantes (id_estudiante)
        ON UPDATE CASCADE ON DELETE CASCADE,

    CONSTRAINT chk_asist_estado
        CHECK (estado IN ('presente', 'tarde', 'ausente', 'justificado')),

    CONSTRAINT chk_pybonus_nonneg
        CHECK (pybonus >= 0),

    CONSTRAINT chk_decimas_nonneg
        CHECK (decimas >= 0)
);

------------------------------------------------------------
-- 5) ÍNDICES
------------------------------------------------------------

CREATE INDEX IF NOT EXISTS idx_estudiantes_matricula ON estudiantes (matricula);
CREATE INDEX IF NOT EXISTS idx_estudiantes_carrera   ON estudiantes (id_carrera);
CREATE INDEX IF NOT EXISTS idx_estudiantes_seccion   ON estudiantes (id_seccion);
CREATE INDEX IF NOT EXISTS idx_lab_id_seccion        ON laboratorio (id_seccion);
CREATE INDEX IF NOT EXISTS idx_sesion_id_laboratorio ON laboratorio_sesion (id_laboratorio);
CREATE INDEX IF NOT EXISTS idx_enrol_id_estudiante   ON laboratorio_enrolamiento (id_estudiante);
CREATE INDEX IF NOT EXISTS idx_asist_id_estudiante   ON laboratorio_asistencia (id_estudiante);
CREATE INDEX IF NOT EXISTS idx_asist_estado          ON laboratorio_asistencia (estado);

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

-- 9.8.1 Vista: laboratorios con sección + ayudantes (lista agregada)
CREATE OR REPLACE VIEW vw_laboratorios_detalle AS
SELECT
    l.id_laboratorio,
    l.nombre_laboratorio,
    s.codigo_seccion,
    s.nombre_asignatura,
    l.activo,
    l.creado_en,
    COALESCE(string_agg(DISTINCT a.nombre, ', '), '') AS ayudantes
FROM laboratorio l
JOIN seccion s ON s.id_seccion = l.id_seccion
LEFT JOIN laboratorio_ayudante la ON la.id_laboratorio = l.id_laboratorio
LEFT JOIN ayudante a ON a.id_ayudante = la.id_ayudante
GROUP BY l.id_laboratorio, l.nombre_laboratorio, s.codigo_seccion, s.nombre_asignatura, l.activo, l.creado_en;

-- 9.8.2 Vista: sesiones con info del lab + sección
CREATE OR REPLACE VIEW vw_laboratorio_sesiones AS
SELECT
    ls.id_sesion,
    ls.fecha,
    ls.hora_inicio,
    ls.hora_fin,
    ls.sala,
    ls.tema,
    l.id_laboratorio,
    l.nombre_laboratorio,
    s.codigo_seccion,
    s.nombre_asignatura
FROM laboratorio_sesion ls
JOIN laboratorio l ON l.id_laboratorio = ls.id_laboratorio
JOIN seccion s ON s.id_seccion = l.id_seccion;

-- 9.8.3 Resumen por estudiante y laboratorio:
-- total sesiones del lab, asistencias, porcentaje, pybonus total, décimas total
CREATE OR REPLACE VIEW vw_laboratorio_resumen_estudiante AS
WITH sesiones_por_lab AS (
    SELECT
        id_laboratorio,
        COUNT(*) AS total_sesiones
    FROM laboratorio_sesion
    GROUP BY id_laboratorio
),
base AS (
    SELECT
        le.id_laboratorio,
        le.id_estudiante
    FROM laboratorio_enrolamiento le
)
SELECT
    b.id_laboratorio,
    l.nombre_laboratorio,
    s.codigo_seccion,
    s.nombre_asignatura,

    b.id_estudiante,
    e.matricula,
    e.apellidos_nombres,

    COALESCE(sp.total_sesiones, 0) AS total_sesiones,

    -- Definición de asistencia efectiva: presente + tarde
    COALESCE(SUM(CASE WHEN la.estado IN ('presente','tarde') THEN 1 ELSE 0 END), 0) AS asistencias,

    COALESCE(SUM(CASE WHEN la.estado = 'justificado' THEN 1 ELSE 0 END), 0) AS justificados,
    COALESCE(SUM(CASE WHEN la.estado = 'ausente' THEN 1 ELSE 0 END), 0) AS ausencias,

    CASE
        WHEN COALESCE(sp.total_sesiones, 0) = 0 THEN 0
        ELSE ROUND(
            (COALESCE(SUM(CASE WHEN la.estado IN ('presente','tarde') THEN 1 ELSE 0 END), 0)::NUMERIC
             / NULLIF(sp.total_sesiones, 0)) * 100
        , 2)
    END AS porcentaje_asistencia,

    COALESCE(SUM(la.pybonus), 0) AS pybonus_total,
    COALESCE(SUM(la.decimas), 0) AS decimas_total

FROM base b
JOIN laboratorio l ON l.id_laboratorio = b.id_laboratorio
JOIN seccion s ON s.id_seccion = l.id_seccion
JOIN estudiantes e ON e.id_estudiante = b.id_estudiante
LEFT JOIN sesiones_por_lab sp ON sp.id_laboratorio = b.id_laboratorio

-- ojo: para sumar pybonus/decimas por sesión del lab, unimos asistencia con sesiones del mismo lab
LEFT JOIN laboratorio_sesion ls
  ON ls.id_laboratorio = b.id_laboratorio
LEFT JOIN laboratorio_asistencia la
  ON la.id_sesion = ls.id_sesion
 AND la.id_estudiante = b.id_estudiante

GROUP BY
    b.id_laboratorio, l.nombre_laboratorio, s.codigo_seccion, s.nombre_asignatura,
    b.id_estudiante, e.matricula, e.apellidos_nombres,
    sp.total_sesiones;

-- 9.8.4 Resumen por laboratorio (agregado del grupo completo)
CREATE OR REPLACE VIEW vw_laboratorio_resumen_general AS
SELECT
    r.id_laboratorio,
    r.nombre_laboratorio,
    r.codigo_seccion,
    r.nombre_asignatura,
    COUNT(*) AS estudiantes_enrolados,
    ROUND(AVG(r.porcentaje_asistencia), 2) AS promedio_asistencia_pct,
    SUM(r.pybonus_total) AS pybonus_total_grupo,
    SUM(r.decimas_total) AS decimas_total_grupo
FROM vw_laboratorio_resumen_estudiante r
GROUP BY
    r.id_laboratorio, r.nombre_laboratorio, r.codigo_seccion, r.nombre_asignatura;
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
