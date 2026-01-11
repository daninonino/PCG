import express from "express";
import { pool } from "../db.js";

const router = express.Router();

// helpers
function toInt(value, fallback) {
  const n = Number.parseInt(value, 10);
  return Number.isFinite(n) ? n : fallback;
}

/**
 * GET /estudiantes/matricula/:matricula
 * Devuelve todas las filas del alumno (si está en varias secciones)
 */
router.get("/matricula/:matricula", async (req, res, next) => {
  try {
    const matricula = String(req.params.matricula).trim();

    const { rows } = await pool.query(
      `SELECT *
       FROM infoda.vw_estudiantes_programacion
       WHERE matricula = $1
       ORDER BY codigo_seccion, apellidos_nombres`,
      [matricula]
    );

    res.json({ total: rows.length, data: rows });
  } catch (err) {
    next(err);
  }
});

/**
 * GET /estudiantes/stats/por-carrera
 * Devuelve conteo por carrera (con filtros opcionales)
 * Query params opcionales: seccion, q
 */
router.get("/stats/por-carrera", async (req, res, next) => {
  try {
    const { seccion, q } = req.query;

    const where = [];
    const params = [];

    if (seccion) {
      params.push(String(seccion).trim());
      where.push(`codigo_seccion = $${params.length}`);
    }

    if (q) {
      const qq = `%${String(q).trim()}%`;
      params.push(qq);
      params.push(qq);
      where.push(`(apellidos_nombres ILIKE $${params.length - 1} OR matricula ILIKE $${params.length})`);
    }

    const whereSql = where.length ? `WHERE ${where.join(" AND ")}` : "";

    const sql = `
      SELECT nombre_carrera, COUNT(*)::int AS total
      FROM infoda.vw_estudiantes_programacion
      ${whereSql}
      GROUP BY nombre_carrera
      ORDER BY total DESC, nombre_carrera ASC
    `;

    const { rows } = await pool.query(sql, params);
    res.json({ total: rows.length, data: rows });
  } catch (err) {
    next(err);
  }
});

/**
 * GET /estudiantes/stats/por-seccion
 * Devuelve conteo por sección (y asignatura si existe en la vista)
 * Query params opcionales: carrera, q
 */
router.get("/stats/por-seccion", async (req, res, next) => {
  try {
    const { carrera, q } = req.query;

    const where = [];
    const params = [];

    if (carrera) {
      params.push(`%${String(carrera).trim()}%`);
      where.push(`nombre_carrera ILIKE $${params.length}`);
    }

    if (q) {
      const qq = `%${String(q).trim()}%`;
      params.push(qq);
      params.push(qq);
      where.push(`(apellidos_nombres ILIKE $${params.length - 1} OR matricula ILIKE $${params.length})`);
    }

    const whereSql = where.length ? `WHERE ${where.join(" AND ")}` : "";

    const sql = `
      SELECT
        codigo_seccion,
        COALESCE(nombre_asignatura, '') AS nombre_asignatura,
        COUNT(*)::int AS total
      FROM infoda.vw_estudiantes_programacion
      ${whereSql}
      GROUP BY codigo_seccion, COALESCE(nombre_asignatura, '')
      ORDER BY codigo_seccion ASC
    `;

    const { rows } = await pool.query(sql, params);
    res.json({ total: rows.length, data: rows });
  } catch (err) {
    next(err);
  }
});

export default router;


/**
 * GET /estudiantes
 * Query params:
 *  - seccion: "503203-3"
 *  - carrera: texto (match parcial, case-insensitive)
 *  - q: búsqueda por nombre o matrícula (match parcial)
 *  - limit: default 50, max 200
 *  - offset: default 0
 *  - sort: "nombre" | "matricula" | "creado_en" (default "nombre")
 *  - order: "asc" | "desc" (default "asc")
 *
 * Respuesta:
 *  { total, limit, offset, data: [...] }
 */
router.get("/", async (req, res, next) => {
  try {
    const {
      seccion,
      carrera,
      q,
      sort = "nombre",
      order = "asc",
    } = req.query;

    const limit = Math.min(toInt(req.query.limit, 50), 200);
    const offset = Math.max(toInt(req.query.offset, 0), 0);

    // columnas permitidas para ORDER BY (whitelist)
    const sortMap = {
      nombre: "apellidos_nombres",
      matricula: "matricula",
      creado_en: "creado_en",
    };
    const sortCol = sortMap[String(sort)] ?? sortMap.nombre;
    const sortDir = String(order).toLowerCase() === "desc" ? "DESC" : "ASC";

    const where = [];
    const params = [];

    if (seccion) {
      params.push(String(seccion).trim());
      where.push(`codigo_seccion = $${params.length}`);
    }

    if (carrera) {
      params.push(`%${String(carrera).trim()}%`);
      where.push(`nombre_carrera ILIKE $${params.length}`);
    }

    if (q) {
      const qq = `%${String(q).trim()}%`;
      params.push(qq);
      params.push(qq);
      where.push(`(apellidos_nombres ILIKE $${params.length - 1} OR matricula ILIKE $${params.length})`);
    }

    const whereSql = where.length ? `WHERE ${where.join(" AND ")}` : "";

    // total
    const countSql = `
      SELECT COUNT(*)::int AS total
      FROM infoda.vw_estudiantes_programacion
      ${whereSql}
    `;

    const { rows: countRows } = await pool.query(countSql, params);
    const total = countRows[0]?.total ?? 0;

    // data
    params.push(limit);
    params.push(offset);

    const dataSql = `
      SELECT *
      FROM infoda.vw_estudiantes_programacion
      ${whereSql}
      ORDER BY ${sortCol} ${sortDir}
      LIMIT $${params.length - 1}
      OFFSET $${params.length}
    `;

    const { rows } = await pool.query(dataSql, params);
    res.json({ total, limit, offset, data: rows });
  } catch (err) {
    next(err);
  }
});

/**
 * GET /estudiantes/:id (por id_estudiante)
 */
router.get("/:id/estudiantes", async (req, res, next) => {
  try {
    const id = Number(req.params.id);
    if (!Number.isInteger(id)) {
      return res.status(400).json({ ok: false, error: "ID de carrera inválido" });
    }

    const limit = Math.min(Number.parseInt(req.query.limit ?? "50", 10) || 50, 200);
    const offset = Math.max(Number.parseInt(req.query.offset ?? "0", 10) || 0, 0);

    const countSql = `SELECT COUNT(*)::int AS total FROM infoda.estudiantes WHERE id_carrera = $1`;
    const { rows: countRows } = await pool.query(countSql, [id]);
    const total = countRows[0]?.total ?? 0;

    const sql = `
      SELECT *
      FROM infoda.vw_estudiantes_programacion
      WHERE id_carrera = $1
      ORDER BY apellidos_nombres
      LIMIT $2 OFFSET $3
    `;
    const { rows } = await pool.query(sql, [id, limit, offset]);

    res.json({ total, limit, offset, data: rows });
  } catch (err) {
    next(err);
  }
});



