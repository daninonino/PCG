import express from "express";
import { pool } from "../db.js";

const router = express.Router();

/**
 * GET /secciones
 * Query params opcionales:
 *  - q: filtra por codigo_seccion o nombre_asignatura
 */
router.get("/", async (req, res, next) => {
  try {
    const { q } = req.query;

    const params = [];
    let where = "";

    if (q) {
      const qq = `%${String(q).trim()}%`;
      params.push(qq);
      params.push(qq);
      where = `WHERE (codigo_seccion ILIKE $1 OR COALESCE(nombre_asignatura,'') ILIKE $2)`;
    }

    const sql = `
      SELECT id_seccion, codigo_seccion, nombre_asignatura
      FROM infoda.seccion
      ${where}
      ORDER BY codigo_seccion
    `;
    const { rows } = await pool.query(sql, params);
    res.json({ total: rows.length, data: rows });
  } catch (err) {
    next(err);
  }
});

/**
 * GET /secciones/:codigo/estudiantes
 * Query params opcionales: limit, offset, carrera, q
 */
router.get("/:codigo/estudiantes", async (req, res, next) => {
  try {
    const codigo = String(req.params.codigo).trim();
    const limit = Math.min(Number.parseInt(req.query.limit ?? "50", 10) || 50, 200);
    const offset = Math.max(Number.parseInt(req.query.offset ?? "0", 10) || 0, 0);

    const where = [`codigo_seccion = $1`];
    const params = [codigo];

    if (req.query.carrera) {
      params.push(`%${String(req.query.carrera).trim()}%`);
      where.push(`nombre_carrera ILIKE $${params.length}`);
    }

    if (req.query.q) {
      const qq = `%${String(req.query.q).trim()}%`;
      params.push(qq);
      params.push(qq);
      where.push(`(apellidos_nombres ILIKE $${params.length - 1} OR matricula ILIKE $${params.length})`);
    }

    const whereSql = `WHERE ${where.join(" AND ")}`;

    const countSql = `
      SELECT COUNT(*)::int AS total
      FROM infoda.vw_estudiantes_programacion
      ${whereSql}
    `;
    const { rows: countRows } = await pool.query(countSql, params);
    const total = countRows[0]?.total ?? 0;

    params.push(limit);
    params.push(offset);

    const sql = `
      SELECT *
      FROM infoda.vw_estudiantes_programacion
      ${whereSql}
      ORDER BY apellidos_nombres
      LIMIT $${params.length - 1}
      OFFSET $${params.length}
    `;
    const { rows } = await pool.query(sql, params);

    res.json({ total, limit, offset, data: rows });
  } catch (err) {
    next(err);
  }
});

export default router;
