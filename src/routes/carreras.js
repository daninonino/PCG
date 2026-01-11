import express from "express";
import { pool } from "../db.js";

const router = express.Router();

/**
 * GET /carreras
 * Query params opcionales:
 *  - q: filtra por nombre
 */
router.get("/", async (req, res, next) => {
  try {
    const { q } = req.query;

    const params = [];
    let where = "";

    if (q) {
      params.push(`%${String(q).trim()}%`);
      where = `WHERE nombre_carrera ILIKE $1`;
    }

    const sql = `
      SELECT id_carrera, codigo_carrera, nombre_carrera
      FROM infoda.carrera
      ${where}
      ORDER BY nombre_carrera
    `;

    const { rows } = await pool.query(sql, params);
    res.json({ total: rows.length, data: rows });
  } catch (err) {
    next(err);
  }
});

/**
 * GET /carreras/:id/estudiantes
 * Query params opcionales: limit, offset
 */
router.get("/:id/estudiantes", async (req, res, next) => {
  try {
    const id = Number(req.params.id);
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

export default router;
