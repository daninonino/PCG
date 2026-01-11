import express from "express";
import { pool } from "../db.js";

const router = express.Router();

/**
 * GET /ayudantes?q=
 */
router.get("/", async (req, res, next) => {
  try {
    const { q } = req.query;

    const params = [];
    let where = "";

    if (q) {
      params.push(`%${String(q).trim()}%`);
      where = `WHERE nombre ILIKE $1 OR email ILIKE $1`;
    }

    const sql = `
      SELECT id_ayudante, nombre, email, creado_en
      FROM infoda.ayudante
      ${where}
      ORDER BY nombre
    `;

    const { rows } = await pool.query(sql, params);
    res.json({ total: rows.length, data: rows });
  } catch (err) {
    next(err);
  }
});

/**
 * POST /ayudantes
 * body: { nombre, email? }
 */
router.post("/", async (req, res, next) => {
  try {
    const { nombre, email } = req.body ?? {};

    if (!nombre || !String(nombre).trim()) {
      return res.status(400).json({ error: "nombre is required" });
    }

    const sql = `
      INSERT INTO infoda.ayudante (nombre, email)
      VALUES ($1, $2)
      RETURNING id_ayudante, nombre, email, creado_en
    `;
    const params = [String(nombre).trim(), email ? String(email).trim() : null];

    const { rows } = await pool.query(sql, params);
    res.status(201).json(rows[0]);
  } catch (err) {
    next(err);
  }
});

/**
 * GET /ayudantes/:id
 */
router.get("/:id", async (req, res, next) => {
  try {
    const id = Number(req.params.id);
    if (!Number.isFinite(id)) return res.status(400).json({ error: "invalid id" });

    const sql = `
      SELECT id_ayudante, nombre, email, creado_en
      FROM infoda.ayudante
      WHERE id_ayudante = $1
    `;
    const { rows } = await pool.query(sql, [id]);
    if (!rows.length) return res.status(404).json({ error: "not found" });

    res.json(rows[0]);
  } catch (err) {
    next(err);
  }
});

/**
 * PUT /ayudantes/:id
 * body: { nombre, email? }
 */
router.put("/:id", async (req, res, next) => {
  try {
    const id = Number(req.params.id);
    const { nombre, email } = req.body ?? {};

    if (!Number.isFinite(id)) return res.status(400).json({ error: "invalid id" });
    if (!nombre || !String(nombre).trim()) return res.status(400).json({ error: "nombre is required" });

    const sql = `
      UPDATE infoda.ayudante
      SET nombre = $2, email = $3
      WHERE id_ayudante = $1
      RETURNING id_ayudante, nombre, email, creado_en
    `;
    const params = [id, String(nombre).trim(), email ? String(email).trim() : null];

    const { rows } = await pool.query(sql, params);
    if (!rows.length) return res.status(404).json({ error: "not found" });

    res.json(rows[0]);
  } catch (err) {
    next(err);
  }
});

/**
 * DELETE /ayudantes/:id
 */
router.delete("/:id", async (req, res, next) => {
  try {
    const id = Number(req.params.id);
    if (!Number.isFinite(id)) return res.status(400).json({ error: "invalid id" });

    const sql = `
      DELETE FROM infoda.ayudante
      WHERE id_ayudante = $1
      RETURNING id_ayudante
    `;
    const { rows } = await pool.query(sql, [id]);
    if (!rows.length) return res.status(404).json({ error: "not found" });

    res.json({ ok: true, deleted: rows[0].id_ayudante });
  } catch (err) {
    next(err);
  }
});

export default router;
