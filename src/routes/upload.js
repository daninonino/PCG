import express from "express";
import multer from "multer";
import fs from "fs";
import { parse } from "csv-parse";
import { pool } from "../db.js";

const router = express.Router();

// carpeta temporal
const upload = multer({ dest: "tmp/" });

/**
 * POST /upload-csv
 * body: multipart/form-data
 * field: file (CSV)
 */
router.post("/", upload.single("file"), async (req, res, next) => {
  const client = await pool.connect();

  try {
    if (!req.file) {
      return res.status(400).json({ ok: false, error: "No se envió archivo CSV" });
    }

    const filePath = req.file.path;
    const fileName = req.file.originalname;

    const rows = [];

    // 1️⃣ Leer CSV
    await new Promise((resolve, reject) => {
      fs.createReadStream(filePath)
        .pipe(parse({ columns: true, trim: true }))
        .on("data", (row) => rows.push(row))
        .on("end", resolve)
        .on("error", reject);
    });

    if (rows.length === 0) {
      return res.status(400).json({ ok: false, error: "CSV vacío" });
    }

    // 2️⃣ Transacción
    await client.query("BEGIN");

    // 3️⃣ Registrar archivo origen
    const { rows: archivoRows } = await client.query(
      `INSERT INTO infoda.archivo_origen (nombre_archivo)
       VALUES ($1)
       ON CONFLICT (nombre_archivo) DO UPDATE SET fecha_carga = NOW()
       RETURNING id_archivo`,
      [fileName]
    );
    const idArchivo = archivoRows[0].id_archivo;

    // 4️⃣ Insertar en staging
    for (const r of rows) {
      await client.query(
        `INSERT INTO infoda.staging_estudiantes
        (matricula, apellidos_nombres, codigo_carrera, nombre_carrera,
         codigo_seccion, nombre_asignatura, archivo_origen)
         VALUES ($1,$2,$3,$4,$5,$6,$7)`,
        [
          r.matricula,
          r.apellidos_nombres,
          r.codigo_carrera ? Number(r.codigo_carrera) : null,
          r.nombre_carrera,
          r.codigo_seccion,
          r.nombre_asignatura,
          fileName,
        ]
      );
    }

    // 5️⃣ Normalización (igual a models.sql)
    await client.query(`
      INSERT INTO infoda.carrera (codigo_carrera, nombre_carrera)
      SELECT DISTINCT codigo_carrera, btrim(nombre_carrera)
      FROM infoda.staging_estudiantes
      WHERE nombre_carrera IS NOT NULL
      ON CONFLICT (nombre_carrera) DO NOTHING;
    `);

    await client.query(`
      INSERT INTO infoda.seccion (codigo_seccion, nombre_asignatura)
      SELECT DISTINCT btrim(codigo_seccion), NULLIF(btrim(nombre_asignatura),'')
      FROM infoda.staging_estudiantes
      WHERE codigo_seccion IS NOT NULL
      ON CONFLICT (codigo_seccion) DO NOTHING;
    `);

    await client.query(`
      INSERT INTO infoda.estudiantes
      (matricula, apellidos_nombres, id_carrera, id_seccion, id_archivo)
      SELECT
        btrim(s.matricula),
        btrim(s.apellidos_nombres),
        c.id_carrera,
        sec.id_seccion,
        $1
      FROM infoda.staging_estudiantes s
      JOIN infoda.carrera c ON c.codigo_carrera = s.codigo_carrera
      JOIN infoda.seccion sec ON sec.codigo_seccion = btrim(s.codigo_seccion)
      ON CONFLICT (matricula, id_seccion) DO NOTHING;
    `, [idArchivo]);

    await client.query("COMMIT");

    // 6️⃣ Limpieza
    fs.unlinkSync(filePath);

    res.json({
      ok: true,
      archivo: fileName,
      filas_leidas: rows.length,
      mensaje: "CSV cargado y normalizado correctamente",
    });

  } catch (err) {
    await client.query("ROLLBACK");
    next(err);
  } finally {
    client.release();
  }
});

export default router;
