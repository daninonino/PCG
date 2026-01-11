import express from "express";
import "dotenv/config";

import estudiantesRoutes from "./routes/estudiantes.js";
import carrerasRoutes from "./routes/carreras.js";
import seccionesRoutes from "./routes/secciones.js";
import uploadRoutes from "./routes/upload.js";


const app = express();
app.use(express.json());

// Healthcheck
app.get("/health", (req, res) => {
  res.json({ ok: true, service: "infoda-api" });
});

// Rutas API
app.use("/estudiantes", estudiantesRoutes);
app.use("/carreras", carrerasRoutes);
app.use("/secciones", seccionesRoutes);
app.use("/upload-csv", uploadRoutes);


// 404 JSON
app.use((req, res) => {
  res.status(404).json({ ok: false, error: "Ruta no encontrada" });
});

// Error handler JSON (evita HTML)
app.use((err, req, res, next) => {
  console.error(err);
  const status = err.statusCode ?? 500;
  res.status(status).json({ ok: false, error: err.message ?? "Error interno" });
});

const PORT = Number(process.env.PORT ?? 3000);
app.listen(PORT, () => {
  console.log(`Server running on http://localhost:${PORT}`);
});
