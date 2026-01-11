import pg from "pg";
import "dotenv/config";

const { Pool } = pg;

export const pool = new Pool({
  host: process.env.PGHOST,
  port: Number(process.env.PGPORT ?? 5432),
  user: process.env.PGUSER,
  password: process.env.PGPASSWORD,
  database: process.env.PGDATABASE,
  max: 10,                 // conexiones máximas en el pool
  idleTimeoutMillis: 30000 // cierra conexiones inactivas
});

// Log simple si hay error de conexión en el pool
pool.on("error", (err) => {
  console.error("Unexpected PG pool error:", err);
});
