import os
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

load_dotenv()

CSV_MAESTRO = os.getenv("CSV_MAESTRO", "estudiantes_maestro_infoda.csv")

PGHOST = os.getenv("PGHOST", "localhost")
PGPORT = int(os.getenv("PGPORT", "5432"))
PGUSER = os.getenv("PGUSER", "postgres")
PGPASSWORD = os.getenv("PGPASSWORD", "")
PGDATABASE = os.getenv("PGDATABASE", "infoda_estudiantes")

def connect():
    return psycopg2.connect(
        host=PGHOST,
        port=PGPORT,
        user=PGUSER,
        password=PGPASSWORD,
        dbname=PGDATABASE,
    )

def run():
    df = pd.read_csv(CSV_MAESTRO, encoding="utf-8-sig", dtype=str).fillna("")

    required = {"seccion","asignatura","matricula","apellidos_nombres","codigo_carrera","carrera","archivo_origen"}
    if not required.issubset(df.columns):
        raise ValueError(f"CSV debe tener columnas: {sorted(required)}. Tiene: {list(df.columns)}")

    df["matricula"] = df["matricula"].str.strip()
    df["apellidos_nombres"] = df["apellidos_nombres"].str.strip()
    df["carrera"] = df["carrera"].str.strip()
    df["seccion"] = df["seccion"].str.strip()
    df["asignatura"] = df["asignatura"].str.strip()
    df["archivo_origen"] = df["archivo_origen"].str.strip()

    # codigo_carrera puede venir vacío
    df["codigo_carrera"] = df["codigo_carrera"].astype(str).str.strip()
    df.loc[df["codigo_carrera"] == "", "codigo_carrera"] = None

    df = df[(df["matricula"] != "") & (df["apellidos_nombres"] != "") & (df["carrera"] != "") & (df["seccion"] != "")]
    df = df.drop_duplicates(subset=["matricula","seccion"])

    conn = connect()
    conn.autocommit = False

    try:
        with conn.cursor() as cur:
            # 1) UPSERT carreras (con codigo si existe)
            carreras = []
            for cc, name in df[["codigo_carrera","carrera"]].drop_duplicates().itertuples(index=False):
                carreras.append((int(cc) if cc is not None else None, name))

            execute_values(cur, """
                INSERT INTO infoda.carrera (codigo_carrera, nombre_carrera)
                VALUES %s
                ON CONFLICT (nombre_carrera) DO UPDATE
                    SET codigo_carrera = COALESCE(infoda.carrera.codigo_carrera, EXCLUDED.codigo_carrera)
            """, carreras)

            # 2) UPSERT secciones (codigo_seccion + nombre_asignatura)
            secciones = []
            for sec, asig in df[["seccion","asignatura"]].drop_duplicates().itertuples(index=False):
                secciones.append((sec, asig))

            execute_values(cur, """
                INSERT INTO infoda.seccion (codigo_seccion, nombre_asignatura)
                VALUES %s
                ON CONFLICT (codigo_seccion) DO UPDATE
                    SET nombre_asignatura = EXCLUDED.nombre_asignatura
            """, secciones)

            # 3) UPSERT archivos origen
            archivos = [(a,) for a in sorted(set(df["archivo_origen"].tolist())) if a]
            execute_values(cur, """
                INSERT INTO infoda.archivo_origen (nombre_archivo)
                VALUES %s
                ON CONFLICT (nombre_archivo) DO NOTHING
            """, archivos)

            # 4) Mapas ID
            cur.execute("SELECT id_carrera, codigo_carrera, nombre_carrera FROM infoda.carrera;")
            carrera_by_nombre = {}
            carrera_by_codigo = {}
            for cid, cc, name in cur.fetchall():
                carrera_by_nombre[name] = cid
                if cc is not None:
                    carrera_by_codigo[int(cc)] = cid

            cur.execute("SELECT id_seccion, codigo_seccion FROM infoda.seccion;")
            seccion_map = {code: sid for sid, code in cur.fetchall()}

            cur.execute("SELECT id_archivo, nombre_archivo FROM infoda.archivo_origen;")
            archivo_map = {name: aid for aid, name in cur.fetchall()}

            # 5) Insert estudiantes
            rows = []
            for r in df.itertuples(index=False):
                cc = int(r.codigo_carrera) if r.codigo_carrera is not None else None
                id_carrera = carrera_by_codigo.get(cc) if cc is not None else carrera_by_nombre.get(r.carrera)
                id_seccion = seccion_map.get(r.seccion)
                id_archivo = archivo_map.get(r.archivo_origen)

                if id_carrera and id_seccion:
                    rows.append((r.matricula, r.apellidos_nombres, id_carrera, id_seccion, id_archivo))

            execute_values(cur, """
                INSERT INTO infoda.estudiantes (matricula, apellidos_nombres, id_carrera, id_seccion, id_archivo)
                VALUES %s
                ON CONFLICT (matricula, id_seccion) DO NOTHING
            """, rows, page_size=5000)

        conn.commit()
        print(f" Carga completa. Filas intentadas: {len(rows)} (sin duplicar matrícula+sección).")

    except Exception as e:
        conn.rollback()
        raise
    finally:
        conn.close()

if __name__ == "__main__":
    run()
