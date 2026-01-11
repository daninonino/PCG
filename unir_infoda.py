from pathlib import Path
import pandas as pd
import re

CARPETA_LISTAS = Path("infoda_listas")
SALIDA = Path("estudiantes_maestro_infoda.csv")

def _read_raw_infoda_csv(path: Path) -> pd.DataFrame:
    # INFODA suele venir con ; y encoding latin-1
    # Leemos sin header (hay encabezados “decorativos” arriba)
    try:
        return pd.read_csv(path, sep=";", header=None, encoding="latin-1", dtype=str)
    except UnicodeDecodeError:
        return pd.read_csv(path, sep=";", header=None, encoding="utf-8-sig", dtype=str)

def _extraer_seccion(df_raw: pd.DataFrame) -> str | None:
    # Busca algo como "(503203-4) PROGRAMACIÓN" en alguna celda
    pattern = re.compile(r"\(\s*\d+\-\d+\s*\)")
    for col in df_raw.columns:
        series = df_raw[col].dropna().astype(str)
        hit = series[series.str.contains(pattern)]
        if not hit.empty:
            text = hit.iloc[0]
            m = re.search(r"\(([^)]+)\)", text)
            return m.group(1).strip() if m else None
    return None

def _encontrar_header_idx(df_raw: pd.DataFrame) -> int | None:
    # Encuentra la fila donde aparece "Corr." (tabla de alumnos)
    for i in range(min(len(df_raw), 200)):  # suele estar cerca del inicio
        row = df_raw.iloc[i].astype(str).str.strip().tolist()
        if any(cell == "Corr." for cell in row):
            return i
    return None

def _procesar_archivo_infoda(path: Path) -> pd.DataFrame:
    df_raw = _read_raw_infoda_csv(path)

    seccion = _extraer_seccion(df_raw)
    header_idx = _encontrar_header_idx(df_raw)
    if header_idx is None:
        raise ValueError(f"No se encontró encabezado 'Corr.' en {path.name}")

    # La tabla comienza después del encabezado
    data = df_raw.iloc[header_idx + 1:].copy()

    # Heurística INFODA típica:
    # - matrícula suele ser una columna numérica (o texto numérico)
    # - nombre alumno suele estar unas columnas más a la derecha
    #
    # En el archivo que revisamos antes, calzaba:
    #   matricula -> col 2
    #   apellidos_nombres -> col 6
    #   carrera -> col 8 (a veces viene "(3309) INGENIERIA ...")
    #
    # Si algún CSV INFODA te viene distinto, esto se ajusta fácil.
    col_matricula = 2
    col_nombre = 6
    col_carrera = 8

    # Filas válidas: matrícula numérica
    matricula_series = data[col_matricula].fillna("").astype(str).str.strip()
    mask = matricula_series.str.match(r"^\d+$")
    data = data[mask].copy()

    df = pd.DataFrame({
        "matricula": data[col_matricula].astype(str).str.strip(),
        "apellidos_nombres": data[col_nombre].astype(str).str.strip(),
        "carrera": data[col_carrera].astype(str).str.strip(),
        "seccion": seccion if seccion else "",
        "archivo_origen": path.name
    })

    # Limpiar carrera: quitar "(3309)" si aparece al inicio
    df["carrera"] = df["carrera"].str.replace(r"^\(\d+\)\s*", "", regex=True).str.strip()

    # Limpieza básica
    df = df[df["matricula"].ne("") & df["apellidos_nombres"].ne("") & df["carrera"].ne("")]

    return df

def main():
    if not CARPETA_LISTAS.exists():
        raise FileNotFoundError(f"No existe la carpeta: {CARPETA_LISTAS.resolve()}")

    frames = []
    errores = []

    for csv_path in sorted(CARPETA_LISTAS.glob("*.csv")):
        try:
            frames.append(_procesar_archivo_infoda(csv_path))
            print(f"OK: {csv_path.name}")
        except Exception as e:
            errores.append((csv_path.name, str(e)))
            print(f"ERROR: {csv_path.name} -> {e}")

    if not frames:
        raise RuntimeError("No se pudo procesar ningún CSV válido.")

    df_total = pd.concat(frames, ignore_index=True)

    # Evitar duplicados (misma matrícula en misma sección)
    df_total = df_total.drop_duplicates(subset=["matricula", "seccion"])

    df_total.to_csv(SALIDA, index=False, encoding="utf-8-sig")
    print(f"\n✅ Maestro generado: {SALIDA.resolve()}")
    print(f"Filas finales: {len(df_total)}")

    if errores:
        print("\n⚠️ Archivos con problemas:")
        for name, msg in errores:
            print(f"- {name}: {msg}")

if __name__ == "__main__":
    main()
