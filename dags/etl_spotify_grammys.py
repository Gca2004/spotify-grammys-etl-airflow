
import os
import sys
from datetime import datetime, timedelta

from airflow.sdk import dag, task
from airflow.sdk.exceptions import AirflowFailException

# los modulos de apoyo (transformaciones.py y validaciones.py) viven en la carpeta dags/
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# ---------------------------------------------------------------- configuracion
CONN_ID = "postgres_workshop"  # conexion de Airflow hacia PostgreSQL
TABLA_GRAMMYS = "grammy_awards"  # fuente 2 (ya cargada en la BD)
TABLA_FINAL = "artistas_spotify_grammys"  # destino del pipeline

DATA_DIR = os.getenv("ETL_DATA_DIR", "/opt/airflow/data")
OUTPUT_DIR = os.getenv("ETL_OUTPUT_DIR", "/opt/airflow/output")
SPOTIFY_CSV = os.path.join(DATA_DIR, "dataset_spotify.csv")
STAGING_DIR = os.path.join(DATA_DIR, "staging")
CSV_FINAL = os.path.join(OUTPUT_DIR, "artistas_spotify_grammys.csv")
CSV_RECHAZADOS = os.path.join(OUTPUT_DIR, "rechazados_spotify.csv")

DDL_TABLA_FINAL = f"""
CREATE TABLE IF NOT EXISTS {TABLA_FINAL} (
    artista_key          TEXT PRIMARY KEY,
    artista              TEXT NOT NULL,
    en_spotify           BOOLEAN NOT NULL,
    tiene_grammy         BOOLEAN NOT NULL,
    n_canciones          INTEGER NOT NULL,
    genero_principal     TEXT,
    popularidad_prom     DOUBLE PRECISION,
    duracion_min_prom    DOUBLE PRECISION,
    prop_explicit        DOUBLE PRECISION,
    danceability_prom    DOUBLE PRECISION,
    energy_prom          DOUBLE PRECISION,
    valence_prom         DOUBLE PRECISION,
    acousticness_prom    DOUBLE PRECISION,
    tempo_prom           DOUBLE PRECISION,
    premios_grammy       INTEGER NOT NULL,
    primer_anio          INTEGER,
    ultimo_anio          INTEGER,
    categorias_distintas INTEGER NOT NULL
)
"""


# ---------------------------------------------------------------- utilidades
def _guardar(df, nombre):
    """Guarda un DataFrame intermedio como parquet y devuelve su ruta."""
    os.makedirs(STAGING_DIR, exist_ok=True)
    ruta = os.path.join(STAGING_DIR, f"{nombre}.parquet")
    df.to_parquet(ruta, index=False)
    return ruta


def _exigir_validacion(resultado):
    """Hace fallar la tarea (sin reintentos) si el dataset no paso la validacion."""
    if not resultado["paso"]:
        raise AirflowFailException(
            f"{resultado['dataset']} NO paso la validacion: "
            f"{resultado['errores']} casos. Detalle: {resultado['resumen']}"
        )


# ---------------------------------------------------------------- DAG
@dag(
    dag_id="etl_spotify_grammys",
    description="ETL Spotify (CSV) + Grammys (PostgreSQL) -> PostgreSQL + CSV",
    doc_md=__doc__,
    start_date=datetime(2026, 1, 1),
    schedule=None,  # ejecucion manual
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "taller2", "retries": 2, "retry_delay": timedelta(seconds=30)},
    tags=["taller2", "etl", "spotify", "grammys"],
)
def etl_spotify_grammys():
    # ------------------------------------------------ rama Spotify (CSV)
    @task
    def read_csv():
        import pandas as pd

        df = pd.read_csv(SPOTIFY_CSV)
        print(f"[read_csv] {len(df)} filas y {df.shape[1]} columnas leidas de {SPOTIFY_CSV}")
        return {"ruta": _guardar(df, "spotify_raw"), "filas": len(df)}

    @task
    def validate_csv(entrada):
        import pandas as pd
        from validaciones import limpiar_spotify, perfilar, validar_spotify

        df = pd.read_parquet(entrada["ruta"])
        perfilar(df, "spotify")

        # anomalia conocida: filas con duracion <= 0. Se apartan y se dejan registradas.
        rechazados = df[df["duration_ms"] <= 0]
        if len(rechazados):
            os.makedirs(OUTPUT_DIR, exist_ok=True)
            rechazados.to_csv(CSV_RECHAZADOS, index=False, encoding="utf-8-sig")
            print(f"[validate_csv] {len(rechazados)} fila(s) rechazada(s) -> {CSV_RECHAZADOS}")
        df = limpiar_spotify(df)

        resultado = validar_spotify(df)
        _exigir_validacion(resultado)
        return {
            "ruta": _guardar(df, "spotify_validado"),
            "filas": len(df),
            "rechazadas": len(rechazados),
        }

    @task
    def transform_csv(entrada):
        import pandas as pd
        from transformaciones import transformar_spotify

        resultado = transformar_spotify(pd.read_parquet(entrada["ruta"]))
        return {"ruta": _guardar(resultado, "spotify_artistas"), "filas": len(resultado)}

    # ------------------------------------------------ rama Grammys (PostgreSQL)
    @task
    def read_db():
        import pandas as pd
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        hook = PostgresHook(postgres_conn_id=CONN_ID)
        df = pd.read_sql(f"SELECT * FROM {TABLA_GRAMMYS}", hook.get_sqlalchemy_engine())
        print(f"[read_db] {len(df)} filas leidas de la tabla {TABLA_GRAMMYS}")
        return {"ruta": _guardar(df, "grammys_raw"), "filas": len(df)}

    @task
    def validate_db(entrada):
        import pandas as pd
        from validaciones import perfilar, validar_grammys

        df = pd.read_parquet(entrada["ruta"])
        perfilar(df, "grammys")
        resultado = validar_grammys(df)
        _exigir_validacion(resultado)
        return {"ruta": entrada["ruta"], "filas": len(df)}  # sin limpieza: pasa tal cual

    @task
    def transform_db(entrada):
        import pandas as pd
        from transformaciones import transformar_grammys

        df = pd.read_parquet(entrada["ruta"])
        # cada fila debe ser un premio ganado; se deja constancia y se filtra por seguridad
        no_ganadores = int((~df["winner"]).sum())
        print(f"[transform_db] filas que no son ganadores: {no_ganadores}")
        resultado = transformar_grammys(df[df["winner"]])
        return {"ruta": _guardar(resultado, "grammys_artistas"), "filas": len(resultado)}

    # ------------------------------------------------ integracion
    @task
    def merge(spotify, grammys):
        import pandas as pd
        from transformaciones import SIN_DATO, combinar

        final = combinar(pd.read_parquet(spotify["ruta"]), pd.read_parquet(grammys["ruta"]))
        # almacenamiento: faltante = NULL ("sin dato" existe solo en el CSV exportado)
        final["genero_principal"] = final["genero_principal"].mask(
            final["genero_principal"] == SIN_DATO
        )
        return {
            "ruta": _guardar(final, "final"),
            "total": len(final),
            "en_ambos": int((final["en_spotify"] & final["tiene_grammy"]).sum()),
            "solo_spotify": int((final["en_spotify"] & ~final["tiene_grammy"]).sum()),
            "solo_grammys": int((~final["en_spotify"]).sum()),
        }

    @task
    def load(entrada):
        import pandas as pd
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        from sqlalchemy import text

        df = pd.read_parquet(entrada["ruta"])
        engine = PostgresHook(postgres_conn_id=CONN_ID).get_sqlalchemy_engine()

        # una sola transaccion: si algo falla, la tabla queda como estaba (idempotente)
        with engine.begin() as conn:
            conn.execute(text(DDL_TABLA_FINAL))
            conn.execute(text(f"TRUNCATE TABLE {TABLA_FINAL}"))
            df.to_sql(TABLA_FINAL, conn, if_exists="append", index=False, chunksize=5000)
            cargadas = conn.execute(text(f"SELECT COUNT(*) FROM {TABLA_FINAL}")).scalar_one()
            if cargadas != len(df):
                raise AirflowFailException(
                    f"Se esperaban {len(df)} filas en {TABLA_FINAL} y hay {cargadas}"
                )
        print(f"[load] {cargadas} filas cargadas en la tabla {TABLA_FINAL}")
        return {"tabla": TABLA_FINAL, "filas": int(cargadas)}

    @task
    def store(entrada, cargado):
        # `cargado` solo existe para que `store` espere a `load` (dependencia por datos)
        import pandas as pd
        from transformaciones import para_csv

        df = pd.read_parquet(entrada["ruta"])
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        para_csv(df).to_csv(CSV_FINAL, index=False, encoding="utf-8-sig")
        print(f"[store] {len(df)} filas exportadas a {CSV_FINAL}")
        return {"ruta": CSV_FINAL, "filas": len(df)}

    # ------------------------------------------------ dependencias
    spotify = transform_csv(validate_csv(read_csv()))
    grammys = transform_db(validate_db(read_db()))
    combinado = merge(spotify, grammys)
    store(combinado, load(combinado))


etl_spotify_grammys()