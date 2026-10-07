import sys
from datetime import datetime

from airflow.sdk import dag, task

sys.path.append("/opt/airflow/dags")


@dag(
    dag_id="validar_datasets",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["taller2", "validacion"],
)
def validar_datasets():
    @task
    def validar_spotify_task():
        import pandas as pd
        from validaciones import limpiar_spotify, perfilar, validar_spotify

        df = pd.read_csv("/opt/airflow/data/dataset_spotify.csv")
        perfilar(df, "spotify")
        df = limpiar_spotify(df)
        resultado = validar_spotify(df)
        if not resultado["paso"]:
            raise ValueError(f"Spotify no paso la validacion: {resultado['errores']} casos")
        return resultado

    @task
    def validar_grammys_task():
        import pandas as pd
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        from validaciones import perfilar, validar_grammys

        hook = PostgresHook(postgres_conn_id="postgres_workshop")
        df = pd.read_sql("SELECT * FROM grammy_awards", hook.get_sqlalchemy_engine())
        perfilar(df, "grammys")
        resultado = validar_grammys(df)
        if not resultado["paso"]:
            raise ValueError(f"Grammys no paso la validacion: {resultado['errores']} casos")
        return resultado

    validar_spotify_task()
    validar_grammys_task()


validar_datasets()