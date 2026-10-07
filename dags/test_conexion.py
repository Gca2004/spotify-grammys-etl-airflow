from datetime import datetime

from airflow.sdk import dag, task


@dag(
    dag_id="test_conexion_postgres",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
)
def test_conexion_postgres():
    @task
    def contar_grammys():
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        hook = PostgresHook(postgres_conn_id="postgres_workshop")
        total = hook.get_first("SELECT COUNT(*) FROM grammy_awards")[0]
        print(f"Filas en grammy_awards: {total}")
        return total

    contar_grammys()


test_conexion_postgres()