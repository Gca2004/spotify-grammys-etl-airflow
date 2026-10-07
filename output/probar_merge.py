import sys

sys.path.append("/opt/airflow/dags")

import pandas as pd
from transformaciones import combinar, para_csv, transformar_grammys, transformar_spotify
from validaciones import limpiar_spotify

spotify = limpiar_spotify(pd.read_csv("/opt/airflow/data/dataset_spotify.csv"))
grammys = pd.read_csv("/opt/airflow/data/the_grammy_awards.csv")

final = combinar(transformar_spotify(spotify), transformar_grammys(grammys))

print("\nForma del resultado:", final.shape)

print("\nNulos que quedan en la tabla (esto es lo que va a PostgreSQL):")
nulos = final.isna().sum()
print(nulos[nulos > 0].to_string())

print("\nTop 10 artistas con mas premios Grammy:")
cols = ["artista", "premios_grammy", "en_spotify", "popularidad_prom"]
top = final.sort_values("premios_grammy", ascending=False)
print(top[cols].head(10).to_string(index=False))

print("\nU2 en el resultado:")
print(final[final["artista_key"] == "u2"][cols].to_string(index=False))

print("\nEjemplo de artistas solo en Grammys, como saldran en el CSV:")
solo = final[~final["en_spotify"]].head(5)
print(para_csv(solo).to_string(index=False))