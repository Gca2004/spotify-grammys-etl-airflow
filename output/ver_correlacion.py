import sys

sys.path.append("/opt/airflow/dags")

import pandas as pd
from transformaciones import combinar, transformar_grammys, transformar_spotify
from validaciones import limpiar_spotify

spotify = limpiar_spotify(pd.read_csv("/opt/airflow/data/dataset_spotify.csv"))
grammys = pd.read_csv("/opt/airflow/data/the_grammy_awards.csv")
final = combinar(transformar_spotify(spotify), transformar_grammys(grammys))

print("\nCorrelacion premios vs categorias (si es cercana a 1, una sobra):")
print(final[["premios_grammy", "categorias_distintas"]].corr().round(2).to_string())

base = final[final["en_spotify"]]
cols = ["popularidad_prom", "duracion_min_prom", "prop_explicit", "danceability_prom",
        "energy_prom", "valence_prom", "acousticness_prom", "tempo_prom"]

print("\nCorrelacion entre variables de audio (artistas en Spotify):")
print(base[cols].corr().round(2).to_string())

print("\nPromedio por grupo, sin Grammy (False) vs con Grammy (True):")
print(base.groupby("tiene_grammy")[cols].mean().round(3).T.to_string())