import sys

sys.path.append("/opt/airflow/dags")

import pandas as pd
from transformaciones import combinar, transformar_grammys, transformar_spotify
from validaciones import limpiar_spotify

spotify = limpiar_spotify(pd.read_csv("/opt/airflow/data/dataset_spotify.csv"))
grammys = pd.read_csv("/opt/airflow/data/the_grammy_awards.csv")
final = combinar(transformar_spotify(spotify), transformar_grammys(grammys))

print("\nCOLUMNAS DEL MERGE:")
info = pd.DataFrame(
    {
        "tipo": final.dtypes.astype(str),
        "nulos": final.isna().sum(),
        "distintos": final.nunique(),
        "ejemplo": final.iloc[0].astype(str),
    }
)
print(info.to_string())