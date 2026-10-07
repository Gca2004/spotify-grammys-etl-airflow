import pandas as pd

pd.set_option("display.width", 200)
pd.set_option("display.max_columns", 50)

df = pd.read_csv("/opt/airflow/data/dataset_spotify.csv")

print("FORMA (filas, columnas):", df.shape)

print("\nCOLUMNAS Y TIPOS:")
print(df.dtypes)

print("\nVALORES FALTANTES POR COLUMNA:")
print(df.isna().sum())

print("\nTRACK_ID REPETIDOS:", df["track_id"].duplicated().sum())
print("FILAS TOTALMENTE REPETIDAS:", df.duplicated().sum())

print("\nMINIMO Y MAXIMO DE LAS COLUMNAS NUMERICAS:")
print(df.describe().T[["min", "max"]])

print("\nPRIMERAS 3 FILAS:")
print(df.head(3).T)