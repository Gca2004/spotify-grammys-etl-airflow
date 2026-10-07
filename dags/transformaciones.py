import re
import unicodedata

import pandas as pd

SIN_DATO = "sin dato"


def _normalizar_nombre(texto):
    if pd.isna(texto):
        return None
    texto = unicodedata.normalize("NFKD", str(texto))
    texto = "".join(c for c in texto if not unicodedata.combining(c))
    texto = re.sub(r"[()]", "", texto)  # quita parentesis: "(Depeche Mode)" -> "Depeche Mode"
    texto = re.sub(r"\s+", " ", texto).strip().lower()
    if texto in ("", "various artists"):  # no es un artista real
        return None
    return texto



def transformar_spotify(df: pd.DataFrame) -> pd.DataFrame:
    df = df.loc[:, ~df.columns.str.startswith("Unnamed")].copy()
    antes = len(df)
    df = df.dropna(subset=["artists", "track_name"])
    print(f"[spotify] {antes - len(df)} fila(s) sin artista o nombre descartadas")

    # una fila por (cancion, artista)
    df["artista"] = df["artists"].str.split(";")
    df = df.explode("artista")
    df["artista_key"] = df["artista"].map(_normalizar_nombre)
    df = df.dropna(subset=["artista_key"])
    df["duracion_min"] = df["duration_ms"] / 60000

    # genero principal: se calcula ANTES de quitar repetidos por genero
    conteo = df.groupby(["artista_key", "track_genre"]).size().reset_index(name="n")
    genero = (
        conteo.sort_values("n", ascending=False)
        .drop_duplicates("artista_key")[["artista_key", "track_genre"]]
        .rename(columns={"track_genre": "genero_principal"})
    )

    # una misma cancion aparece una vez por genero: dejamos una sola
    canciones = df.drop_duplicates(subset=["track_id", "artista_key"])
    resumen = canciones.groupby("artista_key").agg(
        artista=("artista", "first"),
        n_canciones=("track_id", "nunique"),
        popularidad_prom=("popularity", "mean"),
        duracion_min_prom=("duracion_min", "mean"),
        prop_explicit=("explicit", "mean"),
        danceability_prom=("danceability", "mean"),
        energy_prom=("energy", "mean"),
        valence_prom=("valence", "mean"),
        acousticness_prom=("acousticness", "mean"),
        tempo_prom=("tempo", "mean"),
    ).reset_index()

    resultado = resumen.merge(genero, on="artista_key", how="left").round(3)
    print(f"[spotify] {len(resultado)} artistas distintos")
    return resultado


def transformar_grammys(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    antes = len(df)
    df = df.dropna(subset=["artist"])
    print(f"[grammys] {antes - len(df)} fila(s) sin artista descartadas")

    df["artista_key"] = df["artist"].map(_normalizar_nombre)
    df = df.dropna(subset=["artista_key"])

    # el CSV solo trae ganadores: cada fila es un premio
    resultado = df.groupby("artista_key").agg(
        artista_grammys=("artist", "first"),
        premios_grammy=("category", "size"),
        primer_anio=("year", "min"),
        ultimo_anio=("year", "max"),
        categorias_distintas=("category", "nunique"),
    ).reset_index()
    print(f"[grammys] {len(resultado)} artistas distintos")
    return resultado


def combinar(spotify_art: pd.DataFrame, grammys_art: pd.DataFrame) -> pd.DataFrame:
    final = spotify_art.merge(grammys_art, on="artista_key", how="outer", indicator=True)

    final["en_spotify"] = final["_merge"].isin(["both", "left_only"])
    final = final.drop(columns="_merge")

    # artistas que solo estan en Grammys: usamos su nombre de Grammys
    final["artista"] = final["artista"].fillna(final["artista_grammys"])
    final = final.drop(columns="artista_grammys")

    # conteos: 0 es un dato real (no tiene canciones / no tiene premios)
    for col in ["premios_grammy", "categorias_distintas", "n_canciones"]:
        final[col] = final[col].fillna(0).astype(int)
    final[["primer_anio", "ultimo_anio"]] = final[["primer_anio", "ultimo_anio"]].astype("Int64")
    final["tiene_grammy"] = final["premios_grammy"] > 0

    # columnas de texto: "sin dato" desde ya
    final["genero_principal"] = final["genero_principal"].fillna(SIN_DATO)

    columnas = [
        "artista_key", "artista", "en_spotify", "tiene_grammy",
        "n_canciones", "genero_principal", "popularidad_prom", "duracion_min_prom",
        "prop_explicit", "danceability_prom", "energy_prom", "valence_prom",
        "acousticness_prom", "tempo_prom",
        "premios_grammy", "primer_anio", "ultimo_anio", "categorias_distintas",
    ]
    final = final[columnas]

    ambos = int((final["en_spotify"] & final["tiene_grammy"]).sum())
    solo_spotify = int((final["en_spotify"] & ~final["tiene_grammy"]).sum())
    solo_grammys = int((~final["en_spotify"]).sum())
    print(f"[merge] {len(final)} artistas en total")
    print(f"[merge] en ambos datasets: {ambos}")
    print(f"[merge] solo en Spotify: {solo_spotify}")
    print(f"[merge] solo en Grammys: {solo_grammys}")
    return final


def para_csv(df: pd.DataFrame) -> pd.DataFrame:
    """Version legible para el CSV: todo valor vacio se escribe como 'sin dato'."""
    return df.astype(object).where(df.notna(), SIN_DATO)