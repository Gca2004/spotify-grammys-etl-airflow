import pandas as pd
import pandera.pandas as pa
from pandera import Check
from pandera.errors import SchemaErrors

MAX_NULOS_TEXTO = 0.01  # se tolera hasta 1% de nulos en columnas de texto


def _max_nulos(maximo=MAX_NULOS_TEXTO):
    # ignore_na=False es necesario: si no, Pandera descarta los nulos antes de revisar
    return Check(
        lambda s: s.isna().mean() <= maximo,
        ignore_na=False,
        error=f"mas de {maximo:.0%} de valores nulos",
    )


def _entre_0_y_1():
    return pa.Column(float, Check.in_range(0.0, 1.0), nullable=False, coerce=True)


# ---------- SPOTIFY ----------
schema_spotify = pa.DataFrameSchema(
    {
        "track_id": pa.Column(str, nullable=False),
        "artists": pa.Column(str, nullable=True, checks=_max_nulos()),
        "album_name": pa.Column(str, nullable=True, checks=_max_nulos()),
        "track_name": pa.Column(str, nullable=True, checks=_max_nulos()),
        "popularity": pa.Column(int, Check.in_range(0, 100), coerce=True),
        "duration_ms": pa.Column(int, Check.gt(0), coerce=True),
        "explicit": pa.Column(bool, coerce=True),
        "danceability": _entre_0_y_1(),
        "energy": _entre_0_y_1(),
        "key": pa.Column(int, Check.in_range(-1, 11), coerce=True),
        "loudness": pa.Column(float, Check.in_range(-60.0, 10.0), coerce=True),
        "mode": pa.Column(int, Check.isin([0, 1]), coerce=True),
        "speechiness": _entre_0_y_1(),
        "acousticness": _entre_0_y_1(),
        "instrumentalness": _entre_0_y_1(),
        "liveness": _entre_0_y_1(),
        "valence": _entre_0_y_1(),
        "tempo": pa.Column(float, Check.ge(0), coerce=True),
        "time_signature": pa.Column(int, Check.in_range(0, 7), coerce=True),
        "track_genre": pa.Column(str, nullable=False),
    },
    strict=False,  # permite columnas extra (ej. el indice "Unnamed: 0")
)

# ---------- GRAMMYS ----------
schema_grammys = pa.DataFrameSchema(
    {
        "year": pa.Column(int, Check.in_range(1958, 2019), coerce=True),
        "title": pa.Column(str, nullable=False),
        "published_at": pa.Column(nullable=True),
        "updated_at": pa.Column(nullable=True),
        "category": pa.Column(str, nullable=False),
        "nominee": pa.Column(str, nullable=True),
        "artist": pa.Column(str, nullable=True),
        "workers": pa.Column(str, nullable=True),
        "img": pa.Column(str, nullable=True),
        "winner": pa.Column(bool, nullable=False, coerce=True),
    },
    strict=False,
)


def _validar(schema, df, nombre):
    try:
        schema.validate(df, lazy=True)  # lazy=True: reporta TODOS los errores, no solo el primero
        print(f"[{nombre}] VALIDACION APROBADA: {len(df)} filas, 0 errores")
        return {"dataset": nombre, "paso": True, "filas": len(df), "errores": 0, "resumen": []}
    except SchemaErrors as exc:
        fallos = exc.failure_cases
        resumen = (
            fallos.groupby(["column", "check"], dropna=False)
            .size()
            .reset_index(name="casos")
            .sort_values("casos", ascending=False)
        )
        print(f"[{nombre}] VALIDACION FALLIDA: {len(fallos)} casos con problemas")
        print(resumen.to_string(index=False))
        return {
            "dataset": nombre,
            "paso": False,
            "filas": len(df),
            "errores": int(len(fallos)),
            "resumen": resumen.astype(str).to_dict("records"),
        }


def validar_spotify(df: pd.DataFrame):
    df = df.loc[:, ~df.columns.str.startswith("Unnamed")]
    return _validar(schema_spotify, df, "spotify")


def validar_grammys(df: pd.DataFrame):
    return _validar(schema_grammys, df, "grammys")


def perfilar(df: pd.DataFrame, nombre: str):
    print(f"\n===== PERFIL DE CALIDAD: {nombre} =====")
    print(f"Filas: {len(df)} | Columnas: {df.shape[1]}")
    print(f"Filas totalmente duplicadas: {df.duplicated().sum()}")
    resumen = pd.DataFrame(
        {
            "tipo": df.dtypes.astype(str),
            "nulos": df.isna().sum(),
            "pct_nulos": (df.isna().mean() * 100).round(2),
            "distintos": df.nunique(),
        }
    )
    print(resumen.to_string())
    numericas = df.select_dtypes("number")
    if not numericas.empty:
        print("\nMinimo y maximo de columnas numericas:")
        print(numericas.describe().T[["min", "max"]].to_string())



def limpiar_spotify(df: pd.DataFrame):
    antes = len(df)
    df = df[df["duration_ms"] > 0].copy()
    print(f"[spotify] Limpieza: {antes - len(df)} fila(s) eliminada(s) por duration_ms <= 0")
    return df

