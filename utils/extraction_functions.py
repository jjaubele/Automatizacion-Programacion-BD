import pandas as pd
import numpy as np
from utils.utils import int_to_excel_col, next_excel_col

FIRST_ROW = 12
LAST_ROW = 61

def extraer_planificacion(file, sheet):
    df_planificacion = pd.read_excel(file, sheet_name=sheet, header=None)
    df_planificacion.index = range(1, len(df_planificacion) + 1)
    df_planificacion.columns = [int_to_excel_col(i) for i in range(1, len(df_planificacion.columns) + 1)]

    return df_planificacion

def extraer_bts(file, sheet):
    df_bts = pd.read_excel(file, sheet_name=sheet, header=0)
    df_bts.index = range(1, len(df_bts) + 1)
    df_bts = df_bts[["N° Referencia", "Nombre programa", "Nombre del BT", "Abrev."]]
    df_bts.drop_duplicates(subset=["N° Referencia"], keep="first", inplace=True)
    
    return df_bts

def extraer_descargas(df_planificacion):
    df_descargas = pd.DataFrame({"Fecha": [], "Abrev.": [], "Volumen": [], "Columna": []})
    columnas = ["M", "S", "Y", "AE", "AK", "AQ", "AW", "BC", "BI", "BO", "BU", "CA", "CF", "CK", "CP", "CU", "DA"]
    for col in columnas:
        descargas_parciales = df_planificacion.loc[FIRST_ROW:LAST_ROW, ["B", col, next_excel_col(col)]].dropna()
        descargas_parciales.columns = ["Fecha", "Abrev.", "Volumen"]
        descargas_parciales["Columna"] = [col] * len(descargas_parciales)
        df_descargas = pd.concat([df_descargas, descargas_parciales], ignore_index=True)
    df_descargas = df_descargas[(df_descargas["Abrev."] != "Enap") & (df_descargas["Abrev."] != "Puma")]

    return df_descargas

def extraer_tiempos_de_viaje(file, sheet):
    VELOCIDAD_MEDIA = 12  # nudos
    df_distancias = pd.read_excel(file, sheet_name=sheet, header=3, index_col=0)
    df_distancias.dropna(axis=1, how='all', inplace=True)
    df_distancias.dropna(axis=0, how='all', inplace=True)
    matriz_de_tiempos = np.ceil(df_distancias / VELOCIDAD_MEDIA).astype(int)

    return matriz_de_tiempos

def extraer_programas(df_planificacion):
    df_programas = df_planificacion.loc[FIRST_ROW:LAST_ROW, ["B", "J"]].dropna()
    df_programas.columns = ["ETA", "Nombre programa"]
    df_programas["ETA"] = pd.to_datetime(df_programas["ETA"]).apply(lambda dt: dt.replace(hour=12, minute=0, second=0) if pd.notna(dt) else dt)
    return df_programas

def extraer_nueva_ficha(file, sheet):
    df_nueva_ficha = pd.read_excel(file, sheet_name=sheet, header=3)
    df_nueva_ficha = df_nueva_ficha[["N° Referencia", "Proveedor", "Inicio Ventana",
                                     "Fin Ventana", "Inicio Ventana Corta", "Fin Ventana Corta"]]
    df_nueva_ficha.drop_duplicates(subset=["N° Referencia"], keep="first", inplace=True)
    
    return df_nueva_ficha