import pandas as pd
import numpy as np
from datetime import timedelta

def int_to_excel_col(n):
    result = ""
    while n > 0:
        n, remainder = divmod(n - 1, 26)
        result = chr(65 + remainder) + result
    return result

def excel_col_to_int(col):
    result = 0
    for char in col:
        result = result * 26 + (ord(char.upper()) - ord('A') + 1)
    return result

def next_excel_col(col):
    col_int = excel_col_to_int(col)
    next_col_int = col_int + 1
    return int_to_excel_col(next_col_int)

def extraer_planificacion(file, sheet):
    df_planificacion = pd.read_excel(file, sheet_name=sheet, header=None)
    df_planificacion.index = range(1, len(df_planificacion) + 1)
    df_planificacion.columns = [int_to_excel_col(i) for i in range(1, len(df_planificacion.columns) + 1)]

    return df_planificacion

def extraer_bts(file, sheet):
    df_bts = pd.read_excel(file, sheet_name=sheet, header=0)
    df_bts.index = range(1, len(df_bts) + 1)
    df_bts = df_bts[["N° Referencia", "Nombre programa", "Nombre del BT", "Abrev."]]
    df_bts.drop_duplicates(subset=["Abrev."], keep="first", inplace=True)
    
    return df_bts

def extraer_descargas(df_planificacion):
    df_descargas = pd.DataFrame({"Fecha": [], "Abrev.": [], "Volumen": [], "Columna": []})
    columnas = ["M", "S", "Y", "AE", "AK", "AQ", "AW", "BC", "BI", "BO", "BU", "CA", "CF", "CK", "CP", "CU", "DA"]
    for col in columnas:
        descargas_parciales = df_planificacion.loc[12:61, ["B", col, next_excel_col(col)]].dropna()
        descargas_parciales.columns = ["Fecha", "Abrev.", "Volumen"]
        descargas_parciales["Columna"] = [col] * len(descargas_parciales)
        df_descargas = pd.concat([df_descargas, descargas_parciales], ignore_index=True)
    df_descargas = df_descargas[(df_descargas["Abrev."] != "Enap") & (df_descargas["Abrev."] != "Puma")]

    return df_descargas

def extraer_distancias(file, sheet):
    df_distancias = pd.read_excel(file, sheet_name=sheet, header=3, index_col=0)
    df_distancias.dropna(axis=1, how='all', inplace=True)
    df_distancias.dropna(axis=0, how='all', inplace=True)
    return df_distancias

def extraer_programas(df_planificacion):
    df_programas = df_planificacion.loc[12:61, ["B", "J"]].dropna()
    df_programas.columns = ["Fecha", "Nombre programa"]
    return df_programas

def rellenar_etas(df_descargas, df_tiempos):
    # Copiamos para no modificar el original
    df = df_descargas.copy()
    
    for programa, group in df.groupby('Nombre programa'):
        group = group.sort_values('N° Descarga').copy()
        for i in range(len(group)):
            if pd.isna(group.iloc[i]['ETA']):
                if i == 0:
                    continue  # No hay anterior
                prev = group.iloc[i-1]
                ciudad_origen = prev['Ciudad']
                ciudad_destino = group.iloc[i]['Ciudad']

                # Hora de salida: fecha fin anterior a las 23:00
                hora_salida = prev['Fecha fin'].replace(hour=23, minute=0, second=0)
                
                if pd.isna(ciudad_origen) or pd.isna(ciudad_destino):
                    df.loc[group.index[i], 'ETA'] = hora_salida
                    continue
                
                # Horas de viaje desde la matriz
                horas_viaje = df_tiempos.loc[ciudad_origen, ciudad_destino.upper()]
                horas_viaje = horas_viaje
                
                # Rellenar ETA
                df.loc[group.index[i], 'ETA'] = hora_salida + timedelta(hours=int(horas_viaje))

    return df