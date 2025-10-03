import pandas as pd
from datetime import timedelta

MESES = {1: "Enero", 2: "Febrero", 3: "Marzo", 4: "Abril", 
         5: "Mayo", 6: "Junio", 7: "Julio", 8: "Agosto", 
         9: "Septiembre", 10: "Octubre", 11: "Noviembre", 12: "Diciembre"}

def agrupar_descargas(df_descargas_completo):
    df = df_descargas_completo.copy()
    df["Fecha"] = pd.to_datetime(df["Fecha"])
    df["diff"] = (df.groupby(["Nombre programa", "Producto", "Planta"])["Fecha"].diff().dt.days)
    df["Grupo"] = (df["diff"] != 1).cumsum()
    df_descargas_agrupadas = (df.groupby("Grupo", as_index=False).agg({"Fecha": ["min", "max"],
                                                                       "Volumen": "sum"}))
    df_descargas_agrupadas.columns = ["Grupo", "Fecha inicio", "Fecha fin", "Volumen total"]
    df_descargas_completo_sin_duplicados = df.drop_duplicates(subset=["Grupo"])
    df_descargas_agrupadas = df_descargas_agrupadas.merge(
        df_descargas_completo_sin_duplicados, on="Grupo").drop(columns=["diff", "Fecha", "Volumen"])
    df_descargas_agrupadas = df_descargas_agrupadas[[
        "Nombre programa", "N° Referencia", "Nombre del BT", "Producto", "Planta", "Ciudad", "Alias",
        "Fecha inicio", "Fecha fin", "Volumen total"]]

    # Ordenar por Nombre programa y Fecha inicio, y agregar N° Descarga
    df_descargas_agrupadas = df_descargas_agrupadas.sort_values(by=["Nombre programa", "Fecha inicio"])
    df_descargas_agrupadas["N° Descarga"] = (df_descargas_agrupadas.groupby("Nombre programa").cumcount() + 1)
    df_descargas_agrupadas.index = range(1, len(df_descargas_agrupadas) + 1)
    
    return df_descargas_agrupadas

def rellenar_etas(df_descargas_agrupadas, matriz_de_tiempos):
    for programa, group in df_descargas_agrupadas.groupby('Nombre programa'):
        group = group.sort_values('N° Descarga').copy()
        for i in range(len(group)):
            if pd.isna(group.iloc[i]['ETA']):
                if i == 0:
                    continue  # No se puede calcular ETA para la primera descarga sin ETA
                prev = group.iloc[i-1]
                ciudad_origen = prev['Ciudad']
                ciudad_destino = group.iloc[i]['Ciudad']

                # Hora de salida: fecha fin anterior a las 23:00
                hora_salida = prev['Fecha fin'].replace(hour=23, minute=0, second=0)
                
                # Si alguna ciudad es NaN (probablemente PUMA o ENAP), tiempo de viaje 0.
                if pd.isna(ciudad_origen) or pd.isna(ciudad_destino):
                    df_descargas_agrupadas.loc[group.index[i], 'ETA'] = hora_salida
                    continue
                horas_viaje = matriz_de_tiempos.loc[ciudad_origen, ciudad_destino.upper()]
                df_descargas_agrupadas.loc[group.index[i], 'ETA'] = hora_salida + timedelta(hours=int(horas_viaje))

def asignar_año_mes(df_descargas_completo, df_BD):
    df = df_descargas_completo.copy()
    df["Fecha"] = pd.to_datetime(df["Fecha"])
    df["Mes_Año"] = df["Fecha"].dt.to_period("M")
    vol_por_mes = df.groupby(["N° Referencia", "Mes_Año"], as_index=False)["Volumen"].sum()
    mes_mayor_volumen = vol_por_mes.loc[vol_por_mes.groupby("N° Referencia")["Volumen"].idxmax()]
    df_BD = df_BD.merge(mes_mayor_volumen[["N° Referencia", "Mes_Año"]],
                left_on="CC", right_on="N° Referencia", how="left").drop(columns=["N° Referencia"])
    df_BD["Mes"] = df_BD["Mes_Año"].dt.month
    df_BD["Año"] = df_BD["Mes_Año"].dt.year
    df_BD.drop(columns=["Mes_Año"], inplace=True)
    df_BD.index = range(1, len(df_BD) + 1)
    df_BD["Mes"] = df_BD["Mes"].map(MESES)
    
    return df_BD
