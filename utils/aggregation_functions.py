import pandas as pd
import numpy as np
from datetime import timedelta
from utils.utils import get_week_of_month

HORAS_LAYTIME = 132
MESES = {1: "Enero", 2: "Febrero", 3: "Marzo", 4: "Abril", 
         5: "Mayo", 6: "Junio", 7: "Julio", 8: "Agosto", 
         9: "Septiembre", 10: "Octubre", 11: "Noviembre", 12: "Diciembre"}

def agrupar_descargas(df_descargas_completo):
    df = df_descargas_completo.copy()
    df["diff"] = (df.groupby(["Nombre programa", "Producto", "Planta"])["Fecha"].diff().dt.days)
    df["Descarga"] = (df["diff"] != 1).cumsum()
    df_descargas_agrupadas = (df.groupby("Descarga", as_index=False).agg({"Fecha": ["min", "max"],
                                                                       "Volumen": "sum"}))
    df_descargas_agrupadas.columns = ["Descarga", "Fecha inicio", "Fecha fin", "Volumen total"]
    df_descargas_completo_sin_duplicados = df.drop_duplicates(subset=["Descarga"])
    df_descargas_agrupadas = df_descargas_agrupadas.merge(
        df_descargas_completo_sin_duplicados, on="Descarga").drop(columns=["diff", "Fecha", "Volumen"])
    df_descargas_agrupadas = df_descargas_agrupadas[[
        "Nombre programa", "N° Referencia", "Nombre del BT", "Producto", "Planta", "Ciudad", "Alias",
        "Fecha inicio", "Fecha fin", "Volumen total"]]

    # Ordenar por Nombre programa y Fecha inicio, y agregar N° Descarga
    df_descargas_agrupadas = df_descargas_agrupadas.sort_values(by=["Nombre programa", "Fecha inicio"])
    df_descargas_agrupadas["N° Descarga"] = (df_descargas_agrupadas.groupby("Nombre programa").cumcount() + 1)
    df_descargas_agrupadas.index = range(1, len(df_descargas_agrupadas) + 1)
    df_descargas_agrupadas["Fecha inicio"] = pd.to_datetime(df_descargas_agrupadas["Fecha inicio"]).apply(lambda dt: dt.replace(hour=15, minute=00, second=00) if pd.notna(dt) else dt)
    df_descargas_agrupadas["Fecha fin"] = pd.to_datetime(df_descargas_agrupadas["Fecha fin"]).apply(lambda dt: dt.replace(hour=23, minute=00, second=00) if pd.notna(dt) else dt)

    return df_descargas_agrupadas

def asignar_ciudad_a_puma(i, group):
    current = group.iloc[i]
    prev_bool = False
    next_bool = False
    if i - 1 >= 0:
        prev = group.iloc[i-1]
        prev_bool = True
    if i + 1 < len(group):
        next = group.iloc[i+1]
        next_bool = True
    if prev_bool and next_bool:
        delta_1 = (current['Fecha inicio'] - prev['Fecha fin']).total_seconds()
        delta_2 = (next['Fecha inicio'] - current['Fecha fin']).total_seconds()
        if delta_1 <= delta_2:
            current['Ciudad'] = prev['Ciudad']
        else:
            current['Ciudad'] = next['Ciudad']
    elif prev_bool:
        current['Ciudad'] = prev['Ciudad']
    elif next_bool:
        current['Ciudad'] = next['Ciudad']

    return current['Ciudad']

def rellenar_etas(df_descargas_agrupadas, matriz_de_tiempos):
    df_descargas_agrupadas["NOR + 6"] = True
    df_descargas_agrupadas["Shifting"] = np.nan
    for programa, group in df_descargas_agrupadas.groupby('Nombre programa'):
        group = group.sort_values('N° Descarga').copy()
        for i in range(len(group)):
            current = group.iloc[i]
            if pd.isna(current['ETA']):
                if i == 0:
                    continue  # No se calcula ETA si es la primera descarga
                prev = group.iloc[i-1]
                ciudad_origen = prev['Ciudad']
                ciudad_destino = current['Ciudad']
                hora_salida = prev['Fecha fin']
                if pd.isna(ciudad_origen):
                    ciudad_origen = asignar_ciudad_a_puma(i-1, group)
                    df_descargas_agrupadas.loc[group.index[i-1], 'Ciudad'] = ciudad_origen
                if pd.isna(ciudad_destino):
                    ciudad_destino = asignar_ciudad_a_puma(i, group)
                    df_descargas_agrupadas.loc[group.index[i], 'Ciudad'] = ciudad_destino
                # Puma-Puma
                if pd.isna(ciudad_origen) or pd.isna(ciudad_destino):
                    horas_viaje = 0
                else:
                    horas_viaje = matriz_de_tiempos.loc[ciudad_origen, ciudad_destino.upper()]
                df_descargas_agrupadas.loc[group.index[i], 'ETA'] = hora_salida + timedelta(hours=int(horas_viaje))
                # Puma-Puerto / Puerto-Puma / Puerto-Puerto
                if horas_viaje == 0:
                    df_descargas_agrupadas.loc[group.index[i], 'NOR + 6'] = False
                    # Puma-Puerto
                    if prev['Planta'] == 'BT PUMA' and current['Planta'] != 'BT PUMA':
                        df_descargas_agrupadas.loc[group.index[i], 'Shifting'] = 50000
                    # Puerto-Puma
                    elif prev['Planta'] != 'BT PUMA' and current['Planta'] == 'BT PUMA':
                        df_descargas_agrupadas.loc[group.index[i], 'Shifting'] = 5000
                    # Puerto-Puerto
                    elif prev['Planta'] != 'BT PUMA' and current['Planta'] != 'BT PUMA':
                        df_descargas_agrupadas.loc[group.index[i], 'Shifting'] = 50000

    return df_descargas_agrupadas

def asignar_año_mes(df_descargas_completo, df_BD):
    df = df_descargas_completo.copy()
    df["Mes_Año"] = df["Fecha"].dt.to_period("M")
    vol_por_mes = df.groupby(["N° Referencia", "Mes_Año"], as_index=False)["Volumen"].sum()
    mes_mayor_volumen = vol_por_mes.loc[vol_por_mes.groupby("N° Referencia")["Volumen"].idxmax()]
    df_BD = df_BD.merge(mes_mayor_volumen[["N° Referencia", "Mes_Año"]],
                left_on="CC", right_on="N° Referencia", how="left").drop(columns=["N° Referencia"])
    df_BD["Mes"] = df_BD["Mes_Año"].dt.month
    df_BD["Año"] = df_BD["Mes_Año"].dt.year
    df_BD.drop(columns=["Mes_Año"], inplace=True)
    df_BD["Mes"] = df_BD["Mes"].map(MESES)
    
    return df_BD

def estimar_demurrage(df_descargas_por_programa):
    df = df_descargas_por_programa.copy()
    inicio_laytime = pd.Series([pd.NaT] * len(df))
    Arribo = pd.Series([pd.NA] * len(df))
    # Condición 1: ETA < Inicio Ventana y N° de descarga = 1
    # Inicio laytime = min(Inicio Ventana + 6 horas, Inicio Descarga)
    cond_1 = (df['N° Descarga'] == 1) & (df['ETA'] < df['Inicio Ventana'])
    inicio_laytime = inicio_laytime.mask(cond_1, pd.concat([df['Inicio Ventana'] + pd.Timedelta(hours=6), df['Fecha inicio']], axis=1).min(axis=1))
    Arribo = Arribo.mask(cond_1, "Antes")
    # Condición 2: ETA en ventana y N° de descarga = 1
    # Inicio laytime = min(Inicio Descarga, ETA + 6 horas)
    cond_2 = (df['N° Descarga'] == 1) & (df['ETA'] >= df['Inicio Ventana']) & (df['ETA'] <= df['Fin Ventana'])
    inicio_laytime = inicio_laytime.mask(cond_2, pd.concat([df['Fecha inicio'], df['ETA'] + pd.Timedelta(hours=6)], axis=1).min(axis=1))
    Arribo = Arribo.mask(cond_2, "Dentro")
    # Condición 3: ETA > Fin Ventana y N° de descarga = 1
    # Inicio laytime = Inicio Descarga
    cond_3 = (df['N° Descarga'] == 1) & (df['ETA'] > df['Fin Ventana'])
    inicio_laytime = inicio_laytime.mask(cond_3, df_descargas_por_programa['Fecha inicio'])
    Arribo = Arribo.mask(cond_3, "Después")
    # Condición 4: N° de descarga > 1 y NOR + 6 = True (Horas de viaje > 0)
    # Inicio laytime = min(Inicio Descarga, ETA + 6 horas)
    cond_4 = (df['N° Descarga'] > 1) & (df['NOR + 6'] == True)
    inicio_laytime = inicio_laytime.mask(cond_4, pd.concat([df['Fecha inicio'], df['ETA'] + pd.Timedelta(hours=6)], axis=1).min(axis=1))
    # Condición 5: N° de descarga > 1 y NOR + 6 = False (Horas de viaje = 0)
    # Inicio laytime = ETA (Fin Descarga anterior)
    cond_5 = (df['N° Descarga'] > 1) & (df['NOR + 6'] == False)
    inicio_laytime = inicio_laytime.mask(cond_5, df['ETA'])

    df["Arribo"] = Arribo
    df["Arribo"] = df.groupby("Nombre programa")["Arribo"].transform('first')
    df['Inicio Laytime'] = inicio_laytime
    df["Tiempo descarga (Horas)"] = (df["Fecha fin"] - df["Inicio Laytime"]).dt.total_seconds() / 3600
    df["Tiempo programa (Horas)"] = df.groupby("Nombre programa")["Tiempo descarga (Horas)"].transform('sum')

    df["Laytime pactado (Horas)"] = [HORAS_LAYTIME] * len(df)
    df["Demurrage programa (Horas)"] = df.apply(lambda row: max(0, row["Tiempo programa (Horas)"] - row["Laytime pactado (Horas)"]), axis=1)
    df["Demurrage descarga (Horas)"] = df.apply(lambda row: row["Demurrage programa (Horas)"] * row["Tiempo descarga (Horas)"] / row["Tiempo programa (Horas)"], axis=1)
    df["Estimación demurrage"] = np.ceil(df["Demurrage descarga (Horas)"] * (df["MONTO ($/DIA)"] / 24))
    df["Demurrage unitario"] = df.apply(lambda row: row["Estimación demurrage"] / row["Volumen total"] if row["Volumen total"] > 0 else 0, axis=1)

    return df

def formato_BD(df_estimacion, df_descargas_completo, fecha_de_programacion):
    df_BD = pd.DataFrame({
        "Fecha de programación": pd.Series([fecha_de_programacion] * len(df_estimacion)).dt.strftime("%d-%m-%Y"),
        "Semana": [get_week_of_month(fecha_de_programacion.year, fecha_de_programacion.month, fecha_de_programacion.day)] * len(df_estimacion),
        "Año": [np.nan] * len(df_estimacion),
        "Mes": [np.nan] * len(df_estimacion),
        "Horas Laytime": df_estimacion["Laytime pactado (Horas)"],
        "CC": df_estimacion["N° Referencia"],
        "Nombre BT": df_estimacion["Nombre del BT"],
        "Proveedor": df_estimacion["Proveedor"],
        "Producto": df_estimacion["Producto"],
        "Demurrage": df_estimacion["MONTO ($/DIA)"],
        "Puerto": df_estimacion["Alias"],
        "Volumen": df_estimacion["Volumen total"],
        "Inicio Ventana": df_estimacion["Inicio Ventana"].dt.strftime("%d-%m-%Y"),
        "Final Ventana": df_estimacion["Fin Ventana"].dt.strftime("%d-%m-%Y"),
        "ETA": df_estimacion["ETA"].dt.strftime("%d-%m-%Y %H:%M"),
        "Fin descarga": df_estimacion["Fecha fin"].dt.strftime("%d-%m-%Y"),
        "Estimación demurrage": df_estimacion["Estimación demurrage"], 
        "Demurrage unitario": df_estimacion["Demurrage unitario"],
        "Shifting": df_estimacion["Shifting"]
    })

    df_BD = asignar_año_mes(df_descargas_completo, df_BD)
    
    return df_BD

def formato_lista_vertical(df_descargas_agrupadas):
    df_lista_vertical = df_descargas_agrupadas.copy()
    df_lista_vertical["Operación"] = [np.nan] * len(df_lista_vertical)
    df_lista_vertical = df_lista_vertical[["Nombre del BT", "N° Referencia", "Fecha inicio", "Fecha fin", "Operación", "Planta", "Producto", "Volumen total"]]
    df_lista_vertical.columns = ["BT", "CC", "Fecha inicio", "Fecha fin", "Operación", "Planta", "Producto", "Volumen"]
    df_lista_vertical["Fecha inicio"] = df_lista_vertical["Fecha inicio"].dt.strftime('%d-%m-%Y')
    df_lista_vertical["Fecha fin"] = df_lista_vertical["Fecha fin"].dt.strftime('%d-%m-%Y')
    
    return df_lista_vertical

