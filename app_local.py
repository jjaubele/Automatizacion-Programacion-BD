import pandas as pd
import streamlit as st
import signal
import os
from utils.aggregation_functions import rellenar_etas, agrupar_descargas, formato_BD, estimar_demurrage
from utils.extraction_functions import (extraer_bts, extraer_descargas, extraer_tiempos_de_viaje,
                   extraer_planificacion, extraer_programas, extraer_nueva_ficha, extraer_productos_plantas)

st.title("Automatización Programación Descargas")

# Subir archivos
PATH_DISTANCIAS = "Distancias entre puertos.xlsx"
PATH_PROGRAMACION = st.file_uploader("Sube la Programación de Descargas", type=["xlsx", "xls"])
PATH_NUEVA_FICHA = st.file_uploader("Sube la Nueva Ficha", type=["xlsx", "xls"])

# Selección de fecha
FECHA_PROGRAMACION = pd.to_datetime(st.date_input("Selecciona la fecha de programación"))
FILE_NAME = f"Base de datos Estimación Semanal {FECHA_PROGRAMACION.strftime('%d-%m-%Y')}.xlsx"

if st.button("Procesar Archivos"):
    if not (PATH_DISTANCIAS and PATH_PROGRAMACION and PATH_NUEVA_FICHA):
        st.error("Faltan uno o más archivos por subir.")
    else:
        try:
            df_bts = extraer_bts(PATH_PROGRAMACION, "Buques")
            df_planificacion = extraer_planificacion(PATH_PROGRAMACION, "Planificación")
            df_descargas = extraer_descargas(df_planificacion, ignore_not_bts=True, df_bts=df_bts)
            df_programas = extraer_programas(df_planificacion)
            df_productos_plantas = extraer_productos_plantas()
            df_nueva_ficha = extraer_nueva_ficha(PATH_NUEVA_FICHA, "Programación de buques", df_programas=df_programas)
            matriz_de_tiempos = extraer_tiempos_de_viaje("Distancias entre puertos.xlsx", "Datos")

            df_descargas_productos_plantas = df_descargas.merge(df_productos_plantas, on=["Columna"]).drop(columns=["Columna"])
            df_descargas_completo = df_descargas_productos_plantas.merge(df_bts, on=["Abrev."]).drop(columns=["Abrev."])
            df_descargas_completo = df_descargas_completo[["Fecha", "N° Referencia", "Nombre programa", "Nombre del BT",
                                                        "Producto", "Planta", "Ciudad", "Alias", "Volumen"]]
            df_descargas_agrupadas = agrupar_descargas(df_descargas_completo)

            df_programas_completo = df_programas.merge(df_nueva_ficha, on="N° Referencia", how="left")
            df_programas_completo["Inicio Ventana"] = df_programas_completo["Inicio Ventana Corta"].combine_first(df_programas_completo["Inicio Ventana"])
            df_programas_completo["Fin Ventana"] = df_programas_completo["Fin Ventana Corta"].combine_first(df_programas_completo["Fin Ventana"])
            df_programas_completo["ETA"] = df_programas_completo["ETA"].combine_first(df_programas_completo["ETA Programa"])
            df_programas_completo["MONTO ($/DIA)"].fillna(df_programas_completo["MONTO ($/DIA)"].mean(), inplace=True)
            df_programas_completo = df_programas_completo.drop(columns=["Inicio Ventana Corta", "Fin Ventana Corta", "ETA Programa"])

            df_descargas_por_programa = df_descargas_agrupadas.merge(df_programas_completo, on="N° Referencia", how="right")
            df_descargas_por_programa["ETA"] = df_descargas_por_programa["ETA"][[True if descarga == 1 else False for descarga in df_descargas_por_programa["N° Descarga"]]]
            rellenar_etas(df_descargas_por_programa, matriz_de_tiempos)
            df_estimacion = estimar_demurrage(df_descargas_por_programa)

            df_BD = formato_BD(df_estimacion, df_descargas_completo, FECHA_PROGRAMACION)
            df_BD.to_excel(FILE_NAME, index=False)
            st.success(f"Archivo procesado y guardado como {FILE_NAME}")
        except Exception as e:
            st.error(f"Error al procesar los archivos: {e}")

if st.button("Cerrar aplicación"):
    os.kill(os.getpid(), signal.SIGTERM)
