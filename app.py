import streamlit as st
import pandas as pd
import numpy as np
import signal
import os
from utils import (extraer_bts, extraer_descargas, extraer_distancias,
                   extraer_planificacion, extraer_programas, rellenar_etas,
                   extraer_nueva_ficha)

st.title("Automatización Programación Descargas")

# Subir archivos
PATH_DISTANCIAS = "Distancias entre puertos.xlsx"
PATH_PROGRAMACION = st.file_uploader("Sube la Programación de Descargas", type=["xlsx", "xls"])
PATH_NUEVA_FICHA = st.file_uploader("Sube la Nueva Ficha", type=["xlsx", "xls"])

# Selección de fecha
fecha_programacion = st.date_input("Selecciona la fecha de programación")
FILE_NAME = f"Base de datos Estimación Semanal {fecha_programacion.strftime('%d-%m-%Y')}.xlsx"

if st.button("Procesar Archivos"):
    if not (PATH_DISTANCIAS and PATH_PROGRAMACION and PATH_NUEVA_FICHA):
        st.error("Faltan uno o más archivos por subir.")
    else:
        try:
            df_bts = extraer_bts(PATH_PROGRAMACION, "Buques")
            df_planificacion = extraer_planificacion(PATH_PROGRAMACION, "Planificación")
            df_descargas = extraer_descargas(df_planificacion)
            df_programas = extraer_programas(df_planificacion)
            df_programas.columns = ["ETA", "Nombre programa"]
            df_nueva_ficha = extraer_nueva_ficha(PATH_NUEVA_FICHA, "Programación de buques")
            matriz_de_distancias = extraer_distancias("Distancias entre puertos.xlsx", "Datos")
            VELOCIDAD_MEDIA = 12  # nudos
            # Calcular matriz de tiempos aproximando en horas al entero superior
            matriz_de_tiempos = np.ceil(matriz_de_distancias / VELOCIDAD_MEDIA).astype(int)

            df_productos = pd.DataFrame({"Producto": ["Diesel A1", "Gas 93", "Gas 97", "Jet A1"]})
            df_plantas = pd.DataFrame({"Planta": ["PLANTA IQUIQUE", "PLANTA MEJILLONES", "PLANTA CALDERA",
                                                "TERMINAL TPI", "OXIQUIM QUINTERO", "OXIQUIM CORONEL",
                                                "PLANTA PUREO", "ENAP QUINTERO", "BT PUMA"],
                                    "Ciudad": ["Iquique", "Mejillones", "Caldera", "Quintero", "Quintero",
                                                "Coronel", "Calbuco", np.nan, np.nan],
                                    "Alias": ["Iquique", "Mejillones", "Caldera", "TPI", "Oxiquim Quintero",
                                                "Coronel", "Pureo", "Quintero", "Puma"]})

            df_productos_plantas = df_productos.merge(df_plantas, how='cross')
            df_productos_plantas["Columna"] = ["M", "S", "Y", "AE", "BC", "BI", "BO", "BU", "CA",
                                            np.nan, "DA", np.nan, "AQ", np.nan, np.nan, np.nan, np.nan, "CK",
                                            np.nan, np.nan, np.nan, np.nan, "AW", np.nan, np.nan, np.nan, "CP",
                                            np.nan, "CU", np.nan, "AK", np.nan, np.nan, np.nan, np.nan, "CF"]
            df_productos_plantas.dropna(subset=["Columna"], inplace=True)

            df_descargas_productos_plantas = df_descargas.merge(df_productos_plantas, on=["Columna"]).drop(columns=["Columna"])
            df_descargas_completo = df_descargas_productos_plantas.merge(df_bts, on=["Abrev."]).drop(columns=["Abrev."])

            df_descargas_completo = df_descargas_completo[["Fecha", "N° Referencia", "Nombre programa", "Nombre del BT",
                                                        "Producto", "Planta", "Ciudad", "Alias", "Volumen"]]

            df_descargas_completo["Fecha"] = pd.to_datetime(df_descargas_completo["Fecha"])
            df_descargas_completo["diff"] = (
                df_descargas_completo.groupby(["Nombre programa", "Producto", "Planta"])["Fecha"]
                .diff()
                .dt.days
            )

            df_descargas_completo["Grupo"] = (df_descargas_completo["diff"] != 1).cumsum()
            df_descargas_agrupadas = (
                df_descargas_completo.groupby("Grupo", as_index=False).agg({
                    "Fecha": ["min", "max"],   # rango de fechas
                    "Volumen": "sum"
                })
            )
            df_descargas_agrupadas.columns = ["Grupo", "Fecha inicio", "Fecha fin", "Volumen total"]

            df_descargas_completo_sin_duplicados = df_descargas_completo.drop_duplicates(subset=["Grupo"])
            df_descargas_agrupadas = df_descargas_agrupadas.merge(
                df_descargas_completo_sin_duplicados, on="Grupo").drop(columns=["diff", "Fecha", "Volumen"])
            df_descargas_agrupadas = df_descargas_agrupadas[[
                "Nombre programa", "N° Referencia", "Nombre del BT", "Producto", "Planta", "Ciudad", "Alias",
                "Fecha inicio", "Fecha fin", "Volumen total"]]

            # Ordenar por Nombre programa y Fecha inicio, y agregar N° Descarga
            df_descargas_agrupadas = df_descargas_agrupadas.sort_values(by=["Nombre programa", "Fecha inicio"])
            df_descargas_agrupadas["N° Descarga"] = (
                df_descargas_agrupadas.groupby("Nombre programa").cumcount() + 1
            )

            df_descargas_agrupadas = df_programas.merge(df_descargas_agrupadas, on="Nombre programa")
            df_descargas_agrupadas["ETA"] = df_descargas_agrupadas["ETA"][[True if descarga == 1 else False for descarga in df_descargas_agrupadas["N° Descarga"]]]

            df_descargas_agrupadas = rellenar_etas(df_descargas_agrupadas, matriz_de_tiempos)
            df_BD = df_descargas_agrupadas[["N° Referencia", "Nombre del BT", "Producto", "Alias", "Volumen total", "ETA", "Fecha fin"]]
            df_BD.columns = ["CC", "Nombre BT", "Producto", "Puerto", "Volumen", "ETA", "Fin descarga"]

            # Agregar proveedor y ventanas desde nueva ficha
            df_nueva_ficha = df_nueva_ficha[df_nueva_ficha["N° Referencia"].isin(df_BD["CC"])]
            for col in ["Inicio Ventana Corta", "Fin Ventana Corta"]:
                df_nueva_ficha[col] = pd.to_datetime(df_nueva_ficha[col], errors="coerce")

            df_nueva_ficha["Inicio Ventana"] = df_nueva_ficha["Inicio Ventana Corta"].combine_first(pd.to_datetime(df_nueva_ficha["Inicio Ventana"]))
            df_nueva_ficha["Final Ventana"] = df_nueva_ficha["Fin Ventana Corta"].combine_first(pd.to_datetime(df_nueva_ficha["Fin Ventana"]))

            # Eliminar columnas de ventanas cortas y antigua ventana
            df_nueva_ficha = df_nueva_ficha.drop(columns=["Inicio Ventana Corta", "Fin Ventana Corta", "Fin Ventana"])
            df_BD = df_BD.merge(df_nueva_ficha, left_on="CC", right_on="N° Referencia").drop(columns=["N° Referencia"])

            # Agregar columnas vacías
            columnas_vacias = ["Fecha de programación", "Semana", "Año", "Mes", "Horas Laytime", 
                            "Demurrage"]
            for col in columnas_vacias:
                df_BD[col] = [np.nan] * len(df_BD)

            # Reordenar columnas
            df_BD = df_BD[["Fecha de programación", "Semana", "Año", "Mes", "Horas Laytime",
                        "CC", "Nombre BT", "Proveedor", "Producto", "Demurrage", "Puerto",
                        "Volumen", "Inicio Ventana", "Final Ventana", "ETA", "Fin descarga"]]

            for col in ["Inicio Ventana", "Final Ventana", "Fin descarga"]:
                df_BD[col] = pd.to_datetime(df_BD[col]).dt.strftime("%d-%m-%Y")
            df_BD["ETA"] = pd.to_datetime(df_BD["ETA"]).dt.strftime("%d-%m-%Y %H:%M")

            df_BD.to_excel(FILE_NAME, index=False)
            st.success(f"Archivo procesado y guardado como {FILE_NAME}")
        except Exception as e:
            st.error(f"Error al procesar los archivos: {e}")

if st.button("Cerrar aplicación"):
    os.kill(os.getpid(), signal.SIGTERM)
