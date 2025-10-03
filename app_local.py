import streamlit as st
import pandas as pd
import numpy as np
import signal
import os
from utils.aggregation_functions import rellenar_etas, agrupar_descargas, asignar_año_mes
from utils.utils import get_week_of_month
from utils.extraction_functions import (extraer_bts, extraer_descargas, extraer_tiempos_de_viaje,
                   extraer_planificacion, extraer_programas, extraer_nueva_ficha)

st.title("Automatización Programación Descargas")

# Subir archivos
PATH_DISTANCIAS = "Distancias entre puertos.xlsx"
PATH_PROGRAMACION = st.file_uploader("Sube la Programación de Descargas", type=["xlsx", "xls"])
PATH_NUEVA_FICHA = st.file_uploader("Sube la Nueva Ficha", type=["xlsx", "xls"])

# Selección de fecha
FECHA_PROGRAMACION = st.date_input("Selecciona la fecha de programación")
FILE_NAME = f"Base de datos Estimación Semanal {FECHA_PROGRAMACION.strftime('%d-%m-%Y')}.xlsx"

if st.button("Procesar Archivos"):
    if not (PATH_DISTANCIAS and PATH_PROGRAMACION and PATH_NUEVA_FICHA):
        st.error("Faltan uno o más archivos por subir.")
    else:
        try:
            # ========= Parte 1 =============
            df_bts = extraer_bts(PATH_PROGRAMACION, "Buques")
            df_planificacion = extraer_planificacion(PATH_PROGRAMACION, "Planificación")
            df_descargas = extraer_descargas(df_planificacion)
            df_programas = extraer_programas(df_planificacion)
            matriz_de_tiempos = extraer_tiempos_de_viaje("Distancias entre puertos.xlsx", "Datos")

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

            df_descargas_agrupadas = agrupar_descargas(df_descargas_completo)
            df_descargas_agrupadas = df_programas.merge(df_descargas_agrupadas, on="Nombre programa")
            df_descargas_agrupadas["ETA"] = df_descargas_agrupadas["ETA"][[True if descarga == 1 else False for descarga in df_descargas_agrupadas["N° Descarga"]]]
            rellenar_etas(df_descargas_agrupadas, matriz_de_tiempos)

            df_BD = df_descargas_agrupadas[["N° Referencia", "Nombre del BT", "Producto", "Alias", "Volumen total", "ETA", "Fecha fin"]]
            df_BD.columns = ["CC", "Nombre BT", "Producto", "Puerto", "Volumen", "ETA", "Fin descarga"]

            # ========= Parte 2 =============
            df_nueva_ficha = extraer_nueva_ficha(PATH_NUEVA_FICHA, "Programación de buques")
            df_nueva_ficha = df_nueva_ficha[df_nueva_ficha["N° Referencia"].isin(df_BD["CC"])]
            for col in ["Inicio Ventana Corta", "Fin Ventana Corta"]:
                df_nueva_ficha[col] = pd.to_datetime(df_nueva_ficha[col], errors="coerce")
            df_nueva_ficha["Inicio Ventana"] = df_nueva_ficha["Inicio Ventana Corta"].combine_first(pd.to_datetime(df_nueva_ficha["Inicio Ventana"]))
            df_nueva_ficha["Final Ventana"] = df_nueva_ficha["Fin Ventana Corta"].combine_first(pd.to_datetime(df_nueva_ficha["Fin Ventana"]))
            df_nueva_ficha = df_nueva_ficha.drop(columns=["Inicio Ventana Corta", "Fin Ventana Corta", "Fin Ventana"])
            df_BD = df_BD.merge(df_nueva_ficha, left_on="CC", right_on="N° Referencia").drop(columns=["N° Referencia"])

            # ========= Parte 3 =============
            # Agregar columnas vacías
            columnas_vacias = ["Fecha de programación", "Semana", "Año", "Mes", "Horas Laytime", "Demurrage"]
            for col in columnas_vacias:
                df_BD[col] = [np.nan] * len(df_BD)

            df_BD["Fecha de programación"] = FECHA_PROGRAMACION
            df_BD["Semana"] = get_week_of_month(FECHA_PROGRAMACION.year, FECHA_PROGRAMACION.month, FECHA_PROGRAMACION.day)

            # Reordenar columnas
            df_BD = df_BD[["Fecha de programación", "Semana", "Año", "Mes", "Horas Laytime",
                        "CC", "Nombre BT", "Proveedor", "Producto", "Demurrage", "Puerto",
                        "Volumen", "Inicio Ventana", "Final Ventana", "ETA", "Fin descarga"]]
            
            # Formatear fechas
            for col in ["Inicio Ventana", "Final Ventana", "Fin descarga", "Fecha de programación"]:
                df_BD[col] = pd.to_datetime(df_BD[col]).dt.strftime("%d-%m-%Y")
            df_BD["ETA"] = pd.to_datetime(df_BD["ETA"]).dt.strftime("%d-%m-%Y %H:%M")

            # ========== Parte 4 =============
            # Agregar mes y año de mayor volumen de descarga a un programa
            df_BD = asignar_año_mes(df_descargas_completo, df_BD)
            df_BD.to_excel(FILE_NAME, index=False)
            
            st.success(f"Archivo procesado y guardado como {FILE_NAME}")
        except Exception as e:
            st.error(f"Error al procesar los archivos: {e}")

if st.button("Cerrar aplicación"):
    os.kill(os.getpid(), signal.SIGTERM)
