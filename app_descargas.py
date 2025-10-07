import streamlit as st
import pandas as pd
import numpy as np
import os
from utils.aggregation_functions import agrupar_descargas
from utils.extraction_functions import (extraer_bts, extraer_descargas, 
                                        extraer_planificacion)

st.title("Extracción de Descargas")

PATH_PROGRAMACION = st.file_uploader("Sube la Programación de Descargas", type=["xlsx", "xls"])
FECHA_PROGRAMACION = st.date_input("Selecciona la fecha de programación")
FILE_NAME = f"Descargas Programación {FECHA_PROGRAMACION.strftime('%d-%m-%Y')}.xlsx"

if st.button("Procesar Archivos"):
    if not (PATH_PROGRAMACION):
        st.error("Falta archivo de programación.")
    else:
        try:
            df_bts = extraer_bts(PATH_PROGRAMACION, "Buques")
            df_planificacion = extraer_planificacion(PATH_PROGRAMACION, "Planificación")
            df_descargas = extraer_descargas(df_planificacion, ignore_not_bts=False)

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
            df_descargas_completo = df_bts.merge(df_descargas_productos_plantas, on=["Abrev."])
            df_descargas_completo = df_descargas_completo[["Fecha", "N° Referencia", "Nombre programa", "Nombre del BT",
                                                        "Producto", "Planta", "Ciudad", "Alias", "Volumen"]]

            df_descargas_agrupadas = agrupar_descargas(df_descargas_completo)
            df_descargas_agrupadas["Operación"] = [np.nan] * len(df_descargas_agrupadas)
            df_descargas_agrupadas = df_descargas_agrupadas[["Nombre del BT", "N° Referencia", "Fecha inicio", "Fecha fin", "Operación", "Planta", "Producto", "Volumen total"]]
            df_descargas_agrupadas.columns = ["BT", "CC", "Fecha inicio", "Fecha fin", "Operación", "Planta", "Producto", "Volumen"]

            # Formatear fechas y guardar archivo
            df_descargas_agrupadas["Fecha inicio"] = pd.to_datetime(df_descargas_agrupadas["Fecha inicio"]).dt.strftime('%d-%m-%Y')
            df_descargas_agrupadas["Fecha fin"] = pd.to_datetime(df_descargas_agrupadas["Fecha fin"]).dt.strftime('%d-%m-%Y')
            df_descargas_agrupadas.to_excel(FILE_NAME, index=False)
            # Descargar archivo
            with open(FILE_NAME, "rb") as file:
                btn = st.download_button(
                    label="Descargar Archivo",
                    data=file,
                    file_name=FILE_NAME,
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )
            st.success(f"Archivo procesado y guardado como {FILE_NAME}")
            os.remove(FILE_NAME)
        except Exception as e:
            st.error(f"Error al procesar los archivos: {e}")