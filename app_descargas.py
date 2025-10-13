import streamlit as st
import pandas as pd
import numpy as np
import os
from utils.aggregation_functions import agrupar_descargas, formato_lista_vertical
from utils.extraction_functions import (extraer_bts, extraer_descargas, 
                                        extraer_planificacion, extraer_productos_plantas)

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
            puma_row = {'N° Referencia': np.nan, 'Nombre programa': 'Puma', 'Nombre del BT': 'Puma', 'Abrev.': 'Puma'}
            enap_row = {'N° Referencia': np.nan, 'Nombre programa': 'Enap', 'Nombre del BT': 'Enap', 'Abrev.': 'Enap'}
            df_bts = pd.concat([df_bts, pd.DataFrame([puma_row, enap_row])], ignore_index=True) 
            
            df_planificacion = extraer_planificacion(PATH_PROGRAMACION, "Planificación")
            df_descargas = extraer_descargas(df_planificacion, ignore_not_bts=False)
            df_productos_plantas = extraer_productos_plantas()
            
            df_descargas_productos_plantas = df_descargas.merge(df_productos_plantas, on=["Columna"]).drop(columns=["Columna"])
            df_descargas_completo = df_bts.merge(df_descargas_productos_plantas, on=["Abrev."])
            df_descargas_completo = df_descargas_completo[["Fecha", "N° Referencia", "Nombre programa", "Nombre del BT",
                                                        "Producto", "Planta", "Ciudad", "Alias", "Volumen"]]

            df_descargas_agrupadas = agrupar_descargas(df_descargas_completo)
            df_lista_vertical = formato_lista_vertical(df_descargas_agrupadas)

            df_lista_vertical.to_excel(FILE_NAME, index=False)
            
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