import pandas as pd
import streamlit as st
import os
from utils.aggregation_functions import rellenar_etas, agrupar_descargas, formato_BD, estimar_demurrage, formato_lista_vertical
from utils.extraction_functions import (extraer_bts, extraer_descargas, extraer_tiempos_de_viaje,
                   extraer_planificacion, extraer_programas, extraer_nueva_ficha,
                   extraer_productos_plantas, extraer_reporte_tankers)
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from utils.loading_functions import (create_programacion, create_descargas,
                                     create_estimaciones_programas, BD_URI)

st.title("Automatización Programación Descargas")

# Subir archivos
PATH_DISTANCIAS = "Distancias entre puertos.xlsx"
PATH_PROGRAMACION = st.file_uploader("Sube la Programación de Descargas", type=["xlsx", "xls"])
PATH_NUEVA_FICHA = st.file_uploader("Sube la Nueva Ficha", type=["xlsx", "xls"])
PATH_REPORTE_TANKERS = st.file_uploader("Sube el Reporte de Tankers (opcional)", type=["pdf"])

# Selección de fecha
FECHA_PROGRAMACION = pd.to_datetime(st.date_input("Selecciona la fecha de programación"))
FILE_NAMES = {
    "estimacion": f"Estimación Demurrage Completo {FECHA_PROGRAMACION.strftime('%d-%m-%Y')}.xlsx",
    "bd": f"Base de Datos Estimación Semanal {FECHA_PROGRAMACION.strftime('%d-%m-%Y')}.xlsx",
    "lista_vertical": f"Lista Vertical {FECHA_PROGRAMACION.strftime('%d-%m-%Y')}.xlsx",
    "descargas": f"Descargas Programación {FECHA_PROGRAMACION.strftime('%d-%m-%Y')}.xlsx",
    "descargas_puma_enap": f"Descargas Programación con Puma y Enap {FECHA_PROGRAMACION.strftime('%d-%m-%Y')}.xlsx"
}

if st.button("Procesar Archivos"):
    if not (PATH_DISTANCIAS and PATH_PROGRAMACION and PATH_NUEVA_FICHA):
        st.error("Faltan uno o más archivos por subir.")
    else:
        try:
            df_bts = extraer_bts(PATH_PROGRAMACION, "Buques")
            df_bts_puma_enap = extraer_bts(PATH_PROGRAMACION, "Buques", add_puma=True, add_enap=True)
            df_planificacion = extraer_planificacion(PATH_PROGRAMACION, "Planificación")
            df_descargas = extraer_descargas(df_planificacion, ignore_not_bts=True, df_bts=df_bts)
            df_descargas_puma_enap = extraer_descargas(df_planificacion, ignore_not_bts=False)
            df_programas = extraer_programas(df_planificacion)
            df_productos_plantas = extraer_productos_plantas()
            df_nueva_ficha = extraer_nueva_ficha(PATH_NUEVA_FICHA, "Programación de buques", df_programas=df_programas)
            matriz_de_tiempos = extraer_tiempos_de_viaje("Distancias entre puertos.xlsx", "Datos")
            if PATH_REPORTE_TANKERS:
                df_reporte_tankers = extraer_reporte_tankers(PATH_REPORTE_TANKERS)

            df_descargas_productos_plantas = df_descargas.merge(df_productos_plantas, on=["Columna"]).drop(columns=["Columna"])
            df_descargas_productos_plantas_puma_enap = df_descargas_puma_enap.merge(df_productos_plantas, on=["Columna"]).drop(columns=["Columna"])
            df_descargas_completo = df_descargas_productos_plantas.merge(df_bts, on=["Abrev."]).drop(columns=["Abrev."])
            df_descargas_completo_puma_enap = df_descargas_productos_plantas_puma_enap.merge(df_bts_puma_enap, on=["Abrev."]).drop(columns=["Abrev."])
            df_descargas_completo = df_descargas_completo[["Fecha", "N° Referencia", "Nombre programa", "Nombre del BT",
                                                        "Producto", "Planta", "Ciudad", "Alias", "Volumen"]]
            df_descargas_completo_puma_enap = df_descargas_completo_puma_enap[["Fecha", "N° Referencia", "Nombre programa", "Nombre del BT",
                                                        "Producto", "Planta", "Ciudad", "Alias", "Volumen"]]
            df_descargas_agrupadas = agrupar_descargas(df_descargas_completo)
            df_descargas_agrupadas_puma_enap = agrupar_descargas(df_descargas_completo_puma_enap)
            df_lista_vertical = formato_lista_vertical(df_descargas_agrupadas_puma_enap)

            df_programas_completo = df_programas.merge(df_nueva_ficha, on="N° Referencia", how="left")
            df_programas_completo["Inicio Ventana"] = df_programas_completo["Inicio Ventana Corta"].combine_first(df_programas_completo["Inicio Ventana"])
            df_programas_completo["Fin Ventana"] = df_programas_completo["Fin Ventana Corta"].combine_first(df_programas_completo["Fin Ventana"])
            df_programas_completo["ETA"] = df_programas_completo["ETA"].combine_first(df_programas_completo["ETA Programa"])
            # Comentar si no se desea llenar montos faltantes con el promedio
            # df_programas_completo["MONTO ($/DIA)"] = df_programas_completo["MONTO ($/DIA)"].fillna(df_programas_completo["MONTO ($/DIA)"].mean()).astype(int)
            # Comentar si no se desea llenar montos faltantes con 35.000
            df_programas_completo["MONTO ($/DIA)"] = df_programas_completo["MONTO ($/DIA)"].fillna(35000).astype(int)
            df_programas_completo = df_programas_completo.drop(columns=["Inicio Ventana Corta", "Fin Ventana Corta", "ETA Programa"])

            if PATH_REPORTE_TANKERS:
                df_programas_completo = df_programas_completo.merge(df_reporte_tankers, on=["N° Referencia"], how="left", suffixes=("", " Reporte Tankers"))
                df_programas_completo["Inicio Ventana"] = df_programas_completo["Inicio Ventana Reporte Tankers"].combine_first(df_programas_completo["Inicio Ventana"])
                df_programas_completo["Fin Ventana"] = df_programas_completo["Fin Ventana Reporte Tankers"].combine_first(df_programas_completo["Fin Ventana"])
                df_programas_completo["ETA"] = df_programas_completo["ETA Reporte Tankers"].combine_first(df_programas_completo["ETA"])
                df_programas_completo = df_programas_completo.drop(columns=["Inicio Ventana Reporte Tankers", "Fin Ventana Reporte Tankers", "ETA Reporte Tankers"])

            df_descargas_descartadas = df_descargas_agrupadas[~df_descargas_agrupadas["N° Referencia"].isin(df_programas_completo["N° Referencia"])]

            df_descargas_por_programa = df_descargas_agrupadas.merge(df_programas_completo, on="N° Referencia", how="right")
            df_descargas_por_programa = df_descargas_por_programa[df_descargas_por_programa["Producto"].notna()]
            df_descargas_por_programa["Nombre del BT_x"] = df_descargas_por_programa["Nombre del BT_x"].combine_first(df_descargas_por_programa["Nombre del BT_y"])
            df_descargas_por_programa.rename(columns={"Nombre del BT_x": "Nombre del BT"}, inplace=True)
            df_descargas_por_programa = df_descargas_por_programa.drop(columns=["Nombre del BT_y"])
            df_descargas_por_programa.index = range(0, len(df_descargas_por_programa))
            df_descargas_por_programa["ETA"] = df_descargas_por_programa["ETA"][[True if descarga == 1 else False for descarga in df_descargas_por_programa["N° Descarga"]]]
            df_descargas_por_programa = rellenar_etas(df_descargas_por_programa, matriz_de_tiempos)
            df_estimacion = estimar_demurrage(df_descargas_por_programa)

            df_BD = formato_BD(df_estimacion, df_descargas_completo, FECHA_PROGRAMACION)
            df_lista_vertical = formato_lista_vertical(df_descargas_agrupadas_puma_enap)


            df_estimacion.to_excel(FILE_NAMES["estimacion"], index=False)
            df_BD.to_excel(FILE_NAMES["bd"], index=False)
            df_lista_vertical.to_excel(FILE_NAMES["lista_vertical"], index=False)
            df_descargas_agrupadas.to_excel(FILE_NAMES["descargas"], index=False)
            df_descargas_agrupadas_puma_enap.to_excel(FILE_NAMES["descargas_puma_enap"], index=False)

            with st.expander("Descargas Agrupadas por Programa"):
                st.dataframe(df_descargas_agrupadas)
                with open(FILE_NAMES["descargas"], "rb") as file:
                    btn = st.download_button(
                        label="Descargar Descargas Agrupadas",
                        data=file,
                        file_name=FILE_NAMES["descargas"],
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                    )
                os.remove(FILE_NAMES["descargas"])

            with st.expander("Descargas Agrupadas por Programa (Puma y Enap incluidos)"):
                st.dataframe(df_descargas_agrupadas_puma_enap)
                with open(FILE_NAMES["descargas_puma_enap"], "rb") as file:
                    btn = st.download_button(
                        label="Descargar Descargas Agrupadas con Puma y Enap",
                        data=file,
                        file_name=FILE_NAMES["descargas_puma_enap"],
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                    )
                os.remove(FILE_NAMES["descargas_puma_enap"])

            with st.expander("Formato Lista Vertical (formato solicitado por Nicolás)"):
                st.dataframe(df_lista_vertical)
                with open(FILE_NAMES["lista_vertical"], "rb") as file:
                    btn = st.download_button(
                        label="Descargar Lista Vertical",
                        data=file,
                        file_name=f"Lista Vertical {FECHA_PROGRAMACION.strftime('%d-%m-%Y')}.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                    )
                os.remove(FILE_NAMES["lista_vertical"])

            with st.expander("Descargas descartadas para estimación (ETAs previos al inicio de la programación)."):
                st.dataframe(df_descargas_descartadas)

            with st.expander("Base de Datos Estimación Demurrage (Formato Completo)"):
                st.dataframe(df_estimacion)
                with open(FILE_NAMES["estimacion"], "rb") as file:
                    btn = st.download_button(
                        label="Descargar Estimación Completa",
                        data=file,
                        file_name=FILE_NAMES["estimacion"],
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                    )
                os.remove(FILE_NAMES["estimacion"])

            with st.expander("Base de Datos Estimación Demurrage (formato Base de Datos)"):
                st.dataframe(df_BD)
                with open(FILE_NAMES["bd"], "rb") as file:
                    btn = st.download_button(
                        label="Descargar Base de Datos",
                        data=file,
                        file_name=FILE_NAMES["bd"],
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                    )
                os.remove(FILE_NAMES["bd"])

            st.write("Todo bien 1")
            engine = create_engine(BD_URI)
            st.write("Todo bien 2")
            with Session(engine) as session:
                st.write("Todo bien 3")
                programacion = create_programacion(session, FECHA_PROGRAMACION)
                st.write("Todo bien 4")
                descargas_descartadas = create_descargas(session, df_descargas_descartadas, FECHA_PROGRAMACION, estimacion=False)
                df_descargas_con_estimacion = create_descargas(session, df_estimacion, FECHA_PROGRAMACION, estimacion=True)

                df_estimacion_con_año_mes = df_estimacion.merge(df_BD[["CC", "Año", "Mes"]], left_on="N° Referencia", right_on="CC", how="left")
                df_estimacion_programas_con_año_mes = df_estimacion_con_año_mes.drop_duplicates(subset=["N° Referencia"])
                create_estimaciones_programas(session, df_estimacion_programas_con_año_mes)
            st.success("Datos cargados exitosamente en la base de datos.")

        except Exception as e:
            if e.args[0] == "Abreviaturas duplicadas encontradas en la hoja \"Buques\".":
                st.error(f"Error al procesar los archivos: {e.args[0]}")
                st.dataframe(e.args[1])
            else:
                st.error(f"Error al procesar los archivos: {e}")