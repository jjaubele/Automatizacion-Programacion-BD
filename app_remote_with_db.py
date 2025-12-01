import pandas as pd
import streamlit as st
import os
from utils.aggregation_functions import rellenar_etas, agrupar_descargas, formato_BD, estimar_demurrage, formato_lista_vertical
from utils.extraction_functions import (extraer_bts, extraer_descargas, extraer_tiempos_de_viaje,
                   extraer_planificacion, extraer_programas, extraer_nueva_ficha,
                   extraer_productos_plantas, extraer_reporte_tankers)
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from utils.loading_functions import (update_programas, get_programacion, create_programacion, 
                                     create_descargas, update_estimaciones_programas)

st.title("Automatización Programación Descargas")

# Subir archivos
PATH_PROGRAMACION = st.file_uploader("Sube la Programación de Descargas", type=["xlsx", "xls"])
PATH_NUEVA_FICHA = st.file_uploader("Sube la Nueva Ficha", type=["xlsx", "xls"])
PATH_REPORTE_TANKERS = st.file_uploader("Sube el Reporte de Tankers (opcional)", type=["pdf"])

if st.button("Procesar Archivos"):
    if not (PATH_PROGRAMACION and PATH_NUEVA_FICHA):
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

            FECHA_PROGRAMACION = pd.to_datetime(df_planificacion.loc[13, "B"], format="%d-%m-%Y", errors="coerce")
            df_BD = formato_BD(df_estimacion, df_descargas_completo, FECHA_PROGRAMACION)
            df_lista_vertical = formato_lista_vertical(df_descargas_agrupadas_puma_enap)

            engine = create_engine(st.secrets["connections"]["BD_URI"])
            with Session(engine) as session:
                programas = update_programas(session, df_programas_completo) # Se actualizan los programas con la nueva ficha + reporte tankers
                programacion = get_programacion(session, FECHA_PROGRAMACION)
                if programacion:
                    session.delete(programacion)
                    session.commit()
                programacion = create_programacion(session, FECHA_PROGRAMACION) # Se crea la nueva programación
                descargas_descartadas = create_descargas(session, df_descargas_descartadas, programacion, estimacion=False)
                df_descargas_con_estimacion = create_descargas(session, df_estimacion, programacion, estimacion=True)

                df_estimacion_con_año_mes = df_estimacion.merge(df_BD[["CC", "Año", "Mes"]], left_on="N° Referencia", right_on="CC", how="left")
                df_estimacion_programas_con_año_mes = df_estimacion_con_año_mes.drop_duplicates(subset=["N° Referencia"])
                update_estimaciones_programas(session, df_estimacion_programas_con_año_mes)
                st.success("Datos cargados exitosamente en la base de datos.")

        except Exception as e:
            if e.args[0] == "Abreviaturas duplicadas encontradas en la hoja \"Buques\".":
                st.error(f"Error al procesar los archivos: {e.args[0]}")
                st.dataframe(e.args[1])
            else:
                st.error(f"Error al procesar los archivos: {e}")