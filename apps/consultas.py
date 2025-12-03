import streamlit as st
import os
import pandas as pd
from sqlalchemy import select, func
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from modelo import (Planta, Timelog, Programacion, Programa, Descarga,
                    EstimacionDescarga, EstimacionPrograma)

def get_timelogs(session, filtro_fechas, filtro_plantas):
    fecha_inicio, fecha_fin = filtro_fechas
    query = select(Timelog).where(Timelog.planta_id.in_(filtro_plantas))
    if fecha_inicio:
        query = query.where(Timelog.fecha >= fecha_inicio)
    if fecha_fin:
        query = query.where(Timelog.fecha <= fecha_fin)
    timelogs = session.execute(query).scalars().all()
    return timelogs

def consulta_timelogs(session):
    st.title("Consultas Timelogs")
    plantas = session.execute(select(Planta)).scalars().all()
    planta_options = {planta.alias: planta.id for planta in plantas}
    plantas_seleccionadas = st.multiselect(
        "Selecciona las plantas:", list(planta_options.keys()))
    planta_ids_seleccionadas = [planta_options[alias] for alias in plantas_seleccionadas]
    fecha_inicio = st.date_input("Fecha inicio:", value=None)
    fecha_fin = st.date_input("Fecha fin:", value=None)
    if st.button("Consultar Timelogs"):
        filtro_fechas = (fecha_inicio, fecha_fin)
        timelogs = get_timelogs(session, filtro_fechas, planta_ids_seleccionadas)
        if timelogs:
            data = [{
                "Nombre timelog": timelog.nombre,
                "Fecha": timelog.fecha,
                "Planta": timelog.planta.alias,
                "arribo_inicio_amarre": timelog.arribo_inicio_amarre,
                "inicio_amarre_fin_amarre": timelog.inicio_amarre_fin_amarre,
                "fin_amarre_inicio_conexion": timelog.fin_amarre_inicio_conexion,
                "inicio_conexion_fin_conexion": timelog.inicio_conexion_fin_conexion,
                "fin_conexion_inicio_descarga": timelog.fin_conexion_inicio_descarga,
                "inicio_descarga_fin_descarga": timelog.inicio_descarga_fin_descarga,
                "fin_descarga_despachado": timelog.fin_descarga_despachado,
                "tiempo_total": timelog.tiempo_total
            } for timelog in timelogs]
            df_timelogs = pd.DataFrame(data)
            st.dataframe(df_timelogs)
            FILE_NAME = "consulta_timelogs.xlsx"
            df_timelogs.to_excel(FILE_NAME, index=False)
            with open(FILE_NAME, "rb") as file:
                st.download_button(
                    label="Descargar Timelogs",
                    data=file,
                    file_name=FILE_NAME,
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )
            os.remove(FILE_NAME)
        else:
            st.write("No se encontraron timelogs para los filtros seleccionados.")

def consultas_programaciones(session):
    st.title("Consulta Programaciones")
    programaciones = session.execute(select(Programacion)).scalars().all()
    if programaciones:
        data = [{
            "ID": prog.id,
            "Fecha de Programación": prog.fecha,
        } for prog in programaciones]
        df_programaciones = pd.DataFrame(data)
        st.dataframe(df_programaciones)

def consultas_programas(session):
    st.title("Consulta Programas")
    st.write("Próximamente...")

def consultas_descargas(session):
    st.title("Consulta Descargas")
    st.write("Próximamente...")

def app():
    consulta_options = {"Consulta Timelogs": consulta_timelogs,
                        "Consulta Programaciones": consultas_programaciones,
                        "Consulta Programas": consultas_programas,
                        "Consulta Descargas": consultas_descargas}
    st.sidebar.title("Consultas")
    selection = st.sidebar.radio("Ir a", list(consulta_options.keys()))

    engine = create_engine(st.secrets["connections"]["BD_URI"])
    with Session(engine) as session:
        consulta_options[selection](session)

    if st.button("Cerrar sesión"):
        st.session_state.clear()
        st.rerun()