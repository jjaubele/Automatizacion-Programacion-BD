import pandas as pd
import streamlit as st
from pathlib import Path
from utils.extraction_functions import (extraer_timelog, timelog_to_db_row)
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from utils.loading_functions import (create_timelog, get_timelog, update_timelog)

PASSWORD = st.secrets["auth"]["password"]

def login():
    st.title("🔐 Acceso Restringido")

    pwd = st.text_input("Contraseña", type="password")

    if st.button("Entrar"):
        if pwd == PASSWORD:
            st.session_state["logged"] = True
            st.rerun()
        else:
            st.error("Contraseña incorrecta")

def app():
    st.title("Carga de Timelogs a la Base de Datos")

    PATHS_TIMELOGS = st.file_uploader("Sube los Timelogs", type=["xlsm"], accept_multiple_files=True)

    if st.button("Subir Archivo"):
        if not PATHS_TIMELOGS:
            st.warning("Por favor, sube al menos un archivo de Timelog.")
        else:
            try:
                engine = create_engine(st.secrets["connections"]["BD_URI"])
                with Session(engine) as session:
                    for PATH_TIMELOG in PATHS_TIMELOGS:
                        try:
                            timelog_name = Path(PATH_TIMELOG.name).stem
                            df_timelog = extraer_timelog(PATH_TIMELOG, sheet="BITACORA (1)")
                            db_row = timelog_to_db_row(df_timelog, timelog_name)
                            timelog = get_timelog(session, db_row["nombre"])
                            if timelog:
                                timelog = update_timelog(session, db_row, timelog)
                                st.success(f"Timelog {timelog_name} actualizado correctamente.")
                            else:
                                timelog = create_timelog(session, db_row)
                                st.success(f"Timelog {timelog_name} creado correctamente.")
                            campos_nulos = []
                            for key in timelog.__dict__:
                                if getattr(timelog, key) is None:
                                    campos_nulos.append(key)
                            if len(campos_nulos) > 0:
                                st.warning(f"El timelog '{timelog.nombre}' tiene como campos nulos: {campos_nulos}")
                        except Exception as e:
                            st.error(f"Error al procesar el archivo {PATH_TIMELOG.name}: {e}")
                    session.commit()
            except Exception as e:
                st.error(f"Ocurrió un error al cargar los Timelogs: {e}")

    if st.button("Cerrar sesión"):
        st.session_state.clear()
        st.rerun()

if "logged" not in st.session_state:
    login()
else:
    app()