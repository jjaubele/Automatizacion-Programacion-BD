import streamlit as st
from apps.login import login
import apps.automatizacion
import apps.cargar_programaciones
import apps.cargar_timelogs
import apps.consultas


PASSWORD = st.secrets["auth"]["password"]

if "logged" not in st.session_state:
    login(PASSWORD)
else:
    app_options = {
        "Automatización Programación Descargas": apps.automatizacion,
        "Carga de Programaciones a la Base de Datos": apps.cargar_programaciones,
        "Carga de Timelogs a la Base de Datos": apps.cargar_timelogs,
        "Consultas a la Base de Datos": apps.consultas,
    }

    st.sidebar.title("Navegación")
    selection = st.sidebar.radio("Ir a", list(app_options.keys()))

    app = app_options[selection]
    app.app()