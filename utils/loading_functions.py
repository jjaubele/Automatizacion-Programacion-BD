import sys
import os
import pandas as pd
import numpy as np

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(BASE_DIR)

from sqlalchemy import select, create_engine
from utils.utils import get_week_of_month_int
from utils.aggregation_functions import MESES_REVERSE
from modelo import (Programacion, Descarga, Programa, Planta,
                    EstimacionPrograma, EstimacionDescarga)

USUARIO = "postgres"
PASSWORD = "qwerty"
BD = "Estimacion_Descargas"
BD_URI = f"postgresql+psycopg2://{USUARIO}:{PASSWORD}@localhost:5432/{BD}"
engine = create_engine(BD_URI)

def nueva_ficha_psql_format(df_nueva_ficha):
    df_nueva_ficha_formateado = pd.DataFrame({
    "CC": df_nueva_ficha["N° Referencia"],
    "nombre_bt": df_nueva_ficha["Nombre del BT"],
    "proveedor": df_nueva_ficha["Proveedor"],
    "origen": df_nueva_ficha["Origen"],
    "inicio_ventana": df_nueva_ficha["Inicio Ventana"],
    "fin_ventana": df_nueva_ficha["Fin Ventana"],
    "ETA": df_nueva_ficha["ETA"],
    "monto": df_nueva_ficha["MONTO ($/DIA)"],
    "laytime": [np.nan] * len(df_nueva_ficha),
    "agencia_de_naves": df_nueva_ficha["Agencia de Naves"],
    "surveyor_primario": df_nueva_ficha["Surveyor Primario"],
    "surveyor_secundario": df_nueva_ficha["Surveyor Secundario"],
    })
    return df_nueva_ficha_formateado

def plantas_psql_format(DF_PLANTAS):
    df_plantas_formateado = pd.DataFrame({
    "nombre": DF_PLANTAS["Planta"],
    "ciudad": DF_PLANTAS["Ciudad"],
    "alias": DF_PLANTAS["Alias"],
    })
    return df_plantas_formateado

def create_programacion(session, FECHA_PROGRAMACION):
    nueva_programacion = Programacion(
        fecha=FECHA_PROGRAMACION,
        semana=get_week_of_month_int(FECHA_PROGRAMACION.year, FECHA_PROGRAMACION.month, FECHA_PROGRAMACION.day),
        )
    session.add(nueva_programacion)
    session.commit()

    return nueva_programacion

def create_descarga(session, row, FECHA_PROGRAMACION):
    programa = session.execute(
        select(Programa).where(Programa.CC == row["N° Referencia"])
    ).scalar_one()
    programacion = session.execute(
        select(Programacion).where(Programacion.fecha == FECHA_PROGRAMACION)
    ).scalar_one()
    planta = session.execute(
        select(Planta).where(Planta.nombre == row["Planta"])
    ).scalar_one()
    nueva_descarga = Descarga(
        volumen=row["Volumen total"],
        producto=row["Producto"],
        planta=planta,
        programa=programa,
        programacion=programacion,
        fecha_inicio=row["Fecha inicio"],
        fecha_fin=row["Fecha fin"],
    )
    session.add(nueva_descarga)
    session.commit()

    return nueva_descarga

def create_estimacion_descarga(session, row, nueva_descarga):
    nueva_estimacion_descarga = EstimacionDescarga(
        descarga=nueva_descarga,
        ETA=row["ETA"],
        inicio_laytime=row["Inicio Laytime"],
        tiempo_descarga=row["Tiempo descarga (Horas)"],
        demurrage_descarga=row["Demurrage descarga (Horas)"],
        estimacion_demurrage=row["Estimación demurrage"],
        demurrage_unitario=row["Demurrage unitario"],
        shifting=row["Shifting"],
    )
    session.add(nueva_estimacion_descarga)
    session.commit()

    return nueva_estimacion_descarga

def create_estimacion_programa(session, row):
    programa = session.execute(
        select(Programa).where(Programa.CC == row["N° Referencia"])
    ).scalar_one()
    nueva_estimacion_programa = EstimacionPrograma(
        programa=programa,
        tiempo_programa=row["Tiempo programa (Horas)"],
        demurrage_programa=row["Demurrage programa (Horas)"],
        mes=MESES_REVERSE[row["Mes"]],
        año=row["Año"],
    )
    session.add(nueva_estimacion_programa)
    session.commit()

    return nueva_estimacion_programa

def create_descargas(session, df, FECHA_PROGRAMACION, estimacion=True):
    descargas = []
    for _, row in df.iterrows():
        nueva_descarga = create_descarga(session, row, FECHA_PROGRAMACION)
        descargas.append(nueva_descarga)
        if estimacion:
            create_estimacion_descarga(session, row, nueva_descarga)
    return descargas

def create_estimaciones_programas(session, df):
    estimaciones_programas = []
    for _, row in df.iterrows():
        nueva_estimacion_programa = create_estimacion_programa(session, row)
        estimaciones_programas.append(nueva_estimacion_programa)
    return estimaciones_programas