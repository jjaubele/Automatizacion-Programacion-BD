import sys
import os
import pandas as pd

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(BASE_DIR)

from sqlalchemy import select
from utils.utils import get_week_of_month_int
from utils.aggregation_functions import MESES_REVERSE
from modelo import (Programacion, Descarga, Programa, Planta,
                    EstimacionPrograma, EstimacionDescarga)

USUARIO = "postgres"
PASSWORD = "qwerty"
BD = "Estimacion_Descargas"
BD_URI = f"postgresql+psycopg2://{USUARIO}:{PASSWORD}@localhost:5432/{BD}"

def nueva_ficha_psql_format(df):
    df_formateado = pd.DataFrame({
    "CC": df["N° Referencia"],
    "nombre_bt": df["Nombre del BT"],
    "proveedor": df["Proveedor"],
    "origen": df["Origen"],
    "inicio_ventana": df["Inicio Ventana"],
    "fin_ventana": df["Fin Ventana"],
    "ETA": df["ETA"],
    "monto": df["MONTO ($/DIA)"],
    "laytime": [None] * len(df),
    "agencia_de_naves": df["Agencia de Naves"],
    "surveyor_primario": df["Surveyor Primario"],
    "surveyor_secundario": df["Surveyor Secundario"],
    })
    for column in df_formateado.columns:
        df_formateado[column] = df_formateado[column].apply(lambda x: None if pd.isna(x) else x)
    return df_formateado

def plantas_psql_format(DF_PLANTAS):
    df_plantas_formateado = pd.DataFrame({
    "nombre": DF_PLANTAS["Planta"],
    "ciudad": DF_PLANTAS["Ciudad"],
    "alias": DF_PLANTAS["Alias"],
    })
    return df_plantas_formateado

### Programa ###

def create_programa(session, row):
    nuevo_programa = Programa(
        CC=row["CC"],
        nombre_bt=row["nombre_bt"],
        proveedor=row["proveedor"],
        origen=row["origen"],
        inicio_ventana=row["inicio_ventana"],
        fin_ventana=row["fin_ventana"],
        ETA=row["ETA"],
        monto=row["monto"],
        laytime=row["laytime"],
        agencia_de_naves=row["agencia_de_naves"],
        surveyor_primario=row["surveyor_primario"],
        surveyor_secundario=row["surveyor_secundario"],
    )
    session.add(nuevo_programa)

    return nuevo_programa

def update_programa(session, row, programa):
    programa.nombre_bt=row["nombre_bt"]
    programa.proveedor=row["proveedor"]
    programa.origen=row["origen"]
    programa.inicio_ventana=row["inicio_ventana"]
    programa.fin_ventana=row["fin_ventana"]
    programa.ETA=row["ETA"]
    programa.monto=row["monto"]
    programa.laytime=row["laytime"]
    programa.agencia_de_naves=row["agencia_de_naves"]
    programa.surveyor_primario=row["surveyor_primario"]
    programa.surveyor_secundario=row["surveyor_secundario"]

    session.add(programa)

    return programa

def get_programa(session, CC):
    programa = session.execute(
        select(Programa).where(Programa.CC == CC)
    ).scalar_one_or_none()
    return programa

def delete_programa(session, CC):
    programa = session.execute(
        select(Programa).where(Programa.CC == CC)
    ).scalar_one_or_none()
    if programa:
        session.delete(programa)

def update_programas(session, df):
    df_formateado = nueva_ficha_psql_format(df)
    programas = []
    for _, row in df_formateado.iterrows():
        programa = get_programa(session, row["CC"])
        if not programa:
            nuevo_programa = create_programa(session, row)
            programas.append(nuevo_programa)
        else:
            programa_actualizado = update_programa(session, row, programa)
            programas.append(programa_actualizado)
    session.commit()
    return programas

### Programacion ###

def get_programacion(session, FECHA_PROGRAMACION):
    programacion = session.execute(
        select(Programacion).where(Programacion.fecha == FECHA_PROGRAMACION)
    ).scalar_one_or_none()
    return programacion

def create_programacion(session, FECHA_PROGRAMACION):
    nueva_programacion = Programacion(
        fecha=FECHA_PROGRAMACION,
        semana=get_week_of_month_int(FECHA_PROGRAMACION.year, FECHA_PROGRAMACION.month, FECHA_PROGRAMACION.day),
        )
    session.add(nueva_programacion)

    return nueva_programacion

### Descarga y EstimacionDescarga ###

def create_descarga(session, row, programacion):
    programa = session.execute(
        select(Programa).where(Programa.CC == row["N° Referencia"])
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

    return nueva_estimacion_descarga

def create_descargas(session, df, programacion, estimacion=True):
    descargas = []
    for _, row in df.iterrows():
        nueva_descarga = create_descarga(session, row, programacion)
        descargas.append(nueva_descarga)
        if estimacion:
            create_estimacion_descarga(session, row, nueva_descarga)
    session.commit()
    return descargas

### EstimacionPrograma ###

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

    return nueva_estimacion_programa

def update_estimacion_programa(session, row, estimacion_programa):
    estimacion_programa.tiempo_programa=row["Tiempo programa (Horas)"]
    estimacion_programa.demurrage_programa=row["Demurrage programa (Horas)"]
    estimacion_programa.mes=MESES_REVERSE[row["Mes"]]
    estimacion_programa.año=row["Año"]

    session.add(estimacion_programa)

    return estimacion_programa

def update_estimaciones_programas(session, df):
    estimaciones_programas = []
    for _, row in df.iterrows():
        estimacion_programa = session.execute(
            select(EstimacionPrograma).where(EstimacionPrograma.programa.has(CC=row["N° Referencia"]))
        ).scalar_one_or_none()
        if not estimacion_programa:
            nueva_estimacion_programa = create_estimacion_programa(session, row)
            estimaciones_programas.append(nueva_estimacion_programa)
        else:
            actualizado_estimacion_programa = update_estimacion_programa(session, row, estimacion_programa)
            estimaciones_programas.append(actualizado_estimacion_programa)
    session.commit()
    return estimaciones_programas