import pandas as pd
import numpy as np
import pdfplumber
from pdfminer.high_level import extract_text
import re
from utils.utils import int_to_excel_col, next_excel_col

MESES_UPPERCASE = {"ENERO": 1, "FEBRERO": 2, "MARZO": 3, "ABRIL": 4, 
                   "MAYO": 5, "JUNIO": 6, "JULIO": 7, "AGOSTO": 8, 
                   "SEPTIEMBRE": 9, "OCTUBRE": 10, "NOVIEMBRE": 11, "DICIEMBRE": 12}
MESES_ABREVIADOS = {"ene": 1, "feb": 2, "mar": 3, "abr": 4, 
                   "may": 5, "jun": 6, "jul": 7, "ago": 8,
                   "sept": 9, "oct": 10, "nov": 11, "dic": 12}

FIRST_ROW = 12
LAST_ROW = 61

DF_PRODUCTOS = pd.DataFrame({"Producto": ["Diesel A1", "Gas 93", "Gas 97", "Jet A1"]})
DF_PLANTAS = pd.DataFrame({"Planta": ["PLANTA IQUIQUE", "PLANTA MEJILLONES", "PLANTA CALDERA",
                                    "TERMINAL TPI", "OXIQUIM QUINTERO", "OXIQUIM CORONEL",
                                    "PLANTA PUREO", "ENAP QUINTERO", "BT PUMA"],
                        "Ciudad": ["Iquique", "Mejillones", "Caldera",
                                   "Quintero", "Quintero", "Coronel",
                                   "Calbuco", np.nan, np.nan],
                        "Alias": ["Iquique", "Mejillones", "Caldera",
                                  "TPI", "Oxiquim Quintero", "Coronel",
                                  "Pureo", "Quintero", "Puma"]})

def extraer_productos_plantas():
    df_productos_plantas = DF_PRODUCTOS.merge(DF_PLANTAS, how='cross')
    df_productos_plantas["Columna"] = ["M", "S", "Y", "AE", "BC", "BI", "BO", "BU", "CA",
                                    np.nan, "DA", np.nan, "AQ", np.nan, np.nan, np.nan, np.nan, "CK",
                                    np.nan, np.nan, np.nan, np.nan, "AW", np.nan, np.nan, np.nan, "CP",
                                    np.nan, "CU", np.nan, "AK", np.nan, np.nan, np.nan, np.nan, "CF"]
    df_productos_plantas.dropna(subset=["Columna"], inplace=True)
    return df_productos_plantas

def extraer_planificacion(file, sheet):
    df_planificacion = pd.read_excel(file, sheet_name=sheet, header=None)
    df_planificacion.index = range(1, len(df_planificacion) + 1)
    df_planificacion.columns = [int_to_excel_col(i) for i in range(1, len(df_planificacion.columns) + 1)]

    return df_planificacion

def extraer_bts(file, sheet):
    df_bts = pd.read_excel(file, sheet_name=sheet, header=0)
    df_bts.index = range(1, len(df_bts) + 1)
    df_bts = df_bts[["N° Referencia", "Nombre programa", "Nombre del BT", "Abrev."]]
    df_bts.drop_duplicates(subset=["N° Referencia"], keep="first", inplace=True)
    
    return df_bts

def extraer_descargas(df_planificacion, ignore_not_bts=False, df_bts=None):
    df_descargas = pd.DataFrame({"Fecha": [], "Abrev.": [], "Volumen": [], "Columna": []})
    columnas = ["M", "S", "Y", "AE", "AK", "AQ", "AW", "BC", "BI", "BO", "BU", "CA", "CF", "CK", "CP", "CU", "DA"]
    for col in columnas:
        descargas_parciales = df_planificacion.loc[FIRST_ROW:LAST_ROW, ["B", col, next_excel_col(col)]].dropna()
        descargas_parciales.columns = ["Fecha", "Abrev.", "Volumen"]
        descargas_parciales["Columna"] = [col] * len(descargas_parciales)
        df_descargas = pd.concat([df_descargas, descargas_parciales], ignore_index=True)
    if ignore_not_bts:
        df_descargas = df_descargas[df_descargas["Abrev."].isin(df_bts["Abrev."].tolist())]
    df_descargas["Fecha"] = pd.to_datetime(df_descargas["Fecha"], errors="coerce")
    df_descargas["Volumen"] = pd.to_numeric(df_descargas["Volumen"], errors="coerce")
    return df_descargas

def extraer_tiempos_de_viaje(file, sheet):
    VELOCIDAD_MEDIA = 12  # nudos
    df_distancias = pd.read_excel(file, sheet_name=sheet, header=3, index_col=0)
    df_distancias.dropna(axis=1, how='all', inplace=True)
    df_distancias.dropna(axis=0, how='all', inplace=True)
    matriz_de_tiempos = np.ceil(df_distancias / VELOCIDAD_MEDIA).astype(int)

    return matriz_de_tiempos

def extraer_programas(df_planificacion):
    df_programas = df_planificacion.loc[FIRST_ROW:LAST_ROW, ["B", "J"]].dropna()
    df_programas.columns = ["ETA Programa", "Nombre programa"]
    df_programas["ETA Programa"] = pd.to_datetime(df_programas["ETA Programa"])
    df_programas["N° Referencia"] = df_programas["Nombre programa"].apply(lambda x: "CC " + x.split()[0])
    df_programas = df_programas[["ETA Programa", "N° Referencia"]]

    return df_programas

def extraer_nueva_ficha(file, sheet, df_programas=None):
    df_nueva_ficha = pd.read_excel(file, sheet_name=sheet, header=3)
    df_nueva_ficha = df_nueva_ficha[["N° Referencia", "Proveedor", "Inicio Ventana",
                                     "Fin Ventana", "Inicio Ventana Corta", "Fin Ventana Corta", 
                                     "ETA", "MONTO ($/DIA)"]]
    
    df_nueva_ficha.drop_duplicates(subset=["N° Referencia"], keep="first", inplace=True)
    if df_programas is not None:
        df_nueva_ficha = df_nueva_ficha[df_nueva_ficha["N° Referencia"].isin(df_programas["N° Referencia"])]
    df_nueva_ficha["Inicio Ventana"] = pd.to_datetime(df_nueva_ficha["Inicio Ventana"], errors="coerce")
    df_nueva_ficha["Fin Ventana"] = pd.to_datetime(df_nueva_ficha["Fin Ventana"], errors="coerce").apply(lambda dt: dt.replace(hour=23, minute=59, second=59) if pd.notna(dt) else dt)
    df_nueva_ficha["Inicio Ventana Corta"] = pd.to_datetime(df_nueva_ficha["Inicio Ventana Corta"], errors="coerce")
    df_nueva_ficha["Fin Ventana Corta"] = pd.to_datetime(df_nueva_ficha["Fin Ventana Corta"], errors="coerce").apply(lambda dt: dt.replace(hour=23, minute=59, second=59) if pd.notna(dt) else dt)
    df_nueva_ficha["ETA"] = pd.to_datetime(df_nueva_ficha["ETA"], errors="coerce")
    df_nueva_ficha["MONTO ($/DIA)"] = pd.to_numeric(df_nueva_ficha["MONTO ($/DIA)"], errors="coerce")
    
    return df_nueva_ficha

def calcular_fecha(fecha, fecha_reporte):
    dia_fecha = int(fecha.split('-')[0])
    mes_fecha = MESES_ABREVIADOS[fecha.split('-')[1].lower()]
    # Si en un reporte de fin de año (ej. 01-dic-24) aparece una fecha de mes hasta 6 meses menor 
    # (ej. 02-ene), se asume que es del próximo año.
    if fecha_reporte.month - mes_fecha > 6:
        año_fecha = fecha_reporte.year + 1
    # Si en un reporte de inicio de año (ej. 01-ene-25) aparece una fecha de mes hasta 6 meses mayor
    # (ej. 30-dic), se asume que es del año anterior.
    elif fecha_reporte.month - mes_fecha < -6:
        año_fecha = fecha_reporte.year - 1
    # Si la fecha es del mismo mes o hasta 6 meses antes o después, se asume que es del mismo año.
    else:
        año_fecha = fecha_reporte.year
    return pd.Timestamp(year=año_fecha, month=mes_fecha, day=dia_fecha)


def extraer_reporte_tankers(file):
    with pdfplumber.open(file) as pdf:
        page = pdf.pages[0]
        texto = page.extract_text()
    # Fecha del reporte siempre está en la segunda línea formato dd MES yyyy.
    dia_reporte, mes_reporte, año_reporte = texto.split("\n")[1].split()
    fecha_reporte = pd.Timestamp(year=int(año_reporte), month=MESES_UPPERCASE[mes_reporte.upper()], day=int(dia_reporte))

    pattern = re.compile(
        r'(CC\s\d{2,4}/\d{2})'               # CC
        r'.*?(\d{2}-[a-z]{3,4})'             # Ventana inicio
        r'\s+(\d{2}-[a-z]{3,4})'             # Ventana fin
        r'.*?(\d{2}-[a-z]{3,4}|TBD|-)'       # PreBook
        r'.*?(\d{2}-[a-z]{3,4}|TBD|-)\*{0,2}?'    # ETA
        r'\s+(\d{1,2}:\d{2}|-)?')            # Hora (opcional)
    
    matches = pattern.findall(texto)
    df = pd.DataFrame(matches, columns=['N° Referencia', 'Inicio Ventana', 'Fin Ventana', 'PreBook', 'ETA', 'Hora'])
    df["Inicio Ventana"] = pd.to_datetime(df["Inicio Ventana"].apply(lambda x: calcular_fecha(x, fecha_reporte) if x not in ['TBD', '-'] else pd.NaT))
    df["Fin Ventana"] = pd.to_datetime(df["Fin Ventana"].apply(lambda x: calcular_fecha(x, fecha_reporte) if x not in ['TBD', '-'] else pd.NaT)).apply(lambda dt: dt.replace(hour=23, minute=59, second=59) if pd.notna(dt) else dt)
    df["ETA"] = pd.to_datetime(df["ETA"].apply(lambda x: calcular_fecha(x, fecha_reporte) if x not in ['TBD', '-'] else pd.NaT))
    df["Hora"] = df["Hora"].apply(lambda x: pd.to_datetime(x, format="%H:%M").time() if x not in [None, '-'] else None)
    df["ETA"] = df.apply(lambda x: pd.Timestamp.combine(x["ETA"].date(), x["Hora"]) if pd.notna(x["ETA"]) and x["Hora"] is not None else x["ETA"], axis=1)

    df.drop(columns=['PreBook'], inplace=True)
    df.drop(columns=['Hora'], inplace=True)

    return df