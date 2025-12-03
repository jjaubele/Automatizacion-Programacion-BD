from datetime import datetime
from sqlalchemy import (Column, Integer, String, Float, DateTime, Date, Enum, ForeignKey, func)
from sqlalchemy.orm import relationship, declarative_base
import enum

Base = declarative_base()

class ProductoEnum(enum.Enum):
    DIESEL_A1 = "Diesel A1"
    JET_A1 = "Jet A1"
    GAS_93 = "Gas 93"
    GAS_97 = "Gas 97"
    DIESEL_B = "Diesel B"
    FUEL_6 = "Fuel 6"
    VLSFO = "VLSFO"
    IFO_380 = "IFO-380"


class Programa(Base):
    __tablename__ = "programas"

    id = Column(Integer, primary_key=True, autoincrement=True)
    CC = Column(String, unique=True, nullable=False)
    nombre_bt = Column(String)
    proveedor = Column(String)
    origen = Column(String)
    inicio_ventana = Column(DateTime)
    fin_ventana = Column(DateTime)
    ETA = Column(DateTime)
    monto = Column(Float)
    laytime = Column(Integer)
    agencia_de_naves = Column(String)
    surveyor_primario = Column(String)
    surveyor_secundario = Column(String)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())

    descargas = relationship("Descarga", back_populates="programa", cascade="all, delete-orphan")
    estimacion_programa = relationship("EstimacionPrograma", back_populates="programa", cascade="all, delete-orphan")
    plantas = relationship("Planta", secondary="descargas", viewonly=True)
    programaciones = relationship("Programacion", secondary="descargas", viewonly=True)
    timelogs = relationship("Timelog", back_populates="programa", cascade="all, delete-orphan")


class Planta(Base):
    __tablename__ = "plantas"

    id = Column(Integer, primary_key=True, autoincrement=True)
    nombre = Column(String, unique=True)
    ciudad = Column(String)
    alias = Column(String)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())

    descargas = relationship("Descarga", back_populates="planta", cascade="all, delete-orphan")
    programas = relationship("Programa", secondary="descargas", viewonly=True)
    programaciones = relationship("Programacion", secondary="descargas", viewonly=True)
    timelogs = relationship("Timelog", back_populates="planta", cascade="all, delete-orphan")


class Programacion(Base):
    __tablename__ = "programaciones"

    id = Column(Integer, primary_key=True, autoincrement=True)
    fecha = Column(Date, unique=True)
    semana = Column(Integer)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())

    descargas = relationship("Descarga", back_populates="programacion", cascade="all, delete-orphan")
    programas = relationship("Programa", secondary="descargas", viewonly=True)
    plantas = relationship("Planta", secondary="descargas", viewonly=True)


class Descarga(Base):
    __tablename__ = "descargas"

    id = Column(Integer, primary_key=True, autoincrement=True)
    volumen = Column(Integer)
    producto = Column(Enum(ProductoEnum, values_callable=lambda x: [e.value for e in x], native_enum=False))
    planta_id = Column(Integer, ForeignKey("plantas.id", ondelete="CASCADE"))
    programa_id = Column(Integer, ForeignKey("programas.id", ondelete="CASCADE"))
    programacion_id = Column(Integer, ForeignKey("programaciones.id", ondelete="CASCADE"))
    fecha_inicio = Column(DateTime)
    fecha_fin = Column(DateTime)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())

    programa = relationship("Programa", back_populates="descargas")
    planta = relationship("Planta", back_populates="descargas")
    programacion = relationship("Programacion", back_populates="descargas")

    estimacion_descarga = relationship("EstimacionDescarga", back_populates="descarga", cascade="all, delete-orphan")
    

class EstimacionDescarga(Base):
    __tablename__ = "estimaciones_descargas"

    id = Column(Integer, primary_key=True, autoincrement=True)
    descarga_id = Column(Integer, ForeignKey("descargas.id", ondelete="CASCADE"), unique=True)
    ETA = Column(DateTime)
    inicio_laytime = Column(DateTime)
    tiempo_descarga = Column(Float)
    demurrage_descarga = Column(Float)
    estimacion_demurrage = Column(Float)
    demurrage_unitario = Column(Float)
    shifting = Column(Float)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())

    descarga = relationship("Descarga", back_populates="estimacion_descarga", uselist=False)


class EstimacionPrograma(Base):
    __tablename__ = "estimaciones_programas"

    id = Column(Integer, primary_key=True, autoincrement=True)
    programa_id = Column(Integer, ForeignKey("programas.id", ondelete="CASCADE"), unique=True)
    tiempo_programa = Column(Float)
    demurrage_programa = Column(Float)
    mes = Column(Integer)
    año = Column(Integer)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())

    programa = relationship("Programa", back_populates="estimacion_programa", uselist=False)

class Timelog(Base):
    __tablename__ = "timelogs"

    id = Column(Integer, primary_key=True, autoincrement=True)
    nombre = Column(String, unique=True)
    fecha = Column(Date)
    programa_id = Column(Integer, ForeignKey("programas.id", ondelete="CASCADE"))
    planta_id = Column(Integer, ForeignKey("plantas.id", ondelete="CASCADE"))
    vessel_arrived = Column(DateTime)
    start_mooring = Column(DateTime)
    end_mooring = Column(DateTime)
    start_hose_connection = Column(DateTime)
    end_hose_connection = Column(DateTime)
    start_discharge = Column(DateTime)
    end_discharge = Column(DateTime)
    vessel_dispatched = Column(DateTime)
    #########################
    nor_tendered = Column(DateTime)
    vessel_anchored = Column(DateTime)
    free_practique = Column(DateTime)
    all_fast = Column(DateTime)
    #########################
    arribo_inicio_amarre = Column(Float)
    inicio_amarre_fin_amarre = Column(Float)
    fin_amarre_inicio_conexion = Column(Float)
    inicio_conexion_fin_conexion = Column(Float)
    fin_conexion_inicio_descarga = Column(Float)
    inicio_descarga_fin_descarga = Column(Float)
    fin_descarga_despachado = Column(Float)
    tiempo_total = Column(Float)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())

    programa = relationship("Programa", back_populates="timelogs")
    planta = relationship("Planta", back_populates="timelogs")