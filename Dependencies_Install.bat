REM 1. Crear entorno virtual si no existe
if not exist "venv" (
    py -m venv venv
)

REM 2. Activar entorno virtual
call venv\Scripts\activate.bat

REM 3. Actualizar pip e instalar dependencias
pip install --upgrade pip
pip install -r requirements.txt