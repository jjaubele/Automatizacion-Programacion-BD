# Automatizacion-Programacion-BD

## Prerrequisitos
- Python 3.8+
- PostgreSQL

## Antes de comenzar
- Ejecutar ``pip install --upgrade pip`` y ``pip install -r requirements.txt``

Para sembrar la base de datos se requiere de un .env con el siguiente formato:

```env
USUARIO_LOCAL=postgres
PASSWORD_LOCAL=xxxxxx
BD_LOCAL=xxxxxx
HOST_LOCAL=localhost
PORT_LOCAL=5432
BD_URI_LOCAL=postgresql+psycopg2://${USUARIO_LOCAL}:${PASSWORD_LOCAL}@${HOST_LOCAL}:${PORT_LOCAL}/${BD_LOCAL}

USUARIO_REMOTE=postgres
PASSWORD_REMOTE=xxxxxx
BD_REMOTE=xxxxxx
HOST_REMOTE=xxxxxx
PORT_REMOTE=xxxxxx
BD_URI_REMOTE=postgresql+psycopg2://${USUARIO_REMOTE}:${PASSWORD_REMOTE}@${HOST_REMOTE}:${PORT_REMOTE}/${BD_REMOTE}
```

Para pruebas en local, además agregar `.streamlit/secrets.toml` al repo de trabajo local con el siguiente formato:

```toml
[connections]
BD_URI = "postgresql+psycopg2://usuario:contraseña@host:puerto/nombre_bd"

[auth]
password = "contraseña"
```

El código para sembrar la Base de Datos se encuentra en Load_db.ipynb.

