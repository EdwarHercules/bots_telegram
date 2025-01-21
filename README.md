# Telegram Bot: Automatización y Gestor de Tareas

Este proyecto implementa un bot de Telegram utilizando Python y las bibliotecas `python-telegram-bot` y `SQLAlchemy`. El bot gestiona tareas como registro de usuarios, menús interactivos, y planificación de actividades, integrándose con una base de datos MySQL.

## Características

- **Flujo de Conversación**: Manejadores de comandos y estados para personalizar la experiencia del usuario.
- **Conexión a Base de Datos**: Uso de SQLAlchemy para gestionar datos almacenados en MySQL.
- **Registros**: Implementación de logs detallados para rastrear eventos y errores.
- **Planificación Automática**: Uso de `JobQueue` para realizar tareas programadas.
- **Gestor de Errores**: Captura y manejo de errores de manera robusta.

## Requisitos

1. Python 3.8 o superior.
2. Librerías requeridas (instalar con `pip install -r requirements.txt`):
   - `python-telegram-bot`
   - `SQLAlchemy`
   - `pandas`
   - `python-dotenv`
   - `PyMySQL`

## Instalación

1. Clona el repositorio:

   ```bash
   git clone https://github.com/EdwarHercules/bots_telegram.git
   cd bots_telegram
   ```

2. Crea y configura un archivo `.env` con tus credenciales:

   ```env
   DB_USER=tu_usuario
   DB_PASSWORD=tu_contrasena
   DB_HOST=tu_host
   DB_NAME=tu_base_de_datos
   SSL_CERT_PATH=ruta/a/tu/mysql.crt
   SSL_KEY_PATH=ruta/a/tu/mysql.key
   YOUR_TOKEN=tu_token_de_bot
   ```

3. Instala las dependencias:

   ```bash
   pip install -r requirements.txt
   ```

4. Ejecuta el bot:

   ```bash
   python bot_md.py
   ```

## Uso

### Comandos Disponibles

1. `/start`: Inicia el proceso de registro.
2. `/menu`: Accede al menú principal.
3. `/planificacion`: Gestiona la planificación de tareas.

### Flujo de Conversación
- **Registro**: Solicita información inicial del usuario.
- **Menú Interactivo**: Opciones para realizar acciones personalizadas.
- **Planificación**: Permite subir archivos (como Excel) y procesar información.

### Ejemplo de Configuración del JobQueue
El `JobQueue` ejecuta tareas repetitivas, como procesar solicitudes cada 10 segundos.

```python
job_queue.run_repeating(procesar_solicitudes, interval=10, first=0)
```

## Arquitectura

El bot está compuesto por:

1. **Handlers**:
   - `registro_handler`: Maneja el registro inicial de usuarios.
   - `menu_handler`: Gestiona el menú principal y las opciones.
   - `planificacion_handler`: Maneja la subida y procesamiento de archivos.

2. **Base de Datos**:
   Conexión establecida con SQLAlchemy y configurada para usar SSL.

3. **Logging**:
   Registra eventos y errores en `bot.log`.

## Estructura del Proyecto

```plaintext
bots_telegram/
|-- bot_md.py               # Código principal del bot
|-- bot_me.py               # Módulo auxiliar (opcional)
|-- requirements.txt        # Dependencias del proyecto
|-- .env                    # Configuración sensible (no incluida en el repositorio)
|-- bot.log                 # Archivo de registros
```

## Seguridad

- **Variables de Entorno**: Información sensible como contraseñas y tokens deben estar en el archivo `.env`.
- **SSL**: Uso de certificados para conexiones seguras a la base de datos.

## Recursos Adicionales

- [Documentación de python-telegram-bot](https://python-telegram-bot.readthedocs.io/)
- [SQLAlchemy](https://www.sqlalchemy.org/)
- [MySQL Connector](https://pymysql.readthedocs.io/)

## Contribuciones

Si deseas contribuir a este proyecto, realiza un fork del repositorio y crea un pull request con tus cambios.

---

**Autor**: Edwar Hércules  
**Repositorio**: [https://github.com/EdwarHercules/bots_telegram](https://github.com/EdwarHercules/bots_telegram)

