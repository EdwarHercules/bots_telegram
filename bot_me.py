"""
## Bot de Telegram para Planificación de Medidores

Este bot permite registrar usuarios autorizados, planificar medidores enviando archivos Excel o listas de medidores, y ejecutar consultas a una base de datos MySQL. Usa `telegram.ext` para gestionar comandos y `SQLAlchemy` para la interacción con la base de datos.

### Dependencias:
- `telegram.ext` para manejar comandos y mensajes.
- `SQLAlchemy` para conexión con la base de datos.
- `pandas` para manejar archivos Excel.
- `logging` para registrar eventos y errores.

### Estados:
El bot utiliza un `ConversationHandler` con diferentes estados:
1. `REGISTRO`: Maneja la solicitud de registro del usuario.
2. `PLANIFICACION`: Gestiona la planificación de medidores por parte del usuario.
3. `SELECCIONAR_OPCION`: Selecciona las opciones disponibles en el menú.
4. `SELECCIONAR_MARCA`: Selecciona la marca de los medidores.
5. `INGRESAR_MEDIDOR`: Se usa para ingresar medidores manualmente o por archivo.
6. `PROCESAR_SOLICITUDES`: Procesa solicitudes de usuarios autorizados.
"""

from telegram import Update, ReplyKeyboardMarkup
from telegram.ext import Application, CommandHandler, MessageHandler, filters, CallbackContext, ConversationHandler, JobQueue
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from dotenv import load_dotenv
import urllib.parse
import logging
import pandas as pd
from datetime import datetime, timedelta
from threading import Thread
import threading
import time
import asyncio
import io



# Definir los estados
REGISTRO, PLANIFICACION, SELECCIONAR_OPCION, SELECCIONAR_MARCA, INGRESAR_MEDIDOR, PROCESAR_SOLICITUDES = range(6)

# Configuración de logging
logging.basicConfig(
    level = logging.INFO, # Nivel minimo de severidad pa  ra registrar 
    format='%(asctime)s - %(levelname)s - %(message)s', # Formato de registro
    filename='botME.log', # Nombre del archivo del registro
    filemode='a' # Modo de archivo: 'a' para agregar, 'w' para sobreescribir
)

# Cargar las variables de entorno desde el archivo .env
load_dotenv()

# Conexión a la base de datos usando SQLAlchemy
usuario = os.getenv('DB_USER')
contrasena = os.getenv('DB_PASSWORD')
host = os.getenv('DB_HOST')
base_datos = os.getenv('DB_NAME')

logging.info("CODIFICANDO CONTRASEÑA")
encoded_password = urllib.parse.quote_plus(contrasena)

ssl_args = {
    'ssl_cert': os.getenv('SSL_CERT_PATH'),
    'ssl_key': os.getenv('SSL_KEY_PATH')
}
engine = create_engine(f"mysql+pymysql://{usuario}:{encoded_password}@{host}/{base_datos}", connect_args=ssl_args)

def solicitud_query(QUERY):
    """
    ## Funcion Solicitud Query:
    Ejecuta una consulta SQL y devuelve el resultado como un DataFrame de pandas.

    Args:
        QUERY (str): Consulta SQL a ejecutar.

    Returns:
        pd.DataFrame: Resultado de la consulta en forma de DataFrame.
    """
    # Ejecutar una consulta y devolver el resultado como DataFrame
    df = pd.read_sql_query(QUERY, engine)
    return df


# Función para iniciar el registro
async def start(update: Update, context: CallbackContext):
    """
    ## Funcion Start:
    Inicia el proceso de registro. Verifica si el usuario ya está registrado.

    Args:
        update (Update): Contiene la información del mensaje recibido.
        context (CallbackContext): Contexto del comando.

    Returns:
        int: Estado del flujo de conversación.
    """
    user_id = update.message.from_user.id
    usuarios = solicitud_query("SELECT * FROM bot_usuarios_autorizados_me;")
    iD_TELEGRAM = usuarios['ID_TELEGRAM'].tolist()

    if int(user_id) not in iD_TELEGRAM:
        await update.message.reply_text('¡Bienvenido! Por favor, ingresa tu nombre completo para validar tus datos:')
        return REGISTRO
    else:
        await update.message.reply_text('Ya estás registrado. Usa el comando /menu para acceder a las opciones.')
        return ConversationHandler.END

# Función para manejar el registro
async def registro(update: Update, context: CallbackContext):
    """
    ## Funcion Registro:
    Maneja el registro de un nuevo usuario.

    Args:
        update (Update): Contiene la información del mensaje recibido.
        context (CallbackContext): Contexto del comando.

    Returns:
        int: Estado del flujo de conversación.
    """
    user_name = update.message.text
    user_id = update.message.from_user.id
    user_name_id = update.message.from_user.username
    user_first_name = update.message.from_user.first_name

    try:
        with engine.begin() as con:
            query_update = text("""
                UPDATE bot_usuarios_autorizados_me 
                SET ID_TELEGRAM=:user_id,
                ROL='SUPERVISOR'
                WHERE NOMBRE_COMPLETO=:user_name 
                OR NOMBRE_TELEGRAM=:user_first_name 
                OR USUARIO_TELEGRAM=:user_name_id
            """)
            con.execute(query_update, {'user_id': user_id, 'user_name': user_name, 'user_first_name': user_first_name, 'user_name_id': user_name_id})
            await update.message.reply_text(f'Te has registrado con éxito como {user_name}. Usa el comando /menu para acceder a las opciones.')
    except SQLAlchemyError as e:
        print(e)
        logging.error(f"Error al registrar usuario: {e}")
        await update.message.reply_text('Ocurrió un error durante el registro. Por favor, inténtalo de nuevo.')

    return ConversationHandler.END

# Funcion para verificar el rol del usuario
async def verificar_rol(update: Update, context: CallbackContext):
    """
    ## Funcion verificar rol:

    Verifica el rol del usuario y permite la planificación si está autorizado.

    Args:
        update (Update): Contiene la información del mensaje recibido.
        context (CallbackContext): Contexto del comando.

    Returns:
        int: Estado del flujo de conversación.
    """
    user_id = update.message.from_user.id
    usuarios = solicitud_query("SELECT * FROM bot_usuarios_autorizados_me;")
    iD_TELEGRAM = usuarios['ID_TELEGRAM'].tolist()
    Rol_user = solicitud_query(f"SELECT ROL FROM bot_usuarios_autorizados_me where ID_TELEGRAM = {user_id};")
    roluser = Rol_user.iloc[0, 0]

    if int(user_id) in iD_TELEGRAM and roluser == 'PLANIFICADOR' or roluser== 'ADMINISTRADOR':
        await update.message.reply_text(
            '¡Bienvenido! Por favor, ingresa los medidores que planificarán enviando un archivo Excel, '
            'una lista de medidores separada por comas, o un listado de medidores en diferentes líneas.'
        )
        return PLANIFICACION
    else:
        await update.message.reply_text('No estás autorizado para realizar una planificación.')
        return ConversationHandler.END

# Funcion para ingresar la planificacion
async def planificacion(update: Update, context: CallbackContext):
    """
    ## Funcion planificacion:
    Maneja la planificación de medidores. Permite al usuario enviar un archivo Excel o una lista de medidores.

    Args:
        update (Update): Contiene la información del mensaje recibido.
        context (CallbackContext): Contexto del comando.

    Returns:
        int: Estado del flujo de conversación.
    """
    user_id = update.message.from_user.id
    user_first_name = update.message.from_user.first_name
    print("Realizando la planificación")

    # Obtener datos de usuarios autorizados
    usuarios = solicitud_query("SELECT * FROM bot_usuarios_autorizados_me;")
    iD_TELEGRAM = usuarios['ID_TELEGRAM'].tolist()

    if int(user_id) in iD_TELEGRAM:
        usuario_encontrado = usuarios.loc[usuarios['ID_TELEGRAM'] == int(user_id)].iloc[0]
        nombre_completo = usuario_encontrado['NOMBRE_COMPLETO'] if pd.notna(usuario_encontrado['NOMBRE_COMPLETO']) else user_first_name
        print(f"Nombre completo: {nombre_completo}")

        # Manejo de archivos enviados
        if update.message.document:
            file = update.message.document
            print("El usuario envió un documento")

            # Verificación del tipo de archivo
            if file.file_name.endswith('.xlsx') or file.file_name.endswith('.xls'):
                try:
                    file_path = await context.bot.get_file(file.file_id)
                    file_data = await file_path.download_as_bytearray()
                    print("Datos del archivo descargado")

                    # Convertir bytearray a BytesIO para que pandas pueda leerlo
                    file_stream = io.BytesIO(file_data)

                    # Leer datos de Excel usando BytesIO
                    excel_data = pd.read_excel(file_stream)
                    print("Datos del Excel leídos")
                    print(excel_data)

                    # Limpiar el DataFrame: eliminar filas donde la columna 'Clave' es nula, vacía o ""
                    excel_data = excel_data.dropna(subset=['Clave'])  # Eliminar filas con NaN en 'Clave'
                    excel_data = excel_data[excel_data['Clave'].astype(str).str.strip() != '']  # Eliminar filas con cadenas vacías
                    print(excel_data)
                

                    records = [(user_id, nombre_completo, row['Clave'], row['Fecha de Programación'].strftime('%Y-%m-%d'))
                           for index, row in excel_data.iterrows()]
                
                    try:
                        with engine.begin() as con:
                            # Usar la sintaxis de SQLAlchemy para múltiples inserciones
                            query_insert = text("""
                                INSERT INTO pnrp.bot_planificacion_me(ID_TELEGRAM, NOMBRE, CLAVE, FECHA_PLANIFICACION, REVISION, CANTIDAD_CONSULTAS)
                                VALUES(:user_id, :user_nombre, :clave, :fecha, 0, 0)
                            """)
                            # Insertar múltiples registros usando executemany
                            con.execute(query_insert, [dict(zip(['user_id', 'user_nombre', 'clave', 'fecha'], record)) for record in records])
                    except SQLAlchemyError as e:
                        logging.error(f"Error al insertar los medidores: {e}")
                        await update.message.reply_text('Error al registrar los medidores. Por favor, inténtalo de nuevo.')


                    await update.message.reply_text('Todos los medidores han sido registrados con éxito. Usa el comando /menu para acceder a las opciones.')

                except Exception as e:
                    print(e)
                    logging.error(f"Error al procesar los datos del archivo Excel: {e}")
                    await update.message.reply_text('Error al procesar los datos del archivo Excel. Por favor, verifica el formato y vuelve a intentarlo.')
            
            else:
                await update.message.reply_text('Por favor, envía un archivo Excel (.xlsx o .xls).')

    # Manejo de entrada de texto
    else:
        print("El usuario envió un texto")
        user_text = update.message.text.strip()

        if ',' in user_text:
            medidores = [medidor.strip() for medidor in user_text.split(',')]
        else:
            medidores = [line.strip() for line in user_text.split('\n') if line.strip()]

        if not medidores:
            await update.message.reply_text('No se encontraron medidores en el texto proporcionado. Por favor, envía una lista válida.')
            return ConversationHandler.END

        try:
            for medidor in medidores:
                fecha_planificacion = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                try:
                    with engine.begin() as con:
                        query_insert = text("""
                            INSERT INTO bot_planificacion_me(ID_TELEGRAM, NOMBRE, MEDIDOR, FECHA_PLANIFICACION, REVISION)
                            VALUES(:user_id, :user_nombre, :medidor, :fecha, 0)
                        """)
                        con.execute(query_insert, user_id=user_id, user_nombre=user_first_name, medidor=medidor, fecha=fecha_planificacion)
                except SQLAlchemyError as e:
                    logging.error(f"Error al insertar el medidor {medidor}: {e}")
                    await update.message.reply_text(f'Error al registrar el medidor {medidor}. Por favor, inténtalo de nuevo.')
            
            await update.message.reply_text('Todos los medidores han sido registrados con éxito. Usa el comando /menu para acceder a las opciones.')
        
        except Exception as e:
            logging.error(f"Error al procesar los datos del texto: {e}")
            await update.message.reply_text('Error al procesar los datos del texto. Por favor, verifica el formato y vuelve a intentarlo.')

    return ConversationHandler.END

# Función para iniciar el menú de opciones
async def iniciar_menu(update: Update, context: CallbackContext):
    """
    ## Funcion iniciar menu:
    Muestra el menú de opciones al usuario, cada parte del menu tiene una logica diferente.

    El menu que le muestra al usuario son:
        1. Información del medidor
        2. Estado de comunicación del medidor
        3. Alarmas del medidor
        4. Órdenes de servicio asociadas al medidor
        5. Comentarios de telegestión

    Args:
        update (Update): Contiene la información del mensaje recibido.
        context (CallbackContext): Contexto del comando.

    Returns:
        int: Estado del siguiente paso en la conversación (SELECCIONAR_OPCION).
    """
    opciones = [
        ['Información del medidor', '1'],
        ['Comunicación del medidor', '2'],
        ['Alarmas del medidor', '3'],
        ['Órdenes de servicio del medidor', '4'],
        ['Comentario de Telegestion', '5']
    ]
    reply_markup = ReplyKeyboardMarkup([[opcion[0]] for opcion in opciones], one_time_keyboard=True)
    context.user_data['opciones'] = opciones
    await update.message.reply_text('Por favor, selecciona una opción:', reply_markup=reply_markup)
    return SELECCIONAR_OPCION

# Función para manejar la selección de opciones
async def seleccionar_opcion(update: Update, context: CallbackContext):
    """
    ## Funcion seleccionar opcion: 
    Maneja la opción seleccionada por el usuario y solicita la marca del medidor.

    Args:
        update (Update): Contiene la información del mensaje recibido.
        context (CallbackContext): Contexto del comando.

    Returns:
        int: Estado del siguiente paso en la conversación (SELECCIONAR_MARCA).
    """
    opcion_texto = update.message.text
    opciones = context.user_data['opciones']
    
    # Buscar el ID correspondiente a la opción seleccionada
    for opcion in opciones:
        if opcion[0] == opcion_texto:
            context.user_data['user_command'] = opcion[1]
            break
    
    # Opciones de marca
    marcas = [['Union'], ['Hexing']]
    reply_markup = ReplyKeyboardMarkup(marcas, one_time_keyboard=True)
    await update.message.reply_text(f'Seleccionaste: {opcion_texto}. Ahora selecciona la marca del medidor:', reply_markup=reply_markup)
    return SELECCIONAR_MARCA

# Función para manejar la selección de marca
async def seleccionar_marca(update: Update, context: CallbackContext):
    """
    ## Funcion seleccionar marca:
    Maneja la selección de la marca del medidor.

    Args:
        update (Update): Contiene la información del mensaje recibido.
        context (CallbackContext): Contexto del comando.

    Returns:
        int: Estado del siguiente paso en la conversación (INGRESAR_MEDIDOR).
    """
    marca_texto = update.message.text
    if marca_texto == 'Regresar':
        # Si selecciona "Regresar", vuelve a seleccionar la opción
        return await seleccionar_opcion(update, context)
    else:
        context.user_data['marca'] = update.message.text
        await update.message.reply_text(f'Seleccionaste: {update.message.text}. Ahora ingresa el número del medidor:')
        return INGRESAR_MEDIDOR

# Función para manejar la entrada del número de medidor
async def ingresar_medidor(update: Update, context: CallbackContext):
    """
    ## Funcion Ingresar medidor:
    Maneja la entrada del número del medidor y lo inserta en la base de datos.

    Args:
        update (Update): Contiene la información del mensaje recibido.
        context (CallbackContext): Contexto del comando.

    Returns:
        int: Estado de finalización de la conversación (ConversationHandler.END).
    """
    user_medidor = update.message.text
    user_id = update.message.from_user.id
    user_first_name = update.message.from_user.first_name
    user_command = context.user_data['user_command']
    user_marca = context.user_data['marca']
    fecha_instantanea = datetime.now()
    usuarios = solicitud_query("SELECT * FROM bot_usuarios_autorizados_me;")
    iD_TELEGRAM = usuarios['ID_TELEGRAM'].tolist()
    if user_marca == "Union":
        def convertir_medidor(user_medidor):
            # Si el user_medidor empieza con '7', quita el '7' y agrega ceros al principio hasta completar 12 dígitos
            if user_medidor.startswith('7'):
                user_medidor = user_medidor[1:]  # Elimina el '7'
            
            # Si el user_medidor es numérico y tiene menos de 12 dígitos, añade ceros al principio
            if len(user_medidor) < 12:
                user_medidor = user_medidor.zfill(12)  # Rellena con ceros a la izquierda hasta completar 12 dígitos
            
            return user_medidor

        if len(user_medidor) > 2 and len(user_medidor) < 6 or user_medidor.startswith('7'):
            user_medidor = convertir_medidor(user_medidor)
            
    if int(user_id) in iD_TELEGRAM:
        try:
            usuario_encontrado = usuarios.loc[usuarios['ID_TELEGRAM'] == int(user_id)].iloc[0]
            nombre_completo = usuario_encontrado['NOMBRE_COMPLETO'] if pd.notna(usuario_encontrado['NOMBRE_COMPLETO']) else user_first_name
            rol_user = usuario_encontrado['ROL']
            print(rol_user)
            print(f" rol supervisor {rol_user == 'SUPERVISOR'}")
            print(f"rol diferente a supervisor {rol_user == 'ADMINISTRADOR' or rol_user == 'ANALISTA' or rol_user == 'PLANIFICADOR'}")


            if rol_user == "SUPERVISOR":

                if user_marca == "Hexing":
                    clave = solicitud_query(f"select CLAVE_CATALOGO from pnrp.airflow_hexing_universo where MEDIDOR_CATALOGO = {user_medidor} limit 1;")
                    if not clave.empty:
                        clave = clave.iloc[0,0]
                    else:
                        clave = "EMPTY"
                if user_marca == "Union":
                    def convertir_medidor(user_medidor):
                        # Si el user_medidor empieza con '7', quita el '7' y agrega ceros al principio hasta completar 12 dígitos
                        if user_medidor.startswith('7'):
                            user_medidor = user_medidor[1:]  # Elimina el '7'
                        
                        # Si el user_medidor es numérico y tiene menos de 12 dígitos, añade ceros al principio
                        if user_medidor.isdigit() and len(user_medidor) < 12:
                            user_medidor = user_medidor.zfill(12)  # Rellena con ceros a la izquierda hasta completar 12 dígitos

                        return user_medidor
                    
                    if len(user_medidor) > 2 and len(user_medidor) < 6 or user_medidor.startswith('7'):
                        user_medidor = convertir_medidor(user_medidor)

                    clave = solicitud_query(f"SELECT CLAVE_CATALOGO from pnrp.airflow_union_universo where MEDIDOR_CATALOGO = '{user_medidor}' limit 1;")
                    if not clave.empty:
                        clave = clave.iloc[0,0]
                    else:
                        clave = "EMPTY"
                
                if clave != 'EMPTY':
                    planificaciones = solicitud_query(f"SELECT * FROM pnrp.bot_planificacion_me WHERE CLAVE={clave} order by FECHA_PLANIFICACION DESC;")
                    if not planificaciones.empty:
                        planificacion = planificaciones.iloc[0]
                        fecha_planificacion = planificacion['FECHA_PLANIFICACION']
                        clave_planificada = planificacion['CLAVE']
                        id_clave_planificada = planificacion['id']
                        cantidad = planificacion['CANTIDAD_CONSULTAS']
                        print(planificacion)
                        if cantidad == None or cantidad == 0:
                            cantidad = 1
                        else:
                            cantidad += 1

                        diferencia = fecha_instantanea - fecha_planificacion

                        print(diferencia)
                        print()

                        if diferencia <= timedelta(days=3) and clave_planificada == clave:

                            with engine.begin() as con:
                                logging.info(f"Insertando consulta a la base de datos, nombre: {nombre_completo}, medidor:{user_medidor}, comando:{user_command}, fecha:{fecha_instantanea}")
                                print(f"el nombre completo es: {nombre_completo} y el medidor que ingreso es: {user_medidor}, comando: {user_command}")
                                query_insert = text("INSERT INTO bot_solicitudes_me (ID_TG, COMANDO, MEDIDOR, MARCA, FECHA, PROCESO, ENVIADO, NOMBRE) VALUES (:user_id, :user_command, :user_medidor, :user_marca , :fecha_instantanea, 0, 0, :nombre_completo)")
                                con.execute(query_insert, {
                                    'user_id': user_id,
                                    'user_command': user_command,
                                    'user_medidor': user_medidor,
                                    'user_marca' : user_marca,
                                    'fecha_instantanea': fecha_instantanea,
                                    'nombre_completo': nombre_completo
                                })
                                logging.info(f"Insercion exitosa, nombre: {nombre_completo}, medidor:{user_medidor}, marca:{user_marca} ,comando:{user_command}, fecha:{fecha_instantanea}")
                                await update.message.reply_text(f"La solicitud de validación está en proceso para el medidor: {user_medidor}")

                            with engine.begin() as con:
                                if cantidad > 0:
                                    query_update = text("UPDATE bot_planificacion_me set REVISION = '1', CANTIDAD_CONSULTAS = :cantidad where id = :id;")
                                else:
                                    query_update = text("UPDATE bot_planificacion_me set CANTIDAD_CONSULTAS = :cantidad where id = :id;")

                                con.execute(query_update, {
                                    'cantidad' : cantidad,
                                    'id' : id_clave_planificada
                                })
                    
                    if planificaciones.empty:
                        await update.message.reply_text(f"Medidor: {user_medidor} no ha sido planificado.")

                if clave == "EMPTY":
                    await update.message.reply_text(f"No se tiene informacion de medidor: {user_medidor}.")

            if rol_user == "ADMINISTRADOR" or rol_user == "ANALISTA" or rol_user == "PLANIFICADOR":
                with engine.begin() as con:
                    logging.info(f"Insertando consulta a la base de datos, nombre: {nombre_completo}, medidor:{user_medidor}, comando:{user_command}, fecha:{fecha_instantanea}")
                    print(f"el nombre completo es: {nombre_completo} y el medidor que ingreso es: {user_medidor}, comando: {user_command}")
                    query_insert = text("INSERT INTO bot_solicitudes_me (ID_TG, COMANDO, MEDIDOR, MARCA, FECHA, PROCESO, ENVIADO, NOMBRE) VALUES (:user_id, :user_command, :user_medidor, :user_marca , :fecha_instantanea, 0, 0, :nombre_completo)")
                    con.execute(query_insert, {
                        'user_id': user_id,
                        'user_command': user_command,
                        'user_medidor': user_medidor,
                        'user_marca' : user_marca,
                        'fecha_instantanea': fecha_instantanea,
                        'nombre_completo': nombre_completo
                    })
                    logging.info(f"Insercion exitosa, nombre: {nombre_completo}, medidor:{user_medidor}, marca:{user_marca} ,comando:{user_command}, fecha:{fecha_instantanea}")
                    await update.message.reply_text(f"La solicitud de validación está en proceso para el medidor: {user_medidor}")


        except SQLAlchemyError as e:
            logging.error(f"Error al insertar solicitud en la base de datos: {e}")
            await update.message.reply_text(f"Error en la inserción de la solicitud para el medidor: {user_medidor}")

    return PROCESAR_SOLICITUDES

# Funcion para las respuestas a los usuarios
async def procesar_solicitudes(context: CallbackContext):
    """
    ## Funcion Procesar solicitudes:
    Procesa las solicitudes pendientes de un bot y envía respuestas personalizadas a los usuarios.

    Este proceso involucra la consulta de una base de datos para obtener solicitudes que no han sido procesadas
    ni enviadas. Para cada solicitud, se determina el comando del usuario y se recupera la información relevante
    del medidor asociado. Según el comando, se construye un mensaje que se envía al usuario a través de un bot.

    Los comandos posibles son:
        1. Información del medidor
        2. Estado de comunicación del medidor
        3. Alarmas del medidor
        4. Órdenes de servicio asociadas al medidor
        5. Comentarios de telegestión

    Parámetros:
        application (object): La aplicación que contiene el bot para enviar mensajes.

    Manejo de errores:
        - Registra errores en el procesamiento de solicitudes y el envío de mensajes.

    Requisitos:
        - Conexión a la base de datos utilizando `engine`.
        - Funciones auxiliares para realizar consultas a la base de datos.
    """
    print("procesando solicitud")
    try:
        logging.info(f"procesando solicitud")
        with engine.connect() as conn:
            solicitudes_df = solicitud_query("SELECT * FROM bot_solicitudes_me WHERE PROCESO = 0 AND ENVIADO = 0;")
            # logging.info(f"Solicitudes encontradas: {solicitudes_df}")

            for _, solicitud in solicitudes_df.iterrows():
                solicitud_id = solicitud['id']
                user_id = solicitud['ID_TG']
                user_command = solicitud['COMANDO']
                medidor = solicitud['MEDIDOR']
                user_marca = solicitud['MARCA']
                user_first_name = solicitud['NOMBRE']

                print(f"{solicitud_id},  {user_id}, {user_command}, {medidor}, {user_first_name}")
                
                with engine.begin() as conn:
                    query_update_proceso = text("UPDATE bot_solicitudes_me SET PROCESO='1' WHERE id = :id")
                    conn.execute(query_update_proceso, {'id': solicitud_id})
                
                mensaje = None
                if user_marca == "Hexing":
                    clave = solicitud_query(f"select CLAVE_CATALOGO from pnrp.airflow_hexing_universo where MEDIDOR_CATALOGO = {medidor} limit 1;")
                    if not clave.empty:
                        clave = clave.iloc[0,0]
                    else:
                        clave = "EMPTY"
                if user_marca == "Union":
                    def convertir_medidor(medidor):
                        # Si el medidor empieza con '7', quita el '7' y agrega ceros al principio hasta completar 12 dígitos
                        if medidor.startswith('7'):
                            medidor = medidor[1:]  # Elimina el '7'
                        
                        # Si el medidor es numérico y tiene menos de 12 dígitos, añade ceros al principio
                        if medidor.isdigit() and len(medidor) < 12:
                            medidor = medidor.zfill(12)  # Rellena con ceros a la izquierda hasta completar 12 dígitos

                        return medidor
                    
                    if len(medidor) > 2 and len(medidor) < 6 or medidor.startswith('7'):
                        medidor = convertir_medidor(medidor)

                    clave = solicitud_query(f"SELECT CLAVE_CATALOGO from pnrp.airflow_union_universo where MEDIDOR_CATALOGO = '{medidor}' limit 1;")
                    if not clave.empty:
                        clave = clave.iloc[0,0]
                    else:
                        clave = "EMPTY"

                    print(clave)

                # print(clave)
                if user_command == "1":
                    if user_marca == 'Hexing':
                        if clave != "EMPTY":   
                            informacion_medidor = solicitud_query(f"SELECT * FROM pnrp.airflow_hexing_universo WHERE MEDIDOR_CATALOGO = {medidor};")
                            
                            if not informacion_medidor.empty:
                                medidor_info = informacion_medidor.iloc[0]
                                mensaje = (
                                    f"Hola Ingeniero {user_first_name}\n\n"
                                    f"El siguiente reporte es para el medidor: {medidor}.\n\n"
                                    f"Clave: {medidor_info['CLAVE_INCMS']}\n"
                                    f"Nombre Abonado: {medidor_info['NOMBRE_ABONADO_INCMS']}\n"
                                    f"Medidor: {medidor_info['MEDIDOR_INCMS']}\n"
                                    f"Multiplicador: {medidor_info['MULTIPLICADOR_INCMS']}\n"
                                    f"Último Consumo: {medidor_info['ULTIMO_CONSUMO']}\n"
                                    f"Lectura Actual: {medidor_info['LECTURA_ACTUAL']}\n"
                                    f"Código de Lectura: {medidor_info['CODIGO_LECTURA']}\n"
                                    f"Tarifa: {medidor_info['TARIFA']}\n"
                                    f"Tipo de Medida: {medidor_info['TIPO_MEDIDA']}\n"
                                    f"Zona: {medidor_info['ZONA']}\n"
                                    f"Región PNRP: {medidor_info['REGION_PNRP']}\n"
                                    f"Circuito: {medidor_info['CIRCUITO']}\n"
                                    f"Subestación: {medidor_info['SUBESTACION']}\n"
                                    f"Coord. (X,Y):  {medidor_info['COORD_U_Y']}, {medidor_info['COORD_U_X']}\n"
                                    f"Coord. UTM (X,Y): {medidor_info['COORD_Y']}, {medidor_info['COORD_X']}\n"
                                    f"Ubicacion de medidor: https://www.google.com/maps?q={medidor_info['COORD_U_Y']},{medidor_info['COORD_U_X']}"
                                )
                            
                            if informacion_medidor.empty:
                                mensaje = f"Hola ingeniero {user_first_name}, no tenemos informacion del medidor: {medidor}"
                        else:
                            mensaje = f"No se encontro informacion del medidor: {medidor}"
                            logging.warning(f"No se encontró información para el medidor: {medidor}")
                    if user_marca == 'Union':
                        if clave != 'EMPTY':
                            informacion_medidor = solicitud_query(f"SELECT * FROM pnrp.airflow_union_universo WHERE CLAVE_CATALOGO = '{clave}';")

                            if not informacion_medidor.empty:
                                medidor_info = informacion_medidor.iloc[0]
                                mensaje = (
                                    f"Hola Ingeniero {user_first_name}\n\n"
                                    f"El siguiente reporte es para el medidor: {medidor}.\n\n"
                                    f"Clave: {medidor_info['CLAVE_INCMS']}\n"
                                    f"Nombre Abonado: {medidor_info['NOMBRE_ABONADO_INCMS']}\n"
                                    f"Medidor: {medidor_info['MEDIDOR_INCMS']}\n"
                                    f"Multiplicador: {medidor_info['MULTIPLICADOR_INCMS']}\n"
                                    f"Último Consumo: {medidor_info['ULTIMO_CONSUMO']}\n"
                                    f"Lectura Actual: {medidor_info['LECTURA_ACTUAL']}\n"
                                    f"Código de Lectura: {medidor_info['CODIGO_LECTURA']}\n"
                                    f"Tarifa: {medidor_info['TARIFA']}\n"
                                    f"Tipo de Medida: {medidor_info['TIPO_MEDIDA']}\n"
                                    f"Zona: {medidor_info['ZONA']}\n"
                                    f"Región PNRP: {medidor_info['REGION_PNRP']}\n"
                                    f"Circuito: {medidor_info['CIRCUITO']}\n"
                                    f"Subestación: {medidor_info['SUBESTACION']}\n"
                                    f"Coord. (X,Y):  {medidor_info['COORD_U_Y']}, {medidor_info['COORD_U_X']}\n"
                                    f"Coord. UTM (X,Y): {medidor_info['COORD_Y']}, {medidor_info['COORD_X']}\n"
                                    f"Ubicacion de medidor: https://www.google.com/maps?q={medidor_info['COORD_U_Y']},{medidor_info['COORD_U_X']}"
                                )
                            if informacion_medidor.empty:
                                mensaje = f"Hola ingeniero {user_first_name}, no tenemos informacion del medidor: {medidor}"
                        else:
                            mensaje = f"No se encontro informacion del medidor: {medidor}"
                            logging.warning(f"No se encontró información para el medidor: {medidor}")
                
                if user_command == "2":

                    if user_marca == 'Hexing':
                        if clave != "EMPTY":
                            comunicacion = solicitud_query(f"SELECT * FROM pnrp.airflow_hexing_ulti_comu where clave = '{clave}' or medidor='{medidor}';")
                            promedio_comunicacion = solicitud_query(f"""SELECT 
                                                                        -- Porcentaje de comunicación últimos 7 días
                                                                        ((Rango7Dias.TotalIntervalos - COALESCE(SinLectura7Dias.IntervalosSinLectura, 0)) / Rango7Dias.TotalIntervalos) * 100 AS PorcentajeComunicacion7Dias,
                                                                        
                                                                        -- Porcentaje de comunicación últimos 30 días
                                                                        ((Rango30Dias.TotalIntervalos - COALESCE(SinLectura30Dias.IntervalosSinLectura, 0)) / Rango30Dias.TotalIntervalos) * 100 AS PorcentajeComunicacion30Dias
                                                                    FROM
                                                                        -- Porcentaje de comunicación últimos 7 días
                                                                        (SELECT COUNT(DISTINCT UC.FECHA) AS IntervalosSinLectura
                                                                        FROM pnrp.airflow_hexing_sinlectura UC
                                                                        WHERE UC.CLAVE = '{clave}'
                                                                        AND UC.NOMBRE_EVENTO = 'Sin Lectura'
                                                                        AND UC.FECHA BETWEEN DATE_SUB(CURDATE(), INTERVAL 7 DAY) AND CURDATE()) AS SinLectura7Dias
                                                                        RIGHT JOIN
                                                                        (SELECT (TIMESTAMPDIFF(MINUTE, DATE_SUB(CURDATE(), INTERVAL 7 DAY), CURDATE()) / 15) AS TotalIntervalos) AS Rango7Dias ON 1=1
                                                                        
                                                                        -- Porcentaje de comunicación últimos 30 días
                                                                        LEFT JOIN
                                                                        (SELECT COUNT(DISTINCT UC.FECHA) AS IntervalosSinLectura
                                                                        FROM pnrp.airflow_hexing_sinlectura UC
                                                                        WHERE UC.CLAVE = '{clave}'
                                                                        AND UC.NOMBRE_EVENTO = 'Sin Lectura'
                                                                        AND UC.FECHA BETWEEN DATE_SUB(CURDATE(), INTERVAL 30 DAY) AND CURDATE()) AS SinLectura30Dias ON 1=1
                                                                        LEFT JOIN
                                                                        (SELECT (TIMESTAMPDIFF(MINUTE, DATE_SUB(CURDATE(), INTERVAL 30 DAY), CURDATE()) / 15) AS TotalIntervalos) AS Rango30Dias ON 1=1;
                                                                    """)
                            if not comunicacion.empty and not promedio_comunicacion.empty:
                                medidor_comunicacion = comunicacion.iloc[0]
                                medidor_promedio = promedio_comunicacion.iloc[0]
                                mensaje = (
                                    f"Hola Ingeniero {user_first_name}\n\n"
                                    f"El siguiente reporte es para el medidor: {medidor} con clave: {clave}.\n\n"
                                    f"Ultima Fecha de comunicacion: {medidor_comunicacion['FECHA']}\n"
                                    f"Ultima Lectura: {medidor_comunicacion['LECTURA']}\n\n"
                                    f"Promedio de comunicaciones:\n"
                                    f"Promedio en los ultimos 7 dias: {medidor_promedio['PorcentajeComunicacion7Dias']}.\n"
                                    f"Promedio en los ultimos 30 dias: {medidor_promedio['PorcentajeComunicacion30Dias']}.\n"
                                )
                            
                            if not promedio_comunicacion.empty and comunicacion.empty:
                                medidor_promedio = promedio_comunicacion.iloc[0]
                                mensaje = (
                                    f"Hola Ingeniero {user_first_name}\n\n"
                                    f"El siguiente reporte es para el medidor: {medidor} con clave: {clave}.\n\n"
                                    f"No se obtuvo la ultima comunicacion\n\n"
                                    f"Promedio de comunicaciones:\n"
                                    f"Promedio en los ultimos 7 dias: {medidor_promedio['PorcentajeComunicacion7Dias']}.\n"
                                    f"Promedio en los ultimos 30 dias: {medidor_promedio['PorcentajeComunicacion30Dias']}.\n"
                                )
                            
                            if promedio_comunicacion.empty and not comunicacion.empty:
                                medidor_comunicacion = comunicacion.iloc[0]
                                mensaje = (
                                    f"Hola Ingeniero {user_first_name}\n\n"
                                    f"El siguiente reporte es para el medidor: {medidor} con clave: {clave}.\n\n"
                                    f"Ultima Fecha de comunicacion: {medidor_comunicacion['FECHA']}\n"
                                    f"Ultima Lectura: {medidor_comunicacion['LECTURA']}\n\n"
                                    f"Promedio de comunicaciones:\n"
                                    f"No se Obtuvo el promedio de la comunicacion"
                                )
                            if promedio_comunicacion.empty and comunicacion.empty:
                                mensaje = f"Hola ingeniero {user_first_name}, no se obtuvo la comunicacion del medidor: {medidor}"

                        else:
                            mensaje = f"No se encontro informacion del medidor: {medidor}"

                    
                    if user_marca == 'Union':
                        if clave != "EMPTY":
                            comunicacion_union = solicitud_query(F"""SELECT * FROM pnrp.airflow_union_ulti_comu WHERE CLAVE = '{clave}';""")
                            comunicacion = solicitud_query(f"""SELECT 
                                                                    (Rango7Dias.TotalDias - COALESCE(SinLectura7Dias.DiasSinLectura, 0)) / Rango7Dias.TotalDias * 100 AS PorcentajeComunicacion7Dias,
                                                                    (Rango1Mes.TotalDias - COALESCE(SinLectura1Mes.DiasSinLectura, 0)) / Rango1Mes.TotalDias * 100 AS PorcentajeComunicacion1Mes,
                                                                    (Rango3Meses.TotalDias - COALESCE(SinLectura3Meses.DiasSinLectura, 0)) / Rango3Meses.TotalDias * 100 AS PorcentajeComunicacion3Meses,
                                                                    (Rango1Ano.TotalDias - COALESCE(SinLectura1Ano.DiasSinLectura, 0)) / Rango1Ano.TotalDias * 100 AS PorcentajeComunicacion1Ano
                                                                FROM
                                                                    -- Porcentaje de comunicación últimos 7 días
                                                                    (SELECT COUNT(DISTINCT UC.FECHA) AS DiasSinLectura
                                                                    FROM pnrp.Alarmas_Union_Consumo UC
                                                                    WHERE UC.CLAVE = '{clave}'
                                                                    AND UC.NOMBRE_EVENTO = 'Día sin lectura'
                                                                    AND UC.FECHA BETWEEN DATE_SUB(CURDATE(), INTERVAL 7 DAY) AND CURDATE()) AS SinLectura7Dias
                                                                    RIGHT JOIN
                                                                    (SELECT 7 AS TotalDias) AS Rango7Dias ON 1=1
                                                                    
                                                                    -- Porcentaje de comunicación último mes
                                                                    LEFT JOIN
                                                                    (SELECT COUNT(DISTINCT UC.FECHA) AS DiasSinLectura
                                                                    FROM pnrp.Alarmas_Union_Consumo UC
                                                                    WHERE UC.CLAVE = '{clave}'
                                                                    AND UC.NOMBRE_EVENTO = 'Día sin lectura'
                                                                    AND UC.FECHA BETWEEN DATE_SUB(CURDATE(), INTERVAL 1 MONTH) AND CURDATE()) AS SinLectura1Mes
                                                                    ON 1=1
                                                                    LEFT JOIN
                                                                    (SELECT DATEDIFF(CURDATE(), DATE_SUB(CURDATE(), INTERVAL 1 MONTH)) + 1 AS TotalDias) AS Rango1Mes ON 1=1
                                                                    
                                                                    -- Porcentaje de comunicación últimos 3 meses
                                                                    LEFT JOIN
                                                                    (SELECT COUNT(DISTINCT UC.FECHA) AS DiasSinLectura
                                                                    FROM pnrp.Alarmas_Union_Consumo UC
                                                                    WHERE UC.CLAVE = '{clave}'
                                                                    AND UC.NOMBRE_EVENTO = 'Día sin lectura'
                                                                    AND UC.FECHA BETWEEN DATE_SUB(CURDATE(), INTERVAL 3 MONTH) AND CURDATE()) AS SinLectura3Meses
                                                                    ON 1=1
                                                                    LEFT JOIN
                                                                    (SELECT DATEDIFF(CURDATE(), DATE_SUB(CURDATE(), INTERVAL 3 MONTH)) + 1 AS TotalDias) AS Rango3Meses ON 1=1
                                                                    
                                                                    -- Porcentaje de comunicación último año
                                                                    LEFT JOIN
                                                                    (SELECT COUNT(DISTINCT UC.FECHA) AS DiasSinLectura
                                                                    FROM pnrp.Alarmas_Union_Consumo UC
                                                                    WHERE UC.CLAVE = '{clave}'
                                                                    AND UC.NOMBRE_EVENTO = 'Día sin lectura'
                                                                    AND UC.FECHA BETWEEN DATE_SUB(CURDATE(), INTERVAL 1 YEAR) AND CURDATE()) AS SinLectura1Ano
                                                                    ON 1=1
                                                                    LEFT JOIN
                                                                    (SELECT DATEDIFF(CURDATE(), DATE_SUB(CURDATE(), INTERVAL 1 YEAR)) + 1 AS TotalDias) AS Rango1Ano ON 1=1;
                                                                """)
                            if not comunicacion.empty and not comunicacion_union.empty:
                                row_union = comunicacion_union.iloc[0]
                                row = comunicacion.iloc[0]
                                mensaje = (f"Hola ingeniero {user_first_name}\n\n"
                                        f"El reporte de comunicación para el medidor: {medidor} es el siguiente:\n\n"
                                        f"- Ultima fecha de comunicacion: {row_union['FECHA']}\n"
                                        F"- Ultima lectura: {row_union['LECTURA']}\n"
                                        f"- Últimos 7 días: {row['PorcentajeComunicacion7Dias']:.2f}%\n"
                                        f"- Último mes: {row['PorcentajeComunicacion1Mes']:.2f}%\n"
                                        f"- Últimos 3 meses: {row['PorcentajeComunicacion3Meses']:.2f}%\n"
                                        f"- Último año: {row['PorcentajeComunicacion1Ano']:.2f}%\n\n"
                                        f"Por favor revise los porcentajes de comunicación mencionados.")
                            
                            if not comunicacion.empty and comunicacion_union.empty:
                                row = comunicacion.iloc[0]
                                mensaje = (f"Hola ingeniero {user_first_name}\n\n"
                                        f"El reporte de comunicación para el medidor: {medidor} es el siguiente:\n\n"
                                        f"Porcentaje de Comunicacion\n"
                                        f"- Últimos 7 días: {row['PorcentajeComunicacion7Dias']:.2f}%\n"
                                        f"- Último mes: {row['PorcentajeComunicacion1Mes']:.2f}%\n"
                                        f"- Últimos 3 meses: {row['PorcentajeComunicacion3Meses']:.2f}%\n"
                                        f"- Último año: {row['PorcentajeComunicacion1Ano']:.2f}%\n\n"
                                        f"Por favor revise los porcentajes de comunicación mencionados.")
                            
                            if comunicacion.empty and not comunicacion_union.empty:
                                row_union = comunicacion_union.iloc[0]
                                mensaje = (f"Hola ingeniero {user_first_name}\n\n"
                                        f"El reporte de comunicación para el medidor: {medidor} es el siguiente:\n\n"
                                        f"- Ultima fecha de comunicacion: {row_union['FECHA']}\n"
                                        f"- Ultima lectura: {row_union['LECTURA']}\n"
                                        f"- No se obtuvo el promedio de comunicacion\n"
                                        f"Por favor revise los porcentajes de comunicación mencionados.")
                            if comunicacion.empty and comunicacion_union.empty:
                                mensaje = f"Hola ingeniero {user_first_name}, no se obtuvo la comunicacion del medidor: {medidor}"

                        else:
                            mensaje = (f"No se encontró información de comunicación para el medidor {medidor} con clave {clave}.")



                if user_command == "3":

                    if user_marca == 'Hexing':
                        if clave != "EMPTY":
                                
                            alarmas_medidor = solicitud_query(f"SELECT ALARM_DESC, MAX(FECHA) AS FECHA,  count(ALARM_DESC) AS CANTIDAD  FROM pnrp.airflow_hexing_alarmas WHERE clave = '{clave}' GROUP BY ALARM_DESC ORDER BY FECHA DESC LIMIT 30;")
                            # print(alarmas_medidor)
                            if not alarmas_medidor.empty:
                                mensaje= (f"Hola ingeniero {user_first_name}\n\n"
                                    f"El siguiente reporte es para el medidor: {medidor}.\n\n"
                                    f"Alarmas del medidor:\n\n"
                                )
                                for index, row in alarmas_medidor.iterrows():
                                    mensaje += (f"- {row['ALARM_DESC']} \n(Ultima Fecha Detectada: {row['FECHA']}, Cantidad: {row['CANTIDAD']})\n\n")

                                mensaje += "\nPor favor revise las alarmas mencionadas."
                            
                            if alarmas_medidor.empty:
                                mensaje = f"Hola ingeniero {user_first_name}, No se encontraron alarmas para el medidor: {medidor}"
                        else:
                            logging.warning(f"No se encontró información para el medidor: {medidor} o clave: {clave}")
                            mensaje = f"No se encontró información de alarmas para el medidor {medidor}."

                    if user_marca == 'Union':
                        if clave != "EMPTY":
                            alarmas_medidor = solicitud_query(f"SELECT NOMBRE_EVENTO, MAX(FECHA) AS FECHA, count(NOMBRE_EVENTO) AS CANTIDAD FROM pnrp.Alarmas_Union_Consumo where clave= '{clave}' GROUP BY NOMBRE_EVENTO ORDER BY FECHA DESC LIMIT 30;")
                            if not alarmas_medidor.empty:
                                mensaje = (f"hola ingeniero {user_first_name}\n\n"
                                    f"El siguiente reporte es para el medidor: {medidor}\n\n"
                                    f"Alarmas del medidor:\n\n"
                                )
                                for index, row in alarmas_medidor.iterrows():
                                    mensaje += (f"- {row['NOMBRE_EVENTO']} \n(Ultima Fecha Detectada: {row['FECHA']}, Cantidad: {row['CANTIDAD']})\n\n")
                                
                                mensaje += "\nPor favor revise las alarmas mencionadas."
                            if alarmas_medidor.empty:
                                mensaje = f"Hola ingeniero {user_first_name}, No se encontraron alarmas para el medidor: {medidor}"
                    else: 
                        logging.warning(f"No se encontro una informacion para el medidor: {medidor} o clave: {clave}")
                        mensjae = f"No se encontro informacion de la alarmas para el medidor {medidor}."
                        
                if user_command == "4":
                    if user_marca == "Hexing":
                        if clave != "EMPTY":
                            # Consultar las órdenes para el medidor específico
                            ordenes = solicitud_query(f"SELECT * FROM pnrp.airflow_hexing_os WHERE clave = '{clave}' ORDER BY FECHA_EJECUCION DESC;")
                            
                            # Verificar si el DataFrame no está vacío
                            if not ordenes.empty:
                                # Crear el mensaje concatenando la información de cada orden
                                mensaje = (f"Hola ingeniero {user_first_name},\n\n"
                                        f"El siguiente reporte es para el medidor: {medidor}.\n\n"
                                        f"Alarmas del medidor:\n\n" + 
                                        "\n\n".join(
                                            f"Número de OS: {orden['OS']}\n"
                                            f"Estado de la OS: {orden['ESTADO']}\n"
                                            f"Tipo de Gestion: {orden['DESCRIPCION_OS']}"
                                            f"Categoría de la anomalía: {orden['CATEGORIA']}\n"
                                            f"Descripción de OS: {orden['DESCRIPCION']}\n"
                                            f"Fecha Generada: {orden['FECHA_GENERADA']}\n"
                                            f"Fecha de Ejecución: {orden['FECHA_EJECUCION']}\n"
                                            for _, orden in ordenes.iterrows()
                                        ))
                            if ordenes.empty:
                                mensaje = f"Hola ingeniero {user_first_name}, no se encontraron Ordenes de Servicio para el medidor: {medidor} con clave: {clave}"
                        else:
                            logging.warning(f"No se encontró información para el medidor: {medidor} o clave: {clave}")
                            mensaje = f"No se encontró ordenes de servicio para el medidor {medidor} con clave {clave}."

                    if user_marca == "Union":
                        if clave != "EMPTY":
                            # Consultar las órdenes para el medidor específico
                            ordenes = solicitud_query(f"SELECT * FROM pnrp.airflow_union_os WHERE clave = '{clave}' ORDER BY FECHA_EJECUCION DESC;")
                            
                            # Verificar si el DataFrame no está vacío
                            if not ordenes.empty:
                                # Crear el mensaje concatenando la información de cada orden
                                mensaje = (f"Hola ingeniero {user_first_name},\n\n"
                                        f"El siguiente reporte es para el medidor: {medidor}.\n\n"
                                        f"Alarmas del medidor:\n\n" + 
                                        "\n\n".join(
                                            f"Número de OS: {orden['OS']}\n"
                                            f"Estado de la OS: {orden['ESTADO']}\n"
                                            f"Categoría de la anomalía: {orden['CATEGORIA']}\n"
                                            f"Descripción de OS: {orden['DESCRIPCION']}\n"
                                            f"Fecha Generada: {orden['FECHA_GENERADA']}\n"
                                            f"Fecha de Ejecución: {orden['FECHA_EJECUCION']}\n"
                                            for _, orden in ordenes.iterrows()
                                        ))

                            if ordenes.empty:
                                mensaje = f"Hola ingeniero {user_first_name}, no se encontraron Ordenes de Servicio para el medidor: {medidor} con clave: {clave}"
                        else:
                            logging.warning(f"No se encontró información para el medidor: {medidor} o clave: {clave}")
                            mensaje = f"No se encontró ordenes de servicio para el medidor {medidor} con clave {clave}."
                
                if user_command == '5':
                    comentario_telegestion = solicitud_query(f"SELECT * FROM bitacora_ac where clave = '{clave}' and ESTADO <> 'ANULADO' and REQUIERE_OS = TRUE order by fecha_asignacion desc;")
                    if not comentario_telegestion.empty:
                        mensaje = (f"Hola ingeniero {user_first_name}, \n\n"
                                   f"el siguiente reporte es para el medidor: {medidor}\n\n"
                                   f"El departamento de telegestion ha hecho una o mas revisiones al medidor.\n"
                                   "\n\n".join(
                                        f"Fecha de analisis: {comentario['FECHA_ANALISIS']}\n"
                                        f"Alarma encontrada: {comentario['ALARMA']}\n"
                                        f"Fecha de la alarma encontrada: {comentario['FECHA_ALARMA']}\n"
                                        f"Comentario del analista: {comentario['COMENTARIO_ANALISTA']}\n"
                                        f"Criticidad de la alarma: {comentario['CRITICIDAD_ALARMA']}\n"
                                        f"Estado de la revision: {comentario['ESTADO']}\n"
                                        for _, comentario in comentario_telegestion.iterrows() 
                                   ) 
                        )
                    if comentario_telegestion.empty:
                        mensaje = f"Hola ingeniero {user_first_name}, no se ha realizado analisis para el medidor: {medidor} con clave: {clave}"

                if mensaje:
                    try:
                        await context.bot.send_message(chat_id=user_id, text=mensaje)
                        logging.info(f"Mensaje enviado a ID_TG: {user_id}")
                        
                        with engine.begin() as conn:
                            print("actualizando campo enviado")
                            query_update_enviado = text("UPDATE bot_solicitudes_me SET ENVIADO='1' WHERE id = :id")
                            conn.execute(query_update_enviado, {'id': solicitud_id})

                    except Exception as e:
                        logging.error(f"Error al enviar mensaje a ID_TG: {user_id}: {e}")

    except Exception as e:
        logging.error(f"Error en procesamiento de solicitudes: {e}")
    

# Función para manejar mensajes que no son comandos
async def manejar_mensaje(update: Update, context: CallbackContext):
    """
    ## Funcion manejar mensajes:
    Maneja los mensajes que no son comandos. Verifica si el usuario está registrado y
    redirige al menú de opciones si es así, o responde solicitando que se registre
    si no lo está.

    Args:
        update (Update): Contiene la información sobre el mensaje recibido.
        context (CallbackContext): Proporciona información adicional sobre el contexto
                                    del comando o mensaje.

    Returns:
        None
    """
    user_id = update.message.from_user.id
    usuarios = solicitud_query("SELECT * FROM bot_usuarios_autorizados_me;")
    iD_TELEGRAM = usuarios['ID_TELEGRAM'].tolist()

    if int(user_id) in iD_TELEGRAM:
        # Usuario registrado, iniciar flujo de selección de opciones
        return await iniciar_menu(update, context)
    else:
        # Usuario no registrado, ignorar el mensaje o enviar una respuesta
        await update.message.reply_text('Por favor, usa el comando /start para registrarte.')


# Función para manejar comandos desconocidos
async def cancel(update: Update, context: CallbackContext):
    """
    ## Funcion cancelar:
    Maneja el comando de cancelación. Informa al usuario que la operación ha sido
    cancelada y termina la conversación.

    Args:
        update (Update): Contiene la información sobre el mensaje recibido.
        context (CallbackContext): Proporciona información adicional sobre el contexto
                                    del comando o mensaje.

    Returns:
        ConversationHandler.END: Termina la conversación.
    """
    await update.message.reply_text('Cancelado.')
    return ConversationHandler.END

# Función para manejar errores
async def error(update: Update, context: CallbackContext):
    """
    ## Funcion error:
    Maneja errores que ocurren durante la ejecución del bot. Registra el error en
    los logs para su posterior revisión.

    Args:
        update (Update): Contiene la información sobre el mensaje que causó el error.
        context (CallbackContext): Proporciona información adicional sobre el contexto
                                    del error.

    Returns:
        None
    """
    logging.error(f"Actualización {update} causó el error {context.error}")

def main():
    """
    ## Funcion main:
    Función principal para iniciar el bot. Configura los manejadores de comandos y
    mensajes, gestiona el flujo de conversación y establece un JobQueue para tareas
    programadas.

    Returns:
        None
    """
    # Tu token de bot aquí
    application = Application.builder().token(os.getenv('YOUR_TOKEN')).build()
    
    
    # Primer ConversationHandler para el registro
    registro_handler = ConversationHandler(
        entry_points=[CommandHandler('start', start)],
        states={
            REGISTRO: [MessageHandler(filters.TEXT & ~filters.COMMAND, registro)],
        },
        fallbacks=[],
    )

    # Segundo ConversationHandler para manejar el menú y otras opciones
    menu_handler = ConversationHandler(
        entry_points=[CommandHandler('menu', iniciar_menu)],
        states={
            SELECCIONAR_OPCION: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, seleccionar_opcion),
                CommandHandler('menu', manejar_mensaje)],
            SELECCIONAR_MARCA: [MessageHandler(filters.TEXT & ~filters.COMMAND, seleccionar_marca),
                CommandHandler('menu', manejar_mensaje)],
            INGRESAR_MEDIDOR: [MessageHandler(filters.TEXT & ~filters.COMMAND, ingresar_medidor),
                CommandHandler('menu', manejar_mensaje)],
        },
        fallbacks=[CommandHandler('menu', manejar_mensaje)],
    )

    # Tercer ConversationHandler para la planificacion
    planificacion_handler = ConversationHandler(
        entry_points=[CommandHandler('planificacion', verificar_rol)],
        states={
            PLANIFICACION: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, planificacion),
                MessageHandler(filters.Document.FileExtension("xls") | filters.Document.FileExtension("xlsx"), planificacion)
            ],
        },
        fallbacks=[],
    )



    # Añadir los manejadores al bot
    application.add_handler(planificacion_handler)
    application.add_handler(registro_handler)
    application.add_handler(menu_handler)
    # Manejador de errores
    application.add_error_handler(error)


    # Asegúrate de que la job_queue esté disponible
    if application.job_queue is None:
        logging.error("JobQueue no está disponible. Asegúrate de que python-telegram-bot esté instalado con el extra `job-queue`.")
        return

    # Configuración del JobQueue
    job_queue = application.job_queue
    job_queue.run_repeating(procesar_solicitudes, interval=10, first=0)


    """# Ejecutar el bot en un hilo separado para no bloquear el hilo principal
    hilo_asincrono = threading.Thread(target=iniciar_proceso_asincrono, args=(application,))
    hilo_asincrono.start()"""



    application.run_polling()


if __name__ == '__main__':
    main()
